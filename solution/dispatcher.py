"""
Core workflow dispatcher engine.
Manages concurrency, dial workers, watchdog reconciliation, and failure recovery.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

import httpx

from .clock import IST, SimClock
from .config import Config
from .db import Database
from .scheduler import Scheduler

logger = logging.getLogger("dispatcher.core")


class Dispatcher:
    def __init__(
        self,
        cfg: Config,
        db: Database,
        clock: SimClock,
        provider_url: str,
        callback_url: str,
        wake_event: asyncio.Event,
        output_csv_path: str = "final_state.csv",
    ):
        self.cfg = cfg
        self.db = db
        self.clock = clock
        self.provider_url = provider_url.rstrip("/")
        self.callback_url = callback_url
        self.wake_event = wake_event
        self.output_csv_path = output_csv_path
        self.scheduler = Scheduler(cfg, db, clock)
        self._running = False
        self._client: Optional[httpx.AsyncClient] = None
        self._active_tasks: set[asyncio.Task] = set()

    async def get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=10.0)
        return self._client

    async def close(self):
        self._running = False
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def startup_recovery(self):
        """
        Runs immediately on startup: reconciles any dialing or active calls from a previous crash.
        """
        logger.info("Starting crash recovery check for in-flight calls...")
        in_flight = self.db.get_in_flight_calls()
        if not in_flight:
            logger.info("No in-flight calls to recover.")
            return

        logger.info("Found %d in-flight call(s) to reconcile.", len(in_flight))
        client = await self.get_client()

        for call in in_flight:
            req_id = call["request_id"]
            state = call["state"]
            sid = call["call_sid"]

            try:
                if state == "dialing":
                    resp = await client.get(f"{self.provider_url}/v1/calls", params={"request_id": req_id})
                    if resp.status_code == 200:
                        data = resp.json()
                        call_sid = data.get("call_sid")
                        self.db.mark_call_accepted(req_id, call_sid)
                        if data.get("state") == "ended" and "result" in data:
                            self.db.process_call_result(data["result"], self.cfg.retry_policy)
                        logger.info("Recovered dialing call req=%s -> accepted sid=%s", req_id, call_sid)
                    elif resp.status_code == 404:
                        self.db.mark_call_failed_to_place(req_id)
                        logger.info("Recovered dialing call req=%s -> not placed (reverted)", req_id)
                elif state == "active":
                    lookup_url = f"{self.provider_url}/v1/calls/{sid}" if sid else f"{self.provider_url}/v1/calls?request_id={req_id}"
                    resp = await client.get(lookup_url)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data.get("state") == "ended" and "result" in data:
                            self.db.process_call_result(data["result"], self.cfg.retry_policy)
                            logger.info("Recovered active call sid=%s -> ended", sid)
                        else:
                            logger.info("Call sid=%s is still active in provider.", sid)
                    elif resp.status_code == 404:
                        logger.warning("Active call sid=%s not found on provider!", sid)
            except Exception as e:
                logger.error("Error during recovery of call req=%s: %s", req_id, e)

        logger.info("Crash recovery completed.")

    async def reconcile_unknown_call(self, request_id: str):
        """
        Handles 504 / timeout on POST /v1/calls: determines whether the call was actually placed.
        """
        client = await self.get_client()
        for attempt in range(5):
            try:
                resp = await client.get(f"{self.provider_url}/v1/calls", params={"request_id": request_id})
                if resp.status_code == 200:
                    data = resp.json()
                    call_sid = data.get("call_sid")
                    self.db.mark_call_accepted(request_id, call_sid)
                    if data.get("state") == "ended" and "result" in data:
                        self.db.process_call_result(data["result"], self.cfg.retry_policy)
                    logger.info("504 resolution: call %s WAS placed with sid %s", request_id, call_sid)
                    return
                elif resp.status_code == 404:
                    self.db.mark_call_failed_to_place(request_id)
                    logger.info("504 resolution: call %s was NOT placed", request_id)
                    return
            except Exception as e:
                logger.warning("Error querying request_id=%s attempt %d: %s", request_id, attempt, e)

            await asyncio.sleep(0.3 * (attempt + 1))

        # If still unresolved, leave in dialing state; watchdog will reconcile
        logger.warning("Could not immediately resolve 504 for %s, watchdog will take over", request_id)

    async def dial_lead(self, cid: str, lid: str, phone: str, tenant_id: str, attempt_no: int):
        """
        Attempts to claim a lead and dispatch the call through the provider.
        """
        req_id = f"req_{cid}_{lid}_{attempt_no}_{uuid.uuid4().hex[:8]}"
        now_iso = self.clock.now().isoformat()
        tenant_cfg = self.cfg.tenants[tenant_id]
        camp_cfg = self.cfg.campaigns[cid]

        claimed = self.db.claim_lead_for_dialing(
            campaign_id=cid,
            lead_id=lid,
            tenant_id=tenant_id,
            phone=phone,
            attempt_no=attempt_no,
            sim_now_iso=now_iso,
            request_id=req_id,
            max_tenant_concurrency=tenant_cfg.max_concurrent_calls,
            max_attempts=camp_cfg.max_attempts,
        )

        if not claimed:
            return

        client = await self.get_client()
        payload = {
            "request_id": req_id,
            "campaign_id": cid,
            "lead_id": lid,
            "phone": phone,
            "callback_url": self.callback_url,
        }

        try:
            resp = await client.post(f"{self.provider_url}/v1/calls", json=payload)
            if resp.status_code in (201, 200):
                data = resp.json()
                call_sid = data.get("call_sid")
                self.db.mark_call_accepted(req_id, call_sid)
                logger.info("Call accepted: req=%s sid=%s lead=%s/%s (attempt %d)", req_id, call_sid, cid, lid, attempt_no)
            elif resp.status_code == 503:
                # Transient 503: call not placed
                self.db.mark_call_failed_to_place(req_id)
                logger.info("Provider 503 on req=%s lead=%s/%s; will retry later", req_id, cid, lid)
            elif resp.status_code == 504:
                # Ambiguous 504: call may or may not be placed
                logger.warning("Provider 504 on req=%s lead=%s/%s; reconciling...", req_id, cid, lid)
                await self.reconcile_unknown_call(req_id)
            else:
                # 4xx or unexpected error (e.g. violation)
                logger.error("Provider returned %d on req=%s: %s", resp.status_code, req_id, resp.text)
                self.db.mark_call_failed_to_place(req_id)
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            logger.warning("Network timeout/error dispatching req=%s: %s; reconciling...", req_id, exc)
            await self.reconcile_unknown_call(req_id)
        except Exception as exc:
            logger.error("Unexpected error dispatching req=%s: %s", req_id, exc)
            await self.reconcile_unknown_call(req_id)
        finally:
            self.wake_event.set()

    async def watchdog_task(self):
        """
        Periodically reconciles active and dialing calls to recover from lost webhooks.
        """
        while self._running:
            try:
                await asyncio.sleep(0.8)
                in_flight = self.db.get_in_flight_calls()
                if not in_flight:
                    continue

                client = await self.get_client()
                import time
                now_real = time.time()

                for call in in_flight:
                    req_id = call["request_id"]
                    state = call["state"]
                    sid = call["call_sid"]
                    created_at_real = call["created_at_real"]

                    if state == "active" and sid:
                        try:
                            resp = await client.get(f"{self.provider_url}/v1/calls/{sid}")
                            if resp.status_code == 200:
                                data = resp.json()
                                if data.get("state") == "ended" and "result" in data:
                                    logger.info("Watchdog detected ended call sid=%s (webhook lost or slow)", sid)
                                    self.db.process_call_result(data["result"], self.cfg.retry_policy)
                                    self.wake_event.set()
                        except Exception as e:
                            logger.debug("Watchdog error checking sid=%s: %s", sid, e)

                    elif state == "dialing" and (now_real - created_at_real > 3.0):
                        try:
                            resp = await client.get(f"{self.provider_url}/v1/calls", params={"request_id": req_id})
                            if resp.status_code == 200:
                                data = resp.json()
                                self.db.mark_call_accepted(req_id, data.get("call_sid"))
                                if data.get("state") == "ended" and "result" in data:
                                    self.db.process_call_result(data["result"], self.cfg.retry_policy)
                                self.wake_event.set()
                            elif resp.status_code == 404:
                                self.db.mark_call_failed_to_place(req_id)
                                self.wake_event.set()
                        except Exception as e:
                            logger.debug("Watchdog error checking req=%s: %s", req_id, e)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Unexpected error in watchdog: %s", e)

    async def dispatch_loop(self):
        """
        Main dispatch loop. Scans open campaigns, checks concurrency, and launches dials.
        """
        logger.info("Starting main dispatch loop...")
        self._running = True

        # Run startup crash recovery
        await self.startup_recovery()

        # Start background watchdog
        watchdog = asyncio.create_task(self.watchdog_task())

        try:
            while self._running:
                # Check if all leads reached terminal / final state
                if self.db.is_finished():
                    logger.info("All leads have reached a final state!")
                    break

                now = self.clock.now()
                now_iso = now.isoformat()
                open_cids = self.scheduler.get_open_campaign_ids(now)

                dispatched_any = False

                if open_cids:
                    # Iterate through tenants
                    for tenant_id, tenant_cfg in self.cfg.tenants.items():
                        tenant_open_cids = [
                            cid for cid in open_cids
                            if self.cfg.campaigns[cid].tenant_id == tenant_id
                        ]
                        if not tenant_open_cids:
                            continue

                        active_calls = self.db.get_active_tenant_count(tenant_id)
                        available_slots = tenant_cfg.max_concurrent_calls - active_calls

                        if available_slots <= 0:
                            continue

                        eligible_leads = self.db.get_eligible_leads_for_tenant(
                            tenant_id=tenant_id,
                            sim_now_iso=now_iso,
                            open_campaign_ids=tenant_open_cids,
                            limit=available_slots,
                        )

                        for lead in eligible_leads:
                            dispatched_any = True
                            task = asyncio.create_task(
                                self.dial_lead(
                                    cid=lead["campaign_id"],
                                    lid=lead["lead_id"],
                                    phone=lead["phone"],
                                    tenant_id=tenant_id,
                                    attempt_no=lead["attempts"] + 1,
                                )
                            )
                            self._active_tasks.add(task)
                            task.add_done_callback(self._active_tasks.discard)

                # Clean up finished tasks
                self._active_tasks = {t for t in self._active_tasks if not t.done()}

                # Determine sleep duration
                wait_seconds = 0.5  # default sleep

                # If windows closed or waiting on callbacks/retries, calculate sleep time
                if not dispatched_any:
                    # 1. Earliest next eligible lead among open campaigns
                    earliest_eligible_str = self.scheduler.get_earliest_next_eligible(open_cids)
                    if earliest_eligible_str:
                        earliest_dt = datetime.fromisoformat(earliest_eligible_str)
                        wait_seconds = min(wait_seconds, self.clock.real_seconds_until(earliest_dt))

                    # 2. Earliest window opening if all open campaigns have no pending leads
                    if not open_cids:
                        nxt_open = self.scheduler.get_next_campaign_open_time(now)
                        if nxt_open:
                            real_wait = self.clock.real_seconds_until(nxt_open)
                            wait_seconds = max(0.1, min(real_wait, 1.0))

                # Cap wait_seconds between 0.05 and 1.0 to ensure responsiveness
                wait_seconds = max(0.05, min(wait_seconds, 1.0))

                # Wait on wake event or timeout
                try:
                    await asyncio.wait_for(self.wake_event.wait(), timeout=wait_seconds)
                except asyncio.TimeoutError:
                    pass
                finally:
                    self.wake_event.clear()

            # Wait for any in-flight dial tasks to complete
            if self._active_tasks:
                await asyncio.gather(*self._active_tasks, return_exceptions=True)

            # Export final_state.csv
            self.db.export_final_state_csv(self.output_csv_path)
            stats = self.db.get_summary_stats()
            logger.info("Dispatch finished. Final state summary: %s", stats)

        finally:
            self._running = False
            watchdog.cancel()
            try:
                await watchdog
            except asyncio.CancelledError:
                pass
            await self.close()

