"""
Main entry point for the durable campaign dispatcher.
Usage:
    python -m solution [--fixtures ...] [--provider-url ...] [--callback-url ...] [--port 8200]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from datetime import datetime

import httpx
import uvicorn

from .clock import SimClock
from .config import Config
from .db import Database
from .dispatcher import Dispatcher
from .receiver import create_receiver_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("dispatcher.main")

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_FIXTURES = os.path.join(HERE, "..", "assignment-b-workflow", "fixtures", "campaigns.json")


async def wait_for_provider_clock(provider_url: str, max_retries: int = 15) -> dict:
    """
    Waits for the mock telephony provider to become available and retrieves clock config.
    """
    url = f"{provider_url.rstrip('/')}/v1/clock"
    async with httpx.AsyncClient(timeout=5.0) as client:
        for attempt in range(max_retries):
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    return resp.json()
            except Exception:
                pass
            logger.info("Waiting for mock telephony provider at %s...", url)
            await asyncio.sleep(1.0)
    raise RuntimeError(f"Could not connect to provider at {url} after {max_retries} attempts.")


async def verify_against_provider(provider_url: str, output_csv_path: str):
    """
    Compares the generated final_state.csv with the provider's /admin/leads and /admin/report.
    """
    logger.info("Verifying final state against mock telephony provider...")
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            report_resp = await client.get(f"{provider_url.rstrip('/')}/admin/report")
            if report_resp.status_code == 200:
                report = report_resp.json()
                logger.info(
                    "Provider Report: Total Calls=%d, Active=%d, Violations=%d, Concurrency Seen=%s",
                    report.get("calls_total", 0),
                    report.get("calls_active", 0),
                    report.get("violation_count", 0),
                    report.get("max_concurrency_seen", {}),
                )
                if report.get("violation_count", 0) > 0:
                    logger.error("VIOLATIONS DETECTED: %s", report.get("violations_by_type"))
                    for v in report.get("violations", []):
                        logger.error("  Violation: %s - %s", v["type"], v["detail"])
                else:
                    logger.info("SUCCESS: 0 violations reported by provider!")

            leads_resp = await client.get(f"{provider_url.rstrip('/')}/admin/leads")
            if leads_resp.status_code == 200:
                expected_leads = leads_resp.json()
                # Read final_state.csv
                import csv
                actual_leads = {}
                with open(output_csv_path, "r", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        actual_leads[(row["campaign_id"], row["lead_id"])] = row

                mismatches = 0
                for exp in expected_leads:
                    key = (exp["campaign_id"], exp["lead_id"])
                    act = actual_leads.get(key)
                    if not act:
                        logger.error("Missing lead in final_state.csv: %s", key)
                        mismatches += 1
                        continue

                    if act["final_state"] != exp["expected_state"]:
                        logger.error(
                            "Mismatch for %s: expected state '%s', got '%s'",
                            key,
                            exp["expected_state"],
                            act["final_state"],
                        )
                        mismatches += 1

                if mismatches == 0:
                    logger.info("SUCCESS: All %d leads match provider expectations perfectly!", len(expected_leads))
                else:
                    logger.error("FAILURE: %d mismatches found against provider expectations.", mismatches)

        except Exception as e:
            logger.warning("Could not fetch verification report from provider: %s", e)


async def async_main():
    parser = argparse.ArgumentParser(description="Agentic Universe - Durable Campaign Dispatcher")
    parser.add_argument("--fixtures", default=DEFAULT_FIXTURES, help="Path to campaigns.json")
    parser.add_argument("--provider-url", default="http://127.0.0.1:8099", help="Mock telephony provider URL")
    parser.add_argument("--callback-url", default="http://127.0.0.1:8200/webhooks/telephony", help="Webhook callback URL")
    parser.add_argument("--host", default="127.0.0.1", help="Receiver bind host")
    parser.add_argument("--port", type=int, default=8200, help="Receiver bind port")
    parser.add_argument("--db-path", default="dispatcher.db", help="SQLite database path")
    parser.add_argument("--output", default="final_state.csv", help="Output path for final_state.csv")
    parser.add_argument("--reset", action="store_true", help="Force database reset from fixtures")
    args = parser.parse_args()

    fixtures_path = os.path.abspath(args.fixtures)
    if not os.path.exists(fixtures_path):
        # Fallback to local search
        alt_path = os.path.join(os.getcwd(), "assignment-b-workflow", "fixtures", "campaigns.json")
        if os.path.exists(alt_path):
            fixtures_path = alt_path

    cfg = Config.load(fixtures_path)
    fixtures_dir = os.path.dirname(fixtures_path)

    # 1. Connect to provider clock
    clock_info = await wait_for_provider_clock(args.provider_url)
    provider_real_epoch = float(clock_info["real_epoch"])
    provider_speed = float(clock_info["speed"])
    sim_epoch = datetime.fromisoformat(clock_info["sim_epoch"])

    clock = SimClock(sim_epoch=sim_epoch, real_epoch=provider_real_epoch, speed=provider_speed)
    logger.info("Provider clock synchronized: speed=%.1fx, sim_now=%s", provider_speed, clock.now().isoformat())

    # 2. Database initialization / crash recovery check
    db = Database(args.db_path)
    db.init_schema()

    db_real_epoch_str = db.get_metadata("mock_real_epoch")
    should_reset = args.reset or (db_real_epoch_str is None)

    # If provider was restarted (epoch changed), reset DB cleanly
    if db_real_epoch_str is not None:
        try:
            db_real_epoch = float(db_real_epoch_str)
            if abs(db_real_epoch - provider_real_epoch) > 1.0:
                logger.info("Detected new mock telephony run (epoch changed). Resetting database.")
                should_reset = True
        except ValueError:
            should_reset = True

    if should_reset or not db.is_initialized():
        logger.info("Initializing fresh database from fixtures...")
        db.reset_and_populate(cfg, provider_real_epoch, base_dir=fixtures_dir)
    else:
        logger.info("Resuming from existing database (crash recovery mode).")

    # 3. Setup webhook receiver app and dispatcher
    wake_event = asyncio.Event()
    app = create_receiver_app(cfg, db, wake_event)

    dispatcher = Dispatcher(
        cfg=cfg,
        db=db,
        clock=clock,
        provider_url=args.provider_url,
        callback_url=args.callback_url,
        wake_event=wake_event,
        output_csv_path=args.output,
    )

    # 4. Start Uvicorn receiver server
    uvicorn_config = uvicorn.Config(
        app,
        host=args.host,
        port=args.port,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(uvicorn_config)

    server_task = asyncio.create_task(server.serve())
    dispatcher_task = asyncio.create_task(dispatcher.dispatch_loop())

    try:
        # Run until dispatcher finishes
        await dispatcher_task
        # Verify results
        await verify_against_provider(args.provider_url, args.output)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error("Error in dispatcher: %s", e, exc_info=True)
    finally:
        server.should_exit = True
        await server_task
        await dispatcher.close()


def main():
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")


if __name__ == "__main__":
    main()

