"""
SQLite database engine for state persistence, ACID state transitions, and crash recovery.
"""
from __future__ import annotations

import csv
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .config import Config, IST, TERMINAL_DISPOSITIONS

logger = logging.getLogger("dispatcher.db")

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 10000;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id TEXT PRIMARY KEY,
    max_concurrent_calls INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    window_days TEXT NOT NULL,
    max_attempts INTEGER NOT NULL,
    FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
);

CREATE TABLE IF NOT EXISTS global_dnd (
    phone TEXT PRIMARY KEY,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS leads (
    campaign_id TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    phone TEXT NOT NULL,
    customer_name TEXT,
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 3,
    next_eligible_at TEXT NOT NULL,
    current_request_id TEXT,
    current_call_sid TEXT,
    last_status TEXT,
    last_disposition TEXT,
    last_call_ended_at TEXT,
    callback_at TEXT,
    PRIMARY KEY (campaign_id, lead_id),
    FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id)
);

CREATE INDEX IF NOT EXISTS idx_leads_schedule ON leads (campaign_id, state, priority, next_eligible_at);
CREATE INDEX IF NOT EXISTS idx_leads_phone ON leads (phone);

CREATE TABLE IF NOT EXISTS calls (
    request_id TEXT PRIMARY KEY,
    call_sid TEXT UNIQUE,
    campaign_id TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    phone TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    state TEXT NOT NULL,
    started_at TEXT,
    ended_at TEXT,
    status TEXT,
    disposition TEXT,
    callback_at TEXT,
    duration_sec INTEGER DEFAULT 0,
    created_at_sim TEXT NOT NULL,
    created_at_real REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_calls_state ON calls (state);
CREATE INDEX IF NOT EXISTS idx_calls_tenant_state ON calls (tenant_id, state);
CREATE INDEX IF NOT EXISTS idx_calls_phone_state ON calls (phone, state);
CREATE INDEX IF NOT EXISTS idx_calls_lead ON calls (campaign_id, lead_id);

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    delivery_id TEXT PRIMARY KEY,
    call_sid TEXT,
    request_id TEXT,
    received_at REAL NOT NULL
);
"""


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def get_connection(self) -> sqlite3.Connection:
        if self._conn is None:
            conn = sqlite3.connect(
                self.db_path,
                timeout=10.0,
                check_same_thread=False,
                isolation_level=None,  # autocommit mode; we manage transactions explicitly
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("PRAGMA busy_timeout = 10000;")
            conn.execute("PRAGMA foreign_keys = ON;")
            self._conn = conn
        return self._conn

    @contextmanager
    def transaction(self):
        """
        Executes a BEGIN IMMEDIATE transaction ensuring single-writer serialization.
        """
        conn = self.get_connection()
        conn.execute("BEGIN IMMEDIATE;")
        try:
            yield conn
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            raise

    def init_schema(self):
        conn = self.get_connection()
        conn.executescript(SCHEMA_SQL)

    def get_metadata(self, key: str) -> Optional[str]:
        conn = self.get_connection()
        cur = conn.execute("SELECT value FROM metadata WHERE key = ?", (key,))
        row = cur.fetchone()
        return row["value"] if row else None

    def set_metadata(self, key: str, value: str):
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO metadata (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def is_initialized(self) -> bool:
        conn = self.get_connection()
        cur = conn.execute("SELECT count(*) as cnt FROM leads")
        return cur.fetchone()["cnt"] > 0

    def reset_and_populate(self, cfg: Config, mock_real_epoch: float, base_dir: str):
        """
        Clears and initializes the database from config and fixtures.
        """
        self.init_schema()
        with self.transaction() as conn:
            conn.execute("DELETE FROM webhook_deliveries;")
            conn.execute("DELETE FROM calls;")
            conn.execute("DELETE FROM leads;")
            conn.execute("DELETE FROM global_dnd;")
            conn.execute("DELETE FROM campaigns;")
            conn.execute("DELETE FROM tenants;")
            conn.execute("DELETE FROM metadata;")

            conn.execute(
                "INSERT INTO metadata (key, value) VALUES ('mock_real_epoch', ?)",
                (str(mock_real_epoch),),
            )

            # Insert tenants
            for t in cfg.tenants.values():
                conn.execute(
                    "INSERT INTO tenants (tenant_id, max_concurrent_calls) VALUES (?, ?)",
                    (t.tenant_id, t.max_concurrent_calls),
                )

            # Insert campaigns
            for c in cfg.campaigns.values():
                conn.execute(
                    """
                    INSERT INTO campaigns (campaign_id, tenant_id, window_start, window_end, window_days, max_attempts)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        c.campaign_id,
                        c.tenant_id,
                        c.window.start,
                        c.window.end,
                        ",".join(c.window.days or []),
                        c.max_attempts,
                    ),
                )

                # Load leads
                leads_file = os.path.join(base_dir, c.leads_file)
                if not os.path.exists(leads_file):
                    # Try fixtures subdir
                    leads_file = os.path.join(base_dir, "fixtures", c.leads_file)

                with open(leads_file, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        conn.execute(
                            """
                            INSERT INTO leads (
                                campaign_id, lead_id, phone, customer_name,
                                state, attempts, priority, next_eligible_at
                            ) VALUES (?, ?, ?, ?, 'pending', 0, 3, ?)
                            """,
                            (
                                c.campaign_id,
                                row["lead_id"],
                                row["phone"],
                                row.get("customer_name", ""),
                                cfg.sim_start.isoformat(),
                            ),
                        )
        logger.info("Database initialized successfully with %d campaigns.", len(cfg.campaigns))

    def get_active_tenant_count(self, tenant_id: str) -> int:
        conn = self.get_connection()
        cur = conn.execute(
            "SELECT count(*) as cnt FROM calls WHERE tenant_id = ? AND state IN ('dialing', 'active')",
            (tenant_id,),
        )
        return cur.fetchone()["cnt"]

    def get_eligible_leads_for_tenant(
        self,
        tenant_id: str,
        sim_now_iso: str,
        open_campaign_ids: list[str],
        limit: int,
    ) -> list[sqlite3.Row]:
        """
        Finds highest priority eligible leads for a tenant across open campaigns.
        Ordering: priority ASC (1=callback, 2=retry, 3=fresh), next_eligible_at ASC, lead_id ASC.
        """
        if limit <= 0 or not open_campaign_ids:
            return []

        conn = self.get_connection()
        placeholders = ",".join("?" for _ in open_campaign_ids)
        query = f"""
            SELECT l.campaign_id, l.lead_id, l.phone, l.attempts, l.priority, l.next_eligible_at, c.max_attempts
            FROM leads l
            JOIN campaigns c ON l.campaign_id = c.campaign_id
            WHERE c.tenant_id = ?
              AND l.campaign_id IN ({placeholders})
              AND l.state = 'pending'
              AND l.attempts < c.max_attempts
              AND l.next_eligible_at <= ?
              AND l.phone NOT IN (SELECT phone FROM global_dnd)
              AND l.phone NOT IN (SELECT phone FROM calls WHERE state IN ('dialing', 'active'))
            ORDER BY l.priority ASC, l.next_eligible_at ASC, l.lead_id ASC
            LIMIT ?
        """
        params = [tenant_id] + open_campaign_ids + [sim_now_iso, limit]
        cur = conn.execute(query, params)
        return cur.fetchall()

    def claim_lead_for_dialing(
        self,
        campaign_id: str,
        lead_id: str,
        tenant_id: str,
        phone: str,
        attempt_no: int,
        sim_now_iso: str,
        request_id: str,
        max_tenant_concurrency: int,
        max_attempts: int,
    ) -> bool:
        """
        Atomically checks limits and transitions a lead to 'dialing', inserting a call record.
        Returns True if claim succeeded, False otherwise.
        """
        with self.transaction() as conn:
            # 1. Check tenant concurrency
            cur = conn.execute(
                "SELECT count(*) as cnt FROM calls WHERE tenant_id = ? AND state IN ('dialing', 'active')",
                (tenant_id,),
            )
            if cur.fetchone()["cnt"] >= max_tenant_concurrency:
                return False

            # 2. Check phone not in active calls
            cur = conn.execute(
                "SELECT count(*) as cnt FROM calls WHERE phone = ? AND state IN ('dialing', 'active')",
                (phone,),
            )
            if cur.fetchone()["cnt"] > 0:
                return False

            # 3. Check phone not in global DND
            cur = conn.execute("SELECT 1 FROM global_dnd WHERE phone = ?", (phone,))
            if cur.fetchone() is not None:
                conn.execute(
                    "UPDATE leads SET state = 'dnd_blocked' WHERE campaign_id = ? AND lead_id = ?",
                    (campaign_id, lead_id),
                )
                return False

            # 4. Check lead is still pending and has attempts < max_attempts
            cur = conn.execute(
                "SELECT state, attempts FROM leads WHERE campaign_id = ? AND lead_id = ?",
                (campaign_id, lead_id),
            )
            lead = cur.fetchone()
            if not lead or lead["state"] != "pending" or lead["attempts"] >= max_attempts:
                return False

            # Claim atomically
            import time
            conn.execute(
                """
                INSERT INTO calls (
                    request_id, campaign_id, lead_id, phone, tenant_id,
                    attempt_no, state, created_at_sim, created_at_real
                ) VALUES (?, ?, ?, ?, ?, ?, 'dialing', ?, ?)
                """,
                (
                    request_id,
                    campaign_id,
                    lead_id,
                    phone,
                    tenant_id,
                    attempt_no,
                    sim_now_iso,
                    time.time(),
                ),
            )

            conn.execute(
                """
                UPDATE leads
                SET state = 'dialing', current_request_id = ?
                WHERE campaign_id = ? AND lead_id = ?
                """,
                (request_id, campaign_id, lead_id),
            )

            return True

    def mark_call_accepted(self, request_id: str, call_sid: str) -> bool:
        """
        Marks an accepted call as 'active' and increments lead attempts.
        If the call was already ended (e.g. webhook raced before 201 response),
        we preserve the 'ended' state and only ensure call_sid is recorded.
        """
        with self.transaction() as conn:
            cur = conn.execute("SELECT * FROM calls WHERE request_id = ?", (request_id,))
            call = cur.fetchone()
            if not call:
                return False

            if call["state"] == "ended":
                # Webhook already ended it! Just update call_sid if empty
                if not call["call_sid"]:
                    conn.execute("UPDATE calls SET call_sid = ? WHERE request_id = ?", (call_sid, request_id))
                return True

            conn.execute(
                "UPDATE calls SET state = 'active', call_sid = ? WHERE request_id = ?",
                (call_sid, request_id),
            )

            conn.execute(
                """
                UPDATE leads
                SET state = 'active', current_call_sid = ?, attempts = max(attempts, ?)
                WHERE campaign_id = ? AND lead_id = ? AND current_request_id = ?
                """,
                (call_sid, call["attempt_no"], call["campaign_id"], call["lead_id"], request_id),
            )
            return True

    def mark_call_failed_to_place(self, request_id: str):
        """
        Reverts lead to 'pending' because call was not accepted by provider.
        Attempts counter is NOT incremented.
        """
        with self.transaction() as conn:
            cur = conn.execute("SELECT * FROM calls WHERE request_id = ?", (request_id,))
            call = cur.fetchone()
            if not call or call["state"] == "ended":
                return

            conn.execute("UPDATE calls SET state = 'failed_to_place' WHERE request_id = ?", (request_id,))
            conn.execute(
                """
                UPDATE leads
                SET state = 'pending', current_request_id = NULL
                WHERE campaign_id = ? AND lead_id = ? AND current_request_id = ?
                """,
                (call["campaign_id"], call["lead_id"], request_id),
            )

    def process_call_result(self, result: dict, retry_policy: Any) -> bool:
        """
        Processes a call result (from webhook or watchdog poll) atomically.
        Guarantees idempotency and strictly enforces R5, R6, R7, R8, R9.
        """
        call_sid = result.get("call_sid")
        request_id = result.get("request_id")
        status = result.get("status")
        disposition = result.get("disposition")
        callback_at = result.get("callback_at")
        started_at = result.get("started_at")
        ended_at = result.get("ended_at")
        duration_sec = result.get("duration_sec", 0)

        with self.transaction() as conn:
            # 1. Locate call record
            call = None
            if request_id:
                cur = conn.execute("SELECT * FROM calls WHERE request_id = ?", (request_id,))
                call = cur.fetchone()
            if not call and call_sid:
                cur = conn.execute("SELECT * FROM calls WHERE call_sid = ?", (call_sid,))
                call = cur.fetchone()

            if not call:
                logger.warning("Received result for unknown call request_id=%s sid=%s", request_id, call_sid)
                return False

            if call["state"] == "ended":
                # Already processed idempotently
                return True

            cid = call["campaign_id"]
            lid = call["lead_id"]
            phone = call["phone"]

            # 2. Update call table
            conn.execute(
                """
                UPDATE calls
                SET state = 'ended',
                    call_sid = coalesce(?, call_sid),
                    status = ?,
                    disposition = ?,
                    callback_at = ?,
                    started_at = ?,
                    ended_at = ?,
                    duration_sec = ?
                WHERE request_id = ?
                """,
                (
                    call_sid,
                    status,
                    disposition,
                    callback_at,
                    started_at,
                    ended_at,
                    duration_sec,
                    call["request_id"],
                ),
            )

            # 3. Get current lead state
            cur = conn.execute(
                "SELECT l.*, c.max_attempts FROM leads l JOIN campaigns c ON l.campaign_id = c.campaign_id WHERE l.campaign_id = ? AND l.lead_id = ?",
                (cid, lid),
            )
            lead = cur.fetchone()
            if not lead:
                return False

            # The attempt number of this call is the exact attempt level reached
            new_attempts = max(lead["attempts"], call["attempt_no"])
            max_attempts = lead["max_attempts"]

            # 4. Determine lead next state according to rules
            # Precedence: completed > exhausted > dnd_blocked
            if disposition in TERMINAL_DISPOSITIONS:
                next_state = "completed"
                if disposition == "dnd":
                    # R8: DND is global. Record in global_dnd
                    conn.execute(
                        "INSERT OR IGNORE INTO global_dnd (phone, created_at) VALUES (?, ?)",
                        (phone, ended_at or ""),
                    )
                    # Block all other leads with this phone that haven't completed or exhausted
                    conn.execute(
                        """
                        UPDATE leads
                        SET state = 'dnd_blocked'
                        WHERE phone = ?
                          AND state NOT IN ('completed', 'exhausted')
                          AND NOT (campaign_id = ? AND lead_id = ?)
                        """,
                        (phone, cid, lid),
                    )

                conn.execute(
                    """
                    UPDATE leads
                    SET state = 'completed',
                        attempts = ?,
                        current_request_id = NULL,
                        current_call_sid = NULL,
                        last_status = ?,
                        last_disposition = ?,
                        last_call_ended_at = ?
                    WHERE campaign_id = ? AND lead_id = ?
                    """,
                    (new_attempts, status, disposition, ended_at, cid, lid),
                )

            elif disposition == "callback_requested":
                # R6: Callbacks
                if new_attempts >= max_attempts:
                    next_state = "exhausted"
                    conn.execute(
                        """
                        UPDATE leads
                        SET state = 'exhausted',
                            attempts = ?,
                            current_request_id = NULL,
                            current_call_sid = NULL,
                            last_status = ?,
                            last_disposition = ?,
                            last_call_ended_at = ?
                        WHERE campaign_id = ? AND lead_id = ?
                        """,
                        (new_attempts, status, disposition, ended_at, cid, lid),
                    )
                else:
                    # Check if phone is globally DND blocked
                    cur_dnd = conn.execute("SELECT 1 FROM global_dnd WHERE phone = ?", (phone,))
                    if cur_dnd.fetchone() is not None:
                        next_state = "dnd_blocked"
                    else:
                        next_state = "pending"

                    conn.execute(
                        """
                        UPDATE leads
                        SET state = ?,
                            attempts = ?,
                            priority = 1,
                            callback_at = ?,
                            next_eligible_at = ?,
                            current_request_id = NULL,
                            current_call_sid = NULL,
                            last_status = ?,
                            last_disposition = ?,
                            last_call_ended_at = ?
                        WHERE campaign_id = ? AND lead_id = ?
                        """,
                        (
                            next_state,
                            new_attempts,
                            callback_at,
                            callback_at,
                            status,
                            disposition,
                            ended_at,
                            cid,
                            lid,
                        ),
                    )

            else:
                # status in ('no_answer', 'busy', 'failed')
                if new_attempts >= max_attempts:
                    next_state = "exhausted"
                    conn.execute(
                        """
                        UPDATE leads
                        SET state = 'exhausted',
                            attempts = ?,
                            current_request_id = NULL,
                            current_call_sid = NULL,
                            last_status = ?,
                            last_disposition = ?,
                            last_call_ended_at = ?
                        WHERE campaign_id = ? AND lead_id = ?
                        """,
                        (new_attempts, status, disposition, ended_at, cid, lid),
                    )
                else:
                    # R5: Retries
                    cur_dnd = conn.execute("SELECT 1 FROM global_dnd WHERE phone = ?", (phone,))
                    if cur_dnd.fetchone() is not None:
                        next_state = "dnd_blocked"
                        next_eligible_at = ended_at
                    else:
                        next_state = "pending"
                        delay_mins = retry_policy.get_retry_minutes(status)
                        ended_dt = datetime.fromisoformat(ended_at) if ended_at else datetime.now(IST)
                        next_eligible_dt = ended_dt + timedelta(minutes=delay_mins)
                        next_eligible_at = next_eligible_dt.isoformat()

                    conn.execute(
                        """
                        UPDATE leads
                        SET state = ?,
                            attempts = ?,
                            priority = 2,
                            next_eligible_at = ?,
                            current_request_id = NULL,
                            current_call_sid = NULL,
                            last_status = ?,
                            last_disposition = ?,
                            last_call_ended_at = ?
                        WHERE campaign_id = ? AND lead_id = ?
                        """,
                        (
                            next_state,
                            new_attempts,
                            next_eligible_at,
                            status,
                            disposition,
                            ended_at,
                            cid,
                            lid,
                        ),
                    )

            return True

    def record_webhook_delivery(self, delivery_id: str, call_sid: Optional[str], request_id: Optional[str]) -> bool:
        """
        Records delivery_id. Returns True if first time seen, False if duplicate.
        """
        import time
        with self.transaction() as conn:
            cur = conn.execute("SELECT 1 FROM webhook_deliveries WHERE delivery_id = ?", (delivery_id,))
            if cur.fetchone() is not None:
                return False
            conn.execute(
                "INSERT INTO webhook_deliveries (delivery_id, call_sid, request_id, received_at) VALUES (?, ?, ?, ?)",
                (delivery_id, call_sid, request_id, time.time()),
            )
            return True

    def get_in_flight_calls(self) -> list[sqlite3.Row]:
        conn = self.get_connection()
        cur = conn.execute("SELECT * FROM calls WHERE state IN ('dialing', 'active')")
        return cur.fetchall()

    def get_summary_stats(self) -> dict:
        conn = self.get_connection()
        cur = conn.execute(
            """
            SELECT
                count(*) as total_leads,
                sum(case when state = 'completed' then 1 else 0 end) as completed,
                sum(case when state = 'exhausted' then 1 else 0 end) as exhausted,
                sum(case when state = 'dnd_blocked' then 1 else 0 end) as dnd_blocked,
                sum(case when state = 'pending' then 1 else 0 end) as pending,
                sum(case when state = 'dialing' then 1 else 0 end) as dialing,
                sum(case when state = 'active' then 1 else 0 end) as active
            FROM leads
            """
        )
        row = dict(cur.fetchone())
        row["incomplete"] = row["pending"] + row["dialing"] + row["active"]
        return row

    def is_finished(self) -> bool:
        conn = self.get_connection()
        cur = conn.execute("SELECT count(*) as cnt FROM leads WHERE state IN ('pending', 'dialing', 'active')")
        return cur.fetchone()["cnt"] == 0

    def export_final_state_csv(self, output_path: str):
        """
        Exports final_state.csv sorted by campaign_id, lead_id.
        Matches exact contract: campaign_id,lead_id,final_state,attempts,last_status,last_disposition
        """
        conn = self.get_connection()
        cur = conn.execute(
            """
            SELECT
                campaign_id,
                lead_id,
                state as final_state,
                attempts,
                coalesce(last_status, '') as last_status,
                coalesce(last_disposition, '') as last_disposition
            FROM leads
            ORDER BY campaign_id ASC, lead_id ASC
            """
        )
        rows = cur.fetchall()
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["campaign_id", "lead_id", "final_state", "attempts", "last_status", "last_disposition"])
            for r in rows:
                writer.writerow([
                    r["campaign_id"],
                    r["lead_id"],
                    r["final_state"],
                    r["attempts"],
                    r["last_status"],
                    r["last_disposition"],
                ])
        logger.info("Exported final_state.csv to %s with %d rows.", output_path, len(rows))
