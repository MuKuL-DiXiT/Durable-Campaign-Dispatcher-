"""
Tests for concurrency rules (tenant limit, one call per phone),
attempts limit, and global DND propagation with precedence rules.
"""
from datetime import datetime, timedelta
import pytest

from solution.clock import IST
from solution.config import RetryPolicy
from solution.db import Database


@pytest.fixture
def multi_campaign_db(tmp_path):
    db_file = str(tmp_path / "multi.db")
    db = Database(db_file)
    db.init_schema()

    with db.transaction() as conn:
        # Tenant 1 has max concurrency of 2, shared across c1 and c2
        conn.execute("INSERT INTO tenants (tenant_id, max_concurrent_calls) VALUES ('t1', 2)")
        conn.execute(
            """
            INSERT INTO campaigns (campaign_id, tenant_id, window_start, window_end, window_days, max_attempts)
            VALUES ('c1', 't1', '10:00', '19:00', 'mon,tue,wed,thu,fri,sat', 3),
                   ('c2', 't1', '10:00', '19:00', 'mon,tue,wed,thu,fri,sat', 3)
            """
        )
    return db


def test_tenant_concurrency_limit_shared(multi_campaign_db):
    now_iso = datetime(2026, 11, 12, 12, 0, 0, tzinfo=IST).isoformat()

    with multi_campaign_db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, state, attempts, priority, next_eligible_at)
            VALUES ('c1', 'l1', '+919000000001', 'pending', 0, 3, ?),
                   ('c1', 'l2', '+919000000002', 'pending', 0, 3, ?),
                   ('c2', 'l3', '+919000000003', 'pending', 0, 3, ?)
            """,
            (now_iso, now_iso, now_iso),
        )

    # Claim 1st call for c1
    c1_claimed = multi_campaign_db.claim_lead_for_dialing(
        campaign_id="c1", lead_id="l1", tenant_id="t1", phone="+919000000001",
        attempt_no=1, sim_now_iso=now_iso, request_id="req_1",
        max_tenant_concurrency=2, max_attempts=3
    )
    assert c1_claimed is True

    # Claim 2nd call for c2
    c2_claimed = multi_campaign_db.claim_lead_for_dialing(
        campaign_id="c2", lead_id="l3", tenant_id="t1", phone="+919000000003",
        attempt_no=1, sim_now_iso=now_iso, request_id="req_2",
        max_tenant_concurrency=2, max_attempts=3
    )
    assert c2_claimed is True

    # Active count is now 2 (max for t1). 3rd claim must fail!
    c3_claimed = multi_campaign_db.claim_lead_for_dialing(
        campaign_id="c1", lead_id="l2", tenant_id="t1", phone="+919000000002",
        attempt_no=1, sim_now_iso=now_iso, request_id="req_3",
        max_tenant_concurrency=2, max_attempts=3
    )
    assert c3_claimed is False


def test_one_call_per_phone(multi_campaign_db):
    now_iso = datetime(2026, 11, 12, 12, 0, 0, tzinfo=IST).isoformat()
    shared_phone = "+919999999999"

    with multi_campaign_db.transaction() as conn:
        # Same phone in both c1 and c2
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, state, attempts, priority, next_eligible_at)
            VALUES ('c1', 'l_phone_c1', ?, 'pending', 0, 3, ?),
                   ('c2', 'l_phone_c2', ?, 'pending', 0, 3, ?)
            """,
            (shared_phone, now_iso, shared_phone, now_iso),
        )

    # Claim for c1
    claimed_1 = multi_campaign_db.claim_lead_for_dialing(
        campaign_id="c1", lead_id="l_phone_c1", tenant_id="t1", phone=shared_phone,
        attempt_no=1, sim_now_iso=now_iso, request_id="req_phone_1",
        max_tenant_concurrency=5, max_attempts=3
    )
    assert claimed_1 is True

    # Claim for c2 with same phone must be rejected (R3: One call per phone)
    claimed_2 = multi_campaign_db.claim_lead_for_dialing(
        campaign_id="c2", lead_id="l_phone_c2", tenant_id="t1", phone=shared_phone,
        attempt_no=1, sim_now_iso=now_iso, request_id="req_phone_2",
        max_tenant_concurrency=5, max_attempts=3
    )
    assert claimed_2 is False

    # Also eligible leads query filters it out
    eligible = multi_campaign_db.get_eligible_leads_for_tenant("t1", now_iso, ["c2"], limit=5)
    assert shared_phone not in [l["phone"] for l in eligible]


def test_max_attempts_exhausted(multi_campaign_db):
    now_iso = datetime(2026, 11, 12, 12, 0, 0, tzinfo=IST).isoformat()
    rp = RetryPolicy()

    with multi_campaign_db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, state, attempts, priority, next_eligible_at)
            VALUES ('c1', 'l_exhaust', '+919876543210', 'active', 2, 3, ?)
            """,
            (now_iso,),
        )
        conn.execute(
            """
            INSERT INTO calls (request_id, campaign_id, lead_id, phone, tenant_id, attempt_no, state, created_at_sim, created_at_real)
            VALUES ('req_att_3', 'c1', 'l_exhaust', '+919876543210', 't1', 3, 'active', ?, 0.0)
            """,
            (now_iso,),
        )

    # Call 3 ends with no_answer (attempts is now 3 = max_attempts)
    result = {
        "call_sid": "CA12345",
        "request_id": "req_att_3",
        "status": "no_answer",
        "disposition": None,
        "ended_at": now_iso,
    }
    multi_campaign_db.process_call_result(result, rp)

    conn = multi_campaign_db.get_connection()
    lead = conn.execute("SELECT state, attempts FROM leads WHERE lead_id = 'l_exhaust'").fetchone()
    assert lead["attempts"] == 3
    assert lead["state"] == "exhausted"


def test_global_dnd_and_precedence(multi_campaign_db):
    now_iso = datetime(2026, 11, 12, 12, 0, 0, tzinfo=IST).isoformat()
    rp = RetryPolicy()
    shared_phone = "+919888888888"

    with multi_campaign_db.transaction() as conn:
        # Lead A in c1 (will get DND)
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, state, attempts, priority, next_eligible_at)
            VALUES ('c1', 'lead_a', ?, 'active', 1, 3, ?)
            """,
            (shared_phone, now_iso),
        )
        conn.execute(
            """
            INSERT INTO calls (request_id, campaign_id, lead_id, phone, tenant_id, attempt_no, state, created_at_sim, created_at_real)
            VALUES ('req_dnd_a', 'c1', 'lead_a', ?, 't1', 1, 'active', ?, 0.0)
            """,
            (shared_phone, now_iso),
        )

        # Lead B in c2 with same phone: currently pending with 0 attempts
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, state, attempts, priority, next_eligible_at)
            VALUES ('c2', 'lead_b', ?, 'pending', 0, 3, ?)
            """,
            (shared_phone, now_iso),
        )

        # Lead C in c2 with same phone: already completed
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, state, attempts, priority, next_eligible_at)
            VALUES ('c2', 'lead_c', ?, 'completed', 1, 3, ?)
            """,
            (shared_phone, now_iso),
        )

        # Lead D in c2 with same phone: already exhausted (3 attempts)
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, state, attempts, priority, next_eligible_at)
            VALUES ('c2', 'lead_d', ?, 'exhausted', 3, 3, ?)
            """,
            (shared_phone, now_iso),
        )

    # Lead A receives DND
    result_dnd = {
        "call_sid": "CA_DND",
        "request_id": "req_dnd_a",
        "status": "completed",
        "disposition": "dnd",
        "ended_at": now_iso,
    }
    multi_campaign_db.process_call_result(result_dnd, rp)

    conn = multi_campaign_db.get_connection()
    lead_a = conn.execute("SELECT state FROM leads WHERE lead_id = 'lead_a'").fetchone()
    lead_b = conn.execute("SELECT state FROM leads WHERE lead_id = 'lead_b'").fetchone()
    lead_c = conn.execute("SELECT state FROM leads WHERE lead_id = 'lead_c'").fetchone()
    lead_d = conn.execute("SELECT state FROM leads WHERE lead_id = 'lead_d'").fetchone()

    # Precedence: completed > exhausted > dnd_blocked
    # Lead A got dnd -> completed
    assert lead_a["state"] == "completed"
    # Lead B had 0 attempts -> dnd_blocked
    assert lead_b["state"] == "dnd_blocked"
    # Lead C was already completed -> remains completed
    assert lead_c["state"] == "completed"
    # Lead D was already exhausted -> remains exhausted
    assert lead_d["state"] == "exhausted"

