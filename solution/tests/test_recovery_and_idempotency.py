"""
Tests for failure handling, idempotency, webhook signature verification,
504 resolution, and crash recovery.
"""
import asyncio
import hashlib
import hmac
import json
from datetime import datetime
import pytest

from fastapi.testclient import TestClient

from solution.clock import IST, SimClock
from solution.config import Config, RetryPolicy
from solution.db import Database
from solution.dispatcher import Dispatcher
from solution.receiver import create_receiver_app


@pytest.fixture
def test_setup(tmp_path):
    db_file = str(tmp_path / "rec.db")
    db = Database(db_file)
    db.init_schema()

    with db.transaction() as conn:
        conn.execute("INSERT INTO tenants (tenant_id, max_concurrent_calls) VALUES ('t1', 5)")
        conn.execute(
            """
            INSERT INTO campaigns (campaign_id, tenant_id, window_start, window_end, window_days, max_attempts)
            VALUES ('c1', 't1', '10:00', '19:00', 'mon,tue,wed,thu,fri,sat', 3)
            """
        )
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, state, attempts, priority, next_eligible_at)
            VALUES ('c1', 'l1', '+919000000001', 'pending', 0, 3, '2026-11-12T12:00:00+05:30')
            """
        )

    wake_event = asyncio.Event()
    return db, wake_event


def test_webhook_signature_and_duplicate(test_setup):
    db, wake_event = test_setup
    secret = "test_webhook_secret_123"

    cfg = Config(
        fixtures_path="",
        sim_start=datetime.now(IST),
        timezone_name="Asia/Kolkata",
        regulatory_window=None,
        retry_policy=RetryPolicy(),
        webhook_secret=secret,
    )

    app = create_receiver_app(cfg, db, wake_event)
    client = TestClient(app)

    now_iso = datetime(2026, 11, 12, 12, 0, 0, tzinfo=IST).isoformat()
    # Insert call record in dialing state
    with db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO calls (request_id, campaign_id, lead_id, phone, tenant_id, attempt_no, state, created_at_sim, created_at_real)
            VALUES ('req_test_sig', 'c1', 'l1', '+919000000001', 't1', 1, 'dialing', ?, 0.0)
            """,
            (now_iso,),
        )
        conn.execute("UPDATE leads SET state = 'dialing', current_request_id = 'req_test_sig' WHERE lead_id = 'l1'")

    payload = {
        "event": "call.completed",
        "call_sid": "CA_SIG_TEST",
        "request_id": "req_test_sig",
        "tenant_id": "t1",
        "campaign_id": "c1",
        "lead_id": "l1",
        "phone": "+919000000001",
        "status": "completed",
        "disposition": "interested",
        "started_at": now_iso,
        "ended_at": now_iso,
        "duration_sec": 45,
    }
    raw_body = json.dumps(payload).encode("utf-8")

    # 1. Test missing signature -> 401
    resp_no_sig = client.post("/webhooks/telephony", content=raw_body)
    assert resp_no_sig.status_code == 401

    # 2. Test invalid signature -> 401
    resp_bad_sig = client.post(
        "/webhooks/telephony",
        content=raw_body,
        headers={"X-AU-Signature": "invalid_hex_signature"},
    )
    assert resp_bad_sig.status_code == 401

    # 3. Test valid signature -> 200
    valid_sig = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    resp_valid = client.post(
        "/webhooks/telephony",
        content=raw_body,
        headers={
            "X-AU-Signature": valid_sig,
            "X-AU-Delivery-Id": "delivery_001",
        },
    )
    assert resp_valid.status_code == 200
    assert resp_valid.json()["is_duplicate_delivery"] is False

    # Lead should now be completed
    conn = db.get_connection()
    lead = conn.execute("SELECT state, attempts FROM leads WHERE lead_id = 'l1'").fetchone()
    assert lead["state"] == "completed"
    assert lead["attempts"] == 1

    # 4. Duplicate webhook delivery -> 200 and idempotent
    resp_dup = client.post(
        "/webhooks/telephony",
        content=raw_body,
        headers={
            "X-AU-Signature": valid_sig,
            "X-AU-Delivery-Id": "delivery_001",
        },
    )
    assert resp_dup.status_code == 200
    assert resp_dup.json()["is_duplicate_delivery"] is True

    # Attempts should remain 1
    lead_after = conn.execute("SELECT state, attempts FROM leads WHERE lead_id = 'l1'").fetchone()
    assert lead_after["state"] == "completed"
    assert lead_after["attempts"] == 1


def test_webhook_arriving_before_post_201_response(test_setup):
    """
    Simulates race condition: webhook arrives and ends the call BEFORE
    the HTTP POST /v1/calls response returns 201 to the dialer.
    """
    db, _ = test_setup
    now_iso = datetime(2026, 11, 12, 12, 0, 0, tzinfo=IST).isoformat()
    rp = RetryPolicy()

    # Lead claimed for dialing
    db.claim_lead_for_dialing(
        campaign_id="c1", lead_id="l1", tenant_id="t1", phone="+919000000001",
        attempt_no=1, sim_now_iso=now_iso, request_id="req_race_1",
        max_tenant_concurrency=5, max_attempts=3
    )

    # Webhook arrives FIRST!
    result = {
        "call_sid": "CA_RACE_1",
        "request_id": "req_race_1",
        "status": "completed",
        "disposition": "interested",
        "ended_at": now_iso,
    }
    db.process_call_result(result, rp)

    # Lead should be completed
    conn = db.get_connection()
    lead_mid = conn.execute("SELECT state, attempts FROM leads WHERE lead_id = 'l1'").fetchone()
    assert lead_mid["state"] == "completed"
    assert lead_mid["attempts"] == 1

    # Now the POST /v1/calls returns 201 accepted!
    db.mark_call_accepted("req_race_1", "CA_RACE_1")

    # State must NOT be corrupted or reverted back to 'active'
    lead_final = conn.execute("SELECT state, attempts FROM leads WHERE lead_id = 'l1'").fetchone()
    assert lead_final["state"] == "completed"
    assert lead_final["attempts"] == 1


def test_reconcile_call_not_placed(test_setup):
    """
    Simulates a 504 / crash where provider returns 404 on request_id lookup.
    The lead must revert to pending with 0 attempts.
    """
    db, _ = test_setup
    now_iso = datetime(2026, 11, 12, 12, 0, 0, tzinfo=IST).isoformat()

    db.claim_lead_for_dialing(
        campaign_id="c1", lead_id="l1", tenant_id="t1", phone="+919000000001",
        attempt_no=1, sim_now_iso=now_iso, request_id="req_not_placed",
        max_tenant_concurrency=5, max_attempts=3
    )

    # Provider says 404 (not placed)
    db.mark_call_failed_to_place("req_not_placed")

    conn = db.get_connection()
    lead = conn.execute("SELECT state, attempts, current_request_id FROM leads WHERE lead_id = 'l1'").fetchone()
    assert lead["state"] == "pending"
    assert lead["attempts"] == 0
    assert lead["current_request_id"] is None

