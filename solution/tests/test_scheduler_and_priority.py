"""
Tests for scheduling rules, priority ordering (due callbacks > retries > fresh leads),
and retry interval math.
"""
from datetime import datetime, timedelta
import pytest
import sqlite3

from solution.clock import IST
from solution.config import RetryPolicy
from solution.db import Database


@pytest.fixture
def test_db(tmp_path):
    db_file = str(tmp_path / "test.db")
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
    return db


def test_retry_intervals():
    rp = RetryPolicy(no_answer_minutes=60, busy_minutes=60, failed_minutes=15)
    assert rp.get_retry_minutes("no_answer") == 60
    assert rp.get_retry_minutes("busy") == 60
    assert rp.get_retry_minutes("failed") == 15


def test_priority_ordering(test_db):
    now = datetime(2026, 11, 12, 12, 0, 0, tzinfo=IST)
    now_iso = now.isoformat()

    past_time_1 = (now - timedelta(minutes=10)).isoformat()
    past_time_2 = (now - timedelta(minutes=5)).isoformat()

    with test_db.transaction() as conn:
        # Fresh lead (priority 3)
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, state, attempts, priority, next_eligible_at)
            VALUES ('c1', 'lead_fresh', '+919000000001', 'pending', 0, 3, ?)
            """,
            (past_time_1,),
        )
        # Retry lead (priority 2)
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, state, attempts, priority, next_eligible_at)
            VALUES ('c1', 'lead_retry', '+919000000002', 'pending', 1, 2, ?)
            """,
            (past_time_1,),
        )
        # Due Callback lead (priority 1)
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, state, attempts, priority, next_eligible_at)
            VALUES ('c1', 'lead_callback', '+919000000003', 'pending', 1, 1, ?)
            """,
            (past_time_2,),
        )

    # Query eligible leads
    leads = test_db.get_eligible_leads_for_tenant("t1", now_iso, ["c1"], limit=10)
    lead_ids = [l["lead_id"] for l in leads]

    # Rule R6: Due callbacks go before retries, and retries before fresh leads
    assert lead_ids == ["lead_callback", "lead_retry", "lead_fresh"]


def test_callback_ordering_by_earliest_due(test_db):
    now = datetime(2026, 11, 12, 12, 0, 0, tzinfo=IST)
    now_iso = now.isoformat()

    cb_early = (now - timedelta(minutes=30)).isoformat()
    cb_late = (now - timedelta(minutes=5)).isoformat()

    with test_db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, state, attempts, priority, next_eligible_at)
            VALUES ('c1', 'cb_late', '+919000000010', 'pending', 1, 1, ?)
            """,
            (cb_late,),
        )
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, state, attempts, priority, next_eligible_at)
            VALUES ('c1', 'cb_early', '+919000000011', 'pending', 1, 1, ?)
            """,
            (cb_early,),
        )

    leads = test_db.get_eligible_leads_for_tenant("t1", now_iso, ["c1"], limit=10)
    assert [l["lead_id"] for l in leads] == ["cb_early", "cb_late"]


def test_future_eligible_leads_not_selected(test_db):
    now = datetime(2026, 11, 12, 12, 0, 0, tzinfo=IST)
    now_iso = now.isoformat()
    future_iso = (now + timedelta(minutes=15)).isoformat()

    with test_db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, state, attempts, priority, next_eligible_at)
            VALUES ('c1', 'future_lead', '+919000000020', 'pending', 1, 2, ?)
            """,
            (future_iso,),
        )

    leads = test_db.get_eligible_leads_for_tenant("t1", now_iso, ["c1"], limit=10)
    assert len(leads) == 0

