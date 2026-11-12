"""
Tests for calling window maths, day-of-week checks, and next open computations.
Uses synthetic dates with no dependency on real time or external services.
"""
from datetime import datetime, timedelta, timezone
import pytest

from solution.clock import (
    IST,
    DAYS,
    is_in_window,
    next_window_open,
    is_campaign_open,
    next_campaign_open,
)


def test_is_in_window_basic():
    # Thursday 12 Nov 2026 14:00 IST
    t = datetime(2026, 11, 12, 14, 0, 0, tzinfo=IST)
    assert DAYS[t.weekday()] == "thu"

    # Within 10:00 - 19:00 on mon-sat
    assert is_in_window(t, "10:00", "19:00", ["mon", "tue", "wed", "thu", "fri", "sat"]) is True

    # Outside hours (e.g. 15:00 - 18:00)
    assert is_in_window(t, "15:00", "18:00", ["thu"]) is False
    assert is_in_window(t, "11:00", "14:00", ["thu"]) is False  # boundary is [start, end)

    # Disallowed day
    assert is_in_window(t, "10:00", "19:00", ["fri", "sat"]) is False


def test_is_in_window_boundary_and_buffer():
    t_start = datetime(2026, 11, 12, 10, 0, 0, tzinfo=IST)
    t_end = datetime(2026, 11, 12, 19, 0, 0, tzinfo=IST)
    t_just_before_end = datetime(2026, 11, 12, 18, 59, 59, tzinfo=IST)

    # Exact start is inside [start, end)
    assert is_in_window(t_start, "10:00", "19:00", ["thu"]) is True

    # Exact end is outside [start, end)
    assert is_in_window(t_end, "10:00", "19:00", ["thu"]) is False

    # Just before end with buffer
    assert is_in_window(t_just_before_end, "10:00", "19:00", ["thu"], buffer_seconds=0.0) is True
    assert is_in_window(t_just_before_end, "10:00", "19:00", ["thu"], buffer_seconds=2.0) is False


def test_next_window_open_same_day():
    # Thursday 08:30 IST, window opens 10:00
    t = datetime(2026, 11, 12, 8, 30, 0, tzinfo=IST)
    nxt = next_window_open(t, "10:00", "19:00", ["thu"])
    assert nxt == datetime(2026, 11, 12, 10, 0, 0, tzinfo=IST)

    # Thursday 14:00 IST (already open)
    t_open = datetime(2026, 11, 12, 14, 0, 0, tzinfo=IST)
    assert next_window_open(t_open, "10:00", "19:00", ["thu"]) == t_open


def test_next_window_open_overnight_and_weekend():
    # Thursday 19:30 IST (after close), next open is Friday 10:00
    t_thu_night = datetime(2026, 11, 12, 19, 30, 0, tzinfo=IST)
    nxt_fri = next_window_open(t_thu_night, "10:00", "19:00", ["thu", "fri"])
    assert nxt_fri == datetime(2026, 11, 13, 10, 0, 0, tzinfo=IST)

    # Friday 18:30 IST for mon-fri window (Zen Winback: closes 18:00)
    # Next open must skip Saturday and Sunday and open Monday 09:30
    t_fri_night = datetime(2026, 11, 13, 18, 30, 0, tzinfo=IST)
    nxt_mon = next_window_open(t_fri_night, "09:30", "18:00", ["mon", "tue", "wed", "thu", "fri"])
    # 2026-11-13 is Friday; 2026-11-16 is Monday
    assert nxt_mon == datetime(2026, 11, 16, 9, 30, 0, tzinfo=IST)
    assert DAYS[nxt_mon.weekday()] == "mon"


def test_regulatory_window_intersection():
    # 08:30 IST is outside regulatory window (09:00 - 21:00)
    t_early = datetime(2026, 11, 12, 8, 30, 0, tzinfo=IST)
    assert is_campaign_open(t_early, "08:00", "18:00", ["thu"]) is False

    # 09:30 IST is inside both
    t_valid = datetime(2026, 11, 12, 9, 30, 0, tzinfo=IST)
    assert is_campaign_open(t_valid, "09:00", "18:00", ["thu"]) is True

    # 21:15 IST is outside regulatory window
    t_late = datetime(2026, 11, 12, 21, 15, 0, tzinfo=IST)
    assert is_campaign_open(t_late, "10:00", "22:00", ["thu"]) is False

