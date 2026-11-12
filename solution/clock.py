"""
Simulated clock and window calculation utilities.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Optional

IST = timezone(timedelta(hours=5, minutes=30))
DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


def parse_hm(s: str) -> tuple[int, int]:
    h, m = s.split(":")
    return int(h), int(m)


def is_in_window(
    t: datetime,
    start: str,
    end: str,
    days: Optional[list[str]] = None,
    buffer_seconds: float = 0.0,
) -> bool:
    """
    Returns True if t is within [start, end - buffer_seconds) on an allowed day.
    Grace is 0 here to strictly avoid provider violations.
    """
    lt = t.astimezone(IST)
    if days is not None and DAYS[lt.weekday()] not in days:
        return False

    sh, sm = parse_hm(start)
    eh, em = parse_hm(end)

    s = lt.replace(hour=sh, minute=sm, second=0, microsecond=0)
    e = lt.replace(hour=eh, minute=em, second=0, microsecond=0)

    if buffer_seconds > 0:
        e = e - timedelta(seconds=buffer_seconds)

    return s <= lt < e


def next_window_open(
    t: datetime,
    start: str,
    end: str,
    days: Optional[list[str]] = None,
) -> datetime:
    """
    Returns the earliest datetime >= t when the window is open.
    If t is already inside [start, end) on an allowed day, returns t.
    """
    lt = t.astimezone(IST)
    allowed_days = days if days is not None else DAYS

    sh, sm = parse_hm(start)
    eh, em = parse_hm(end)

    for d in range(0, 14):  # search up to 2 weeks ahead
        day = (lt + timedelta(days=d)).replace(second=0, microsecond=0)
        day_name = DAYS[day.weekday()]
        if day_name not in allowed_days:
            continue

        s = day.replace(hour=sh, minute=sm)
        e = day.replace(hour=eh, minute=em)

        if d == 0:
            if lt < s:
                return s
            if lt < e:
                return lt
            continue
        return s

    return lt


def is_campaign_open(
    t: datetime,
    campaign_window_start: str,
    campaign_window_end: str,
    campaign_days: list[str],
    regulatory_start: str = "09:00",
    regulatory_end: str = "21:00",
    buffer_seconds: float = 2.0,
) -> bool:
    """
    Returns True if both regulatory window and campaign window are open at t.
    """
    if not is_in_window(t, regulatory_start, regulatory_end, None, buffer_seconds=buffer_seconds):
        return False
    if not is_in_window(t, campaign_window_start, campaign_window_end, campaign_days, buffer_seconds=buffer_seconds):
        return False
    return True


def next_campaign_open(
    t: datetime,
    campaign_window_start: str,
    campaign_window_end: str,
    campaign_days: list[str],
    regulatory_start: str = "09:00",
    regulatory_end: str = "21:00",
) -> datetime:
    """
    Finds the earliest instant >= t when both regulatory window and campaign window are open.
    """
    probe = t.astimezone(IST)
    for _ in range(14 * 24):  # step in intervals if needed
        next_camp = next_window_open(probe, campaign_window_start, campaign_window_end, campaign_days)
        next_reg = next_window_open(next_camp, regulatory_start, regulatory_end, None)
        if next_reg == next_camp:
            # Both open at next_camp
            return next_camp
        probe = next_reg
    return probe


class SimClock:
    """
    Local simulation clock synchronized with mock telephony server.
    """
    def __init__(self, sim_epoch: datetime, real_epoch: float, speed: float = 600.0):
        if sim_epoch.tzinfo is None:
            self.sim_epoch = sim_epoch.replace(tzinfo=IST)
        else:
            self.sim_epoch = sim_epoch.astimezone(IST)
        self.real_epoch = real_epoch
        self.speed = speed

    def now(self) -> datetime:
        elapsed_real = time.time() - self.real_epoch
        return self.sim_epoch + timedelta(seconds=elapsed_real * self.speed)

    def real_seconds_until(self, target_sim_time: datetime) -> float:
        current_sim = self.now()
        sim_delta = (target_sim_time - current_sim).total_seconds()
        if sim_delta <= 0:
            return 0.0
        return sim_delta / self.speed

    def sim_seconds_to_real(self, sim_seconds: float) -> float:
        return max(0.0, sim_seconds / self.speed)

