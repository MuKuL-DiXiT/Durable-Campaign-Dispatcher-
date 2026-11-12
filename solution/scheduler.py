"""
Scheduler helper for checking windows, concurrency, and selecting eligible leads.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Optional

from .clock import SimClock, is_campaign_open, next_campaign_open
from .config import CampaignConfig, Config, TenantConfig
from .db import Database


class Scheduler:
    def __init__(self, cfg: Config, db: Database, clock: SimClock):
        self.cfg = cfg
        self.db = db
        self.clock = clock

    def get_open_campaign_ids(self, now: datetime) -> list[str]:
        """
        Returns list of campaign IDs whose window is open at time 'now'.
        """
        open_cids = []
        for cid, c in self.cfg.campaigns.items():
            if is_campaign_open(
                now,
                c.window.start,
                c.window.end,
                c.window.days or [],
                self.cfg.regulatory_window.start,
                self.cfg.regulatory_window.end,
                buffer_seconds=2.0,  # 2 second safety margin before window close
            ):
                open_cids.append(cid)
        return open_cids

    def get_next_campaign_open_time(self, now: datetime) -> Optional[datetime]:
        """
        Finds the earliest next window open across all campaigns.
        """
        earliest: Optional[datetime] = None
        for c in self.cfg.campaigns.values():
            nxt = next_campaign_open(
                now,
                c.window.start,
                c.window.end,
                c.window.days or [],
                self.cfg.regulatory_window.start,
                self.cfg.regulatory_window.end,
            )
            if earliest is None or nxt < earliest:
                earliest = nxt
        return earliest

    def get_earliest_next_eligible(self, open_cids: list[str]) -> Optional[str]:
        """
        Returns the earliest next_eligible_at among pending leads in open campaigns.
        """
        if not open_cids:
            return None
        conn = self.db.get_connection()
        placeholders = ",".join("?" for _ in open_cids)
        cur = conn.execute(
            f"""
            SELECT min(next_eligible_at) as earliest
            FROM leads
            WHERE state = 'pending'
              AND campaign_id IN ({placeholders})
              AND phone NOT IN (SELECT phone FROM global_dnd)
            """,
            open_cids,
        )
        row = cur.fetchone()
        return row["earliest"] if row and row["earliest"] else None

