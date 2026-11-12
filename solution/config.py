"""
Configuration loader and models for the campaign dispatcher.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional

IST = timezone(timedelta(hours=5, minutes=30))
DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
TERMINAL_DISPOSITIONS = {"interested", "not_interested", "wrong_number", "dnd"}


@dataclass
class WindowConfig:
    start: str  # "HH:MM"
    end: str    # "HH:MM"
    days: Optional[list[str]] = None  # None means all 7 days


@dataclass
class RetryPolicy:
    no_answer_minutes: int = 60
    busy_minutes: int = 60
    failed_minutes: int = 15

    def get_retry_minutes(self, status: str) -> int:
        if status == "no_answer":
            return self.no_answer_minutes
        elif status == "busy":
            return self.busy_minutes
        elif status == "failed":
            return self.failed_minutes
        return self.no_answer_minutes


@dataclass
class TenantConfig:
    tenant_id: str
    max_concurrent_calls: int


@dataclass
class CampaignConfig:
    campaign_id: str
    tenant_id: str
    leads_file: str
    window: WindowConfig
    max_attempts: int = 3


@dataclass
class Config:
    fixtures_path: str
    sim_start: datetime
    timezone_name: str
    regulatory_window: WindowConfig
    retry_policy: RetryPolicy
    webhook_secret: str
    tenants: dict[str, TenantConfig] = field(default_factory=dict)
    campaigns: dict[str, CampaignConfig] = field(default_factory=dict)

    @classmethod
    def load(cls, fixtures_path: str) -> Config:
        with open(fixtures_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        sim_start = datetime.fromisoformat(data["sim_start"])
        if sim_start.tzinfo is None:
            sim_start = sim_start.replace(tzinfo=IST)

        rw = WindowConfig(
            start=data["regulatory_window"]["start"],
            end=data["regulatory_window"]["end"],
            days=None,
        )

        rp = RetryPolicy(
            no_answer_minutes=data["retry_policy"].get("no_answer_minutes", 60),
            busy_minutes=data["retry_policy"].get("busy_minutes", 60),
            failed_minutes=data["retry_policy"].get("failed_minutes", 15),
        )

        tenants = {}
        for t in data["tenants"]:
            tenants[t["tenant_id"]] = TenantConfig(
                tenant_id=t["tenant_id"],
                max_concurrent_calls=t["max_concurrent_calls"],
            )

        campaigns = {}
        for c in data["campaigns"]:
            w = WindowConfig(
                start=c["window"]["start"],
                end=c["window"]["end"],
                days=c["window"]["days"],
            )
            campaigns[c["campaign_id"]] = CampaignConfig(
                campaign_id=c["campaign_id"],
                tenant_id=c["tenant_id"],
                leads_file=c["leads_file"],
                window=w,
                max_attempts=c.get("max_attempts", 3),
            )

        return cls(
            fixtures_path=fixtures_path,
            sim_start=sim_start,
            timezone_name=data.get("timezone", "Asia/Kolkata"),
            regulatory_window=rw,
            retry_policy=rp,
            webhook_secret=data["webhook_secret"],
            tenants=tenants,
            campaigns=campaigns,
        )

