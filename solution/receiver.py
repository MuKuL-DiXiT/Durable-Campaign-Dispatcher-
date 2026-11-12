"""
FastAPI webhook receiver and status server.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import JSONResponse

from .config import Config
from .db import Database

logger = logging.getLogger("dispatcher.receiver")


def create_receiver_app(
    cfg: Config,
    db: Database,
    wake_event: asyncio.Event,
) -> FastAPI:
    app = FastAPI(title="Agentic Universe - Dispatcher Webhook Receiver", version="1.0")

    @app.post("/webhooks/telephony")
    async def telephony_webhook(
        request: Request,
        x_au_signature: Optional[str] = Header(None, alias="X-AU-Signature"),
        x_au_delivery_id: Optional[str] = Header(None, alias="X-AU-Delivery-Id"),
    ):
        raw_body = await request.body()

        # 1. Signature verification
        if not x_au_signature:
            logger.warning("Webhook received without signature")
            raise HTTPException(status_code=401, detail="Missing signature")

        expected_sig = hmac.new(
            cfg.webhook_secret.encode("utf-8"),
            raw_body,
            hashlib.sha256,
        ).hexdigest()

        if not hmac.compare_digest(x_au_signature, expected_sig):
            logger.warning("Webhook invalid signature: got %s, expected %s", x_au_signature, expected_sig)
            raise HTTPException(status_code=401, detail="Invalid signature")

        # 2. Parse payload
        try:
            payload = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON body")

        call_sid = payload.get("call_sid")
        request_id = payload.get("request_id")

        # 3. Check duplicate delivery ID
        is_new_delivery = True
        if x_au_delivery_id:
            is_new_delivery = db.record_webhook_delivery(x_au_delivery_id, call_sid, request_id)

        # 4. Process result atomically and idempotently
        db.process_call_result(payload, cfg.retry_policy)

        # 5. Wake up the dispatch loop
        wake_event.set()

        return JSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "call_sid": call_sid,
                "is_duplicate_delivery": not is_new_delivery,
            },
        )

    @app.get("/health")
    async def health():
        return {"status": "healthy"}

    @app.get("/status")
    async def status():
        stats = db.get_summary_stats()
        return {
            "status": "running" if not db.is_finished() else "finished",
            "stats": stats,
        }

    return app

