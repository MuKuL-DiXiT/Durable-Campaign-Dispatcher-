# AI Usage Disclosure

## Tools used
- Antigravity (Gemini 3.8)

## What I used them for
- Scaffolding the initial FastAPI webhook route templates and HMAC signature verification boilerplate.
- Drafting initial SQL DDL schema and synthetic unit test fixtures.
- Formatting Mermaid diagrams and summary markdown tables.

## What I wrote or decided myself
- **Two-phase pre-POST atomic claim pattern**: Deciding to record `request_id` in SQLite *before* calling `POST /v1/calls` so that any crash mid-call can be unambiguously queried and resolved via `GET /v1/calls?request_id=...`.
- **Hybrid Webhook + Watchdog architecture**: Designing the periodic poll mechanism on active calls to achieve 100% recovery from the 3% dropped webhooks without relying solely on callback redelivery.
- **DND precedence and global propagation**: Enforcing `completed > exhausted > dnd_blocked` precedence across shared tenant campaigns.
- **Provider epoch synchronization**: Implementing automatic detection of mock provider restarts vs mid-run dispatcher crashes using `sim_epoch` / `real_epoch`.

## Where the AI was wrong or unhelpful
- **Attempt count race condition**: The initial attempt tracking logic incremented `lead.attempts` via relative addition (`attempts + 1`) if the call was still in `dialing` state. During rapid webhook arrivals before the 201 response, this caused an attempt desync in test assertions. Fixed by anchoring the lead's attempt count directly to `max(lead.attempts, call.attempt_no)`, making attempt tracking idempotent regardless of arrival order.

## How I verified AI-generated code
- Read every line of generated code against the `GLOSSARY.md` and `mock_telephony/server.py` implementation.
- Created a comprehensive test suite (16 tests) with fake simulated clocks covering window boundary skew, regulatory window overlap, callback priority, tenant limits, and DND precedence.
- Executed full live chaos runs against the mock telephony server under `--failure-scale 1.0` and mid-run `kill -9` crashes, verifying 0 violations and 100% lead state matching.
