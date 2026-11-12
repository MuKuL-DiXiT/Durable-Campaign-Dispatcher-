# Durable Campaign Dispatcher Solution

Package containing the complete workflow dispatcher service and unit tests for Assignment B.

## Module Structure

- `__main__.py`: CLI entry point (`python -m solution`), orchestrates clock sync, DB initialization/recovery, webhook receiver, and dispatch loop.
- `dispatcher.py`: Core workflow engine managing concurrent dialing, ambiguous 504 reconciliation, active call watchdog, and export of `final_state.csv`.
- `receiver.py`: FastAPI webhook receiver implementing HMAC-SHA256 signature verification and idempotent delivery handling on `POST /webhooks/telephony`.
- `db.py`: SQLite engine with WAL mode and atomic `BEGIN IMMEDIATE` transactions enforcing concurrency limits, one-call-per-phone, and global DND precedence.
- `clock.py`: SimClock and calling window calculation logic handling multi-day schedules, regulatory windows (09:00–21:00 IST), and clock skew buffers.
- `config.py`: Typed configuration models for tenants, campaigns, retry policies, and calling windows.
- `scheduler.py`: Scheduling algorithms selecting open campaigns and sorting leads by priority (due callback > retry > fresh).
- `tests/`: 16 comprehensive unit tests covering window maths, priority ordering, concurrency limits, DND propagation, and crash recovery.
