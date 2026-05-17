# SPEC-003 Design

## Architecture

OPS-007 extends the current OPS-006 monitoring path instead of creating a second
operator surface. `/api/status` and WebUI overview remain the source of truth for
runtime alerts. New resource and SQLite snapshots are passed into
`operations_monitor.build_operations_monitor()`, which converts them into
operator-facing warnings and critical alerts.

## SQLite Stability

All production SQLite connections keep using a shared runtime configuration:
busy timeout, WAL for file-backed databases, `synchronous=NORMAL`,
`wal_autocheckpoint`, and `journal_size_limit`. This follows SQLite's official
WAL and PRAGMA guidance while preserving the single-writer reality.

Non-critical telemetry writes are treated separately from business data writes.
API request metrics and service logs become best-effort and are allowed to drop
or retry under busy/locked conditions. Core current-slice and queue writes still
retry and alert when contention persists.

## Retention And Space Release

Table retention is bounded by age and row count:

- `service_logs`: 14 days or 20,000 newest rows.
- `api_request_metrics`: 7 days or 50,000 newest rows.

After cleanup, the telemetry database runs small-step incremental vacuum when
supported. The core database is not full-vacuumed during status checks.

Runtime file cleanup only targets stale log artifacts. Active PID files, stop
files, current worker reports, SQLite files, WAL files, and SHM files are
protected. Active stdout/stderr growth is monitored and documented for Ubuntu
`logrotate`/journald handling.

## Resource Monitoring

The resource snapshot reports disk free space, memory availability, CPU load,
core and telemetry database sizes, WAL/SHM sizes, and runtime log directory size.
Default alert thresholds:

- Disk: warning below 15% free, critical below 10% or 2 GiB.
- Memory: warning below 15% available, critical below 10%.
- CPU: warning when 5-minute load exceeds 1.5x CPU count, critical above 2x.
- Runtime logs: warning above 512 MiB.
- WAL: warning when any monitored WAL exceeds 128 MiB.

## Metrics Worker

The metrics worker wraps retryable SQLite operations with bounded retry/backoff.
If a retryable lock persists, the worker records the retry count in its progress
report, sleeps, and continues polling instead of exiting.

## Deployment Gate

The Linux deployment runbook gains OPS-007 checks: verify no unexpected SQLite
holders, inspect WAL/log sizes, confirm `/api/status.operations` is not critical,
and record retention cleanup status before marking deployment complete.
