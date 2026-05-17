# SPEC-003 OPS-007 Resource And Log Keepalive Monitoring Requirements

## Context

OPS-005 and OPS-006 established that deployments must not be considered healthy
only because the WebUI responds or realtime subscription objects are created.
The next operational risk is resource exhaustion and SQLite write contention:
SQLite lock symptoms can kill or degrade workers, while runtime and table logs can
grow until the server disk becomes unsafe.

## Requirements

- OPS-007-R1: The application must surface disk, memory, CPU, SQLite file, WAL,
  and runtime-log health through the existing `/api/status.operations` object and
  WebUI operations alert banner.
- OPS-007-R2: Non-critical telemetry writes, including API request metrics and
  service logs, must be best-effort under SQLite busy/locked conditions and must
  not make user-facing API endpoints fail.
- OPS-007-R3: The runtime must retain service logs and API request metrics by
  bounded age and row count, then release reclaimable SQLite space without
  blocking core current-slice writes.
- OPS-007-R4: Runtime file-log handling must protect active PID/stop/report
  files and remove only stale log artifacts; active stdout/stderr growth must be
  monitored and handled by the Ubuntu deployment runbook.
- OPS-007-R5: Metrics-worker SQLite busy/locked events must not terminate the
  worker. The worker must retry, record progress evidence, and continue polling.
- OPS-007-R6: The Ubuntu deployment gate must include resource and SQLite health:
  old SQLite holders, abnormal WAL/log growth, critical disk/memory/CPU state, and
  failed retention cleanup prevent a deployment from being marked complete.

## Non-Goals

- Do not migrate the system to PostgreSQL in OPS-007.
- Do not run full `VACUUM` against the production core database during normal API
  status checks.
- Do not delete live SQLite `-wal` or `-shm` files manually.
- Do not introduce external monitoring infrastructure in the first version.

## Acceptance Criteria

- `/api/status` includes resource and SQLite/log-retention health details.
- WebUI overview shows warnings or critical alerts for low disk, low memory,
  high CPU load, oversized runtime logs, telemetry cleanup failure, and recent
  SQLite busy/locked conditions.
- API request metrics and service logs have retention cleanup.
- Metrics worker remains alive across retryable SQLite busy/locked operations.
- Linux deployment documentation contains OPS-007 resource/log gates.
- Unit/API/WebUI tests and project SDLC checks pass.
