# SPEC-003 Implementation Plan

1. Extend SQLite runtime helpers with WAL/checkpoint limits, retry helpers, and
   a lightweight busy/locked tracker.
2. Add telemetry retention helpers for service logs and API request metrics.
3. Add resource snapshot helpers for disk, memory, CPU, SQLite, WAL, and runtime
   log file sizes.
4. Wire the new snapshots and cleanup status into API/WebUI status payloads and
   operations alerts.
5. Harden the metrics worker retry path for retryable SQLite lock failures.
6. Add focused tests for retention, resource alerts, API/WebUI payloads, and
   metrics-worker lock resilience.
7. Update deployment runbook, operations log, task, risk, traceability, project
   status, and handoff records.

## Rollback

OPS-007 is additive. If runtime monitoring causes unexpected overhead, disable
status-triggered cleanup first and keep resource snapshots read-only. The core
SQLite schema remains compatible because retention only deletes bounded
telemetry rows.
