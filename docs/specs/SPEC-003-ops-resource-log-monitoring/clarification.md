# SPEC-003 Clarification

## Confirmed Decisions

- 2026-05-17: The operator approved implementing the reviewed OPS-007 design and
  asked to continue into development after planning.
- 2026-05-17: OPS-007 should use mature SQLite practices rather than treating
  `busy_timeout` or WAL as a complete solution. The plan must reduce lock
  pressure, surface lock symptoms, and make non-critical telemetry writes
  degradable.
- 2026-05-17: Resource monitoring must include disk, CPU, and memory warnings.
- 2026-05-17: Runtime logs are expected to be small, but stale logs must be
  released automatically so they cannot fill the disk unnoticed.

## Open Questions

None for the first implementation slice. Thresholds are intentionally
conservative and can be tuned from operational evidence.
