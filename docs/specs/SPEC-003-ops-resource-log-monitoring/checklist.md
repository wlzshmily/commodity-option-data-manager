# SPEC-003 Checklist

- [x] Requirements have no open clarification markers.
- [x] Design separates core business writes from telemetry writes.
- [x] Design avoids unsafe SQLite WAL/SHM deletion.
- [x] Design avoids production full `VACUUM` during status checks.
- [x] Acceptance criteria include code, UI/API, worker, and deployment evidence.
- [x] User approved the design before implementation.
