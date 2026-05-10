# Tasks

| Task | Status | Files | Verification |
| --- | --- | --- | --- |
| CM-T001 | Done | `docs/specs/SPEC-001-dynamic-contract-manager/*` | Requirements scope confirmed for v0.1 realtime path |
| CM-T002 | Done | `src/option_data_manager/api/app.py`, `tests/test_api_app.py` | Startup discovery bootstrap test |
| CM-T003 | Deferred | `src/option_data_manager/collection_state.py`, `tests/*` | Collection batch rematerialization is already handled by collection plan materialization; more precise stale-batch reset remains future work |
| CM-T004 | Done | `src/option_data_manager/quote_streamer.py`, `tests/test_quote_streamer.py` | Incremental Quote/Kline subscription reconciliation test |
| CM-T005 | Done | `src/option_data_manager/api/*`, `src/option_data_manager/webui/*`, `tests/*` | API/WebUI progress fields and health hint tests via API/quote-stream suite |
| CM-T006 | Done | `docs/tasks/*`, `docs/traceability/*`, `docs/governance/*` | Project memory updated |
