# Progress Board

## In Progress

- None

## Pending

- None

## Done

- Requirements reviewed and implementation plan approved.
- Package structure and `pyproject.toml`.
- Governance baseline.
- Unified local API and API Key management.
- WebUI mounted local API settings/key/refresh endpoints.
- Test harness and smoke validation.
- `odm-collect --max-batches` bounded window control.
- WebUI/API collection batch progress visibility.
- Background refresh worker for continuous bounded full-market collection.
- Metric-only Greeks/IV gaps are treated as visible quality gaps instead of batch blockers.
- Overview time columns now distinguish exchange market time from collector update time.
- Background collection wait cycles are configurable and default to one wait round.
- WSL2 Ubuntu setup, documentation, and Linux secret protection.
- Persistent service logs and WebUI diagnostics for API/settings/background refresh events.
- Repeatable local API/WebUI smoke command and passing 28-test suite after dependency recovery.
- `odm-test-tqsdk` process environment credential resolution fixed; live TQSDK validation reached runtime proxy denial without printing credentials.
- Local TQSDK login passed on Windows with encrypted SQLite credentials.
- Full-market performance tuning sample captured: `option_batch_size=40`, `wait_cycles=1` is the current single-process candidate; 2-4 independent TQSDK worker processes improve throughput with sublinear scaling.
- Quote-only probe confirmed realtime Quote refresh is much cheaper than full Quote/K-line/metrics collection and should be split into bounded long-lived subscription shards.
- Process-level full-market shard command implemented as `odm-collect-parallel`.
- Quote-only long-lived subscription command implemented as `odm-quote-stream`.
- WebUI read model now surfaces latest parallel collection progress while API background refresh can still use the routine scope.
- Stale interrupted acquisition runs can be marked failed so diagnostics do not show false long-running jobs.
- Bounded live full-market acceptance windows completed with tuned 4-process shards.
- Final full-market completion report recorded under ignored `docs/qa/live-evidence/final-parallel-catchup/summary.json`.
- Data collection readiness reached: 864/864 full-market batches succeeded, 0 failed, 27,386 active options covered with Quote rows, K-line rows, and metrics rows.
- Final verification rerun passed: `uv run pytest -q` passed 41 tests, compileall, agentic-SDLC check, and real runtime API/WebUI smoke.
- WebUI/API quote stream controls delivered: operators can start, stop, and inspect quote-only worker shards from the settings page through `/api/quote-stream`.
- Live quote stream control smoke passed and left no running worker process.
