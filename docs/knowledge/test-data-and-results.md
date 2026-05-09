# Test Data And Results

## 2026-05-08

- `uv run pytest -q`: 16 passed.
- `uv run python -m compileall -q src tests option_data_manager`: passed.
- API smoke with in-memory SQLite: `/api/health`, `/api/status`, `/api/settings` returned 200.
- WebUI smoke with in-memory SQLite: `/`, `/api/settings`, `/api/webui/overview` returned 200.
- WebUI read-model test verifies collection progress totals and recent failed batch reporting.
- WebUI read-model test verifies market time is exposed separately from local collector `received_at`.
- Market collector test verifies metric-only Greeks/IV source gaps do not block batch success when current Quote rows are written.
- CLI status test verifies command status is based on durable batch state rather than nonblocking metric warnings.
- Settings tests verify WSL/Linux Fernet file-key secret protection round trips and reuses existing keys.
- WSL smoke verifies `scripts/agentic-sdlc/check-agentic-sdlc.sh`; setup guard refuses Windows-mounted `/mnt/c` paths.
- WSL live TQSDK connection test passed with Python 3.11.15. Python 3.14 produced HTTPS `SSLEOFError` against ShinnyTech auth endpoints and is excluded for runtime.
- 2026-05-08 partial live TQSDK check: market discovery found 27,386 option symbols and 380 underlyings across DCE/CZCE/SHFE/INE/GFEX; a one-underlying collection window wrote quote, K-line, and metrics rows with partial source errors. Full-market acceptance remains pending.
- 2026-05-08 bounded window check: `odm-collect --max-underlyings 1 --max-batches 1 --option-batch-size 5 --wait-cycles 1` completed in a bounded command window and wrote a partial-failure report without secrets.
- 2026-05-09 full-market planning smoke: `odm-collect --max-underlyings 1000000 --max-batches 3 --option-batch-size 20 --wait-cycles 1` materialized 380 underlyings, 27,386 options, and 1,544 active batches; selected batches completed successfully while source quality gaps remained visible.
