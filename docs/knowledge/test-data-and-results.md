# Test Data And Results

## 2026-05-08

- `uv run pytest -q`: 10 passed.
- `uv run python -m compileall -q src tests option_data_manager`: passed.
- API smoke with in-memory SQLite: `/api/health`, `/api/status`, `/api/settings` returned 200.
- WebUI smoke with in-memory SQLite: `/`, `/api/settings`, `/api/webui/overview` returned 200.
- 2026-05-08 partial live TQSDK check: market discovery found 27,386 option symbols and 380 underlyings across DCE/CZCE/SHFE/INE/GFEX; a one-underlying collection window wrote quote, K-line, and metrics rows with partial source errors. Full-market acceptance remains pending.
- 2026-05-08 bounded window check: `odm-collect --max-underlyings 1 --max-batches 1 --option-batch-size 5 --wait-cycles 1` completed in a bounded command window and wrote a partial-failure report without secrets.
