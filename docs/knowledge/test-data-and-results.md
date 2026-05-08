# Test Data And Results

## 2026-05-08

- `uv run pytest -q`: 9 passed.
- `uv run python -m compileall -q src tests option_data_manager`: passed.
- API smoke with in-memory SQLite: `/api/health`, `/api/status`, `/api/settings` returned 200.
- WebUI smoke with in-memory SQLite: `/`, `/api/settings`, `/api/webui/overview` returned 200.
- 2026-05-08 partial live TQSDK check: market discovery found 27,386 option symbols and 380 underlyings across DCE/CZCE/SHFE/INE/GFEX; a one-underlying collection window wrote 75 quote rows, 1,354 K-line rows, and 74 metrics rows with 6 acquisition errors. Full-market acceptance remains pending.
