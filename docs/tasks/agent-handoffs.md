# Agent Handoffs

## 2026-05-08

- Active work: implement approved release plan.
- Important decisions: five commodity exchanges are in scope; WebUI baseline is existing source; API Key enforcement is optional by default.
- Safety note: never expose real TQSDK credentials or full API keys in reports, logs, screenshots, or fixtures.

## 2026-05-09

- Active work: CR-2026-05-09-LOGS implemented to improve local observability.
- Delivered: persistent `service_logs` table, `/api/logs`, WebUI 日志与诊断页, and safe event logging for settings/API/security/background refresh flows.
- Verification: `python -m compileall -q src tests` and service-log repository smoke passed; full `uv run pytest -q` could not recreate dependencies because PyPI scipy download failed through the tunnel.
- Safety note: service-log contexts intentionally record booleans, setting keys, event categories, and key metadata only; do not add raw TQSDK passwords or full API keys.
- Follow-up verification: dependency recovery succeeded; `uv run pytest -q` now passes 28 tests, API/WebUI factory smoke passes via `scripts/smoke-local-app.py`, and bound localhost server smoke passed for `odm-api`/`odm-webui`.
- Remaining blocker: live full-market completion still requires configured local TQSDK credentials and a completed evidence window.
