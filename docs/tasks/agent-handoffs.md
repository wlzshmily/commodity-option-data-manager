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
- Follow-up verification: dependency recovery succeeded; `uv run pytest -q` now passes 30 tests, API/WebUI factory smoke passes via `scripts/smoke-local-app.py`, and bound localhost server smoke passed for `odm-api`/`odm-webui`.
- Remaining blocker: live full-market completion still requires configured local TQSDK credentials and a completed evidence window.
- TQSDK follow-up: transient credentials were supplied only through environment variables; `odm-test-tqsdk` env resolution bug was fixed, then both connection check and bounded collection reached outbound proxy denial to `auth.shinnytech.com` before authentication could complete.
- Local tuning follow-up: Windows local TQSDK login passed with encrypted SQLite credentials. Short-window collector `wait_update` deadline was corrected to `time.time() + 1` and covered by unit test. Perf evidence under ignored `docs/qa/live-evidence/perf-tuning/` favors `option_batch_size=40`, `wait_cycles=1`; quote-only realtime shards are much cheaper than full K-line/metrics refresh; 2-4 independent TQSDK worker processes improve throughput with sublinear scaling. `uv run pytest -q` passes 31 tests.
- Implementation follow-up: `odm-collect-parallel` now runs disjoint underlying ranges across independent TQSDK API processes and per-worker collection scopes. `odm-quote-stream` now runs bounded quote-only subscription shards for realtime UI freshness. Both commands write secret-safe reports only.
- Final acceptance follow-up: local direct TQSDK full-market catch-up completed through `odm-collect-parallel` with 4 workers, 6 waves, 864/864 batches successful, 0 failed, 27,386 active options, 565,000 K-line rows, and 27,386 metrics rows. Evidence is under ignored `docs/qa/live-evidence/final-parallel-catchup/summary.json`; no credential values were written.
- Final verification follow-up: `uv run pytest -q` passed 41 tests, compileall passed, `scripts\agentic-sdlc\check-agentic-sdlc.ps1 -Root .` passed, and real runtime API/WebUI TestClient smoke passed with collection completion `1.0`.
- Quote stream controls follow-up: `/api/quote-stream` status/start/stop and WebUI settings controls are implemented. Tests verify workers start/stop through subprocess orchestration and that passwords are not passed on the command line. Live control smoke started and stopped one 10-symbol worker and left no running worker process.
- Local acceptance UI/API polish follow-up: settings and overview page review issues were addressed locally, including message tone classes, disabled worker controls, fixed table widths, Chinese product labels, API Key copy/delete/full fingerprint display, and no-message hidden states. TQSDK same-name date fields (`expire_datetime`, `last_exercise_datetime`, `delivery_year/month`, `exercise_year/month`) are now persisted/backfilled from quote subscriptions where available and exposed through API responses; derived remaining-day fields are explicitly named as derived. Current local DB rows may show `null` until a real quote subscription provides those TQSDK fields.
- Completed WEBUI-001 / CR-2026-05-09-WEBUI-TQUOTE.
- T型报价 selector changes now choose the first valid row for changed exchange/product and the exact row for changed expiry, then reload the quote chain.
- Remaining expiry days are derived from saved option `expire_datetime` values in the WebUI read model and shown in the overview table and T型报价 toolbar.
- Verification passed: `uv run pytest -q` passed 46 tests; `uv run python -m compileall -q src tests`; `scripts\agentic-sdlc\check-agentic-sdlc.ps1 -Root .`.
