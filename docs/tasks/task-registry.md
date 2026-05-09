# Task Registry

| Task ID | Status | Owner | Summary | Verification |
| --- | --- | --- | --- | --- |
| REL-001 | Done | Codex | Restore package/import/runtime metadata and CLI entrypoints | `uv run python -c "import option_data_manager"`; `uv run odm-collect --help` |
| REL-002 | Done | Codex | Initialize agentic-sdlc project governance | `scripts/agentic-sdlc/check-agentic-sdlc.ps1 -Root .` |
| REL-003 | Done | Codex | Consolidate local API, settings, API keys, and refresh endpoint | pytest API tests |
| REL-004 | Done | Codex | Preserve WebUI baseline and wire real settings/API information | WebUI TestClient smoke |
| REL-005 | Done | Codex | Add focused unit/API tests and sample database fixtures | `uv run pytest -q` |
| REL-006 | Done | Codex | Run bounded live TQSDK collection windows and tune batch thresholds | Background refresh worker; `odm-collect --max-batches` live reports; WebUI batch-progress read model; final tuned parallel full-market evidence |
| REL-007 | Done | Codex | Move local development target to WSL2 Ubuntu and align cloud Linux runtime setup | WSL setup script; Linux secret protector; pytest |
| REL-008 | Done | Codex | Add persistent local service logs and WebUI diagnostics for API/settings/refresh events | `python -m compileall -q src tests`; service log repository smoke |
| REL-009 | Done | Codex | Add repeatable local API/WebUI smoke command and rerun release-gate checks | `uv run pytest -q`; `uv run python scripts/smoke-local-app.py --database data/tmp-smoke/script-smoke.sqlite3`; server smoke checks |
| REL-010 | Done | Codex | Fix `odm-test-tqsdk` process environment credential resolution and record proxy-blocked live TQSDK attempt | `uv run pytest tests/test_tqsdk_cli.py -q`; transient-env `odm-test-tqsdk`; bounded `odm-collect` smoke attempt |
| REL-011 | Done | Codex | Tune full-market TQSDK subscription throughput and implement process-level shards plus quote-only stream worker | Local perf evidence in ignored `docs/qa/live-evidence/perf-tuning/`; `odm-collect-parallel`; `odm-quote-stream`; `uv run pytest -q` |
| REL-012 | Done | Codex | Final readiness pass: complete full-market tuned coverage, verify WebUI/API display with real runtime data, and record signoff evidence | `odm-collect-parallel --until-complete` completed 864/864 batches; real DB WebUI/API smoke passed; `uv run pytest -q`; compileall; agentic-SDLC check |
| REL-013 | Done | Codex | Add WebUI/API controls for quote-only realtime stream workers | `/api/quote-stream` start/stop/status; WebUI settings controls; `uv run pytest -q` passed 41 tests; compileall; real runtime quote-stream control smoke |
| REL-014 | Done | Codex | Local acceptance UI/API polish: settings/overview table ergonomics, API Key copy/delete/full fingerprint, and TQSDK date-field API exposure | `uv run pytest tests/test_webui_read_model.py tests/test_api_app.py tests/test_quote_streamer.py -q`; compileall; local 8766 smoke |
| WEBUI-001 | Done | Codex | Fix T型报价 selectors, Chinese exchange labels, and remaining expiry days display | `uv run pytest -q` passed 46 tests; `uv run python -m compileall -q src tests`; agentic-SDLC check |
