# Project Status

- Current phase: Implementation
- Current release: v0.1 local production release
- Coding allowed: yes, for the approved implementation plan in this thread
- Scope: DCE, CZCE, SHFE, INE, GFEX commodity options
- API auth decision: default localhost with optional API Key enforcement
- WebUI decision: preserve existing WebUI as the approved baseline
- Runtime environment decision: WSL2 Ubuntu is the primary local development and debugging environment.
- Python runtime decision: use Python 3.11 for WSL/cloud runtime; Python 3.14 caused TQSDK HTTPS failures.

## Current Blockers

- Full-market performance thresholds must be confirmed with real collection evidence.
- Latest `uv run pytest -q` retry could not recreate the local `.venv` because the PyPI tunnel failed while downloading `scipy` for the TQSDK dependency chain.

## Verification

- `uv run pytest -q`: previously passed, 25 tests; latest retry was blocked by PyPI tunnel failure while downloading scipy after the local `.venv` was recreated.
- `python -m compileall -q src tests`: passed after service-log changes.
- Service log repository smoke: passed.
- `scripts/agentic-sdlc/check-agentic-sdlc.ps1 -Root .`: passed.
- WSL2 Ubuntu setup path documented in `docs/operations/wsl2-ubuntu.md`; Linux SDLC checker added at `scripts/agentic-sdlc/check-agentic-sdlc.sh`.
- WSL setup guard verified: setup refuses `/mnt/c` Windows-mounted paths and requires a WSL-native clone.
- WSL live TQSDK connection test passed under Python 3.11.
- Standalone `odm-test-tqsdk` command added for credential/network checks outside the WebUI.
- TQSDK startup now retries transient login/TLS failures and reports non-zero collection exits to the WebUI status.
- Quote collection now prefers TQSDK `get_quote_list` batch subscription, and interrupted `running` batches are reset to `pending` on the next plan materialization.
- API factory smoke test: `/api/health`, `/api/status`, and `/api/settings` returned 200.
- WebUI factory smoke test: `/`, `/api/settings`, and `/api/webui/overview` returned 200.
- WebUI/API status now expose collection batch progress, including success, pending, failed, remaining, and recent failed batches.
- Local service logs now persist API/settings/security/background refresh events and are visible from WebUI diagnostics plus `/api/logs` without exposing TQSDK passwords or full API keys.
- WebUI overview now separates exchange market time from local collector update time and renders UTC timestamps in Asia/Hong_Kong.
- Metric-only Greeks/IV source gaps no longer block collection batch success when current Quote rows are written.
- Live bounded collection smoke: `uv run odm-collect --max-underlyings 1 --max-batches 1 --option-batch-size 5 --wait-cycles 1` completed and wrote a partial-failure report without secrets.
- Live full-market planning smoke: `uv run odm-collect --max-underlyings 1000000 --max-batches 3 --option-batch-size 20 --wait-cycles 1` materialized 380 underlyings, 27,386 options, and 1,544 active batches; the selected 3 batches completed successfully without secrets in the report.
- WSL WebUI background full-market refresh is running with 380 underlyings, 27,386 options, and 1,544 active batches discovered; progress has started without failed batches.

## Remaining Acceptance Work

- Let the background refresh worker continue bounded windows until all full-market batches are complete, then record coverage/performance evidence under ignored QA evidence paths.
