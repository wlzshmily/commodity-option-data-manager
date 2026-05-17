# Release Plan

## Release Candidate Criteria

- `uv run pytest` passes.
- `odm-api` and `odm-webui` start locally.
- API docs are reachable at `/docs`.
- A sample SQLite fixture demonstrates UI/API behavior.
- Live TQSDK collection evidence is captured without secrets.
- Full-market tuned catch-up evidence reaches all active shards or records explicit source/API blockers. Current local evidence: 864/864 batches succeeded, 0 failed.
- WebUI/API readiness smoke against the real runtime database shows collection progress, exchange coverage, T-Quote, runs, settings, and logs endpoints.

## Local Smoke Command

- `uv run python scripts/smoke-local-app.py --database data/tmp-smoke/script-smoke.sqlite3` verifies core API/WebUI endpoints without contacting TQSDK.
- `uv run odm-quote-stream --database data/option-data-current.sqlite3 --no-discover --max-symbols 10 --cycles 1 --quote-shard-size 10` verifies realtime Quote subscription wiring without exposing secrets.
- `uv run odm-collect-parallel --database data/option-data-current.sqlite3 --workers 2 --max-batches-per-worker 1 --option-batch-size 40 --wait-cycles 1 --no-discover` verifies process-level TQSDK shard wiring without exposing secrets.
- WebUI/API quote stream controls can be smoke-tested without live TQSDK by monkeypatched API tests; live operation uses the same `odm-quote-stream` CLI path and writes runtime reports under ignored `docs/qa/live-evidence/quote-stream-runtime/`.

## Server Deployment

- Linux server deployment is documented in `docs/operations/linux-server-deployment.md`.
- Deploy explicit GitHub commit SHAs into `/opt/option-data-manager/releases/<short-sha>`, switch `/opt/option-data-manager/current`, and keep runtime SQLite/report state under `/opt/option-data-manager/shared/`.
- Run deployment verification before switching traffic: `pytest -q`, compileall, `scripts/smoke-local-app.py`, and `scripts/agentic-sdlc/check-agentic-sdlc.sh`.
- Before switching a server release, safely stop quote-stream, metrics-worker, contract-manager, and `odm-webui`; verify no old runtime process or unexpected SQLite holder remains.
- After a service restart, restart contract manager, background refresh, realtime subscriptions, and the metrics worker path if operators expect collection to continue immediately.
- A server deployment cannot be recorded as complete until the data-readiness gate passes or the deployment log explicitly marks the remaining current-slice gaps as degraded work. The gate must check for stale `running` batches, pending/failed/running current-slice batches, alive quote/metrics workers, and unexplained K-line/IV/Greeks gaps in the subscribed scope.
- Public `8765/tcp` binding is for temporary test servers only; production access should use SSH tunneling, VPN, Cloudflare Access, or an authenticated HTTPS reverse proxy.

## Acceptance Evidence

- Ubuntu test server update on 2026-05-16: `39.103.49.231` runs GitHub commit `c54847bfe4c6d73c15d87e1a42a9eb74c0b0cd53` from `/opt/option-data-manager/releases/c54847bfe4c6`; server-side verification passed 127 tests, compileall, smoke-local-app, and agentic-SDLC check; systemd `odm-webui` is active on `0.0.0.0:8765`, and public smoke for core UI/API/static endpoints returned 200.
- 2026-05-17 deployment safety correction: OPS-004 was service-smoke complete but later operator inspection found routine current-slice work incomplete after `DCE.pp2606`, one stale `running` batch, and a previously failed metrics worker. OPS-005 hardens the runbook and deployment log so future deployments require safe old-runtime quiescence plus data-readiness gates before being marked complete.
- 2026-05-17 code-level operations monitoring: OPS-006 adds `/api/status` and WebUI overview alerts for stale running batches, incomplete current-slice work, dead/failed metrics workers, and subscription-complete rows with missing K-line/metrics current-slice data, so operators do not need to discover those problems manually from table gaps.
- Final tuned full-market catch-up: `docs/qa/live-evidence/final-parallel-catchup/summary.json` with 4 worker shards, 6 waves, 27,386 active options, 864/864 successful batches, 0 failed batches, 4,325.332 seconds, and 6.332 options/second.
- Real runtime display readiness: WebUI read model reported `completion_ratio=1.0`, `failed_batches=0`, `stale_batches=0`, K-line coverage `1.0`, IV coverage `0.9013`, Greeks coverage `0.8959`, and price-field quote coverage `0.8553`.
- Source quality note: all active options have Quote, K-line, and metrics rows. Remaining IV/Greeks and price-field gaps are retained as visible source-quality diagnostics.
- Operator control evidence: WebUI settings now exposes quote-only worker start/stop controls backed by `/api/quote-stream`, with worker processes launched as independent Python module commands that do not include credential values. A live 10-symbol control smoke started and stopped one worker successfully.

## Rollback

- Stop local services.
- Restore the previous ignored runtime SQLite database backup if needed.
- Keep source changes separate from runtime data.
- Server rollback points `/opt/option-data-manager/current` back to a previous release directory and restarts `odm-webui`; shared SQLite data is preserved unless a separate database restore is explicitly required.

