# Operations Deployment Log

This log records server deployment incidents, operating decisions, and hard
gates that must be carried into future deployments. It is intentionally
secret-safe: do not paste real TQSDK passwords, raw API keys, or live credential
evidence here.

## 2026-05-17 OPS-005 Deployment Safety Gate

Server: `39.103.49.231`

Related release: `0780618709561ccd864fd9b1401d313cf9b31ddb`

Operator finding:

- The WebUI and realtime subscription workers were reachable after deployment,
  and realtime subscription progress later reached 100%.
- Starting at `DCE.pp2606`, K-line and IV/Greeks coverage showed real gaps.
- SQLite inspection showed Quote rows existed for those underlyings, but
  `kline_20d_current` and `option_source_metrics_current` had zero rows for the
  affected range.
- `collection_plan_batches` still had incomplete current-slice work:
  `715/1544` active batches were successful, `828` were pending, and one
  `DCE.pg2610` batch had been stuck in `running` since
  `2026-05-10T17:46:44Z`.
- The metrics worker had failed earlier with
  `sqlite3.OperationalError: database is locked`.

Root cause classification:

- Not a pure code-copy or systemd restart failure.
- The unsafe part was the deployment completion gate: the deployment was marked
  done after service smoke and worker restart, without proving that old runtime
  work had quiesced, stale collection batches were reset or completed, the
  metrics worker remained alive, and the current-slice data coverage matched the
  subscribed realtime scope.
- Realtime subscription completion is only a worker-object lifecycle signal. It
  must not be used as proof that K-line rows or IV/Greeks rows have been written
  into SQLite.

Mandatory rule from this incident:

- A server deployment is not complete while any old quote-stream or metrics
  worker remains, any stale active collection batch is stuck in `running`, any
  active current-slice batch is still pending/failed/running without an explicit
  degradation note, the metrics worker is not running after realtime startup, or
  subscribed underlyings show K-line/IV/Greeks database gaps that are not
  acknowledged as source-quality gaps.

Safe deployment sequence now required:

1. Prepare and verify the new release directory before switching `current`.
2. Ask the running service to stop `quote-stream` and `contract-manager`.
3. Stop `odm-webui` through systemd.
4. Confirm no old `odm-webui`, `quote_stream`, or `metrics_worker` process is
   still alive.
5. Confirm no unexpected SQLite holder remains before switching the release.
6. Switch `/opt/option-data-manager/current` and start `odm-webui`.
7. Start contract manager, confirm it is healthy and has active options.
8. Resume current-slice refresh and realtime subscription.
9. Confirm `quote-stream` and `metrics-worker` are alive after realtime startup.
10. Run the data-readiness gate before marking deployment done:
    active batch work is complete or explicitly degraded, stale running batches
    are absent, and subscribed visible underlyings do not show unexplained
    K-line/IV/Greeks gaps.

Operational consequence:

- Future deployment notes must record the stop/quiesce evidence and the
  data-readiness gate outcome, not only HTTP 200 smoke results.

## 2026-05-17 OPS-006 Code-Level Monitoring Follow-Up

The deployment safety gate is now backed by runtime monitoring in code:

- `/api/status` includes an `operations` object with alert severity, counts, and
  samples.
- WebUI overview renders an operational alert banner when the monitor reports a
  warning or critical condition.
- Collection progress includes stale active `running` batch samples.
- Quote-stream status includes metrics-worker status so the API can tell when
  realtime subscription is alive but IV/Greeks refresh is not.
- Alerts cover stale running batches, incomplete current-slice work, metrics
  worker not-running/failed states, and subscription-complete rows with missing
  K-line or metrics current-slice rows.

Verification:

- `uv run python -m pytest -s -q` passed 141 tests.
- `uv run python -m compileall -q src tests option_data_manager` passed.
- `bash scripts/agentic-sdlc/check-agentic-sdlc.sh` passed.
