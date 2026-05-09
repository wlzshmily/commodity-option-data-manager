# Requirements Traceability Matrix

| Requirement | Implementation Area | Tests/Evidence |
| --- | --- | --- |
| Current commodity option slice | collection, repositories, SQLite | unit tests, live collection reports, perf tuning evidence |
| WebUI overview and T-Quote | webui read model/app | WebUI smoke tests, local smoke command |
| Local API integration | api app | API tests, local smoke command |
| Credential safety | settings, API responses, reports, service logs, TQSDK CLI env handling | unit/API tests, safe log-context review, transient-env TQSDK attempts |
| Resumable shards | collection_state, market_collector | unit tests, live evidence, process-level shard tuning sample |
| TQSDK live reference update model | chain_collector, continuous_collector | `wait_update` absolute deadline unit test, live subscription tuning evidence |
| Tuned full-market catch-up | collect_market_parallel, collection_plan right-bound shards | shard-boundary unit tests, parallel CLI smoke/evidence, final local run 864/864 batches succeeded |
| Realtime Quote subscriptions | quote_streamer, quote_stream CLI | quote-stream unit tests, quote-only live smoke/evidence |
| Realtime Quote operator controls | api app, webui app, service state/logs | `/api/quote-stream` API tests, WebUI settings controls, compileall |
| Local diagnostics/logging | service_state, api app, webui read model/app | service log repository smoke, compileall, API/WebUI tests once dependencies are available |
