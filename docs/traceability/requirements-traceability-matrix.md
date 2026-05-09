# Requirements Traceability Matrix

| Requirement | Implementation Area | Tests/Evidence |
| --- | --- | --- |
| Current commodity option slice | collection, repositories, SQLite | unit tests, live collection report |
| WebUI overview and T-Quote | webui read model/app | WebUI smoke tests, local smoke command |
| Local API integration | api app | API tests, local smoke command |
| Credential safety | settings, API responses, reports, service logs | unit/API tests, safe log-context review |
| Resumable shards | collection_state, market_collector | unit tests, live evidence |
| Local diagnostics/logging | service_state, api app, webui read model/app | service log repository smoke, compileall, API/WebUI tests once dependencies are available |
