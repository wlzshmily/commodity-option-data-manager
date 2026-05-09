# Requirements Traceability Matrix

| Requirement | Implementation Area | Tests/Evidence |
| --- | --- | --- |
| Current commodity option slice | collection, repositories, SQLite | unit tests, live collection report |
| WebUI overview and T-Quote | webui read model/app | WebUI smoke tests |
| Local API integration | api app | API tests |
| Runtime settings safety | api app settings validation | API validation tests |
| WebUI API Key operation | webui app JavaScript/localStorage Authorization headers | WebUI code review, API auth tests |
| Credential safety | settings, API responses, reports | unit/API tests |
| Resumable shards | collection_state, market_collector | unit tests, live evidence |

