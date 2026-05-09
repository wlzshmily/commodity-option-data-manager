# Change Requests

| Change ID | Status | Summary | Traceability |
| --- | --- | --- | --- |
| CR-2026-05-09-LOGS | Approved/Implemented | Add local system logging and WebUI diagnostics so API, settings, credential-test, and background refresh events are visible without exposing secrets. | REL-008; WebUI/API observability |
| CR-2026-05-09-SMOKE | Approved/Implemented | Add a repeatable local API/WebUI smoke command and refresh release-gate verification evidence after dependency recovery. | REL-009; release candidate verification |
| CR-2026-05-09-TQSDK-ENV | Approved/Implemented | Fix standalone TQSDK CLI environment credential resolution and record live TQSDK proxy-blocked validation attempt without storing secrets. | REL-010; credential/connectivity verification |
| CR-2026-05-09-PERF-SHARDS | Approved/Implemented | Tune TQSDK full-market throughput, add process-level catch-up shards, add quote-only stream workers, and surface parallel collection progress in WebUI read models. | REL-011; REL-012; full-market acceptance |
| CR-2026-05-09-QUOTE-CONTROLS | Approved/Implemented | Add local API and WebUI controls for starting, stopping, and observing quote-only stream worker shards without exposing credentials. | REL-013; realtime Quote operations |
| CR-2026-05-09-WEBUI-TQUOTE | Approved/Implemented | Fix T型报价 exchange/product/month switching, localize overview exchange display, and expose remaining expiry days in overview/T型报价. | WEBUI-001; WebUI selector/localization/expiry-days change; `uv run pytest -q` passed 46 tests |
