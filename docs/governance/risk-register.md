# Risk Register

| Risk | Impact | Mitigation | Status |
| --- | --- | --- | --- |
| TQSDK live coverage or IV availability varies by product | Incomplete acceptance | Record source unavailable by field group and capture live evidence; do not fail batches when Quote rows are current but Greeks/IV are unavailable | Open |
| Full-market subscriptions overload source/API | Slow or unstable collection | Bounded shards, configurable batch sizes, retry TQSDK startup, batch quote subscriptions, resume successful/interrupted shards | Open |
| Secrets leak through UI/API/test evidence | Security failure | DPAPI storage, masked responses, ignored runtime DBs, tests | Open |
| Existing WebUI baseline may lack approved prototype details | UX mismatch | Preserve current layout and only fill production behavior | Accepted |
