# Risk Register

| Risk | Impact | Mitigation | Status |
| --- | --- | --- | --- |
| TQSDK live coverage or IV availability varies by product | Incomplete acceptance | Record source unavailable by field group and capture live evidence | Open |
| Full-market subscriptions overload source/API | Slow or unstable collection | Bounded shards, configurable batch sizes, resume successful shards | Open |
| Secrets leak through UI/API/test evidence | Security failure | DPAPI storage, masked responses, ignored runtime DBs, tests | Open |
| Invalid runtime settings make local API/WebUI unusable | Local production outage | Allowlist editable settings and validate ports, booleans, and collection window sizes before saving | Mitigated |
| Enabling API Key auth blocks WebUI API calls | Operator lockout | WebUI stores an operator-provided API Key in browser localStorage and sends Authorization headers on API requests | Mitigated |
| Existing WebUI baseline may lack approved prototype details | UX mismatch | Preserve current layout and only fill production behavior | Accepted |

