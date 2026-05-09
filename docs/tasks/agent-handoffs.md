# Agent Handoffs

## 2026-05-08

- Active work: implement approved release plan.
- Important decisions: five commodity exchanges are in scope; WebUI baseline is existing source; API Key enforcement is optional by default.
- Safety note: never expose real TQSDK credentials or full API keys in reports, logs, screenshots, or fixtures.
- 2026-05-08 update: runtime settings are now validated through an allowlisted `/api/settings/{key}` path, and the WebUI has an operator API Key field that stores only in browser localStorage for authenticated API calls.

