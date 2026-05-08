# Architecture Decisions

- Use SQLite as the v0.1 current-slice store.
- Use FastAPI for local WebUI JSON endpoints and local integration API.
- Use bounded shards for full-market collection to control TQSDK/API pressure.
- Keep API Key enforcement optional by default because the service is localhost-first.

