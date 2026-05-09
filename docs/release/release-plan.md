# Release Plan

## Release Candidate Criteria

- `uv run pytest` passes.
- `odm-api` and `odm-webui` start locally.
- API docs are reachable at `/docs`.
- A sample SQLite fixture demonstrates UI/API behavior.
- Live TQSDK collection evidence is captured without secrets.

## Local Smoke Command

- `uv run python scripts/smoke-local-app.py --database data/tmp-smoke/script-smoke.sqlite3` verifies core API/WebUI endpoints without contacting TQSDK.

## Rollback

- Stop local services.
- Restore the previous ignored runtime SQLite database backup if needed.
- Keep source changes separate from runtime data.

