# Acceptance

Implemented for the v0.1 realtime path on 2026-05-11.

- Startup bootstrap: `/api/quote-stream/start` performs contract discovery before spawning workers, even when active contracts already exist, and blocks with a clear message if discovery fails or returns zero active options.
- Runtime refresh: `odm-quote-stream` supports periodic contract refresh. Worker 0 performs TQSDK discovery; all workers periodically reconcile their shard's in-memory Quote/Kline references against the latest SQLite active universe.
- Incremental reconciliation: tests verify unchanged references are kept, newly active contracts are subscribed, and inactive contracts are removed from local reference maps without rebuilding all subscriptions.
- Verification:
  - `uv run python -m compileall -q src tests`: passed.
  - `uv run pytest -s tests/test_quote_streamer.py tests/test_api_app.py tests/test_realtime_health.py -q`: 29 passed.
  - `uv run pytest -s -q`: 77 passed.
