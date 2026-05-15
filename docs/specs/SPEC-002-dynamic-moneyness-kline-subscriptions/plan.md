# Plan

## Implementation Order

1. Add moneyness domain helpers and focused unit tests.
2. Add Session Manager helpers for TQSDK `trading_time` normalization, product/contract profile caching, and `in_session/out_of_session/unknown` evaluation.
3. Add progress/logging contracts before worker integration, so observability is designed into the feature rather than bolted on later.
4. Extend quote-stream settings, CLI arguments, and API settings payloads.
5. Extend Quote/Kline selection so Quote remains broad and Kline applies the moneyness filter.
6. Extend runtime reconciliation so moneyness recalculation adds newly matching Kline refs without releasing old Kline refs intraday.
7. Extend worker progress reports and API aggregation with current match count, sticky subscription count, skip reasons, and recalculation counters.
8. Extend WebUI settings, overview underlying rows, and T型报价 payload/display with moneyness and trading-session state.
9. Run targeted tests, full suite if time permits, compileall, and agentic-SDLC check.
10. Update acceptance evidence, traceability, risk, progress, and handoff records.
11. Run or schedule a representative trial window and inspect operational counters before final acceptance.

## Dependencies

- Existing `quote_streamer.py` subscription selection and `_reconcile_subscriptions` paths.
- Existing SQLite `instruments` and `quote_current` current-slice tables.
- TQSDK Quote fields: `trading_time`, `datetime`, `last_price`, `option_class`, `strike_price`, `underlying_symbol` via persisted instruments and live Quote refs.

## Risks And Mitigations

- Product-level session errors can create wrong active/inactive displays. Mitigate with contract/product-granular tests and avoid exchange-level inference.
- Missing underlying price can cause false Kline additions. Mitigate by skipping additions and surfacing diagnostics.
- Sticky growth can still increase Kline count during strong trend days. Mitigate by showing current match count versus cumulative subscribed count and preserving contract-month scope controls.
- TQSDK `trading_time` format may vary. Mitigate with normalization tests and unknown-state fallback.
- Feature creep can create hard-to-maintain realtime-worker code. Mitigate by keeping pure helpers in separate modules and limiting worker changes to orchestration and diff application.
- Hidden operational regressions may appear only after a live trading window. Mitigate with aggregate logs/progress counters and a required trial-run inspection before final acceptance.

## Rollback

- Keep default moneyness ranges as all selected so current behavior is preserved.
- If the feature causes runtime instability, disable Kline filtering by setting all ranges selected and requiring workers to restart.
- Session display can degrade to `unknown` without blocking Quote collection.

## Verification

- `uv run pytest -s -q tests/test_quote_streamer.py`
- `uv run pytest -s -q tests/test_api_app.py tests/test_webui_read_model.py tests/test_webui_app.py`
- `uv run python -m compileall -q src tests option_data_manager`
- `bash scripts/agentic-sdlc/check-agentic-sdlc.sh`
- Optional live smoke after local TQSDK runtime is available: start realtime workers with a small symbol cap and inspect moneyness/session progress.
- Trial-run checklist: inspect service logs, worker reports, realtime health, metrics-worker report, WebUI responsiveness, and SQLite busy/lock indicators.
