# Acceptance

Implementation evidence captured on 2026-05-14:

- `uv run pytest -s -q tests/test_moneyness.py tests/test_trading_sessions.py tests/test_quote_streamer.py tests/test_api_app.py tests/test_webui_read_model.py`: passed, 60 tests.
- `uv run python -m compileall -q src tests option_data_manager`: passed.
- `uv run pytest -s -q`: passed, 112 tests.
- `bash scripts/agentic-sdlc/check-agentic-sdlc.sh`: passed.
- Follow-up correction: `uv run pytest -s -q tests/test_trading_sessions.py tests/test_webui_read_model.py tests/test_quote_streamer.py`: passed, 45 tests.
- No-Quote-time state correction: `uv run pytest -s -q tests/test_webui_read_model.py tests/test_trading_sessions.py tests/test_quote_streamer.py`: passed, 48 tests.

Accepted implementation behavior:

- Quote subscriptions remain broad within the configured contract-month and remaining-day scope.
- Kline subscriptions can be filtered by selected `otm/atm/itm` ranges while default all-range behavior preserves the prior broad Kline scope.
- Running workers recalculate moneyness on the configured interval and add newly matching Kline symbols without releasing still-active sticky Kline symbols intraday.
- Running moneyness expansion skips chains whose own underlying `trading_time` profile says the contract is out of session.
- If the underlying Quote source time is fresh, that live Quote progress is authoritative for the trading-session state; theoretical `trading_time` ranges are fallback data when Quote time is stale or absent.
- Before a row is fully subscribed, overview shows `待行情`; after it is `已订阅`, a missing underlying Quote time is treated as `休市`.
- Worker progress/API/WebUI expose current moneyness matches, sticky Kline count, added Kline count, last recalculation time, and aggregate skip reasons.
- Overview underlying rows and T型报价 expose the selected underlying's trading-session state.

Remaining operational acceptance:

- Longer soak monitoring is still prudent, but the representative live market-session trial has now been run successfully on the new timeout-enabled worker path. The live session showed advancing `current_kline_symbol` values, no `kline_subscription_timeout_count` growth, and no repeat of the earlier freeze pattern.
- Browser feedback found live SHFE night-session rows showing `休市` even while Quote time was advancing. The corrected implementation now reports fresh SHFE ad/ag/al Quote rows as `交易中` and keeps stale/non-night AP as `休市`.
