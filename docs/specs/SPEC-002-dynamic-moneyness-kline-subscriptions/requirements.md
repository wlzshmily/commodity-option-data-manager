# Requirements

## User Goal

As the operator of the local option data manager, I need realtime Kline subscriptions to follow strategy-relevant option moneyness ranges during the trading day, so strategies that only need out-of-the-money, at-the-money, or in-the-money options can reduce heavy Kline subscription load while keeping Quote data complete.

The same feature should make the WebUI friendlier by showing whether each underlying contract or selected option chain is currently in its trading session.

## Confirmed Business Rules

- Contract-universe management and moneyness filtering are separate concerns.
- The contract manager keeps the full candidate contract list current at low frequency. Contract additions/removals are expected after settlement or before the next trading day/session, not as a tick-by-tick intraday concern.
- Moneyness changes are intraday and must be recalculated from market movement.
- Realtime Quote subscriptions remain broad for the selected contract-month scope because Quote objects are comparatively cheap and are needed for complete T型报价 display and moneyness evaluation.
- Realtime Kline subscriptions are the expensive resource and are filtered by the operator-selected moneyness range.
- Moneyness recalculation runs on a configurable interval, defaulting to 30 seconds.
- During a trading day, Kline subscriptions use sticky growth: newly matching contracts are added, but contracts that later move out of the selected moneyness range are not released intraday.
- Kline releases for moneyness filtering happen only when the realtime workers restart, the operator changes subscription settings and restarts, or the contract manager rebuilds the candidate universe for a new session.
- Trading-session logic must be maintained at contract/product granularity. Exchange-level assumptions are not safe because products on the same exchange can have different night-session rules, such as CZCE glass/soda ash versus apple.
- A chain's moneyness must be calculated using that chain's own underlying future price. Cross-product or exchange-level activity may never replace the underlying price.

## Functional Requirements

- `MM-001`: The system shall provide runtime settings for selected moneyness ranges: out-of-the-money, at-the-money, and in-the-money. Multiple ranges may be selected; the default shall preserve the current broad Kline behavior.
- `MM-002`: Realtime Quote selection shall remain broad within the existing contract-month and remaining-day scope, independent of the selected moneyness ranges.
- `MM-003`: Realtime Kline selection shall initially include only options matching the selected moneyness ranges plus the required underlying future Kline symbols.
- `MM-004`: During realtime operation, workers shall recalculate option moneyness on a configurable interval, default 30 seconds, and incrementally subscribe newly matching Kline symbols without releasing previously subscribed in-session symbols.
- `MM-005`: Moneyness classification shall be calculated from the option's `option_class`, `strike_price`, and latest available Quote price for its own `underlying_symbol`.
- `MM-006`: At-the-money classification shall be chain-relative: for each underlying and option side, the ATM strike is the strike nearest the underlying price; ties should follow the TQSDK convention by choosing the out-of-the-money side when possible.
- `MM-007`: If the underlying price is missing or too stale, the worker shall skip new Kline additions for that chain and expose a readable "waiting for underlying price" diagnostic instead of using another product's price.
- `MM-008`: The system shall provide a reusable trading-session manager that derives and caches trading-session profiles at contract/product granularity from TQSDK Quote `trading_time` where available.
- `MM-009`: The trading-session manager shall determine whether a chain is theoretically in session using that chain's underlying contract/product profile, including day and night sessions.
- `MM-010`: Moneyness recalculation shall be skipped for chains that are not currently in their own trading session unless forced by startup or operator action.
- `MM-011`: The WebUI underlying/contract list shall display whether each underlying contract is currently in trading time, outside trading time, or unknown due to missing session metadata.
- `MM-012`: The WebUI T型报价 view shall display the selected chain's trading-session state and current moneyness subscription state.
- `MM-013`: Realtime progress/status shall distinguish current moneyness matches from cumulative sticky Kline subscriptions, so operators can understand why subscribed Kline count may exceed the current matching count.
- `MM-014`: The implementation shall not store real TQSDK passwords, raw API keys, or live credential evidence in tracked files.
- `MM-015`: The feature shall be implemented as bounded domain helpers plus thin integration points, not as ad hoc branching scattered through the realtime worker.
- `MM-016`: The system shall log moneyness recalculation and Kline sticky expansion events with aggregate counts and reasons, without per-symbol log storms.
- `MM-017`: The implementation shall expose enough runtime counters for operations review after a trial period, including recalculation count, added Kline count, skipped-chain counts, current match count, cumulative sticky count, and last successful recalculation time.
- `MM-018`: The feature shall preserve existing Quote freshness, T型报价 display, metrics dirty-queue behavior, realtime start/stop responsiveness, and contract-manager health behavior.

## Acceptance Criteria

- Settings/API/WebUI expose a multi-select moneyness Kline filter with a default that preserves current behavior.
- Unit tests prove Quote selection is unchanged by moneyness settings while Kline selection is filtered.
- Unit tests prove CALL/PUT in-the-money, at-the-money, and out-of-the-money classification, including nearest-strike/tie behavior.
- A realtime worker test proves a newly matching option Kline is subscribed on a later recalculation interval without releasing an already subscribed Kline that moved out of range.
- Unit tests prove moneyness recalculation is skipped outside the underlying product's own trading session and does not use same-exchange/different-product activity as a proxy.
- WebUI/API tests show underlying rows and T型报价 payloads include trading-session state.
- Service logs or worker progress reports show aggregate moneyness recalculation events without writing noisy per-contract records.
- Regression tests cover that enabling the default all-range moneyness setting preserves existing Quote/Kline totals.
- Verification runs include targeted tests for quote-stream, API settings, WebUI read-model/app, compileall, and the agentic-SDLC checker.
- Acceptance includes a recommendation for a trial run and post-run inspection of realtime freshness, worker progress, SQLite write pressure, metrics queue behavior, and WebUI responsiveness.

## Non-Goals

- Trading, order placement, strategy scoring, alerting, or backtesting.
- Intraday contract-universe discovery beyond the existing contract manager.
- Server-side hard unsubscribe guarantees for TQSDK Kline references during sticky intraday operation.
- Exchange-level trading-session approximation for product-level decisions.
