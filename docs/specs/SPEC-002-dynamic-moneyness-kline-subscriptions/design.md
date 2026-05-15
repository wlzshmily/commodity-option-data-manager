# Design

## Proposed Shape

Add two bounded modules:

- `Session Manager`: reusable trading-session profile and runtime-state helper.
- `Moneyness Kline Selector`: Quote-preserving, Kline-filtering subscription scope logic.

The existing realtime quote-stream worker remains the owner of TQSDK Quote/Kline references and SQLite progress reporting. The new modules should be called by worker selection/reconciliation code rather than creating a parallel subscription system.

## Architecture Guardrails

- Keep moneyness classification side-effect free. It should accept rows/prices and return classifications or target symbols.
- Keep trading-session evaluation side-effect free except for a small cache/persistence boundary if needed later.
- Keep realtime worker integration thin: read settings, call helpers, apply incremental diff, emit aggregate progress/log events.
- Do not mix contract-universe refresh, moneyness filtering, session scheduling, metrics refresh, and WebUI display logic in one function.
- Prefer one SQLite query per recalculation scope over per-symbol reads.
- Prefer aggregate counters in progress reports over verbose per-symbol payloads.
- Preserve default behavior when all moneyness ranges are selected, so the feature can be disabled operationally by selecting all ranges.

## Session Manager

Responsibilities:

- Read `trading_time.day` and `trading_time.night` from TQSDK Quote objects where available.
- Persist or cache session profiles at least by `(exchange_id, product_id)`, with contract-level overrides if TQSDK data differs.
- Determine whether a specific underlying contract is currently in its own trading session.
- Return one of: `in_session`, `out_of_session`, or `unknown`.
- Provide profile fields useful for WebUI display, including day-session ranges, night-session ranges, and whether the product has night trading.

Important boundary:

- The manager may use Quote `datetime` freshness as a data-health signal, but it must not infer a product's session from another product on the same exchange.

## Moneyness Kline Selector

Inputs:

- Active candidate instruments from SQLite after existing contract-month and minimum-expiry filters.
- Operator-selected moneyness ranges.
- Latest Quote rows or live Quote refs for each underlying future.
- Session Manager state for each underlying contract.

Classification:

- CALL:
  - strike below underlying price: in-the-money.
  - strike above underlying price: out-of-the-money.
- PUT:
  - strike above underlying price: in-the-money.
  - strike below underlying price: out-of-the-money.
- ATM:
  - For each underlying and option side, choose the nearest strike to the underlying price.
  - For an exact tie, prefer the out-of-the-money side when possible, matching TQSDK's documented convention.

Worker behavior:

- Initial startup selects Kline symbols matching the configured moneyness ranges.
- During running phase, every configured interval, each worker recomputes its shard's matching Kline symbols for chains currently in session.
- Newly matching Kline symbols are subscribed incrementally.
- Kline symbols already subscribed remain in memory until worker restart or a new-session/full-scope rebuild.
- Underlying future Kline symbols required for IV/metrics remain included when any option Kline in that chain is subscribed.

## API And WebUI

Settings:

- Add `quote_stream.moneyness_filter` as a multi-value setting, defaulting to all ranges.
- Add `quote_stream.moneyness_recalc_seconds`, default `30`.
- Add a display-only session status surface for contract-manager/quote-stream state.

Status payloads:

- Include current moneyness match count.
- Include cumulative sticky Kline subscribed count.
- Include skipped-chain counts and reasons such as `out_of_session`, `missing_underlying_price`, and `unknown_session`.
- Include last moneyness recalculation time, total recalculation count, and Kline additions caused by moneyness expansion.

Logs:

- Emit service-log or worker-report events at startup, setting changes, each aggregate sticky expansion, and repeated skip conditions.
- Avoid logging every symbol on every 30-second recalculation.
- Include only counts, session states, setting values, and non-secret contract identifiers needed for operations.

WebUI:

- Settings page: show moneyness range checkboxes and recalculation interval.
- Overview/underlying list: show trading-session state per underlying contract.
- T型报价: show the selected chain's trading-session state and moneyness range status.

## Edge Cases

- Product has no night session while another product on the same exchange has night trading.
- Underlying future Quote exists but price fields are empty or stale.
- Chain has active option Quotes but no fresh underlying price.
- ATM strike flips repeatedly around the midpoint between two strikes.
- Operator changes moneyness settings during a running session; recommended first implementation can require restart to apply the new baseline cleanly unless the API already supports safe restart flow.
- TQSDK Quote `trading_time` is missing or malformed; display `unknown` and avoid aggressive new Kline expansion unless startup explicitly forces baseline subscription.

## Trial-Run Observability

After implementation, the feature should be run for at least one representative trading window before treating it as fully accepted. Inspect:

- Quote freshness and T型报价 timestamp movement.
- Kline subscribed total versus current moneyness match total.
- Worker heartbeat and `wait_update` health.
- Metrics dirty-queue backlog and IV/Greeks refresh latency.
- SQLite write latency or busy-lock symptoms.
- WebUI overview/settings responsiveness.
- Contract-manager health and next-session scope rebuild behavior.

## Safety

- Runtime credentials remain in existing encrypted settings/environment paths only.
- No live credential values or raw API keys are written to specs, reports, tests, or service logs.
