# Requirements

## User Goal

As the operator of the local option data manager, I need the system to keep the active commodity option universe current across trading days so long-running collection and realtime subscription jobs automatically include newly listed futures underlyings, newly listed option chains, and new strike-price contracts, while excluding contracts that are no longer active.

## Business Rules

- Contract changes are determined before a new trading session opens, not during an active session.
- New contracts can appear for two reasons:
  - A new futures underlying is listed together with its option chain.
  - The underlying price moves enough that the exchange lists new option strike-price levels for an existing option chain.
- Expired or delisted contracts must stop participating in collection and realtime subscriptions after the authoritative pre-session refresh marks them inactive.
- Realtime subscription updates must be incremental: unchanged Quote/Kline subscriptions stay alive, inactive contracts are released from the subscription set, and newly active contracts are added to the existing subscription set.
- Full realtime worker restart, full unsubscribe, or full resubscribe is not acceptable as the normal contract-change path because full-market subscription creation is too slow.
- If a product has a night session, the authoritative refresh must complete before night trading starts.
- If a product has no night session, including holiday edge cases where the last pre-holiday day has no night session, the authoritative refresh must complete before the next morning day session opens.
- The system must not expect new option contracts to appear or old option contracts to disappear mid-session.
- Existing current-slice data for inactive contracts may remain in SQLite for diagnostics, but inactive contracts must not be selected for new collection plans or realtime subscriptions.

## Functional Requirements

- `CM-001`: The system shall provide a contract manager workflow that refreshes the commodity option contract universe from TQSDK before the relevant trading session opens.
- `CM-002`: The refresh workflow shall detect and summarize newly active contracts, newly inactive contracts, unchanged active contracts, newly active underlyings, and new strike-price contracts on existing underlyings.
- `CM-003`: The refresh workflow shall update `instruments.active`, `inactive_reason`, and `last_seen_at` consistently with the latest authoritative TQSDK discovery result.
- `CM-004`: After a contract-universe change, the system shall rematerialize collection batch plans so newly active contracts become pending work and inactive contracts become stale or excluded from active batches.
- `CM-005`: Realtime Quote/Kline subscription workers shall reconcile subscriptions incrementally after a pre-session refresh changes the active contract universe: keep unchanged subscriptions, release inactive contracts, and add new contracts.
- `CM-006`: The WebUI/API shall expose the latest contract refresh status, including refresh time, added contract count, inactive contract count, and incremental realtime subscription reconciliation status.
- `CM-007`: The workflow shall avoid storing real TQSDK passwords, raw API keys, or live credential evidence in tracked files.

## Acceptance Criteria

- A simulated discovery run that adds a new futures underlying and option chain marks the new records active and adds collection batches for them.
- A simulated discovery run that adds new strike prices to an existing underlying marks those option contracts active and resets only affected collection batches.
- A simulated discovery run that omits previously active contracts marks those contracts inactive and excludes them from quote and kline symbol selection.
- A realtime subscription reconciliation test keeps unchanged Quote/Kline references, releases inactive contract references, and adds new contract references without rebuilding the entire subscription set.
- A refresh with no contract changes preserves existing successful collection batches where the option-symbol payload is unchanged.
- WebUI/API status can distinguish "contract universe current" from "contract universe changed and realtime reconciliation pending/running/completed".
- Unit tests cover new-underlying, new-strike, inactive-contract, and no-change refresh scenarios.

## Non-Goals

- Mid-session contract polling or immediate intraday contract-universe mutation.
- Historical warehousing of every old contract's time series.
- Trading, order placement, strategy scoring, alerting, or backtesting.

## Confirmed Scope

- First implementation includes a built-in realtime contract-management loop, not only manual/API-triggered refresh.
- Realtime startup always performs one authoritative TQSDK contract discovery pass before spawning workers, even when the database already has active contracts.
- During long-running realtime operation, worker 0 periodically refreshes the persisted contract universe from TQSDK; all workers periodically compare their shard's current active symbols with in-memory Quote/Kline references and reconcile incrementally.
- The first implementation uses a conservative global refresh interval instead of exchange/product-specific calendars. It avoids full worker restart and keeps unchanged references alive. More precise pre-session scheduling remains a future refinement, but the mandatory contract-management path is active.
