# Clarification

## 2026-05-10

### Confirmed

- New contracts can come from new futures underlyings with new option chains.
- New contracts can also come from additional strike-price levels listed for an existing option chain after underlying price movement.
- New and inactive contract changes are determined before the next trading session opens.
- If a product has night trading, the contract refresh boundary is before night trading starts.
- If a product has no night trading, including holiday cases where there is no pre-holiday night session, the contract refresh boundary is before the next morning session opens.
- The system should not expect active contract universe changes during a trading session.
- Realtime workers should not be fully restarted for normal contract-universe changes.
- Realtime subscription changes should be reconciled incrementally: compare the latest TQSDK contract list with the current subscription set, release invalid Quote/Kline references, and add subscriptions for newly active contracts while keeping unchanged subscriptions alive.

### Impact

- The contract manager can use a deterministic pre-session refresh model instead of expensive intraday polling.
- Collection batches and realtime subscriptions should be updated around session boundaries.
- Realtime subscription management needs an in-process reconciliation layer, not only process-level worker lifecycle controls.
- The implementation must verify TQSDK Quote/Kline release semantics with a small contract test before relying on reference removal or SDK cleanup behavior.

### Still To Decide

- Whether scheduling is manual/API-triggered in v0.2 or automated inside the local service.
- Whether schedules are global or exchange/product-specific.
