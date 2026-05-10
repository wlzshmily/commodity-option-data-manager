# Design

Implemented baseline for the v0.1 realtime path.

## Proposed Shape

- Add a contract manager service around existing TQSDK market discovery and `InstrumentRepository`.
- Persist contract refresh runs with counts for added, inactivated, unchanged, new-underlying, and new-strike contracts.
- Reuse existing `persist_market_option_symbols`, `mark_missing_inactive`, and collection-plan materialization behavior where possible.
- Add a realtime subscription reconciliation layer inside long-lived Quote/Kline workers:
  - current active symbols come from the latest persisted contract universe;
  - current subscribed symbols come from the worker's in-memory Quote/Kline reference maps;
  - unchanged symbols keep their existing references;
  - inactive symbols are removed from the reference maps and released according to verified TQSDK semantics;
  - newly active symbols receive new Quote/Kline subscriptions without rebuilding the full subscription set.
- Add an API/WebUI status surface for the latest contract refresh and realtime reconciliation status.
- Keep runtime credential handling unchanged: credentials remain in environment variables or encrypted SQLite settings and are never written to tracked evidence.
- `/api/quote-stream/start` performs one parent-process TQSDK discovery pass before spawning workers. This handles both empty databases and non-empty databases that may be stale.
- `odm-quote-stream` keeps `--no-discover` worker startup for speed and boundary clarity, but each long-lived worker has a contract reconciliation loop. Worker 0 performs periodic TQSDK discovery; every worker periodically reselects its own shard's Quote/Kline targets from SQLite and applies an incremental diff.
- Quote reconciliation removes inactive symbols from the in-memory reference map and subscribes only newly added symbols with `get_quote_list` batches.
- Kline reconciliation removes inactive symbols from the in-memory reference map and subscribes only newly added Kline symbols. The default single-symbol Kline batch size remains the stable path.

## Session Boundary Model

The first design assumption is that contract-universe changes are authoritative only before a trading session opens. The implementation should not poll for contract creation/removal during active trading.

## Edge Cases

- New futures underlying plus full option chain.
- Existing underlying with additional strike-price contracts.
- Existing underlying with expired or delisted option contracts.
- Holiday schedule without a night session.
- Refresh succeeds but realtime workers have not yet reconciled their in-memory subscription sets.
- TQSDK release behavior for Quote/Kline references differs between quote-list and kline-serial objects.

## Current Limitations

- Physical server-side unsubscribe semantics are limited by TQSDK's public object/reference model. The implementation releases inactive symbols from local reference maps and stops writing/updating them; unchanged references remain alive and newly active symbols are added without full rebuild.
- The first scheduler uses a global interval inside realtime workers. A future refinement can align refresh timing exactly to exchange/product-specific pre-session windows.
