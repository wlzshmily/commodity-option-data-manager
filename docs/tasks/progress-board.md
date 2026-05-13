# Progress Board

## In Progress

- None

## Pending

- None

## Done

- WEBUI-022 completed: T型报价 no longer waits for the full Kline subscription phase before receiving fresh Quote rows. Quote workers update subscribed data in the backend independently from WebUI pages, bulk-write startup snapshots, periodically refresh Quote snapshots during Kline setup, avoid startup metrics-queue storms, and health checks prefer actual fresh DB quote writes over stale worker statistic fields.
- WEBUI-021 completed: overview row status now follows realtime subscription lifecycle enums instead of fresh Quote write coverage, scoped summary counts align with worker subscription progress, WebUI SQLite runtime uses WAL for concurrent realtime reads/writes, and live local verification returned 128 subscribed underlyings including `CZCE.FG607`/`CZCE.FG608`.
- WEBUI-020 completed: realtime quote-stream lifecycle state is now a stable enum (`blocked`/`starting`/`running`/`stopped`) persisted in service state and returned by API/WebUI; frontend controls no longer infer startup state from Chinese message text, and live local start transitioned `starting` to `running` with 4 worker PIDs.
- WEBUI-019 completed: realtime start now returns a `starting` response immediately while worker launch continues in the background; status prefill skips expensive SQLite range recalculation once worker reports exist, and worker subscription-scope reads retry temporary SQLite locks instead of exiting.
- WEBUI-018 completed: realtime subscription totals and overview underlyings now share one SQLite-backed subscription scope; after realtime start, all intended contracts appear immediately as `待订阅`, partial fresh Quote rows show `订阅中`, and fully fresh chains show `已订阅`.
- WEBUI-017 completed: realtime scope selection now treats the option contract's remaining expiry days as authoritative when excluding invalid near contracts, so a 0-day `FG606` is skipped even if the underlying future delivery date is later; WebUI overview underlyings now use the same subscription scope as Quote/Kline workers instead of only rows that have already written fresh Quote data.
- OPS-002 completed: added `scripts/odm-server.sh` with `start`, `stop`, `restart`, and `status`; it stops realtime workers before WebUI restarts and falls back to process cleanup when the API is unresponsive.
- WEBUI-016 completed: realtime subscription scope still defaults to each product's nearest 2 underlying contract months, but contracts with remaining days below `quote_stream.min_days_to_expiry` are filtered first, so a 0-day `pt2606` no longer consumes a near-month slot and the selector advances to `pt2608`/`pt2610`.
- WEBUI-015 completed: 标的汇总表 now filters to the current realtime quote slice instead of historical cached SQLite rows, sorts underlyings by exchange/product/delivery month, and T型报价 shows `未订阅` until the selected chain has fresh Quote rows from the current realtime subscription run.
- Hardened realtime Quote subscription startup so TQSDK batch Quote timeouts fall back to single-symbol subscription and do not terminate the whole worker; public server hotpatch smoke passed with 20 symbols.
- Reduced realtime subscription pressure by adding a `1/2/3/全部` contract-month scope dropdown in settings; default is 2 months, and Quote/Kline worker selection plus dynamic reconciliation honor the same scope.
- Hardened realtime status and stop endpoints so transient SQLite service-state write locks do not surface as 500 responses; remote start/stop smoke now stops quote and metrics workers cleanly.
- Documented Linux server deployment with GitHub commit releases, shared runtime data, systemd service, verification, public test exposure, upgrade, and rollback steps.
- Fixed T型报价 exchange/product/month selector switching and displayed remaining expiry days in overview/T型报价.
- Reduced T型报价 contract switching latency and filled remaining expiry days from stored TQSDK quote payload fields when instrument expiry columns are still empty.
- Clarified overview realtime Quote status separately from batch collection progress, including worker count/PIDs and latest data write time.
- Added realtime subscription coverage progress and exchange tabs for the overview underlying summary table.
- Corrected realtime progress semantics and quote-stream stop behavior for persisted worker PIDs/process trees.
- Redefined realtime subscription progress to count live Quote subscription objects plus Kline subscription objects, with progress reset on fresh starts/stops.
- Added realtime subscription total elapsed time and average seconds per subscribed object to the overview progress card.
- Split realtime progress into distinct Quote and Kline bars and added estimated remaining time.
- Clarified overview progress cards so batch catch-up is separate from realtime subscription, and changed realtime ETA to use worker-reported cumulative progress plus Quote/Kline phase timestamps with a local countdown between backend reports.
- Added deterministic IV/K-line window comparison evidence and implemented 3-day incremental K-line fetch plus local 20-day cache splice for batch IV calculation; realtime subscription defaults remain at the safer 20 daily bars.
- Added realtime metrics dirty queue and independent metrics worker: Quote price changes enqueue option refreshes, underlying price changes enqueue the active chain with interval limiting, and IV/Greeks refresh is throttled to 30 seconds by default without blocking Quote writes.
- Realtime subscription workers must keep both Quote and Kline object coverage. The rejected Quote-only startup showed why the Kline progress could become `0/0`; the corrected path keeps Kline subscriptions and uses configurable `--kline-data-length` (default 3) only to reduce each serial's history window.
- Realtime subscription order now prioritizes nearest expiry months by default, and the overview progress bar marks the first configured near-expiry month boundary (default 2 months) so operators can see when the most urgent chains are loaded.

- Requirements reviewed and implementation plan approved.
- Package structure and `pyproject.toml`.
- Governance baseline.
- Unified local API and API Key management.
- WebUI mounted local API settings/key/refresh endpoints.
- Test harness and smoke validation.
- `odm-collect --max-batches` bounded window control.
- WebUI/API collection batch progress visibility.
- Background refresh worker for continuous bounded full-market collection.
- Metric-only Greeks/IV gaps are treated as visible quality gaps instead of batch blockers.
- Overview time columns now distinguish exchange market time from collector update time.
- Background collection wait cycles are configurable and default to one wait round.
- WSL2 Ubuntu setup, documentation, and Linux secret protection.
- Persistent service logs and WebUI diagnostics for API/settings/background refresh events.
- Repeatable local API/WebUI smoke command and passing 28-test suite after dependency recovery.
- `odm-test-tqsdk` process environment credential resolution fixed; live TQSDK validation reached runtime proxy denial without printing credentials.
- Local TQSDK login passed on Windows with encrypted SQLite credentials.
- Full-market performance tuning sample captured: `option_batch_size=40`, `wait_cycles=1` is the current single-process candidate; 2-4 independent TQSDK worker processes improve throughput with sublinear scaling.
- Quote-only probe confirmed realtime Quote refresh is much cheaper than full Quote/K-line/metrics collection and should be split into bounded long-lived subscription shards.
- Process-level full-market shard command implemented as `odm-collect-parallel`.
- Realtime long-lived subscription command implemented as `odm-quote-stream`.
- WebUI read model now surfaces latest parallel collection progress while API background refresh can still use the routine scope.
- Stale interrupted acquisition runs can be marked failed so diagnostics do not show false long-running jobs.
- Bounded live full-market acceptance windows completed with tuned 4-process shards.
- Final full-market completion report recorded under ignored `docs/qa/live-evidence/final-parallel-catchup/summary.json`.
- Data collection readiness reached: 864/864 full-market batches succeeded, 0 failed, 27,386 active options covered with Quote rows, K-line rows, and metrics rows.
- Final verification rerun passed: `uv run pytest -q` passed 41 tests, compileall, agentic-SDLC check, and real runtime API/WebUI smoke.
- WebUI/API quote stream controls delivered: operators can start, stop, and inspect realtime worker shards from the settings page through `/api/quote-stream`.
- Live quote stream control smoke passed and left no running worker process.
- Local acceptance UI polish delivered for settings and overview pages, including clearer progress/status messaging, fixed table widths, Chinese product labels, API Key copy/delete/full fingerprint display, and TQSDK same-name date fields exposed through API payloads.
- Realtime open-session network health detection added with conservative holiday/post-close quiet handling.
