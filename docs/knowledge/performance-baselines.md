# Performance Baselines

- Bounded smoke baseline: `max_underlyings=1`, `max_batches=1`, `option_batch_size=5`, `wait_cycles=1` completed in about 21 seconds on the local machine and produced a partial-failure report for one option batch.
- Full-market planning smoke: `max_underlyings=1000000`, `max_batches=3`, `option_batch_size=20`, `wait_cycles=1` completed in about 45 seconds and selected 3 of 1,544 active full-market batches.
- Runtime tuning: background refresh window default increased from 10 to 100 batches to reduce repeated TQSDK connect/discovery/subscription startup cost; API-triggered collection defaults to `wait_cycles=1` to avoid an extra per-batch wait round.
- Pending: complete full-market collection batch size, latency, coverage, and source-unavailable evidence.
