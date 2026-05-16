# SDK Facts

- TQSDK is the approved v0.1 data source.
- Production collection should keep Quote/K-line references alive and drive updates with `api.wait_update()`.
- `OPTION_IMPV` is used with the option quote plus the option/underlying daily K-line serial. Local WSL evidence on 2026-05-10 showed latest IV values for sampled AP610 contracts were effectively unchanged when `data_length` was 1, 2, 5, or 20; keep the diagnostic command available before changing wider production assumptions.
- `query_option_greeks` and `OPTION_IMPV` must not run in the Quote ingestion loop. Realtime Quote changes should enqueue dirty metrics work and let an independent metrics worker process it with throttling, because source calls can be slow or temporarily empty.
- TQSDK 3.9.5 `Quote` exposes option metadata such as `underlying_symbol`, `strike_price`, and `option_class`, plus futures reference fields including `last_price`, bid/ask, `close`, `settlement`, `pre_settlement`, and `pre_close`; it does not expose a ready-made option moneyness/虚实度 field. Project moneyness filtering should therefore classify option strikes against a robust underlying reference price from those Quote fields.
