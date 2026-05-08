# Controlled Market Collection Command

## Status

partial_failure

## SQLite Evidence

- Database: `data\option-data-current.sqlite3`
- Bootstrapped database: `False`

## Window

```json
{
  "max_underlyings": 1,
  "option_batch_size": 5,
  "planned_batches": 15,
  "planned_options": 74,
  "planned_underlyings": 1,
  "start_after_underlying": null
}
```

## Market Discovery

```json
{
  "exchange_counts": {
    "CZCE": 88,
    "DCE": 170,
    "GFEX": 45,
    "INE": 9,
    "SHFE": 68
  },
  "inactive_marked": 0,
  "option_symbol_count": 27386,
  "product_counts": {
    "a": 5,
    "ad": 2,
    "ag": 5,
    "al": 5,
    "ao": 5,
    "ap": 3,
    "au": 5,
    "b": 11,
    "bc": 2,
    "br": 3,
    "bu": 3,
    "bz": 11,
    "c": 5,
    "cf": 4,
    "cj": 3,
    "cs": 5,
    "cu": 9,
    "eb": 10,
    "eg": 11,
    "fg": 6,
    "fu": 4,
    "i": 11,
    "jd": 11,
    "jm": 11,
    "l": 11,
    "lc": 11,
    "lg": 5,
    "lh": 5,
    "m": 7,
    "ma": 6,
    "ni": 4,
    "nr": 2,
    "oi": 3,
    "op": 2,
    "p": 11,
    "pb": 2,
    "pd": 6,
    "pf": 4,
    "pg": 11,
    "pk": 2,
    "pl": 2,
    "pp": 11,
    "pr": 4,
    "ps": 11,
    "pt": 6,
    "px": 4,
    "rb": 4,
    "rm": 6,
    "ru": 4,
    "sa": 7,
    "sc": 5,
    "sf": 5,
    "sh": 5,
    "si": 11,
    "sm": 6,
    "sn": 3,
    "sp": 4,
    "sr": 4,
    "ta": 6,
    "ur": 6,
    "v": 11,
    "y": 7,
    "zc": 2,
    "zn": 4
  },
  "record_count": 27766,
  "underlying_count": 380
}
```

## Collection State

```json
{
  "active_batch_count": 15,
  "inserted_count": 15,
  "pending_or_failed_selected": 15,
  "preserved_count": 0,
  "reset_count": 0,
  "scope": "routine-market-current-slice",
  "stale_count": 0
}
```

## Batch Results

```json
[
  {
    "batch_count": 1,
    "batch_index": 1,
    "collection_batch_status": "failed",
    "error": null,
    "error_count": 2,
    "kline_rows_written": 120,
    "metrics_written": 5,
    "option_count": 5,
    "option_symbols": [
      "CZCE.AP610C6500",
      "CZCE.AP610C6600",
      "CZCE.AP610C6700",
      "CZCE.AP610C6800",
      "CZCE.AP610C6900"
    ],
    "quotes_written": 6,
    "run": {
      "error_count": 2,
      "finished_at": "2026-05-08T15:40:22.873596+00:00",
      "kline_rows_written": 120,
      "message": "Chain collection completed with errors",
      "metrics_written": 5,
      "quotes_written": 6,
      "run_id": 1,
      "started_at": "2026-05-08T15:40:13.618580+00:00",
      "status": "partial_failure",
      "symbols_discovered": 6,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  },
  {
    "batch_count": 1,
    "batch_index": 2,
    "collection_batch_status": "failed",
    "error": null,
    "error_count": 1,
    "kline_rows_written": 120,
    "metrics_written": 5,
    "option_count": 5,
    "option_symbols": [
      "CZCE.AP610C7000",
      "CZCE.AP610C7100",
      "CZCE.AP610C7200",
      "CZCE.AP610C7300",
      "CZCE.AP610C7400"
    ],
    "quotes_written": 6,
    "run": {
      "error_count": 1,
      "finished_at": "2026-05-08T15:40:28.667082+00:00",
      "kline_rows_written": 120,
      "message": "Chain collection completed with errors",
      "metrics_written": 5,
      "quotes_written": 6,
      "run_id": 2,
      "started_at": "2026-05-08T15:40:22.878873+00:00",
      "status": "partial_failure",
      "symbols_discovered": 6,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  },
  {
    "batch_count": 1,
    "batch_index": 3,
    "collection_batch_status": "success",
    "error": null,
    "error_count": 0,
    "kline_rows_written": 120,
    "metrics_written": 5,
    "option_count": 5,
    "option_symbols": [
      "CZCE.AP610C7500",
      "CZCE.AP610C7600",
      "CZCE.AP610C7700",
      "CZCE.AP610C7800",
      "CZCE.AP610C7900"
    ],
    "quotes_written": 6,
    "run": {
      "error_count": 0,
      "finished_at": "2026-05-08T15:40:36.132607+00:00",
      "kline_rows_written": 120,
      "message": "Chain collection completed",
      "metrics_written": 5,
      "quotes_written": 6,
      "run_id": 3,
      "started_at": "2026-05-08T15:40:28.680609+00:00",
      "status": "success",
      "symbols_discovered": 6,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  },
  {
    "batch_count": 1,
    "batch_index": 4,
    "collection_batch_status": "success",
    "error": null,
    "error_count": 0,
    "kline_rows_written": 120,
    "metrics_written": 5,
    "option_count": 5,
    "option_symbols": [
      "CZCE.AP610C8000",
      "CZCE.AP610C8100",
      "CZCE.AP610C8200",
      "CZCE.AP610C8300",
      "CZCE.AP610C8400"
    ],
    "quotes_written": 6,
    "run": {
      "error_count": 0,
      "finished_at": "2026-05-08T15:40:43.611662+00:00",
      "kline_rows_written": 120,
      "message": "Chain collection completed",
      "metrics_written": 5,
      "quotes_written": 6,
      "run_id": 4,
      "started_at": "2026-05-08T15:40:36.142663+00:00",
      "status": "success",
      "symbols_discovered": 6,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  },
  {
    "batch_count": 1,
    "batch_index": 5,
    "collection_batch_status": "success",
    "error": null,
    "error_count": 0,
    "kline_rows_written": 120,
    "metrics_written": 5,
    "option_count": 5,
    "option_symbols": [
      "CZCE.AP610C8500",
      "CZCE.AP610C8600",
      "CZCE.AP610C8700",
      "CZCE.AP610C8800",
      "CZCE.AP610C8900"
    ],
    "quotes_written": 6,
    "run": {
      "error_count": 0,
      "finished_at": "2026-05-08T15:40:48.447532+00:00",
      "kline_rows_written": 120,
      "message": "Chain collection completed",
      "metrics_written": 5,
      "quotes_written": 6,
      "run_id": 5,
      "started_at": "2026-05-08T15:40:43.622159+00:00",
      "status": "success",
      "symbols_discovered": 6,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  },
  {
    "batch_count": 1,
    "batch_index": 6,
    "collection_batch_status": "success",
    "error": null,
    "error_count": 0,
    "kline_rows_written": 120,
    "metrics_written": 5,
    "option_count": 5,
    "option_symbols": [
      "CZCE.AP610C9000",
      "CZCE.AP610C9100",
      "CZCE.AP610C9200",
      "CZCE.AP610C9300",
      "CZCE.AP610C9400"
    ],
    "quotes_written": 6,
    "run": {
      "error_count": 0,
      "finished_at": "2026-05-08T15:40:52.490322+00:00",
      "kline_rows_written": 120,
      "message": "Chain collection completed",
      "metrics_written": 5,
      "quotes_written": 6,
      "run_id": 6,
      "started_at": "2026-05-08T15:40:48.458311+00:00",
      "status": "success",
      "symbols_discovered": 6,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  },
  {
    "batch_count": 1,
    "batch_index": 7,
    "collection_batch_status": "success",
    "error": null,
    "error_count": 0,
    "kline_rows_written": 120,
    "metrics_written": 5,
    "option_count": 5,
    "option_symbols": [
      "CZCE.AP610C9500",
      "CZCE.AP610C9600",
      "CZCE.AP610C9700",
      "CZCE.AP610C9800",
      "CZCE.AP610C9900"
    ],
    "quotes_written": 6,
    "run": {
      "error_count": 0,
      "finished_at": "2026-05-08T15:40:59.361846+00:00",
      "kline_rows_written": 120,
      "message": "Chain collection completed",
      "metrics_written": 5,
      "quotes_written": 6,
      "run_id": 7,
      "started_at": "2026-05-08T15:40:52.500439+00:00",
      "status": "success",
      "symbols_discovered": 6,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  },
  {
    "batch_count": 1,
    "batch_index": 8,
    "collection_batch_status": "success",
    "error": null,
    "error_count": 0,
    "kline_rows_written": 120,
    "metrics_written": 5,
    "option_count": 5,
    "option_symbols": [
      "CZCE.AP610C10000",
      "CZCE.AP610C10200",
      "CZCE.AP610P6500",
      "CZCE.AP610P6600",
      "CZCE.AP610P6700"
    ],
    "quotes_written": 6,
    "run": {
      "error_count": 0,
      "finished_at": "2026-05-08T15:41:06.606995+00:00",
      "kline_rows_written": 120,
      "message": "Chain collection completed",
      "metrics_written": 5,
      "quotes_written": 6,
      "run_id": 8,
      "started_at": "2026-05-08T15:40:59.384063+00:00",
      "status": "success",
      "symbols_discovered": 6,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  },
  {
    "batch_count": 1,
    "batch_index": 9,
    "collection_batch_status": "success",
    "error": null,
    "error_count": 0,
    "kline_rows_written": 120,
    "metrics_written": 5,
    "option_count": 5,
    "option_symbols": [
      "CZCE.AP610P6800",
      "CZCE.AP610P6900",
      "CZCE.AP610P7000",
      "CZCE.AP610P7100",
      "CZCE.AP610P7200"
    ],
    "quotes_written": 6,
    "run": {
      "error_count": 0,
      "finished_at": "2026-05-08T15:41:10.549886+00:00",
      "kline_rows_written": 120,
      "message": "Chain collection completed",
      "metrics_written": 5,
      "quotes_written": 6,
      "run_id": 9,
      "started_at": "2026-05-08T15:41:06.615551+00:00",
      "status": "success",
      "symbols_discovered": 6,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  },
  {
    "batch_count": 1,
    "batch_index": 10,
    "collection_batch_status": "success",
    "error": null,
    "error_count": 0,
    "kline_rows_written": 120,
    "metrics_written": 5,
    "option_count": 5,
    "option_symbols": [
      "CZCE.AP610P7300",
      "CZCE.AP610P7400",
      "CZCE.AP610P7500",
      "CZCE.AP610P7600",
      "CZCE.AP610P7700"
    ],
    "quotes_written": 6,
    "run": {
      "error_count": 0,
      "finished_at": "2026-05-08T15:41:15.856254+00:00",
      "kline_rows_written": 120,
      "message": "Chain collection completed",
      "metrics_written": 5,
      "quotes_written": 6,
      "run_id": 10,
      "started_at": "2026-05-08T15:41:10.560352+00:00",
      "status": "success",
      "symbols_discovered": 6,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  },
  {
    "batch_count": 1,
    "batch_index": 11,
    "collection_batch_status": "success",
    "error": null,
    "error_count": 0,
    "kline_rows_written": 120,
    "metrics_written": 5,
    "option_count": 5,
    "option_symbols": [
      "CZCE.AP610P7800",
      "CZCE.AP610P7900",
      "CZCE.AP610P8000",
      "CZCE.AP610P8100",
      "CZCE.AP610P8200"
    ],
    "quotes_written": 6,
    "run": {
      "error_count": 0,
      "finished_at": "2026-05-08T15:41:21.830021+00:00",
      "kline_rows_written": 120,
      "message": "Chain collection completed",
      "metrics_written": 5,
      "quotes_written": 6,
      "run_id": 11,
      "started_at": "2026-05-08T15:41:15.859841+00:00",
      "status": "success",
      "symbols_discovered": 6,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  },
  {
    "batch_count": 1,
    "batch_index": 12,
    "collection_batch_status": "success",
    "error": null,
    "error_count": 0,
    "kline_rows_written": 120,
    "metrics_written": 5,
    "option_count": 5,
    "option_symbols": [
      "CZCE.AP610P8300",
      "CZCE.AP610P8400",
      "CZCE.AP610P8500",
      "CZCE.AP610P8600",
      "CZCE.AP610P8700"
    ],
    "quotes_written": 6,
    "run": {
      "error_count": 0,
      "finished_at": "2026-05-08T15:41:28.680620+00:00",
      "kline_rows_written": 120,
      "message": "Chain collection completed",
      "metrics_written": 5,
      "quotes_written": 6,
      "run_id": 12,
      "started_at": "2026-05-08T15:41:21.830939+00:00",
      "status": "success",
      "symbols_discovered": 6,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  },
  {
    "batch_count": 1,
    "batch_index": 13,
    "collection_batch_status": "failed",
    "error": null,
    "error_count": 1,
    "kline_rows_written": 120,
    "metrics_written": 5,
    "option_count": 5,
    "option_symbols": [
      "CZCE.AP610P8800",
      "CZCE.AP610P8900",
      "CZCE.AP610P9000",
      "CZCE.AP610P9100",
      "CZCE.AP610P9200"
    ],
    "quotes_written": 6,
    "run": {
      "error_count": 1,
      "finished_at": "2026-05-08T15:41:34.565498+00:00",
      "kline_rows_written": 120,
      "message": "Chain collection completed with errors",
      "metrics_written": 5,
      "quotes_written": 6,
      "run_id": 13,
      "started_at": "2026-05-08T15:41:28.691708+00:00",
      "status": "partial_failure",
      "symbols_discovered": 6,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  },
  {
    "batch_count": 1,
    "batch_index": 14,
    "collection_batch_status": "failed",
    "error": null,
    "error_count": 1,
    "kline_rows_written": 120,
    "metrics_written": 5,
    "option_count": 5,
    "option_symbols": [
      "CZCE.AP610P9300",
      "CZCE.AP610P9400",
      "CZCE.AP610P9500",
      "CZCE.AP610P9600",
      "CZCE.AP610P9700"
    ],
    "quotes_written": 6,
    "run": {
      "error_count": 1,
      "finished_at": "2026-05-08T15:41:40.677377+00:00",
      "kline_rows_written": 120,
      "message": "Chain collection completed with errors",
      "metrics_written": 5,
      "quotes_written": 6,
      "run_id": 14,
      "started_at": "2026-05-08T15:41:34.575986+00:00",
      "status": "partial_failure",
      "symbols_discovered": 6,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  },
  {
    "batch_count": 1,
    "batch_index": 15,
    "collection_batch_status": "failed",
    "error": null,
    "error_count": 1,
    "kline_rows_written": 100,
    "metrics_written": 4,
    "option_count": 4,
    "option_symbols": [
      "CZCE.AP610P9800",
      "CZCE.AP610P9900",
      "CZCE.AP610P10000",
      "CZCE.AP610P10200"
    ],
    "quotes_written": 5,
    "run": {
      "error_count": 1,
      "finished_at": "2026-05-08T15:41:43.596180+00:00",
      "kline_rows_written": 100,
      "message": "Chain collection completed with errors",
      "metrics_written": 4,
      "quotes_written": 5,
      "run_id": 15,
      "started_at": "2026-05-08T15:41:40.702982+00:00",
      "status": "partial_failure",
      "symbols_discovered": 5,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  }
]
```

## Final Table Counts

```json
{
  "acquisition_errors": 6,
  "acquisition_runs": 15,
  "collection_plan_batches": 15,
  "instruments": 27766,
  "kline_20d_current": 1354,
  "option_source_metrics_current": 74,
  "quote_current": 75
}
```

## Metric Coverage

```json
{
  "metrics_rows": 74,
  "rows_with_greeks": 0,
  "rows_with_iv": 68
}
```

## Secret Handling

- TQSDK credentials were supplied through SQLite app_settings in data\option-data-current.sqlite3.
- No credential value was written to this report.
