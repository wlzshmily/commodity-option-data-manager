# Controlled Market Collection Command

## Status

success

## SQLite Evidence

- Database: `data\option-data-current.sqlite3`
- Bootstrapped database: `False`

## Window

```json
{
  "max_batches": 3,
  "max_underlyings": 1000000,
  "option_batch_size": 20,
  "planned_batches": 1544,
  "planned_options": 27386,
  "planned_underlyings": 380,
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
  "active_batch_count": 1544,
  "inserted_count": 1540,
  "pending_or_failed_selected": 3,
  "preserved_count": 0,
  "reset_count": 4,
  "scope": "routine-market-current-slice",
  "stale_count": 11
}
```

## Batch Results

```json
[
  {
    "batch_count": 1,
    "batch_index": 1,
    "collection_batch_status": "success",
    "error": null,
    "error_count": 3,
    "kline_rows_written": 420,
    "metrics_written": 20,
    "option_count": 20,
    "option_symbols": [
      "CZCE.AP610C6500",
      "CZCE.AP610C6600",
      "CZCE.AP610C6700",
      "CZCE.AP610C6800",
      "CZCE.AP610C6900",
      "CZCE.AP610C7000",
      "CZCE.AP610C7100",
      "CZCE.AP610C7200",
      "CZCE.AP610C7300",
      "CZCE.AP610C7400",
      "CZCE.AP610C7500",
      "CZCE.AP610C7600",
      "CZCE.AP610C7700",
      "CZCE.AP610C7800",
      "CZCE.AP610C7900",
      "CZCE.AP610C8000",
      "CZCE.AP610C8100",
      "CZCE.AP610C8200",
      "CZCE.AP610C8300",
      "CZCE.AP610C8400"
    ],
    "quotes_written": 21,
    "run": {
      "error_count": 3,
      "finished_at": "2026-05-08T16:12:51.851245+00:00",
      "kline_rows_written": 420,
      "message": "Chain collection completed with errors",
      "metrics_written": 20,
      "quotes_written": 21,
      "run_id": 17,
      "started_at": "2026-05-08T16:12:41.397050+00:00",
      "status": "partial_failure",
      "symbols_discovered": 21,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  },
  {
    "batch_count": 1,
    "batch_index": 2,
    "collection_batch_status": "success",
    "error": null,
    "error_count": 0,
    "kline_rows_written": 420,
    "metrics_written": 20,
    "option_count": 20,
    "option_symbols": [
      "CZCE.AP610C8500",
      "CZCE.AP610C8600",
      "CZCE.AP610C8700",
      "CZCE.AP610C8800",
      "CZCE.AP610C8900",
      "CZCE.AP610C9000",
      "CZCE.AP610C9100",
      "CZCE.AP610C9200",
      "CZCE.AP610C9300",
      "CZCE.AP610C9400",
      "CZCE.AP610C9500",
      "CZCE.AP610C9600",
      "CZCE.AP610C9700",
      "CZCE.AP610C9800",
      "CZCE.AP610C9900",
      "CZCE.AP610C10000",
      "CZCE.AP610C10200",
      "CZCE.AP610P6500",
      "CZCE.AP610P6600",
      "CZCE.AP610P6700"
    ],
    "quotes_written": 21,
    "run": {
      "error_count": 0,
      "finished_at": "2026-05-08T16:13:02.270366+00:00",
      "kline_rows_written": 420,
      "message": "Chain collection completed",
      "metrics_written": 20,
      "quotes_written": 21,
      "run_id": 18,
      "started_at": "2026-05-08T16:12:51.861229+00:00",
      "status": "success",
      "symbols_discovered": 21,
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
    "kline_rows_written": 420,
    "metrics_written": 20,
    "option_count": 20,
    "option_symbols": [
      "CZCE.AP610P6800",
      "CZCE.AP610P6900",
      "CZCE.AP610P7000",
      "CZCE.AP610P7100",
      "CZCE.AP610P7200",
      "CZCE.AP610P7300",
      "CZCE.AP610P7400",
      "CZCE.AP610P7500",
      "CZCE.AP610P7600",
      "CZCE.AP610P7700",
      "CZCE.AP610P7800",
      "CZCE.AP610P7900",
      "CZCE.AP610P8000",
      "CZCE.AP610P8100",
      "CZCE.AP610P8200",
      "CZCE.AP610P8300",
      "CZCE.AP610P8400",
      "CZCE.AP610P8500",
      "CZCE.AP610P8600",
      "CZCE.AP610P8700"
    ],
    "quotes_written": 21,
    "run": {
      "error_count": 0,
      "finished_at": "2026-05-08T16:13:13.868926+00:00",
      "kline_rows_written": 420,
      "message": "Chain collection completed",
      "metrics_written": 20,
      "quotes_written": 21,
      "run_id": 19,
      "started_at": "2026-05-08T16:13:02.284327+00:00",
      "status": "success",
      "symbols_discovered": 21,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  }
]
```

## Final Table Counts

```json
{
  "acquisition_errors": 11,
  "acquisition_runs": 19,
  "collection_plan_batches": 1555,
  "instruments": 27766,
  "kline_20d_current": 1346,
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
