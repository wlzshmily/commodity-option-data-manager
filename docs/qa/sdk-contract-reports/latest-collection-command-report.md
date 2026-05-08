# Controlled Market Collection Command

## Status

partial_failure

## SQLite Evidence

- Database: `data\option-data-current.sqlite3`
- Bootstrapped database: `False`

## Window

```json
{
  "max_batches": 1,
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
  "inserted_count": 0,
  "pending_or_failed_selected": 1,
  "preserved_count": 15,
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
      "finished_at": "2026-05-08T15:59:19.651469+00:00",
      "kline_rows_written": 120,
      "message": "Chain collection completed with errors",
      "metrics_written": 5,
      "quotes_written": 6,
      "run_id": 16,
      "started_at": "2026-05-08T15:59:16.848065+00:00",
      "status": "partial_failure",
      "symbols_discovered": 6,
      "trigger": "chain-collector"
    },
    "underlying_symbol": "CZCE.AP610"
  }
]
```

## Final Table Counts

```json
{
  "acquisition_errors": 8,
  "acquisition_runs": 16,
  "collection_plan_batches": 15,
  "instruments": 27766,
  "kline_20d_current": 1340,
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
