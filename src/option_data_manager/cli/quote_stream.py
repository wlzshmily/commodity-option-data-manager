"""CLI for long-lived quote and kline TQSDK subscription shards."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sqlite3
import traceback
from typing import Any

from option_data_manager.cli.collect_market import (
    DEFAULT_DATABASE_PATH,
    DEFAULT_SOURCE_DATABASE_PATH,
    _connect_database,
    _discover_and_persist_market,
    _resolve_credentials,
)
from option_data_manager.quote_streamer import (
    DEFAULT_CONTRACT_REFRESH_INTERVAL_SECONDS,
    DEFAULT_CONTRACT_MONTH_LIMIT,
    DEFAULT_KLINE_BATCH_SIZE,
    DEFAULT_KLINE_DATA_LENGTH,
    DEFAULT_KLINE_SUBSCRIPTION_TIMEOUT_SECONDS,
    DEFAULT_MIN_DAYS_TO_EXPIRY,
    DEFAULT_MONEYNESS_FILTER,
    DEFAULT_MONEYNESS_RECALC_SECONDS,
    DEFAULT_QUOTE_SHARD_SIZE,
    DEFAULT_RUNNING_QUOTE_REFRESH_SECONDS,
    stream_quotes,
)
from option_data_manager.tqsdk_connection import create_tqsdk_api_with_retries


DEFAULT_REPORT_PATH = Path("docs/qa/sdk-contract-reports/latest-quote-stream-report.json")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    database_path = Path(args.database)
    report_path = Path(args.report)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    credentials = _resolve_credentials(
        os.environ,
        database_path=database_path,
        source_database_path=Path(args.source_database),
    )
    if credentials is None:
        _write_json(
            report_path,
            {
                "status": "blocked",
                "finished_at": datetime.now(UTC).isoformat(),
                "message": "TQSDK credentials are not configured.",
                "secret_handling": "No credential value was written to this report.",
            },
        )
        print(f"blocked: wrote quote stream report: {report_path}")
        return 2

    api = None
    try:
        api = create_tqsdk_api_with_retries(credentials.account, credentials.password)
        connection = _connect_database(database_path)
        try:
            if args.discover:
                _discover_and_persist_market(api, connection)
            result = stream_quotes(
                api,
                connection,
                worker_index=args.worker_index,
                worker_count=args.worker_count,
                quote_shard_size=args.quote_shard_size,
                kline_batch_size=args.kline_batch_size,
                kline_data_length=args.kline_data_length,
                metrics_dirty_min_interval_seconds=(
                    args.metrics_dirty_min_interval_seconds
                ),
                underlying_chain_dirty_interval_seconds=(
                    args.underlying_chain_dirty_interval_seconds
                ),
                max_symbols=args.max_symbols,
                cycles=args.cycles,
                duration_seconds=args.duration_seconds,
                wait_deadline_seconds=args.wait_deadline_seconds,
                include_futures=not args.options_only,
                include_options=not args.futures_only,
                include_klines=args.include_klines,
                prioritize_near_expiry=args.prioritize_near_expiry,
                near_expiry_months=args.near_expiry_months,
                contract_month_limit=args.contract_month_limit,
                min_days_to_expiry=args.min_days_to_expiry,
                moneyness_filter=args.moneyness_filter,
                moneyness_recalc_seconds=args.moneyness_recalc_seconds,
                progress_callback=_progress_writer(
                    report_path,
                    credential_source=credentials.source,
                    database_path=database_path,
                ),
                stop_requested=_stop_file_requested(Path(args.stop_file))
                if args.stop_file
                else None,
                contract_refresh_callback=(
                    lambda: _discover_and_persist_market(api, connection)
                )
                if args.contract_refresh and args.worker_index == 0
                else None,
                contract_refresh_interval_seconds=(
                    args.contract_refresh_interval_seconds
                    if args.contract_refresh
                    else None
                ),
                running_quote_refresh_seconds=args.running_quote_refresh_seconds,
                kline_subscription_timeout_seconds=(
                    args.kline_subscription_timeout_seconds
                ),
            )
        finally:
            connection.close()
    except KeyboardInterrupt:
        _write_json(
            report_path,
            {
                "status": "interrupted",
                "finished_at": datetime.now(UTC).isoformat(),
                "secret_handling": "Credential values were not written to this report.",
            },
        )
        print(f"interrupted: wrote quote stream report: {report_path}")
        return 130
    except Exception as exc:
        _write_json(
            report_path,
            {
                "status": "failed",
                "finished_at": datetime.now(UTC).isoformat(),
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc(),
                "secret_handling": "Credential values were not written to this report.",
            },
        )
        print(f"failed: {type(exc).__name__}: {exc}")
        print(f"Wrote quote stream report: {report_path}")
        return 1
    finally:
        if api is not None:
            close = getattr(api, "close", None)
            if callable(close):
                close()

    _write_json(
        report_path,
        {
            "status": "success" if result.error_count == 0 else "partial_failure",
            "result": asdict(result),
            "credential_source": credentials.source,
            "database": str(database_path),
            "secret_handling": "No credential value was written to this report.",
        },
    )
    print(f"Wrote quote stream report: {report_path}")
    return 0 if result.error_count == 0 else 1


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a long-lived TQSDK Quote/Kline subscription worker."
    )
    parser.add_argument("--database", default=str(DEFAULT_DATABASE_PATH))
    parser.add_argument("--source-database", default=str(DEFAULT_SOURCE_DATABASE_PATH))
    parser.add_argument("--report", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--worker-index", type=int, default=0)
    parser.add_argument("--worker-count", type=int, default=1)
    parser.add_argument("--quote-shard-size", type=int, default=DEFAULT_QUOTE_SHARD_SIZE)
    parser.add_argument("--kline-batch-size", type=int, default=DEFAULT_KLINE_BATCH_SIZE)
    parser.add_argument("--kline-data-length", type=int, default=DEFAULT_KLINE_DATA_LENGTH)
    parser.add_argument(
        "--kline-subscription-timeout-seconds",
        type=float,
        default=DEFAULT_KLINE_SUBSCRIPTION_TIMEOUT_SECONDS,
        help="Maximum seconds to wait for one Kline subscription call before skipping it.",
    )
    parser.add_argument("--metrics-dirty-min-interval-seconds", type=int, default=30)
    parser.add_argument("--underlying-chain-dirty-interval-seconds", type=int, default=30)
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--cycles", type=int, default=None)
    parser.add_argument("--duration-seconds", type=float, default=None)
    parser.add_argument("--wait-deadline-seconds", type=float, default=1.0)
    parser.add_argument(
        "--running-quote-refresh-seconds",
        type=float,
        default=DEFAULT_RUNNING_QUOTE_REFRESH_SECONDS,
        help="Maximum seconds between full current-Quote snapshots while running.",
    )
    parser.add_argument(
        "--contract-refresh-interval-seconds",
        type=float,
        default=DEFAULT_CONTRACT_REFRESH_INTERVAL_SECONDS,
    )
    parser.add_argument(
        "--no-contract-refresh",
        dest="contract_refresh",
        action="store_false",
    )
    parser.add_argument("--stop-file", default=None)
    parser.add_argument("--no-discover", dest="discover", action="store_false")
    parser.add_argument("--no-klines", dest="include_klines", action="store_false")
    parser.add_argument(
        "--no-prioritize-near-expiry",
        dest="prioritize_near_expiry",
        action="store_false",
    )
    parser.add_argument("--near-expiry-months", type=int, default=2)
    parser.add_argument(
        "--min-days-to-expiry",
        type=int,
        default=DEFAULT_MIN_DAYS_TO_EXPIRY,
        help="Filter underlying contract months with fewer than this many remaining days.",
    )
    parser.add_argument(
        "--contract-months",
        choices=("1", "2", "3", "all"),
        default=str(DEFAULT_CONTRACT_MONTH_LIMIT),
        help="Subscribe contracts through the first N expiry months, or all months.",
    )
    parser.add_argument(
        "--moneyness-filter",
        default=DEFAULT_MONEYNESS_FILTER,
        help="Comma-separated Kline moneyness filter: itm,atm,otm.",
    )
    parser.add_argument(
        "--moneyness-recalc-seconds",
        type=int,
        default=DEFAULT_MONEYNESS_RECALC_SECONDS,
        help="Seconds between intraday moneyness Kline expansion checks.",
    )
    parser.add_argument("--futures-only", action="store_true")
    parser.add_argument("--options-only", action="store_true")
    parser.set_defaults(
        discover=True,
        include_klines=True,
        prioritize_near_expiry=True,
        contract_refresh=True,
    )
    args = parser.parse_args(argv)
    if args.futures_only and args.options_only:
        parser.error("--futures-only and --options-only are mutually exclusive.")
    args.contract_month_limit = (
        None if args.contract_months == "all" else int(args.contract_months)
    )
    return args


def _stop_file_requested(path: Path):
    return path.exists


def _progress_writer(
    path: Path,
    *,
    credential_source: str,
    database_path: Path,
):
    def write_progress(progress: dict[str, Any]) -> None:
        _write_json(
            path,
            {
                "status": progress.get("status", "subscribing"),
                "progress": progress,
                "credential_source": credential_source,
                "database": str(database_path),
                "secret_handling": "No credential value was written to this report.",
            },
        )

    return write_progress


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    temp_path = path.with_name(f"{path.name}.tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_path.replace(path)


if __name__ == "__main__":
    raise SystemExit(main())
