"""CLI for long-lived quote-only TQSDK subscription shards."""

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
from option_data_manager.quote_streamer import DEFAULT_QUOTE_SHARD_SIZE, stream_quotes
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
                max_symbols=args.max_symbols,
                cycles=args.cycles,
                duration_seconds=args.duration_seconds,
                wait_deadline_seconds=args.wait_deadline_seconds,
                include_futures=not args.options_only,
                include_options=not args.futures_only,
                stop_requested=_stop_file_requested(Path(args.stop_file))
                if args.stop_file
                else None,
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
        description="Run a quote-only long-lived TQSDK subscription worker."
    )
    parser.add_argument("--database", default=str(DEFAULT_DATABASE_PATH))
    parser.add_argument("--source-database", default=str(DEFAULT_SOURCE_DATABASE_PATH))
    parser.add_argument("--report", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--worker-index", type=int, default=0)
    parser.add_argument("--worker-count", type=int, default=1)
    parser.add_argument("--quote-shard-size", type=int, default=DEFAULT_QUOTE_SHARD_SIZE)
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--cycles", type=int, default=None)
    parser.add_argument("--duration-seconds", type=float, default=None)
    parser.add_argument("--wait-deadline-seconds", type=float, default=1.0)
    parser.add_argument("--stop-file", default=None)
    parser.add_argument("--no-discover", dest="discover", action="store_false")
    parser.add_argument("--futures-only", action="store_true")
    parser.add_argument("--options-only", action="store_true")
    parser.set_defaults(discover=True)
    args = parser.parse_args(argv)
    if args.futures_only and args.options_only:
        parser.error("--futures-only and --options-only are mutually exclusive.")
    return args


def _stop_file_requested(path: Path):
    return path.exists


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
