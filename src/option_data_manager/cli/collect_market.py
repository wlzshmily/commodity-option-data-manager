"""Controlled command for bounded stateful market acquisition."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import shutil
import sqlite3
import traceback
from typing import Any

from option_data_manager.acquisition import AcquisitionRepository
from option_data_manager.market_collector import collect_market_batches
from option_data_manager.market_discovery import (
    COMMODITY_OPTION_EXCHANGES,
    MarketDiscoverySummary,
    persist_market_option_symbols,
)
from option_data_manager.settings import (
    SecretProtector,
    TQSDK_ACCOUNT_KEY,
    TQSDK_PASSWORD_KEY,
    default_secret_protector,
)
from option_data_manager.sqlite_runtime import configure_sqlite_runtime
from option_data_manager.tqsdk_connection import create_tqsdk_api_with_retries


DEFAULT_DATABASE_PATH = Path("data/option-data-current.sqlite3")
DEFAULT_SOURCE_DATABASE_PATH = Path(
    "docs/qa/sdk-contract-reports/DEV-018-market-discovery.sqlite3"
)
DEFAULT_REPORT_PATH = Path("docs/qa/sdk-contract-reports/latest-collection-command-report.md")
DEFAULT_SCOPE = "routine-market-current-slice"


@dataclass(frozen=True)
class TqsdkCredentials:
    """Resolved TQSDK credentials and their safe source label."""

    account: str
    password: str
    source: str


def main(argv: list[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    """Run the bounded market collection command."""

    args = _parse_args(argv)
    runtime_env = os.environ if env is None else env
    report_path = Path(args.report)
    database_path = Path(args.database)
    source_database_path = Path(args.source_database)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    database_path.parent.mkdir(parents=True, exist_ok=True)

    credentials = _resolve_credentials(
        runtime_env,
        database_path=database_path,
        source_database_path=source_database_path,
    )
    if credentials is None:
        _write_report(
            report_path,
            build_blocked_report(
                "Missing TQSDK credentials. Checked TQSDK_ACCOUNT/TQSDK_PASSWORD "
                "environment variables and SQLite app_settings. No market "
                "acquisition was run."
            ),
        )
        print(f"Wrote blocked collection command report: {report_path}")
        return 2

    try:
        result = run_collection_command(
            account=credentials.account,
            password=credentials.password,
            database_path=database_path,
            source_database_path=source_database_path,
            option_batch_size=args.option_batch_size,
            max_underlyings=args.max_underlyings,
            max_batches=args.max_batches,
            start_after_underlying=args.start_after_underlying,
            end_before_underlying=args.end_before_underlying,
            wait_cycles=max(args.wait_cycles, 0),
            scope=args.scope,
            discover_market=not args.skip_discovery,
        )
        result["credential_source"] = credentials.source
        report = build_completed_report(result=result, database_path=database_path)
    except Exception as exc:
        report = build_failed_report(
            error=f"{type(exc).__name__}: {exc}",
            traceback_text=traceback.format_exc(),
        )
        _write_report(report_path, report)
        print(f"failed: {type(exc).__name__}: {exc}")
        print(f"Wrote collection command report: {report_path}")
        return 1
    _write_report(report_path, report)
    print(f"Wrote collection command report: {report_path}")
    return 0


def run_collection_command(
    *,
    account: str,
    password: str,
    database_path: Path,
    source_database_path: Path,
    option_batch_size: int,
    max_underlyings: int,
    max_batches: int | None,
    start_after_underlying: str | None,
    wait_cycles: int,
    end_before_underlying: str | None = None,
    scope: str = DEFAULT_SCOPE,
    discover_market: bool = True,
    api_factory: Callable[[str, str], Any] | None = None,
    iv_calculator_factory: Callable[[], Callable[[Any, Any], Any] | None] | None = None,
) -> dict[str, Any]:
    """Run collection and return a serializable command result."""

    bootstrapped_database = False
    if not database_path.exists():
        if source_database_path.exists():
            shutil.copyfile(source_database_path, database_path)
            bootstrapped_database = True
        else:
            database_path.parent.mkdir(parents=True, exist_ok=True)

    create_api = api_factory or _create_tqsdk_api
    create_iv_calculator = iv_calculator_factory or _create_option_impv_calculator
    api = create_api(account, password)
    try:
        connection = _connect_database(database_path)
        try:
            _finish_stale_running_runs(connection)
            discovery = (
                _discover_and_persist_market(api, connection)
                if discover_market
                else None
            )
            result = collect_market_batches(
                api,
                connection,
                option_batch_size=option_batch_size,
                max_underlyings=max_underlyings,
                max_batches=max_batches,
                start_after_underlying=start_after_underlying,
                end_before_underlying=end_before_underlying,
                wait_cycles=wait_cycles,
                iv_calculator=create_iv_calculator(),
                scope=scope,
            )
            return _command_result_to_dict(
                result,
                connection,
                bootstrapped_database=bootstrapped_database,
                discovery=discovery,
            )
        finally:
            connection.close()
    finally:
        close = getattr(api, "close", None)
        if callable(close):
            close()


def build_blocked_report(reason: str) -> str:
    """Build a blocked command report without writing credential values."""

    return "\n".join(
        [
            "# Controlled Market Collection Command",
            "",
            "## Status",
            "",
            "Blocked",
            "",
            "## Command Time",
            "",
            datetime.now(UTC).isoformat(),
            "",
            "## Blocker",
            "",
            reason,
            "",
            "## Secret Handling",
            "",
            "- No credential value was printed or written to this report.",
            "",
        ]
    )


def build_failed_report(*, error: str, traceback_text: str) -> str:
    """Build a failed command report without writing credential values."""

    return "\n".join(
        [
            "# Controlled Market Collection Command",
            "",
            "## Status",
            "",
            "Failed",
            "",
            "## Failure",
            "",
            error,
            "",
            "## Traceback",
            "",
            "```text",
            traceback_text,
            "```",
            "",
            "## Secret Handling",
            "",
            "- Credential values were not written to this report.",
            "",
        ]
    )


def build_completed_report(*, result: Mapping[str, Any], database_path: Path) -> str:
    """Build a completed command report."""

    return "\n".join(
        [
            "# Controlled Market Collection Command",
            "",
            "## Status",
            "",
            _collection_status(result),
            "",
            "## SQLite Evidence",
            "",
            f"- Database: `{database_path}`",
            f"- Bootstrapped database: `{result['bootstrapped_database']}`",
            "",
            "## Window",
            "",
            "```json",
            json.dumps(result["window"], ensure_ascii=False, indent=2, sort_keys=True),
            "```",
            "",
            "## Market Discovery",
            "",
            "```json",
            json.dumps(result["discovery"], ensure_ascii=False, indent=2, sort_keys=True),
            "```",
            "",
            "## Collection State",
            "",
            "```json",
            json.dumps(result["collection_state"], ensure_ascii=False, indent=2, sort_keys=True),
            "```",
            "",
            "## Batch Results",
            "",
            "```json",
            json.dumps(result["results"], ensure_ascii=False, indent=2, sort_keys=True),
            "```",
            "",
            "## Final Table Counts",
            "",
            "```json",
            json.dumps(result["tables"], ensure_ascii=False, indent=2, sort_keys=True),
            "```",
            "",
            "## Metric Coverage",
            "",
            "```json",
            json.dumps(result["metric_coverage"], ensure_ascii=False, indent=2, sort_keys=True),
            "```",
            "",
            "## Secret Handling",
            "",
            "- TQSDK credentials were supplied through "
            f"{result.get('credential_source', 'an approved credential source')}.",
            "- No credential value was written to this report.",
            "",
        ]
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run bounded stateful commodity option market acquisition."
    )
    parser.add_argument("--database", default=str(DEFAULT_DATABASE_PATH))
    parser.add_argument("--source-database", default=str(DEFAULT_SOURCE_DATABASE_PATH))
    parser.add_argument("--report", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--option-batch-size", type=int, default=20)
    parser.add_argument("--max-underlyings", type=int, default=1_000_000)
    parser.add_argument("--max-batches", type=int, default=None)
    parser.add_argument("--start-after-underlying", default=None)
    parser.add_argument("--end-before-underlying", default=None)
    parser.add_argument("--wait-cycles", type=int, default=2)
    parser.add_argument("--scope", default=DEFAULT_SCOPE)
    parser.add_argument("--skip-discovery", action="store_true")
    return parser.parse_args(argv)


def _create_tqsdk_api(account: str, password: str) -> Any:
    return create_tqsdk_api_with_retries(account, password)


def _finish_stale_running_runs(connection: sqlite3.Connection) -> int:
    cutoff = (datetime.now(UTC) - timedelta(minutes=30)).isoformat()
    return AcquisitionRepository(connection).finish_stale_running_runs(
        started_before=cutoff,
    )


def _connect_database(database_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database_path, timeout=30)
    configure_sqlite_runtime(connection, enable_wal=True)
    return connection


def _create_option_impv_calculator() -> Callable[[Any, Any], Any]:
    from tqsdk.ta import OPTION_IMPV

    return lambda klines, quote: OPTION_IMPV(klines, quote, r=0.025)


def _discover_and_persist_market(
    api: Any,
    connection: sqlite3.Connection,
) -> MarketDiscoverySummary | None:
    """Discover commodity option symbols when the API supports it."""

    query_quotes = getattr(api, "query_quotes", None)
    if not callable(query_quotes):
        return None

    symbols: list[str] = []
    for exchange_id in sorted(COMMODITY_OPTION_EXCHANGES):
        try:
            exchange_symbols = query_quotes(
                ins_class="OPTION",
                exchange_id=exchange_id,
                expired=False,
            )
        except TypeError:
            try:
                exchange_symbols = query_quotes(
                    ins_class="OPTION",
                    exchange_id=exchange_id,
                )
            except TypeError:
                exchange_symbols = query_quotes(ins_class="OPTION")
        if exchange_symbols:
            symbols.extend(str(symbol) for symbol in exchange_symbols)

    unique_symbols = sorted(set(symbols))
    if not unique_symbols:
        return None
    return persist_market_option_symbols(
        connection,
        unique_symbols,
        last_seen_at=datetime.now(UTC).isoformat(),
    )


def _resolve_credentials(
    runtime_env: Mapping[str, str],
    *,
    database_path: Path,
    source_database_path: Path,
) -> TqsdkCredentials | None:
    account = runtime_env.get("TQSDK_ACCOUNT", "").strip()
    password = runtime_env.get("TQSDK_PASSWORD", "")
    if account and password:
        return TqsdkCredentials(
            account=account,
            password=password,
            source="transient environment variables",
        )

    protector = _default_secret_protector()
    for path in (database_path, source_database_path):
        credentials = _load_credentials_from_database(path, protector)
        if credentials is not None:
            return credentials
    return None


def _default_secret_protector() -> SecretProtector:
    return default_secret_protector()


def _load_credentials_from_database(
    database_path: Path,
    protector: SecretProtector,
) -> TqsdkCredentials | None:
    if not database_path.exists():
        return None
    try:
        connection = sqlite3.connect(database_path)
        connection.row_factory = sqlite3.Row
        try:
            has_settings = connection.execute(
                """
                SELECT COUNT(*)
                FROM sqlite_master
                WHERE type = 'table' AND name = 'app_settings'
                """
            ).fetchone()[0]
            if not has_settings:
                return None
            rows = {
                str(row["key"]): row
                for row in connection.execute(
                    """
                    SELECT key, value, secret_value, is_secret
                    FROM app_settings
                    WHERE key IN (?, ?)
                    """,
                    (TQSDK_ACCOUNT_KEY, TQSDK_PASSWORD_KEY),
                ).fetchall()
            }
        finally:
            connection.close()
    except (OSError, sqlite3.Error):
        return None

    account_row = rows.get(TQSDK_ACCOUNT_KEY)
    password_row = rows.get(TQSDK_PASSWORD_KEY)
    if account_row is None or password_row is None:
        return None
    if bool(account_row["is_secret"]):
        return None
    account = str(account_row["value"] or "").strip()
    protected_password = password_row["secret_value"]
    if not account or not bool(password_row["is_secret"]) or protected_password is None:
        return None
    try:
        password = protector.unprotect(str(protected_password))
    except (ValueError, RuntimeError, OSError):
        return None
    if not password:
        return None
    return TqsdkCredentials(
        account=account,
        password=password,
        source=f"SQLite app_settings in {database_path}",
    )


def _command_result_to_dict(
    result: Any,
    connection: sqlite3.Connection,
    *,
    bootstrapped_database: bool,
    discovery: MarketDiscoverySummary | None,
) -> dict[str, Any]:
    return {
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "bootstrapped_database": bootstrapped_database,
        "discovery": discovery.__dict__ if discovery is not None else None,
        "window": {
            "max_underlyings": result.max_underlyings,
            "max_batches": result.max_batches,
            "start_after_underlying": result.start_after_underlying,
            "end_before_underlying": result.end_before_underlying,
            "option_batch_size": result.option_batch_size,
            "planned_underlyings": result.planned_underlyings,
            "planned_options": result.planned_options,
            "planned_batches": result.planned_batches,
        },
        "collection_state": {
            "scope": result.scope,
            "active_batch_count": result.materialization.active_batch_count,
            "inserted_count": result.materialization.inserted_count,
            "reset_count": result.materialization.reset_count,
            "preserved_count": result.materialization.preserved_count,
            "stale_count": result.materialization.stale_count,
            "pending_or_failed_selected": result.selected_batch_count,
        },
        "underlyings": list(result.underlyings),
        "results": [_batch_result_to_dict(batch) for batch in result.batches],
        "tables": _table_counts(connection),
        "metric_coverage": _metric_coverage(connection),
    }


def _collection_status(result: Mapping[str, Any]) -> str:
    statuses = {
        item.get("collection_batch_status")
        for item in result["results"]
    }
    if not statuses:
        return "empty_window"
    if statuses == {"success"}:
        return "success"
    if "failed" in statuses:
        return "failed"
    return "partial_failure"


def _batch_result_to_dict(batch: Any) -> dict[str, Any]:
    result = asdict(batch)
    result["option_symbols"] = list(batch.option_symbols)
    if batch.run is not None:
        result["run"] = batch.run.__dict__
    return result


def _table_counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        table_name: int(
            connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        )
        for table_name in (
            "instruments",
            "quote_current",
            "kline_20d_current",
            "option_source_metrics_current",
            "acquisition_runs",
            "acquisition_errors",
            "collection_plan_batches",
        )
    }


def _metric_coverage(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        "metrics_rows": int(
            connection.execute(
                "SELECT COUNT(*) FROM option_source_metrics_current"
            ).fetchone()[0]
        ),
        "rows_with_greeks": int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM option_source_metrics_current
                WHERE delta IS NOT NULL
                   OR gamma IS NOT NULL
                   OR theta IS NOT NULL
                   OR vega IS NOT NULL
                   OR rho IS NOT NULL
                """
            ).fetchone()[0]
        ),
        "rows_with_iv": int(
            connection.execute(
                "SELECT COUNT(*) FROM option_source_metrics_current WHERE iv IS NOT NULL"
            ).fetchone()[0]
        ),
    }


def _write_report(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
