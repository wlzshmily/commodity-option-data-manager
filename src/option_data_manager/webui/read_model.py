"""SQLite read models for the local WebUI."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
import json
import re
import sqlite3
from typing import Any

from option_data_manager.acquisition import AcquisitionRepository
from option_data_manager.collection_state import CollectionStateRepository
from option_data_manager.instruments import InstrumentRepository
from option_data_manager.klines import KlineRepository
from option_data_manager.moneyness import (
    MONEYNESS_ATM,
    build_moneyness_classification,
    is_all_moneyness,
    normalize_moneyness_filter,
    underlying_reference_price,
)
from option_data_manager.option_metrics import OptionMetricsRepository
from option_data_manager.quote_streamer import selected_underlying_contract_symbols
from option_data_manager.quotes import QuoteRepository
from option_data_manager.service_state import ServiceLogRepository
from option_data_manager.trading_sessions import (
    SESSION_IN,
    SESSION_OUT,
    SESSION_UNKNOWN,
    trading_session_state_from_payload,
)


class SubscriptionLifecycleStatus(StrEnum):
    """Stable lifecycle states for realtime subscription display."""

    PENDING = "pending"
    SUBSCRIBING = "subscribing"
    SUBSCRIBED = "subscribed"


@dataclass(frozen=True)
class _UnderlyingScopeSql:
    option_filter_sql: str
    option_params: tuple[Any, ...]
    instrument_option_filter_sql: str
    instrument_option_params: tuple[Any, ...]
    quote_scope_filter_sql: str
    quote_scope_params: tuple[Any, ...]
    instrument_quote_scope_filter_sql: str
    instrument_quote_scope_params: tuple[Any, ...]


@dataclass(frozen=True)
class WebuiReadModel:
    """Read-only aggregation facade for current-slice WebUI screens."""

    connection: sqlite3.Connection

    def __post_init__(self) -> None:
        self.connection.row_factory = sqlite3.Row
        _ensure_tables(self.connection)

    def overview(
        self,
        *,
        limit: int = 80,
        prefer_parallel_collection: bool = False,
        current_quote_after: str | None = None,
        require_current_quote: bool = False,
        subscription_scope_enabled: bool = False,
        subscription_contract_month_limit: int | None = None,
        subscription_min_days_to_expiry: int = 1,
        subscription_lifecycle_status: SubscriptionLifecycleStatus | str | None = None,
        subscription_underlying_progress: dict[str, Any] | None = None,
        subscription_fast_totals: bool = False,
    ) -> dict[str, Any]:
        """Return acquisition health summarized by underlying contract."""

        current_unavailable = require_current_quote and not current_quote_after
        subscription_symbols = (
            selected_underlying_contract_symbols(
                self.connection,
                contract_month_limit=subscription_contract_month_limit,
                min_days_to_expiry=subscription_min_days_to_expiry,
            )
            if subscription_scope_enabled and not current_unavailable
            else None
        )
        rows = (
            []
            if current_unavailable
            else _underlying_rows(
                self.connection,
                limit=limit,
                current_quote_after=current_quote_after,
                require_current_quote_rows=not subscription_scope_enabled,
                allowed_underlying_symbols=subscription_symbols,
                subscription_status_enabled=subscription_scope_enabled,
                subscription_lifecycle_status=subscription_lifecycle_status,
                subscription_underlying_progress=subscription_underlying_progress,
            )
        )
        totals = (
            _empty_current_totals(self.connection)
            if current_unavailable
            else _subscription_fast_totals(
                self.connection,
                allowed_underlying_symbols=subscription_symbols,
            )
            if subscription_fast_totals and subscription_symbols is not None
            else _overview_totals(
                self.connection,
                allowed_underlying_symbols=subscription_symbols,
            )
        )
        collection = (
            _preferred_collection_progress(self.connection)
            if prefer_parallel_collection
            else _collection_progress(self.connection)
        )
        return {
            "summary": {
                **totals,
                "latest_update": None
                if current_unavailable
                else _latest_update(self.connection),
                "quote_coverage": _ratio(
                    totals["option_quote_rows"],
                    totals["active_options"],
                ),
                "greeks_coverage": _ratio(
                    totals["rows_with_greeks"],
                    totals["active_options"],
                ),
                "iv_coverage": _ratio(totals["rows_with_iv"], totals["active_options"]),
                "kline_coverage": _ratio(
                    totals["option_kline_symbols"],
                    totals["active_options"],
                ),
            },
            "collection": collection,
            "collection_groups": _collection_groups(self.connection),
            "exchanges": []
            if current_unavailable
            or (subscription_fast_totals and subscription_symbols is not None)
            else _exchange_rows(
                self.connection,
                allowed_underlying_symbols=subscription_symbols,
            ),
            "underlyings": rows,
        }

    def tquote(
        self,
        *,
        underlying_symbol: str | None = None,
        include_selectors: bool = True,
        realtime_started_at: str | None = None,
        subscription_progress: dict[str, Any] | None = None,
        moneyness_filter: str | set[str] | list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        """Return one underlying option chain aligned by strike price."""

        selected = underlying_symbol or _default_underlying(self.connection)
        if selected is None:
            return {
                "underlying": None,
                "selectors": _selector_rows(self.connection) if include_selectors else [],
                "strikes": [],
                "atm_strike": None,
                "maxima": _empty_maxima(),
            }
        selected_progress = _subscription_progress_for_row(
            subscription_progress,
            selected,
        )
        quote_subscribed = _subscription_progress_quote_subscribed(selected_progress)
        underlying = _underlying_summary(
            self.connection,
            selected,
            realtime_started_at=realtime_started_at,
            allow_historical_realtime_data=quote_subscribed,
        )
        option_rows = _option_rows(
            self.connection,
            selected,
            realtime_started_at=realtime_started_at,
            allow_historical_realtime_data=quote_subscribed,
        )
        atm_strike = _apply_moneyness_subscription_scope(
            option_rows,
            underlying_symbol=selected,
            underlying_price=underlying.get("moneyness_reference_price"),
            moneyness_filter=(
                moneyness_filter if realtime_started_at is not None else None
            ),
            mask_out_of_scope=realtime_started_at is not None,
        )
        _apply_realtime_chain_status(
            underlying,
            option_rows,
            realtime_started_at=realtime_started_at,
            subscription_progress=selected_progress,
        )
        strike_rows = _strike_rows(option_rows)
        return {
            "underlying": underlying,
            "selectors": _selector_rows(self.connection) if include_selectors else [],
            "strikes": strike_rows,
            "atm_strike": atm_strike,
            "maxima": _maxima(strike_rows),
        }

    def runs(self, *, limit: int = 30) -> dict[str, Any]:
        """Return recent acquisition runs for the diagnostics page."""

        repository = AcquisitionRepository(self.connection)
        runs = [run.__dict__ for run in repository.list_runs(limit=limit)]
        errors = [
            _row_dict(row)
            for row in self.connection.execute(
                """
                SELECT
                    error_id,
                    run_id,
                    symbol,
                    stage,
                    error_type,
                    message,
                    created_at,
                    retryable
                FROM acquisition_errors
                ORDER BY error_id DESC
                LIMIT 100
                """
            ).fetchall()
        ]
        for error in errors:
            error["retryable"] = bool(error["retryable"])
        service_logs = [
            log.__dict__
            for log in ServiceLogRepository(self.connection).list_logs(limit=limit)
        ]
        return {"runs": runs, "errors": errors, "service_logs": service_logs}


def _ensure_tables(connection: sqlite3.Connection) -> None:
    InstrumentRepository(connection)
    QuoteRepository(connection)
    KlineRepository(connection)
    OptionMetricsRepository(connection)
    AcquisitionRepository(connection)
    CollectionStateRepository(connection)
    ServiceLogRepository(connection)


def _underlying_scope_sql(
    allowed_underlying_symbols: set[str] | None,
) -> _UnderlyingScopeSql:
    symbols = tuple(sorted(allowed_underlying_symbols or ()))
    if not symbols:
        return _UnderlyingScopeSql(
            option_filter_sql="",
            option_params=(),
            instrument_option_filter_sql="",
            instrument_option_params=(),
            quote_scope_filter_sql="",
            quote_scope_params=(),
            instrument_quote_scope_filter_sql="",
            instrument_quote_scope_params=(),
        )
    placeholders = ", ".join("?" for _ in symbols)
    quote_filter = (
        f"AND (symbol IN ({placeholders}) OR underlying_symbol IN ({placeholders}))"
    )
    instrument_quote_filter = (
        f"AND (i.symbol IN ({placeholders}) OR i.underlying_symbol IN ({placeholders}))"
    )
    return _UnderlyingScopeSql(
        option_filter_sql=f"AND underlying_symbol IN ({placeholders})",
        option_params=symbols,
        instrument_option_filter_sql=f"AND i.underlying_symbol IN ({placeholders})",
        instrument_option_params=symbols,
        quote_scope_filter_sql=quote_filter,
        quote_scope_params=(*symbols, *symbols),
        instrument_quote_scope_filter_sql=instrument_quote_filter,
        instrument_quote_scope_params=(*symbols, *symbols),
    )


def _overview_totals(
    connection: sqlite3.Connection,
    *,
    allowed_underlying_symbols: set[str] | None = None,
) -> dict[str, int]:
    scope = _underlying_scope_sql(allowed_underlying_symbols)
    active_options = _scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM instruments
        WHERE active = 1 AND option_class IN ('CALL', 'PUT')
          {scope.option_filter_sql}
        """,
        scope.option_params,
    )
    option_quote_rows = _scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM instruments i
        JOIN quote_current q ON q.symbol = i.symbol
        WHERE i.active = 1 AND i.option_class IN ('CALL', 'PUT')
          {scope.instrument_option_filter_sql}
          AND {_quote_has_market_data_sql("q")}
        """,
        scope.instrument_option_params,
    )
    active_quote_symbols = _scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM instruments
        WHERE active = 1
          AND (ins_class = 'FUTURE' OR option_class IN ('CALL', 'PUT'))
          {scope.quote_scope_filter_sql}
        """,
        scope.quote_scope_params,
    )
    quote_symbol_rows = _scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM instruments i
        JOIN quote_current q ON q.symbol = i.symbol
        WHERE i.active = 1
          AND (i.ins_class = 'FUTURE' OR i.option_class IN ('CALL', 'PUT'))
          {scope.instrument_quote_scope_filter_sql}
        """,
        scope.instrument_quote_scope_params,
    )
    quote_symbol_market_rows = _scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM instruments i
        JOIN quote_current q ON q.symbol = i.symbol
        WHERE i.active = 1
          AND (i.ins_class = 'FUTURE' OR i.option_class IN ('CALL', 'PUT'))
          {scope.instrument_quote_scope_filter_sql}
          AND {_quote_has_market_data_sql("q")}
        """,
        scope.instrument_quote_scope_params,
    )
    option_kline_symbols = _scalar(
        connection,
        f"""
        SELECT COUNT(DISTINCT k.symbol)
        FROM instruments i
        JOIN kline_20d_current k ON k.symbol = i.symbol
        WHERE i.active = 1 AND i.option_class IN ('CALL', 'PUT')
          {scope.instrument_option_filter_sql}
        """,
        scope.instrument_option_params,
    )
    rows_with_greeks = _scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM instruments i
        JOIN option_source_metrics_current m ON m.symbol = i.symbol
        WHERE i.active = 1
          AND i.option_class IN ('CALL', 'PUT')
          {scope.instrument_option_filter_sql}
          AND (
            m.delta IS NOT NULL OR m.gamma IS NOT NULL OR m.theta IS NOT NULL
            OR m.vega IS NOT NULL OR m.rho IS NOT NULL
          )
        """,
        scope.instrument_option_params,
    )
    return {
        "active_underlyings": _scalar(
            connection,
            f"""
            SELECT COUNT(DISTINCT underlying_symbol)
            FROM instruments
            WHERE active = 1 AND option_class IN ('CALL', 'PUT')
              {scope.option_filter_sql}
            """,
            scope.option_params,
        ),
        "active_options": active_options,
        "active_quote_symbols": active_quote_symbols,
        "option_quote_rows": option_quote_rows,
        "quote_symbol_rows": quote_symbol_rows,
        "quote_symbol_market_rows": quote_symbol_market_rows,
        "option_kline_symbols": option_kline_symbols,
        "metrics_rows": _scalar(
            connection,
            f"""
            SELECT COUNT(*)
            FROM instruments i
            JOIN option_source_metrics_current m ON m.symbol = i.symbol
            WHERE i.active = 1 AND i.option_class IN ('CALL', 'PUT')
              {scope.instrument_option_filter_sql}
            """,
            scope.instrument_option_params,
        ),
        "rows_with_greeks": rows_with_greeks,
        "rows_with_iv": _scalar(
            connection,
            f"""
            SELECT COUNT(*)
            FROM instruments i
            JOIN option_source_metrics_current m ON m.symbol = i.symbol
            WHERE i.active = 1
              AND i.option_class IN ('CALL', 'PUT')
              {scope.instrument_option_filter_sql}
              AND m.iv IS NOT NULL
            """,
            scope.instrument_option_params,
        ),
        "acquisition_errors": _scalar(connection, "SELECT COUNT(*) FROM acquisition_errors"),
        "acquisition_runs": _scalar(connection, "SELECT COUNT(*) FROM acquisition_runs"),
        "latest_quote_update": _latest_quote_update(connection),
    }


def _subscription_fast_totals(
    connection: sqlite3.Connection,
    *,
    allowed_underlying_symbols: set[str],
) -> dict[str, int]:
    scope = _underlying_scope_sql(allowed_underlying_symbols)
    active_options = _scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM instruments
        WHERE active = 1 AND option_class IN ('CALL', 'PUT')
          {scope.option_filter_sql}
        """,
        scope.option_params,
    )
    active_underlyings = _scalar(
        connection,
        f"""
        SELECT COUNT(DISTINCT underlying_symbol)
        FROM instruments
        WHERE active = 1 AND option_class IN ('CALL', 'PUT')
          {scope.option_filter_sql}
        """,
        scope.option_params,
    )
    active_quote_symbols = _scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM instruments
        WHERE active = 1
          AND (ins_class = 'FUTURE' OR option_class IN ('CALL', 'PUT'))
          {scope.quote_scope_filter_sql}
        """,
        scope.quote_scope_params,
    )
    return {
        "active_underlyings": active_underlyings,
        "active_options": active_options,
        "active_quote_symbols": active_quote_symbols,
        "option_quote_rows": 0,
        "quote_symbol_rows": 0,
        "quote_symbol_market_rows": 0,
        "option_kline_symbols": 0,
        "metrics_rows": 0,
        "rows_with_greeks": 0,
        "rows_with_iv": 0,
        "acquisition_errors": _scalar(connection, "SELECT COUNT(*) FROM acquisition_errors"),
        "acquisition_runs": _scalar(connection, "SELECT COUNT(*) FROM acquisition_runs"),
        "latest_quote_update": _latest_quote_update(connection),
    }


def _empty_current_totals(connection: sqlite3.Connection) -> dict[str, int]:
    active_options = _scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM instruments
        WHERE active = 1 AND option_class IN ('CALL', 'PUT')
        """,
    )
    active_underlyings = _scalar(
        connection,
        """
        SELECT COUNT(DISTINCT underlying_symbol)
        FROM instruments
        WHERE active = 1 AND option_class IN ('CALL', 'PUT')
        """,
    )
    active_quote_symbols = _scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM instruments
        WHERE active = 1
          AND (ins_class = 'FUTURE' OR option_class IN ('CALL', 'PUT'))
        """,
    )
    return {
        "active_underlyings": active_underlyings,
        "active_options": active_options,
        "active_quote_symbols": active_quote_symbols,
        "option_quote_rows": 0,
        "quote_symbol_rows": 0,
        "quote_symbol_market_rows": 0,
        "option_kline_symbols": 0,
        "metrics_rows": 0,
        "rows_with_greeks": 0,
        "rows_with_iv": 0,
        "acquisition_errors": _scalar(connection, "SELECT COUNT(*) FROM acquisition_errors"),
        "acquisition_runs": _scalar(connection, "SELECT COUNT(*) FROM acquisition_runs"),
        "latest_quote_update": None,
    }


def _collection_progress(
    connection: sqlite3.Connection,
    *,
    scope: str = "routine-market-current-slice",
) -> dict[str, Any]:
    stale_running_batches = _stale_running_batches(connection, scope=scope)
    row = connection.execute(
        """
        SELECT
            COUNT(*) AS total_batches,
            SUM(CASE WHEN stale = 0 THEN 1 ELSE 0 END) AS active_batches,
            SUM(CASE WHEN stale = 0 AND status = 'pending' THEN 1 ELSE 0 END) AS pending_batches,
            SUM(CASE WHEN stale = 0 AND status = 'running' THEN 1 ELSE 0 END) AS running_batches,
            SUM(CASE WHEN stale = 0 AND status = 'success' THEN 1 ELSE 0 END) AS success_batches,
            SUM(CASE WHEN stale = 0 AND status = 'failed' THEN 1 ELSE 0 END) AS failed_batches,
            SUM(CASE WHEN stale = 1 THEN 1 ELSE 0 END) AS stale_batches,
            SUM(CASE WHEN stale = 0 THEN option_count ELSE 0 END) AS planned_options,
            COUNT(DISTINCT CASE WHEN stale = 0 THEN underlying_symbol END) AS planned_underlyings,
            MAX(updated_at) AS latest_batch_update,
            MAX(completed_at) AS latest_batch_completion
        FROM collection_plan_batches
        WHERE plan_scope = ?
        """,
        (scope,),
    ).fetchone()
    data = _row_dict(row) if row is not None else {}
    active_batches = int(data.get("active_batches") or 0)
    success_batches = int(data.get("success_batches") or 0)
    pending_batches = int(data.get("pending_batches") or 0)
    failed_batches = int(data.get("failed_batches") or 0)
    running_batches = int(data.get("running_batches") or 0)
    return {
        "scope": scope,
        "total_batches": int(data.get("total_batches") or 0),
        "active_batches": active_batches,
        "pending_batches": pending_batches,
        "running_batches": running_batches,
        "success_batches": success_batches,
        "failed_batches": failed_batches,
        "stale_batches": int(data.get("stale_batches") or 0),
        "planned_options": int(data.get("planned_options") or 0),
        "planned_underlyings": int(data.get("planned_underlyings") or 0),
        "remaining_batches": pending_batches + failed_batches + running_batches,
        "completion_ratio": _ratio(success_batches, active_batches),
        "latest_batch_update": data.get("latest_batch_update"),
        "latest_batch_completion": data.get("latest_batch_completion"),
        "recent_failures": _collection_failures(connection, scope=scope),
        "stale_running_batches": len(stale_running_batches),
        "recent_stale_running_batches": stale_running_batches[:5],
    }


def _preferred_collection_progress(connection: sqlite3.Connection) -> dict[str, Any]:
    routine = _collection_progress(connection)
    parallel = _parallel_collection_progress(connection)
    if parallel["active_batches"] <= 0:
        return routine
    routine_update = routine.get("latest_batch_update") or ""
    parallel_update = parallel.get("latest_batch_update") or ""
    if parallel_update >= routine_update:
        return parallel
    return routine


def _collection_groups(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    groups = [_collection_progress(connection)]
    parallel = _parallel_collection_progress(connection)
    if parallel["active_batches"] > 0:
        groups.append(parallel)
    return groups


def _parallel_collection_progress(connection: sqlite3.Connection) -> dict[str, Any]:
    return _collection_progress_for_predicate(
        connection,
        scope_label="parallel-market-current-slice",
        where_sql="plan_scope LIKE ?",
        where_params=("parallel-market-current-slice-worker-%",),
    )


def _collection_progress_for_predicate(
    connection: sqlite3.Connection,
    *,
    scope_label: str,
    where_sql: str,
    where_params: tuple[Any, ...],
) -> dict[str, Any]:
    stale_running_batches = _stale_running_batches_for_predicate(
        connection,
        where_sql=where_sql,
        where_params=where_params,
    )
    row = connection.execute(
        f"""
        SELECT
            COUNT(*) AS total_batches,
            SUM(CASE WHEN stale = 0 THEN 1 ELSE 0 END) AS active_batches,
            SUM(CASE WHEN stale = 0 AND status = 'pending' THEN 1 ELSE 0 END) AS pending_batches,
            SUM(CASE WHEN stale = 0 AND status = 'running' THEN 1 ELSE 0 END) AS running_batches,
            SUM(CASE WHEN stale = 0 AND status = 'success' THEN 1 ELSE 0 END) AS success_batches,
            SUM(CASE WHEN stale = 0 AND status = 'failed' THEN 1 ELSE 0 END) AS failed_batches,
            SUM(CASE WHEN stale = 1 THEN 1 ELSE 0 END) AS stale_batches,
            SUM(CASE WHEN stale = 0 THEN option_count ELSE 0 END) AS planned_options,
            COUNT(DISTINCT CASE WHEN stale = 0 THEN underlying_symbol END) AS planned_underlyings,
            MAX(updated_at) AS latest_batch_update,
            MAX(completed_at) AS latest_batch_completion
        FROM collection_plan_batches
        WHERE {where_sql}
        """,
        where_params,
    ).fetchone()
    data = _row_dict(row) if row is not None else {}
    active_batches = int(data.get("active_batches") or 0)
    success_batches = int(data.get("success_batches") or 0)
    pending_batches = int(data.get("pending_batches") or 0)
    failed_batches = int(data.get("failed_batches") or 0)
    running_batches = int(data.get("running_batches") or 0)
    return {
        "scope": scope_label,
        "total_batches": int(data.get("total_batches") or 0),
        "active_batches": active_batches,
        "pending_batches": pending_batches,
        "running_batches": running_batches,
        "success_batches": success_batches,
        "failed_batches": failed_batches,
        "stale_batches": int(data.get("stale_batches") or 0),
        "planned_options": int(data.get("planned_options") or 0),
        "planned_underlyings": int(data.get("planned_underlyings") or 0),
        "remaining_batches": pending_batches + failed_batches + running_batches,
        "completion_ratio": _ratio(success_batches, active_batches),
        "latest_batch_update": data.get("latest_batch_update"),
        "latest_batch_completion": data.get("latest_batch_completion"),
        "recent_failures": _collection_failures_for_predicate(
            connection,
            where_sql=where_sql,
            where_params=where_params,
        ),
        "stale_running_batches": len(stale_running_batches),
        "recent_stale_running_batches": stale_running_batches[:5],
    }


def _stale_running_batches(
    connection: sqlite3.Connection,
    *,
    scope: str,
    limit: int = 10,
    max_age_minutes: int = 30,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
            underlying_symbol,
            batch_index,
            exchange_id,
            product_id,
            option_count,
            status,
            attempt_count,
            last_error,
            updated_at
        FROM collection_plan_batches
        WHERE plan_scope = ?
          AND stale = 0
          AND status = 'running'
        ORDER BY updated_at, underlying_symbol, batch_index
        """,
        (scope,),
    ).fetchall()
    return _filter_stale_running_rows(
        [_row_dict(row) for row in rows],
        limit=limit,
        max_age_minutes=max_age_minutes,
    )


def _stale_running_batches_for_predicate(
    connection: sqlite3.Connection,
    *,
    where_sql: str,
    where_params: tuple[Any, ...],
    limit: int = 10,
    max_age_minutes: int = 30,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        f"""
        SELECT
            underlying_symbol,
            batch_index,
            exchange_id,
            product_id,
            option_count,
            status,
            attempt_count,
            last_error,
            updated_at
        FROM collection_plan_batches
        WHERE {where_sql}
          AND stale = 0
          AND status = 'running'
        ORDER BY updated_at, underlying_symbol, batch_index
        """,
        where_params,
    ).fetchall()
    return _filter_stale_running_rows(
        [_row_dict(row) for row in rows],
        limit=limit,
        max_age_minutes=max_age_minutes,
    )


def _filter_stale_running_rows(
    rows: list[dict[str, Any]],
    *,
    limit: int,
    max_age_minutes: int,
) -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    threshold = timedelta(minutes=max_age_minutes)
    stale_rows: list[dict[str, Any]] = []
    for row in rows:
        updated_at = _parse_datetime(row.get("updated_at"))
        if updated_at is None:
            continue
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=UTC)
        age = now - updated_at.astimezone(UTC)
        if age >= threshold:
            row["age_seconds"] = int(age.total_seconds())
            stale_rows.append(row)
    return stale_rows[:limit]


def _collection_failures(
    connection: sqlite3.Connection,
    *,
    scope: str,
    limit: int = 5,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
            underlying_symbol,
            batch_index,
            exchange_id,
            product_id,
            option_count,
            status,
            attempt_count,
            last_error,
            updated_at
        FROM collection_plan_batches
        WHERE plan_scope = ?
          AND stale = 0
          AND status = 'failed'
        ORDER BY updated_at DESC, underlying_symbol, batch_index
        LIMIT ?
        """,
        (scope, limit),
    ).fetchall()
    return [_row_dict(row) for row in rows]


def _collection_failures_for_predicate(
    connection: sqlite3.Connection,
    *,
    where_sql: str,
    where_params: tuple[Any, ...],
    limit: int = 5,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        f"""
        SELECT
            underlying_symbol,
            batch_index,
            exchange_id,
            product_id,
            option_count,
            status,
            attempt_count,
            last_error,
            updated_at
        FROM collection_plan_batches
        WHERE {where_sql}
          AND stale = 0
          AND status = 'failed'
        ORDER BY updated_at DESC, underlying_symbol, batch_index
        LIMIT ?
        """,
        (*where_params, limit),
    ).fetchall()
    return [_row_dict(row) for row in rows]


def _underlying_rows(
    connection: sqlite3.Connection,
    *,
    limit: int,
    current_quote_after: str | None = None,
    require_current_quote_rows: bool = True,
    allowed_underlying_symbols: set[str] | None = None,
    subscription_status_enabled: bool = False,
    subscription_lifecycle_status: SubscriptionLifecycleStatus | str | None = None,
    subscription_underlying_progress: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if allowed_underlying_symbols is not None and not allowed_underlying_symbols:
        return []
    allowed_symbols = sorted(allowed_underlying_symbols or [])
    allowed_placeholders = ", ".join("?" for _ in allowed_symbols)
    option_underlying_filter = (
        f"AND o.underlying_symbol IN ({allowed_placeholders})"
        if allowed_symbols
        else ""
    )
    coverage_underlying_filter = (
        f"AND i.underlying_symbol IN ({allowed_placeholders})"
        if allowed_symbols
        else ""
    )
    underlying_quote_filter = (
        f"WHERE q.symbol IN ({allowed_placeholders})" if allowed_symbols else ""
    )
    where_parts: list[str] = []
    where_params: list[Any] = []
    if require_current_quote_rows:
        where_parts.append("(? IS NULL OR COALESCE(c.current_quote_count, 0) > 0)")
        where_params.append(current_quote_after)
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    rows = connection.execute(
        f"""
        WITH option_counts AS (
            SELECT
                o.underlying_symbol,
                o.exchange_id,
                o.product_id,
                MIN(COALESCE(NULLIF(o.expire_datetime, ''), json_extract(oq.raw_payload_json, '$.expire_datetime'))) AS option_expire_datetime,
                MIN(COALESCE(NULLIF(o.last_exercise_datetime, ''), json_extract(oq.raw_payload_json, '$.last_exercise_datetime'))) AS option_last_exercise_datetime,
                MIN(CAST(json_extract(oq.raw_payload_json, '$.expire_rest_days') AS INTEGER)) AS option_expire_rest_days,
                SUM(CASE WHEN o.option_class = 'CALL' THEN 1 ELSE 0 END) AS call_count,
                SUM(CASE WHEN o.option_class = 'PUT' THEN 1 ELSE 0 END) AS put_count,
                COUNT(*) AS option_count,
                MIN(COALESCE(NULLIF(o.expire_datetime, ''), json_extract(oq.raw_payload_json, '$.expire_datetime'))) AS expire_datetime
            FROM instruments o
            LEFT JOIN quote_current oq ON oq.symbol = o.symbol
            WHERE o.active = 1 AND o.option_class IN ('CALL', 'PUT')
              {option_underlying_filter}
            GROUP BY o.underlying_symbol, o.exchange_id, o.product_id
        ),
        coverage AS (
            SELECT
                i.underlying_symbol,
                COUNT(DISTINCT CASE
                    WHEN {_quote_has_market_data_sql("q")} THEN q.symbol
                END) AS quote_count,
                COUNT(DISTINCT CASE
                    WHEN (? IS NULL OR q.received_at >= ?) THEN q.symbol
                END) AS current_quote_count,
                COUNT(DISTINCT m.symbol) AS metrics_count,
                COUNT(DISTINCT CASE WHEN m.iv IS NOT NULL THEN m.symbol END) AS iv_count,
                COUNT(DISTINCT k.symbol) AS kline_count,
                MAX(COALESCE(q.received_at, m.received_at, k.received_at)) AS latest_update,
                MAX(CASE
                    WHEN (? IS NULL OR q.received_at >= ?)
                     AND {_quote_has_market_data_sql("q")} THEN q.source_datetime
                END) AS latest_quote_time,
                MAX(CASE
                    WHEN (? IS NULL OR q.received_at >= ?) THEN q.received_at
                END) AS latest_current_quote_received_at,
                MAX(k.bar_datetime) AS latest_kline_time
            FROM instruments i
            LEFT JOIN quote_current q ON q.symbol = i.symbol
            LEFT JOIN option_source_metrics_current m ON m.symbol = i.symbol
            LEFT JOIN kline_20d_current k ON k.symbol = i.symbol
            WHERE i.active = 1 AND i.option_class IN ('CALL', 'PUT')
              {coverage_underlying_filter}
            GROUP BY i.underlying_symbol
        ),
        underlying_quote AS (
            SELECT
                q.symbol,
                CASE
                    WHEN (? IS NULL OR q.received_at >= ?)
                     AND {_quote_has_market_data_sql("q")} THEN q.source_datetime
                END AS source_datetime,
                CASE
                    WHEN (? IS NULL OR q.received_at >= ?) THEN q.received_at
                END AS received_at,
                q.last_price,
                q.raw_payload_json
            FROM quote_current q
            {underlying_quote_filter}
        )
        SELECT
            oc.underlying_symbol,
            oc.exchange_id,
            oc.product_id,
            oc.option_expire_datetime,
            oc.option_last_exercise_datetime,
            oc.option_expire_rest_days,
            oc.call_count,
            oc.put_count,
            oc.option_count,
            oc.expire_datetime,
            COALESCE(c.quote_count, 0) AS quote_count,
            COALESCE(c.current_quote_count, 0) AS current_quote_count,
            COALESCE(c.metrics_count, 0) AS metrics_count,
            COALESCE(c.iv_count, 0) AS iv_count,
            COALESCE(c.kline_count, 0) AS kline_count,
            c.latest_update,
            c.latest_quote_time,
            c.latest_current_quote_received_at,
            c.latest_kline_time,
            uq.source_datetime AS book_time,
            uq.received_at AS underlying_quote_received_at,
            uq.last_price AS underlying_last,
            uq.raw_payload_json AS underlying_quote_raw_payload_json,
            future.instrument_name AS underlying_instrument_name,
            future.expire_datetime AS future_expire_datetime,
            future.delivery_year,
            future.delivery_month
        FROM option_counts oc
        LEFT JOIN coverage c ON c.underlying_symbol = oc.underlying_symbol
        LEFT JOIN underlying_quote uq ON uq.symbol = oc.underlying_symbol
        LEFT JOIN instruments future ON future.symbol = oc.underlying_symbol
        {where_sql}
        ORDER BY
            oc.exchange_id,
            oc.product_id,
            COALESCE(future.delivery_year, 9999),
            COALESCE(future.delivery_month, 99),
            oc.underlying_symbol
        LIMIT ?
        """,
        (
            *allowed_symbols,
            current_quote_after,
            current_quote_after,
            current_quote_after,
            current_quote_after,
            current_quote_after,
            current_quote_after,
            *allowed_symbols,
            current_quote_after,
            current_quote_after,
            current_quote_after,
            current_quote_after,
            *allowed_symbols,
            *where_params,
            limit,
        ),
    ).fetchall()
    return [
        _format_underlying_row(
            row,
            subscription_status_enabled=subscription_status_enabled,
            subscription_lifecycle_status=subscription_lifecycle_status,
            subscription_progress=_subscription_progress_for_row(
                subscription_underlying_progress,
                str(row["underlying_symbol"]),
            ),
        )
        for row in rows
    ]


def _format_underlying_row(
    row: sqlite3.Row,
    *,
    subscription_status_enabled: bool = False,
    subscription_lifecycle_status: SubscriptionLifecycleStatus | str | None = None,
    subscription_progress: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data = _row_dict(row)
    option_count = int(data["option_count"])
    data["expiry_month"] = _expiry_month(str(data["underlying_symbol"]))
    data["product_display_name"] = _product_display_name(
        data.get("product_id"),
        instrument_name=data.get("underlying_instrument_name"),
        raw_payload_json=data.get("underlying_quote_raw_payload_json"),
    )
    data["option_expire_datetime"] = _expiry_date_text(
        data.get("option_expire_datetime")
    )
    data["option_last_exercise_datetime"] = _expiry_date_text(
        data.get("option_last_exercise_datetime")
    )
    data["expire_datetime"] = data["option_expire_datetime"]
    data["days_to_option_expire_datetime"] = _expiry_days(
        data.get("option_expire_datetime"),
        fallback_days=data.get("option_expire_rest_days"),
    )
    data["days_to_option_last_exercise_datetime"] = _days_to_expiry(
        data.get("option_last_exercise_datetime")
    )
    data["days_to_expiry"] = data["days_to_option_expire_datetime"]
    data["quote_coverage"] = _ratio(int(data["quote_count"]), option_count)
    data["current_quote_coverage"] = _ratio(
        int(data.get("current_quote_count") or 0),
        option_count,
    )
    data["metrics_coverage"] = _ratio(int(data["metrics_count"]), option_count)
    data["iv_coverage"] = _ratio(int(data["iv_count"]), option_count)
    data["kline_coverage"] = _ratio(int(data["kline_count"]), option_count)
    data["display_market_time"] = data.get("book_time") or data.get("latest_quote_time")
    data["display_kline_time"] = _daily_kline_close_datetime(
        data.get("latest_kline_time"),
        reference_datetime=data.get("display_market_time") or data.get("latest_update"),
    )
    _apply_subscription_progress(data, subscription_progress)
    data["status"] = (
        _subscription_status_from_counts(
            data,
            option_count,
            lifecycle_status=subscription_lifecycle_status,
            subscription_progress=subscription_progress,
        )
        if subscription_status_enabled
        else _status_from_counts(data, option_count)
    )
    _apply_trading_session_state(
        data,
        data.get("underlying_quote_raw_payload_json"),
        quote_source_datetime=data.get("book_time"),
        quote_pending=_subscription_quote_pending(data["status"], subscription_progress),
    )
    return data


def _exchange_rows(
    connection: sqlite3.Connection,
    *,
    allowed_underlying_symbols: set[str] | None = None,
) -> list[dict[str, Any]]:
    scope = _underlying_scope_sql(allowed_underlying_symbols)
    rows = connection.execute(
        f"""
        WITH options AS (
            SELECT *
            FROM instruments
            WHERE active = 1 AND option_class IN ('CALL', 'PUT')
              {scope.option_filter_sql}
        )
        SELECT
            o.exchange_id,
            COUNT(DISTINCT o.product_id) AS product_count,
            COUNT(DISTINCT o.underlying_symbol) AS underlying_count,
            COUNT(*) AS option_count,
            COUNT(CASE WHEN {_quote_has_market_data_sql("q")} THEN q.symbol END) AS quote_count,
            COUNT(m.symbol) AS metrics_count,
            SUM(CASE WHEN m.iv IS NOT NULL THEN 1 ELSE 0 END) AS iv_count
        FROM options o
        LEFT JOIN quote_current q ON q.symbol = o.symbol
        LEFT JOIN option_source_metrics_current m ON m.symbol = o.symbol
        GROUP BY o.exchange_id
        ORDER BY o.exchange_id
        """,
        scope.option_params,
    ).fetchall()
    return [
        {
            **_row_dict(row),
            "quote_coverage": _ratio(int(row["quote_count"]), int(row["option_count"])),
            "iv_coverage": _ratio(int(row["iv_count"] or 0), int(row["option_count"])),
        }
        for row in rows
    ]


def _underlying_summary(
    connection: sqlite3.Connection,
    symbol: str,
    *,
    realtime_started_at: str | None = None,
    allow_historical_realtime_data: bool = False,
) -> dict[str, Any]:
    row = connection.execute(
        f"""
        SELECT
            i.symbol,
            i.exchange_id,
            i.product_id,
            i.instrument_id,
            i.instrument_name,
            COALESCE(NULLIF(i.expire_datetime, ''), json_extract(q.raw_payload_json, '$.expire_datetime')) AS future_expire_datetime,
            COALESCE(i.delivery_year, json_extract(q.raw_payload_json, '$.delivery_year')) AS delivery_year,
            COALESCE(i.delivery_month, json_extract(q.raw_payload_json, '$.delivery_month')) AS delivery_month,
            expiry.option_expire_datetime,
            expiry.option_last_exercise_datetime,
            expiry.option_expire_rest_days,
            CASE WHEN {_quote_has_market_data_sql("q")} THEN q.source_datetime END AS source_datetime,
            q.received_at,
            q.last_price AS quote_last_price,
            COALESCE(q.last_price, k.last_kline_close_price) AS last_price,
            q.ask_price1,
            q.bid_price1,
            q.close_price,
            q.average_price,
            q.ask_volume1,
            q.bid_volume1,
            COALESCE(NULLIF(q.volume, 0), k.last_kline_volume, q.volume) AS volume,
            q.open_interest,
            k.last_kline_bar_datetime,
            k.last_kline_close_price,
            k.last_kline_volume,
            q.raw_payload_json AS raw_payload_json,
            CASE
                WHEN ? IS NOT NULL AND q.received_at >= ? THEN 1
                ELSE 0
            END AS realtime_quote_fresh,
            CASE
                WHEN ? IS NOT NULL AND q.received_at >= ? THEN q.source_datetime
            END AS realtime_source_datetime,
            CASE
                WHEN ? IS NOT NULL AND q.received_at >= ? THEN q.received_at
            END AS realtime_received_at
        FROM instruments i
        LEFT JOIN quote_current q ON q.symbol = i.symbol
        LEFT JOIN (
            SELECT
                o.underlying_symbol,
                MIN(COALESCE(NULLIF(o.expire_datetime, ''), json_extract(oq.raw_payload_json, '$.expire_datetime'))) AS option_expire_datetime,
                MIN(COALESCE(NULLIF(o.last_exercise_datetime, ''), json_extract(oq.raw_payload_json, '$.last_exercise_datetime'))) AS option_last_exercise_datetime,
                MIN(CAST(json_extract(oq.raw_payload_json, '$.expire_rest_days') AS INTEGER)) AS option_expire_rest_days
            FROM instruments o
            LEFT JOIN quote_current oq ON oq.symbol = o.symbol
            WHERE o.active = 1
              AND o.underlying_symbol = ?
              AND o.option_class IN ('CALL', 'PUT')
            GROUP BY o.underlying_symbol
        ) expiry ON expiry.underlying_symbol = i.symbol
        LEFT JOIN (
            SELECT k1.symbol,
                   k1.bar_datetime AS last_kline_bar_datetime,
                   k1.close_price AS last_kline_close_price,
                   k1.volume AS last_kline_volume
            FROM kline_20d_current k1
            WHERE k1.symbol = ?
            ORDER BY k1.bar_datetime DESC
            LIMIT 1
        ) k ON k.symbol = i.symbol
        WHERE i.symbol = ?
        """,
        (
            realtime_started_at,
            realtime_started_at,
            realtime_started_at,
            realtime_started_at,
            realtime_started_at,
            realtime_started_at,
            symbol,
            symbol,
            symbol,
        ),
    ).fetchone()
    if row is None:
        return {"symbol": symbol, "missing": True}
    data = _row_dict(row)
    data["moneyness_reference_price"] = underlying_reference_price(
        {
            "last_price": data.get("quote_last_price"),
            "ask_price1": data.get("ask_price1"),
            "bid_price1": data.get("bid_price1"),
            "close_price": data.get("close_price"),
            "average_price": data.get("average_price"),
            "raw_payload_json": data.get("raw_payload_json"),
        }
    )
    data["expiry_month"] = _expiry_month(symbol)
    data["product_display_name"] = _product_display_name(
        data.get("product_id"),
        instrument_name=data.get("instrument_name"),
        raw_payload_json=data.get("raw_payload_json"),
    )
    data["future_expire_datetime"] = _expiry_date_text(data.get("future_expire_datetime"))
    data["option_expire_datetime"] = _expiry_date_text(
        data.get("option_expire_datetime")
    )
    data["option_last_exercise_datetime"] = _expiry_date_text(
        data.get("option_last_exercise_datetime")
    )
    data["days_to_option_expire_datetime"] = _expiry_days(
        data.get("option_expire_datetime"),
        fallback_days=data.get("option_expire_rest_days"),
    )
    data["days_to_option_last_exercise_datetime"] = _days_to_expiry(
        data.get("option_last_exercise_datetime")
    )
    expiry_datetime = data.get("option_expire_datetime") or data.get(
        "future_expire_datetime"
    )
    data["expire_datetime"] = expiry_datetime
    data["days_to_expiry"] = _expiry_days(
        expiry_datetime,
        fallback_days=data.get("option_expire_rest_days"),
    )
    data["last_kline_display_datetime"] = _daily_kline_close_datetime(
        data.get("last_kline_bar_datetime"),
        reference_datetime=data.get("source_datetime") or data.get("received_at"),
    )
    _apply_trading_session_state(
        data,
        data.get("raw_payload_json"),
        quote_source_datetime=(
            data.get("realtime_source_datetime")
            if realtime_started_at is not None
            else data.get("source_datetime")
        ),
    )
    if (
        realtime_started_at is not None
        and not allow_historical_realtime_data
        and not data.get("realtime_quote_fresh")
    ):
        _mask_stale_realtime_quote_fields(data)
    return data


def _option_rows(
    connection: sqlite3.Connection,
    underlying_symbol: str,
    *,
    realtime_started_at: str | None = None,
    allow_historical_realtime_data: bool = False,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        f"""
        SELECT
            i.symbol,
            i.underlying_symbol,
            i.option_class,
            i.strike_price,
            COALESCE(NULLIF(i.expire_datetime, ''), json_extract(q.raw_payload_json, '$.expire_datetime')) AS expire_datetime,
            COALESCE(NULLIF(i.last_exercise_datetime, ''), json_extract(q.raw_payload_json, '$.last_exercise_datetime')) AS last_exercise_datetime,
            json_extract(q.raw_payload_json, '$.expire_rest_days') AS expire_rest_days,
            COALESCE(i.exercise_year, json_extract(q.raw_payload_json, '$.exercise_year')) AS exercise_year,
            COALESCE(i.exercise_month, json_extract(q.raw_payload_json, '$.exercise_month')) AS exercise_month,
            q.bid_price1,
            q.ask_price1,
            COALESCE(q.last_price, k.close_price) AS last_price,
            q.bid_volume1,
            q.ask_volume1,
            COALESCE(NULLIF(q.volume, 0), k.volume, q.volume) AS volume,
            q.open_interest,
            CASE WHEN {_quote_has_market_data_sql("q")} THEN q.source_datetime END AS source_datetime,
            q.received_at AS quote_received_at,
            k.bar_datetime AS last_kline_bar_datetime,
            k.close_price AS last_kline_close_price,
            k.volume AS last_kline_volume,
            m.delta,
            m.gamma,
            m.theta,
            m.vega,
            m.rho,
            m.iv,
            m.source_method,
            m.received_at AS metrics_received_at,
            CASE WHEN k.symbol IS NULL THEN 0 ELSE 1 END AS has_kline,
            CASE
                WHEN ? IS NOT NULL AND q.received_at >= ? THEN 1
                ELSE 0
            END AS realtime_quote_fresh,
            CASE
                WHEN ? IS NOT NULL AND q.received_at >= ? THEN q.source_datetime
            END AS realtime_source_datetime,
            CASE
                WHEN ? IS NOT NULL AND q.received_at >= ? THEN q.received_at
            END AS realtime_received_at,
            CASE
                WHEN ? IS NOT NULL AND m.received_at >= ? THEN 1
                ELSE 0
            END AS realtime_metrics_fresh
        FROM instruments i
        LEFT JOIN quote_current q ON q.symbol = i.symbol
        LEFT JOIN option_source_metrics_current m ON m.symbol = i.symbol
        LEFT JOIN kline_20d_current k ON k.symbol = i.symbol
            AND k.bar_datetime = (
                SELECT MAX(k2.bar_datetime)
                FROM kline_20d_current k2
                WHERE k2.symbol = i.symbol
            )
        WHERE i.active = 1
          AND i.underlying_symbol = ?
          AND i.option_class IN ('CALL', 'PUT')
        ORDER BY i.strike_price DESC, i.option_class, i.symbol
        """,
        (
            realtime_started_at,
            realtime_started_at,
            realtime_started_at,
            realtime_started_at,
            realtime_started_at,
            realtime_started_at,
            realtime_started_at,
            realtime_started_at,
            underlying_symbol,
        ),
    ).fetchall()
    return [
        _format_option_row(
            row,
            mask_stale_realtime=realtime_started_at is not None,
            allow_historical_realtime_data=allow_historical_realtime_data,
        )
        for row in rows
    ]


def _format_option_row(
    row: sqlite3.Row,
    *,
    mask_stale_realtime: bool = False,
    allow_historical_realtime_data: bool = False,
) -> dict[str, Any]:
    data = _row_dict(row)
    data["expire_datetime"] = _expiry_date_text(data.get("expire_datetime"))
    data["last_exercise_datetime"] = _expiry_date_text(
        data.get("last_exercise_datetime")
    )
    data["days_to_expire_datetime"] = _expiry_days(
        data.get("expire_datetime"),
        fallback_days=data.get("expire_rest_days"),
    )
    data["days_to_last_exercise_datetime"] = _days_to_expiry(
        data.get("last_exercise_datetime")
    )
    if mask_stale_realtime and not allow_historical_realtime_data:
        if not data.get("realtime_quote_fresh"):
            _mask_stale_realtime_quote_fields(data)
            data["has_kline"] = 0
            data["last_kline_bar_datetime"] = None
            data["last_kline_close_price"] = None
            data["last_kline_volume"] = None
        if not data.get("realtime_metrics_fresh"):
            _mask_stale_realtime_metrics_fields(data)
    return data


def _apply_moneyness_subscription_scope(
    option_rows: list[dict[str, Any]],
    *,
    underlying_symbol: str,
    underlying_price: Any,
    moneyness_filter: str | set[str] | list[str] | tuple[str, ...] | None,
    mask_out_of_scope: bool,
) -> float | None:
    selected = normalize_moneyness_filter(moneyness_filter)

    reference_price = _float_or_none(underlying_price)
    classification = (
        build_moneyness_classification(
            option_rows,
            underlying_prices={underlying_symbol: reference_price},
        )
        if reference_price is not None
        else None
    )
    for row in option_rows:
        symbol = str(row.get("symbol") or "")
        moneyness = (
            classification.classifications.get(symbol)
            if classification is not None
            else None
        )
        in_scope = is_all_moneyness(selected) or moneyness in selected
        row["moneyness"] = moneyness
        row["atm_strike"] = (
            classification.symbol_atm_strikes.get(symbol)
            if classification is not None
            else None
        )
        row["moneyness_in_subscription_scope"] = in_scope
        if mask_out_of_scope and not in_scope:
            _mask_unsubscribed_moneyness_fields(row)
    return classification.primary_atm_strike if classification is not None else None


def _mask_unsubscribed_moneyness_fields(data: dict[str, Any]) -> None:
    _mask_stale_realtime_quote_fields(data)
    _mask_stale_realtime_metrics_fields(data)
    data["has_kline"] = 0
    data["last_kline_bar_datetime"] = None
    data["last_kline_close_price"] = None
    data["last_kline_volume"] = None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _mask_stale_realtime_quote_fields(data: dict[str, Any]) -> None:
    for key in (
        "bid_price1",
        "ask_price1",
        "last_price",
        "bid_volume1",
        "ask_volume1",
        "volume",
        "open_interest",
        "source_datetime",
        "quote_received_at",
        "received_at",
    ):
        data[key] = None


def _mask_stale_realtime_metrics_fields(data: dict[str, Any]) -> None:
    for key in (
        "delta",
        "gamma",
        "theta",
        "vega",
        "rho",
        "iv",
        "source_method",
        "metrics_received_at",
    ):
        data[key] = None


def _apply_trading_session_state(
    data: dict[str, Any],
    raw_payload_json: Any,
    *,
    quote_source_datetime: str | None = None,
    quote_pending: bool = False,
) -> None:
    state = trading_session_state_from_payload(
        str(raw_payload_json) if raw_payload_json is not None else None,
        quote_source_datetime=quote_source_datetime,
    )
    if quote_source_datetime:
        display_state = state.state
    elif quote_pending:
        display_state = SESSION_UNKNOWN
    else:
        display_state = SESSION_OUT
    labels = {
        SESSION_IN: "交易中",
        SESSION_OUT: "休市",
        SESSION_UNKNOWN: "待行情",
    }
    data["trading_session_state"] = display_state
    data["trading_session_label"] = labels.get(display_state, "待行情")
    data["has_night_session"] = state.has_night
    data["trading_day_segments"] = [
        f"{start}-{end}" for start, end in state.day
    ]
    data["trading_night_segments"] = [
        f"{start}-{end}" for start, end in state.night
    ]


def _apply_realtime_chain_status(
    underlying: dict[str, Any],
    option_rows: list[dict[str, Any]],
    *,
    realtime_started_at: str | None,
    subscription_progress: dict[str, Any] | None = None,
) -> None:
    if not realtime_started_at or underlying.get("missing"):
        underlying["realtime_status"] = "未订阅"
        underlying["realtime_subscribed"] = False
        underlying["realtime_started_at"] = realtime_started_at
        underlying["realtime_latest_quote_received_at"] = None
        underlying["realtime_latest_source_datetime"] = None
        return

    fresh_rows: list[dict[str, Any]] = []
    if underlying.get("realtime_quote_fresh"):
        fresh_rows.append(underlying)
    fresh_rows.extend(row for row in option_rows if row.get("realtime_quote_fresh"))
    latest_received = _max_text(row.get("realtime_received_at") for row in fresh_rows)
    latest_source = _max_text(row.get("realtime_source_datetime") for row in fresh_rows)
    quote_subscribed = _subscription_progress_quote_subscribed(subscription_progress)
    if quote_subscribed and latest_received is None:
        current_rows = [underlying, *option_rows]
        latest_received = _max_text(
            row.get("quote_received_at") or row.get("received_at")
            for row in current_rows
        )
        latest_source = _max_text(row.get("source_datetime") for row in current_rows)
    is_fresh = latest_received is not None
    underlying["realtime_status"] = "正常" if (is_fresh or quote_subscribed) else "未订阅"
    underlying["realtime_subscribed"] = is_fresh or quote_subscribed
    underlying["realtime_started_at"] = realtime_started_at
    underlying["realtime_latest_quote_received_at"] = latest_received
    underlying["realtime_latest_source_datetime"] = latest_source


def _max_text(values: Any) -> str | None:
    texts = [str(value) for value in values if value is not None and str(value)]
    return max(texts) if texts else None


def _quote_has_market_data_sql(alias: str) -> str:
    return (
        f"({alias}.last_price IS NOT NULL "
        f"OR {alias}.ask_price1 IS NOT NULL "
        f"OR {alias}.bid_price1 IS NOT NULL "
        f"OR {alias}.close_price IS NOT NULL "
        f"OR {alias}.average_price IS NOT NULL)"
    )


def _strike_rows(option_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_strike: dict[float, dict[str, Any]] = {}
    for option in option_rows:
        strike = option["strike_price"]
        if strike is None:
            continue
        strike_value = float(strike)
        row = by_strike.setdefault(
            strike_value,
            {"strike_price": strike_value, "CALL": None, "PUT": None, "is_atm": False},
        )
        row[str(option["option_class"])] = option
        if option.get("moneyness") == MONEYNESS_ATM:
            row["is_atm"] = True
    return [by_strike[strike] for strike in sorted(by_strike, reverse=True)]


def _selector_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = _underlying_rows(connection, limit=500)
    return [
        {
            "symbol": row["underlying_symbol"],
            "exchange_id": row["exchange_id"],
            "product_id": row["product_id"],
            "expiry_month": row["expiry_month"],
            "expire_datetime": row.get("expire_datetime"),
            "days_to_expiry": row.get("days_to_expiry"),
            "status": row["status"],
        }
        for row in rows
    ]


def _default_underlying(connection: sqlite3.Connection) -> str | None:
    row = connection.execute(
        """
        SELECT i.symbol
        FROM instruments i
        JOIN quote_current q ON q.symbol = i.symbol
        WHERE i.active = 1
          AND i.ins_class = 'FUTURE'
          AND EXISTS (
            SELECT 1
            FROM instruments o
            WHERE o.underlying_symbol = i.symbol
              AND o.active = 1
              AND o.option_class IN ('CALL', 'PUT')
          )
        ORDER BY q.received_at DESC, i.symbol
        LIMIT 1
        """
    ).fetchone()
    if row is not None:
        return str(row["symbol"])
    row = connection.execute(
        """
        SELECT underlying_symbol
        FROM instruments
        WHERE active = 1 AND option_class IN ('CALL', 'PUT')
        GROUP BY underlying_symbol
        ORDER BY underlying_symbol
        LIMIT 1
        """
    ).fetchone()
    return str(row["underlying_symbol"]) if row is not None else None


def _latest_update(connection: sqlite3.Connection) -> str | None:
    row = connection.execute(
        """
        SELECT MAX(value) AS latest_update
        FROM (
            SELECT MAX(received_at) AS value FROM quote_current
            UNION ALL
            SELECT MAX(received_at) AS value FROM kline_20d_current
            UNION ALL
            SELECT MAX(received_at) AS value FROM option_source_metrics_current
            UNION ALL
            SELECT MAX(finished_at) AS value FROM acquisition_runs
        )
        """
    ).fetchone()
    return row["latest_update"] if row and row["latest_update"] else None


def _latest_quote_update(connection: sqlite3.Connection) -> str | None:
    row = connection.execute("SELECT MAX(received_at) AS latest_update FROM quote_current").fetchone()
    return row["latest_update"] if row and row["latest_update"] else None


def _daily_kline_close_datetime(
    value: Any,
    *,
    reference_datetime: Any = None,
) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
        close_dt = datetime.combine(parsed.date(), time(14, 59, 59), parsed.tzinfo)
        if _is_future_display_time(close_dt, reference_datetime):
            return _datetime_display_text(reference_datetime)
        return close_dt.isoformat()
    except ValueError:
        pass
    try:
        parsed_date = date.fromisoformat(text)
    except ValueError:
        return text
    close_dt = datetime.combine(parsed_date, time(14, 59, 59))
    if _is_future_display_time(close_dt, reference_datetime):
        return _datetime_display_text(reference_datetime)
    return close_dt.isoformat()


def _is_future_display_time(
    display_datetime: datetime,
    reference_datetime: Any,
) -> bool:
    reference = _parse_datetime(reference_datetime)
    if reference is None:
        return False
    return _wall_time(display_datetime) > _wall_time(reference)


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _wall_time(value: datetime) -> datetime:
    return value.replace(tzinfo=None)


def _datetime_display_text(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return str(value).strip() if value is not None else None
    return parsed.isoformat()


def _product_display_name(
    product_id: Any,
    *,
    instrument_name: Any = None,
    raw_payload_json: Any = None,
) -> str | None:
    raw_name = _text_or_none(instrument_name)
    if raw_name is None:
        raw_name = _instrument_name_from_payload(raw_payload_json)
    if raw_name is None:
        return None
    product = str(product_id or "").strip()
    if product:
        product_prefix = re.escape(product)
        raw_name = re.sub(
            rf"^(?:{product_prefix}|{product_prefix.upper()})",
            "",
            raw_name,
            count=1,
        )
    display = re.sub(r"\d{3,4}$", "", raw_name).strip()
    return display or raw_name


def _instrument_name_from_payload(raw_payload_json: Any) -> str | None:
    if raw_payload_json is None:
        return None
    try:
        payload = json.loads(str(raw_payload_json))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return _text_or_none(payload.get("instrument_name"))


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_expiry_date(value: Any) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"\d{8}", text):
        try:
            return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
        except ValueError:
            return None
    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        try:
            return datetime.fromtimestamp(float(text)).date()
        except (OverflowError, OSError, ValueError):
            return None
    parsed_datetime = _parse_datetime(text)
    if parsed_datetime is not None:
        return parsed_datetime.date()
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _expiry_date_text(value: Any) -> str | None:
    parsed = _parse_expiry_date(value)
    return parsed.isoformat() if parsed is not None else None


def _days_to_expiry(value: Any) -> int | None:
    parsed = _parse_expiry_date(value)
    if parsed is None:
        return None
    return (parsed - date.today()).days


def _expiry_days(value: Any, *, fallback_days: Any = None) -> int | None:
    days = _days_to_expiry(value)
    if days is not None:
        return days
    try:
        return int(fallback_days)
    except (TypeError, ValueError):
        return None


def _maxima(strike_rows: list[dict[str, Any]]) -> dict[str, float]:
    maxima = _empty_maxima()
    for row in strike_rows:
        for side in ("CALL", "PUT"):
            option = row.get(side)
            if not option:
                continue
            maxima["volume"] = max(maxima["volume"], _num(option.get("volume")))
            maxima["open_interest"] = max(
                maxima["open_interest"],
                _num(option.get("open_interest")),
            )
            maxima["bid_volume1"] = max(
                maxima["bid_volume1"],
                _num(option.get("bid_volume1")),
            )
            maxima["ask_volume1"] = max(
                maxima["ask_volume1"],
                _num(option.get("ask_volume1")),
            )
    return maxima


def _empty_maxima() -> dict[str, float]:
    return {
        "volume": 0.0,
        "open_interest": 0.0,
        "bid_volume1": 0.0,
        "ask_volume1": 0.0,
    }


def _status_from_counts(data: dict[str, Any], option_count: int) -> str:
    if option_count <= 0:
        return "无合约"
    if int(data["quote_count"]) == option_count and int(data["metrics_count"]) == option_count:
        return "正常"
    if int(data["quote_count"]) or int(data["metrics_count"]) or int(data["kline_count"]):
        return "数据缺口"
    return "未采集"


def _subscription_status_from_counts(
    data: dict[str, Any],
    option_count: int,
    *,
    lifecycle_status: SubscriptionLifecycleStatus | str | None = None,
    subscription_progress: dict[str, Any] | None = None,
) -> str:
    if option_count <= 0:
        return "无合约"
    progress_status = _coerce_subscription_lifecycle_status(
        (subscription_progress or {}).get("status")
    )
    if progress_status is not None:
        return _subscription_status_from_progress(
            subscription_progress,
            fallback=progress_status,
        )
    lifecycle = _coerce_subscription_lifecycle_status(lifecycle_status)
    if lifecycle == SubscriptionLifecycleStatus.SUBSCRIBED:
        return "已订阅"
    if lifecycle == SubscriptionLifecycleStatus.SUBSCRIBING:
        return "订阅中"
    if lifecycle == SubscriptionLifecycleStatus.PENDING:
        return "待订阅"
    current_quote_count = int(data.get("current_quote_count") or 0)
    if current_quote_count >= option_count:
        return "已订阅"
    if current_quote_count > 0:
        return "订阅中"
    return "待订阅"


def _subscription_status_from_progress(
    progress: dict[str, Any] | None,
    *,
    fallback: SubscriptionLifecycleStatus,
) -> str:
    if not isinstance(progress, dict):
        return {
            SubscriptionLifecycleStatus.SUBSCRIBED: "已订阅",
            SubscriptionLifecycleStatus.SUBSCRIBING: "订阅中",
            SubscriptionLifecycleStatus.PENDING: "待订阅",
        }[fallback]
    quote_total = int(progress.get("quote_total") or 0)
    quote_subscribed = int(progress.get("quote_subscribed") or 0)
    kline_total = int(progress.get("kline_total") or 0)
    kline_subscribed = int(progress.get("kline_subscribed") or 0)
    if quote_total > 0 and quote_subscribed < quote_total:
        return "订阅中" if quote_subscribed > 0 else "待订阅"
    if quote_total > 0 and kline_total <= 0:
        return "订阅中"
    if kline_total > 0 and kline_subscribed < kline_total:
        return "订阅中"
    if (quote_total > 0 or kline_total > 0) and (
        quote_subscribed >= quote_total and kline_subscribed >= kline_total
    ):
        return "已订阅"
    if fallback == SubscriptionLifecycleStatus.SUBSCRIBED:
        return "已订阅"
    if fallback == SubscriptionLifecycleStatus.SUBSCRIBING:
        return "订阅中"
    return "待订阅"


def _subscription_quote_pending(
    status: str,
    subscription_progress: dict[str, Any] | None,
) -> bool:
    if isinstance(subscription_progress, dict):
        quote_total = int(subscription_progress.get("quote_total") or 0)
        quote_subscribed = int(subscription_progress.get("quote_subscribed") or 0)
        if quote_total > 0:
            return quote_subscribed < quote_total
    return status not in {"正常", "已订阅", "K线待计算", "K线订阅中"}


def _subscription_progress_for_row(
    progress: dict[str, Any] | None,
    underlying_symbol: str,
) -> dict[str, Any] | None:
    if not isinstance(progress, dict):
        return None
    item = progress.get(underlying_symbol)
    return item if isinstance(item, dict) else None


def _apply_subscription_progress(
    data: dict[str, Any],
    progress: dict[str, Any] | None,
) -> None:
    if not isinstance(progress, dict):
        data["subscription_quote_subscribed"] = None
        data["subscription_quote_total"] = None
        data["subscription_kline_subscribed"] = None
        data["subscription_kline_total"] = None
        data["subscription_kline_call_subscribed"] = None
        data["subscription_kline_call_total"] = None
        data["subscription_kline_put_subscribed"] = None
        data["subscription_kline_put_total"] = None
        data["subscription_subscribed"] = None
        data["subscription_total"] = None
        data["subscription_completion_ratio"] = None
        return
    quote_subscribed = int(progress.get("quote_subscribed") or 0)
    quote_total = int(progress.get("quote_total") or 0)
    kline_subscribed = int(progress.get("kline_subscribed") or 0)
    kline_total = int(progress.get("kline_total") or 0)
    kline_call_subscribed = int(progress.get("kline_call_subscribed") or 0)
    kline_call_total = int(progress.get("kline_call_total") or 0)
    kline_put_subscribed = int(progress.get("kline_put_subscribed") or 0)
    kline_put_total = int(progress.get("kline_put_total") or 0)
    subscribed = int(progress.get("subscribed_objects") or 0)
    total = int(progress.get("total_objects") or 0)
    data["subscription_quote_subscribed"] = quote_subscribed
    data["subscription_quote_total"] = quote_total
    data["subscription_kline_subscribed"] = kline_subscribed
    data["subscription_kline_total"] = kline_total
    data["subscription_kline_call_subscribed"] = kline_call_subscribed
    data["subscription_kline_call_total"] = kline_call_total
    data["subscription_kline_put_subscribed"] = kline_put_subscribed
    data["subscription_kline_put_total"] = kline_put_total
    data["subscription_subscribed"] = subscribed
    data["subscription_total"] = total
    data["subscription_completion_ratio"] = subscribed / total if total else 0.0


def _subscription_progress_quote_subscribed(progress: dict[str, Any] | None) -> bool:
    if not isinstance(progress, dict):
        return False
    quote_total = int(progress.get("quote_total") or 0)
    quote_subscribed = int(progress.get("quote_subscribed") or 0)
    if quote_total > 0 and quote_subscribed >= quote_total:
        return True
    return (
        _coerce_subscription_lifecycle_status(progress.get("status"))
        == SubscriptionLifecycleStatus.SUBSCRIBED
    )


def _coerce_subscription_lifecycle_status(
    value: SubscriptionLifecycleStatus | str | None,
) -> SubscriptionLifecycleStatus | None:
    if value is None:
        return None
    if isinstance(value, SubscriptionLifecycleStatus):
        return value
    try:
        return SubscriptionLifecycleStatus(str(value))
    except ValueError:
        return None


def _expiry_month(symbol: str) -> str | None:
    instrument_id = symbol.split(".", 1)[-1]
    match = re.search(r"([0-9]{3,4})", instrument_id)
    if not match:
        return None
    digits = match.group(1)
    if len(digits) == 3:
        return f"2{digits[0]}{digits[1:]}"
    return digits


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def _scalar(
    connection: sqlite3.Connection,
    sql: str,
    params: tuple[Any, ...] = (),
) -> int:
    row = connection.execute(sql, params).fetchone()
    return int(row[0] or 0)


def _row_dict(row: sqlite3.Row) -> dict[str, Any]:
    return dict(row)


def _num(value: Any) -> float:
    return float(value) if value is not None else 0.0
