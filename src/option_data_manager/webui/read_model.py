"""SQLite read models for the local WebUI."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
import re
import sqlite3
from typing import Any

from option_data_manager.acquisition import AcquisitionRepository
from option_data_manager.collection_state import CollectionStateRepository
from option_data_manager.instruments import InstrumentRepository
from option_data_manager.klines import KlineRepository
from option_data_manager.option_metrics import OptionMetricsRepository
from option_data_manager.quotes import QuoteRepository
from option_data_manager.service_state import ServiceLogRepository


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
    ) -> dict[str, Any]:
        """Return acquisition health summarized by underlying contract."""

        rows = _underlying_rows(self.connection, limit=limit)
        totals = _overview_totals(self.connection)
        collection = (
            _preferred_collection_progress(self.connection)
            if prefer_parallel_collection
            else _collection_progress(self.connection)
        )
        return {
            "summary": {
                **totals,
                "latest_update": _latest_update(self.connection),
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
            "exchanges": _exchange_rows(self.connection),
            "underlyings": rows,
        }

    def tquote(
        self,
        *,
        underlying_symbol: str | None = None,
        include_selectors: bool = True,
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
        underlying = _underlying_summary(self.connection, selected)
        option_rows = _option_rows(self.connection, selected)
        strike_rows = _strike_rows(option_rows)
        atm_strike = _atm_strike(strike_rows, underlying.get("last_price"))
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


def _overview_totals(connection: sqlite3.Connection) -> dict[str, int]:
    active_options = _scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM instruments
        WHERE active = 1 AND option_class IN ('CALL', 'PUT')
        """,
    )
    option_quote_rows = _scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM instruments i
        JOIN quote_current q ON q.symbol = i.symbol
        WHERE i.active = 1 AND i.option_class IN ('CALL', 'PUT')
          AND {_quote_has_market_data_sql("q")}
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
    quote_symbol_rows = _scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM instruments i
        JOIN quote_current q ON q.symbol = i.symbol
        WHERE i.active = 1
          AND (i.ins_class = 'FUTURE' OR i.option_class IN ('CALL', 'PUT'))
        """,
    )
    quote_symbol_market_rows = _scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM instruments i
        JOIN quote_current q ON q.symbol = i.symbol
        WHERE i.active = 1
          AND (i.ins_class = 'FUTURE' OR i.option_class IN ('CALL', 'PUT'))
          AND {_quote_has_market_data_sql("q")}
        """,
    )
    option_kline_symbols = _scalar(
        connection,
        """
        SELECT COUNT(DISTINCT k.symbol)
        FROM instruments i
        JOIN kline_20d_current k ON k.symbol = i.symbol
        WHERE i.active = 1 AND i.option_class IN ('CALL', 'PUT')
        """,
    )
    rows_with_greeks = _scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM instruments i
        JOIN option_source_metrics_current m ON m.symbol = i.symbol
        WHERE i.active = 1
          AND i.option_class IN ('CALL', 'PUT')
          AND (
            m.delta IS NOT NULL OR m.gamma IS NOT NULL OR m.theta IS NOT NULL
            OR m.vega IS NOT NULL OR m.rho IS NOT NULL
          )
        """,
    )
    return {
        "active_underlyings": _scalar(
            connection,
            """
            SELECT COUNT(DISTINCT underlying_symbol)
            FROM instruments
            WHERE active = 1 AND option_class IN ('CALL', 'PUT')
            """,
        ),
        "active_options": active_options,
        "active_quote_symbols": active_quote_symbols,
        "option_quote_rows": option_quote_rows,
        "quote_symbol_rows": quote_symbol_rows,
        "quote_symbol_market_rows": quote_symbol_market_rows,
        "option_kline_symbols": option_kline_symbols,
        "metrics_rows": _scalar(
            connection,
            """
            SELECT COUNT(*)
            FROM instruments i
            JOIN option_source_metrics_current m ON m.symbol = i.symbol
            WHERE i.active = 1 AND i.option_class IN ('CALL', 'PUT')
            """,
        ),
        "rows_with_greeks": rows_with_greeks,
        "rows_with_iv": _scalar(
            connection,
            """
            SELECT COUNT(*)
            FROM instruments i
            JOIN option_source_metrics_current m ON m.symbol = i.symbol
            WHERE i.active = 1
              AND i.option_class IN ('CALL', 'PUT')
              AND m.iv IS NOT NULL
            """,
        ),
        "acquisition_errors": _scalar(connection, "SELECT COUNT(*) FROM acquisition_errors"),
        "acquisition_runs": _scalar(connection, "SELECT COUNT(*) FROM acquisition_runs"),
        "latest_quote_update": _latest_quote_update(connection),
    }


def _collection_progress(
    connection: sqlite3.Connection,
    *,
    scope: str = "routine-market-current-slice",
) -> dict[str, Any]:
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
    }


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


def _underlying_rows(connection: sqlite3.Connection, *, limit: int) -> list[dict[str, Any]]:
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
            GROUP BY o.underlying_symbol, o.exchange_id, o.product_id
        ),
        coverage AS (
            SELECT
                i.underlying_symbol,
                COUNT(DISTINCT CASE
                    WHEN {_quote_has_market_data_sql("q")} THEN q.symbol
                END) AS quote_count,
                COUNT(DISTINCT m.symbol) AS metrics_count,
                COUNT(DISTINCT CASE WHEN m.iv IS NOT NULL THEN m.symbol END) AS iv_count,
                COUNT(DISTINCT k.symbol) AS kline_count,
                MAX(COALESCE(q.received_at, m.received_at, k.received_at)) AS latest_update,
                MAX(CASE
                    WHEN {_quote_has_market_data_sql("q")} THEN q.source_datetime
                END) AS latest_quote_time,
                MAX(k.bar_datetime) AS latest_kline_time
            FROM instruments i
            LEFT JOIN quote_current q ON q.symbol = i.symbol
            LEFT JOIN option_source_metrics_current m ON m.symbol = i.symbol
            LEFT JOIN kline_20d_current k ON k.symbol = i.symbol
            WHERE i.active = 1 AND i.option_class IN ('CALL', 'PUT')
            GROUP BY i.underlying_symbol
        ),
        underlying_quote AS (
            SELECT
                q.symbol,
                CASE WHEN {_quote_has_market_data_sql("q")} THEN q.source_datetime END AS source_datetime,
                q.received_at,
                q.last_price
            FROM quote_current q
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
            COALESCE(c.metrics_count, 0) AS metrics_count,
            COALESCE(c.iv_count, 0) AS iv_count,
            COALESCE(c.kline_count, 0) AS kline_count,
            c.latest_update,
            c.latest_quote_time,
            c.latest_kline_time,
            uq.source_datetime AS book_time,
            uq.last_price AS underlying_last,
            future.expire_datetime AS future_expire_datetime,
            future.delivery_year,
            future.delivery_month
        FROM option_counts oc
        LEFT JOIN coverage c ON c.underlying_symbol = oc.underlying_symbol
        LEFT JOIN underlying_quote uq ON uq.symbol = oc.underlying_symbol
        LEFT JOIN instruments future ON future.symbol = oc.underlying_symbol
        ORDER BY
            CASE WHEN COALESCE(c.quote_count, 0) > 0 THEN 0 ELSE 1 END,
            c.latest_update DESC,
            oc.exchange_id,
            oc.product_id,
            oc.underlying_symbol
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [_format_underlying_row(row) for row in rows]


def _format_underlying_row(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_dict(row)
    option_count = int(data["option_count"])
    data["expiry_month"] = _expiry_month(str(data["underlying_symbol"]))
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
    data["metrics_coverage"] = _ratio(int(data["metrics_count"]), option_count)
    data["iv_coverage"] = _ratio(int(data["iv_count"]), option_count)
    data["kline_coverage"] = _ratio(int(data["kline_count"]), option_count)
    data["display_market_time"] = data.get("book_time") or data.get("latest_quote_time")
    data["display_kline_time"] = _daily_kline_close_datetime(
        data.get("latest_kline_time"),
        reference_datetime=data.get("display_market_time") or data.get("latest_update"),
    )
    data["status"] = _status_from_counts(data, option_count)
    return data


def _exchange_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        f"""
        WITH options AS (
            SELECT *
            FROM instruments
            WHERE active = 1 AND option_class IN ('CALL', 'PUT')
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
        """
    ).fetchall()
    return [
        {
            **_row_dict(row),
            "quote_coverage": _ratio(int(row["quote_count"]), int(row["option_count"])),
            "iv_coverage": _ratio(int(row["iv_count"] or 0), int(row["option_count"])),
        }
        for row in rows
    ]


def _underlying_summary(connection: sqlite3.Connection, symbol: str) -> dict[str, Any]:
    row = connection.execute(
        f"""
        SELECT
            i.symbol,
            i.exchange_id,
            i.product_id,
            i.instrument_id,
            COALESCE(NULLIF(i.expire_datetime, ''), json_extract(q.raw_payload_json, '$.expire_datetime')) AS future_expire_datetime,
            COALESCE(i.delivery_year, json_extract(q.raw_payload_json, '$.delivery_year')) AS delivery_year,
            COALESCE(i.delivery_month, json_extract(q.raw_payload_json, '$.delivery_month')) AS delivery_month,
            expiry.option_expire_datetime,
            expiry.option_last_exercise_datetime,
            expiry.option_expire_rest_days,
            CASE WHEN {_quote_has_market_data_sql("q")} THEN q.source_datetime END AS source_datetime,
            q.received_at,
            COALESCE(q.last_price, k.last_kline_close_price) AS last_price,
            q.ask_price1,
            q.bid_price1,
            q.ask_volume1,
            q.bid_volume1,
            COALESCE(NULLIF(q.volume, 0), k.last_kline_volume, q.volume) AS volume,
            q.open_interest,
            k.last_kline_bar_datetime,
            k.last_kline_close_price,
            k.last_kline_volume
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
        (symbol, symbol, symbol),
    ).fetchone()
    if row is None:
        return {"symbol": symbol, "missing": True}
    data = _row_dict(row)
    data["expiry_month"] = _expiry_month(symbol)
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
    return data


def _option_rows(connection: sqlite3.Connection, underlying_symbol: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        f"""
        SELECT
            i.symbol,
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
            CASE WHEN k.symbol IS NULL THEN 0 ELSE 1 END AS has_kline
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
        (underlying_symbol,),
    ).fetchall()
    return [_format_option_row(row) for row in rows]


def _format_option_row(row: sqlite3.Row) -> dict[str, Any]:
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
    return data


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
            {"strike_price": strike_value, "CALL": None, "PUT": None},
        )
        row[str(option["option_class"])] = option
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


def _atm_strike(strike_rows: list[dict[str, Any]], last_price: Any) -> float | None:
    if not strike_rows:
        return None
    if last_price is None:
        middle = len(strike_rows) // 2
        return float(strike_rows[middle]["strike_price"])
    price = float(last_price)
    return float(
        min(strike_rows, key=lambda row: abs(float(row["strike_price"]) - price))[
            "strike_price"
        ]
    )


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


def _scalar(connection: sqlite3.Connection, sql: str) -> int:
    row = connection.execute(sql).fetchone()
    return int(row[0] or 0)


def _row_dict(row: sqlite3.Row) -> dict[str, Any]:
    return dict(row)


def _num(value: Any) -> float:
    return float(value) if value is not None else 0.0
