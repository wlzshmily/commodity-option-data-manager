"""FastAPI WebUI for inspecting current option data slices."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import subprocess

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, PlainTextResponse
import uvicorn

from option_data_manager.api.app import (
    QuoteStreamState,
    create_app as create_local_api_app,
)
from option_data_manager.realtime_health import build_realtime_health
from option_data_manager.service_state import ServiceStateRepository
from option_data_manager.sqlite_runtime import configure_sqlite_runtime
from .read_model import SubscriptionLifecycleStatus, WebuiReadModel


DEFAULT_DATABASE_PATH = "data/option-data-current.sqlite3"


def create_webui_app(
    connection: sqlite3.Connection,
    *,
    database_path: str | None = None,
) -> FastAPI:
    """Create the local WebUI and its read-only JSON endpoints."""

    connection.row_factory = sqlite3.Row
    configure_sqlite_runtime(connection, enable_wal=database_path not in (None, ":memory:"))
    read_model = WebuiReadModel(connection)
    service_state = ServiceStateRepository(connection)
    app = FastAPI(title="期权数据管理工具 WebUI")
    local_api = create_local_api_app(connection, database_path=database_path)
    for route in local_api.router.routes:
        if getattr(route, "path", "") == "/":
            continue
        app.router.routes.append(route)

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        initial_state = {
            "overview": None,
            "selectedUnderlying": None,
            "quote": None,
            "databasePath": database_path,
        }
        return INDEX_HTML.replace(
            "__ODM_INITIAL_STATE__",
            json.dumps(initial_state, ensure_ascii=False),
        )

    @app.get("/assets/webui.css", response_class=PlainTextResponse)
    def css() -> PlainTextResponse:
        return PlainTextResponse(WEBUI_CSS, media_type="text/css; charset=utf-8")

    @app.get("/assets/webui.js", response_class=PlainTextResponse)
    def js() -> PlainTextResponse:
        return PlainTextResponse(
            WEBUI_JS,
            media_type="application/javascript; charset=utf-8",
        )

    @app.get("/api/webui/overview")
    def overview(limit: int = Query(default=80, ge=1, le=500)) -> dict:
        active_read_model, read_connection = _request_read_model(
            read_model,
            database_path=database_path,
        )
        try:
            return {
                **_overview_payload(
                    active_read_model,
                    service_state=service_state,
                    database_path=database_path,
                    limit=limit,
                ),
            }
        finally:
            if read_connection is not None:
                read_connection.close()

    @app.get("/api/webui/tquote")
    def tquote(underlying: str | None = None, selectors: bool = False) -> dict:
        active_read_model, read_connection = _request_read_model(
            read_model,
            database_path=database_path,
        )
        try:
            quote_stream = _quote_stream_payload(
                service_state,
                connection=active_read_model.connection,
            )
            return active_read_model.tquote(
                underlying_symbol=underlying,
                include_selectors=selectors,
                realtime_started_at=_realtime_started_at(quote_stream),
                subscription_progress=(
                    quote_stream.get("progress") or {}
                ).get("underlying_progress"),
                moneyness_filter=(quote_stream.get("progress") or {}).get(
                    "moneyness_filter"
                ),
            )
        finally:
            if read_connection is not None:
                read_connection.close()

    @app.get("/api/webui/runs")
    def runs(limit: int = Query(default=30, ge=1, le=100)) -> dict:
        active_read_model, read_connection = _request_read_model(
            read_model,
            database_path=database_path,
        )
        try:
            return active_read_model.runs(limit=limit)
        finally:
            if read_connection is not None:
                read_connection.close()

    @app.get("/api/webui/api-summary")
    def api_page_summary() -> dict:
        summary = service_state.api_summary()
        return {
            "status": "ok",
            "database_path": database_path,
            "api": {
                "request_count": summary.request_count,
                "error_count": summary.error_count,
                "average_latency_ms": round(summary.average_latency_ms, 3),
            },
            "summary": {
                "active_options": _active_option_count(connection),
                "latest_quote_update": _latest_quote_update(connection),
            },
        }

    return app


def _request_read_model(
    fallback_read_model: WebuiReadModel,
    *,
    database_path: str | None,
) -> tuple[WebuiReadModel, sqlite3.Connection | None]:
    if not database_path or database_path == ":memory:":
        return (fallback_read_model, None)
    connection = sqlite3.connect(database_path, timeout=30)
    configure_sqlite_runtime(connection)
    return (WebuiReadModel(connection), connection)


def _overview_payload(
    read_model: WebuiReadModel,
    *,
    service_state: ServiceStateRepository,
    database_path: str | None,
    limit: int,
) -> dict:
    quote_stream = _quote_stream_payload(
        service_state,
        connection=read_model.connection,
    )
    payload = {
        "database_path": database_path,
        **read_model.overview(
            limit=limit,
            prefer_parallel_collection=True,
            current_quote_after=_realtime_started_at(quote_stream),
            require_current_quote=True,
            subscription_scope_enabled=_quote_stream_scope_active(quote_stream),
            subscription_contract_month_limit=_quote_stream_contract_month_limit(
                quote_stream
            ),
            subscription_min_days_to_expiry=_quote_stream_min_days_to_expiry(
                quote_stream
            ),
            subscription_lifecycle_status=_quote_stream_lifecycle_status(quote_stream),
            subscription_underlying_progress=(
                quote_stream.get("progress") or {}
            ).get("underlying_progress"),
            subscription_fast_totals=_quote_stream_scope_active(quote_stream),
        ),
        "refresh": {
            "running": (
                service_state.get_value("collection.refresh_running") or "false"
            )
            == "true",
            "message": service_state.get_value("collection.refresh_message"),
            "window_count": _int_or_none(
                service_state.get_value("collection.refresh_window_count")
            ),
            "remaining_batches": _int_or_none(
                service_state.get_value("collection.refresh_remaining_batches")
            ),
            "started_at": service_state.get_value("collection.refresh_started_at"),
            "finished_at": service_state.get_value("collection.refresh_finished_at"),
        },
        "quote_stream": quote_stream,
    }
    _align_summary_with_quote_stream_progress(payload, quote_stream)
    return payload


def _align_summary_with_quote_stream_progress(payload: dict, quote_stream: dict) -> None:
    if not _quote_stream_scope_active(quote_stream):
        return
    summary = payload.get("summary")
    progress = quote_stream.get("progress") or {}
    if not isinstance(summary, dict):
        return
    quote_total = int(progress.get("quote_total") or 0)
    kline_total = int(progress.get("kline_total") or 0)
    quote_subscribed = int(progress.get("quote_subscribed") or 0)
    kline_subscribed = int(progress.get("kline_subscribed") or 0)
    if quote_total > 0:
        summary["active_options"] = quote_total
        summary["option_quote_rows"] = min(quote_subscribed, quote_total)
    if kline_total > 0:
        summary["option_kline_symbols"] = min(kline_subscribed, kline_total)
    summary["quote_coverage"] = _ratio_dict_values(
        summary,
        numerator_key="option_quote_rows",
        denominator_key="active_options",
    )
    summary["kline_coverage"] = _ratio_dict_values(
        summary,
        numerator_key="option_kline_symbols",
        denominator_key="active_options",
    )


def _ratio_dict_values(
    data: dict,
    *,
    numerator_key: str,
    denominator_key: str,
) -> float:
    denominator = int(data.get(denominator_key) or 0)
    if denominator <= 0:
        return 0.0
    return round(int(data.get(numerator_key) or 0) / denominator, 4)


def _realtime_started_at(quote_stream: dict) -> str | None:
    if not _quote_stream_scope_active(quote_stream):
        return None
    progress = quote_stream.get("progress") or {}
    return (
        progress.get("started_at")
        or quote_stream.get("started_at")
        or progress.get("updated_at")
    )


def _quote_stream_scope_active(quote_stream: dict) -> bool:
    return bool(quote_stream.get("running")) or (
        _quote_stream_state_from_value(quote_stream.get("status"), allow_running=True)
        == QuoteStreamState.STARTING
    )


def _quote_stream_contract_month_limit(quote_stream: dict) -> int | None:
    progress = quote_stream.get("progress") or {}
    value = progress.get("contract_months") or quote_stream.get("contract_months")
    if value is None:
        return None
    text = str(value).strip().lower()
    if text == "all":
        return None
    try:
        parsed = int(text)
    except ValueError:
        return 2
    return parsed if parsed > 0 else None


def _quote_stream_min_days_to_expiry(quote_stream: dict) -> int:
    progress = quote_stream.get("progress") or {}
    for value in (
        progress.get("min_days_to_expiry"),
        quote_stream.get("min_days_to_expiry"),
    ):
        if value is None:
            continue
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        return max(parsed, 0)
    return 1


def _quote_stream_lifecycle_status(
    quote_stream: dict,
) -> SubscriptionLifecycleStatus | None:
    if not _quote_stream_scope_active(quote_stream):
        return None
    status = _quote_stream_state_from_value(quote_stream.get("status"), allow_running=True)
    progress = quote_stream.get("progress") or {}
    quote_total = int(progress.get("quote_total") or 0)
    kline_total = int(progress.get("kline_total") or 0)
    quote_subscribed = int(progress.get("quote_subscribed") or 0)
    kline_subscribed = int(progress.get("kline_subscribed") or 0)
    if (
        status == QuoteStreamState.RUNNING
        and quote_total > 0
        and kline_total > 0
        and quote_subscribed >= quote_total
        and kline_subscribed >= kline_total
    ):
        return SubscriptionLifecycleStatus.SUBSCRIBED
    if status == QuoteStreamState.STARTING:
        return SubscriptionLifecycleStatus.PENDING
    return SubscriptionLifecycleStatus.SUBSCRIBING


def _progress_with_cached_expected_counts(
    progress: dict,
    *,
    service_state: ServiceStateRepository,
) -> dict:
    quote_total = _int_or_none(service_state.get_value("quote_stream.expected_quote_total"))
    kline_total = _int_or_none(service_state.get_value("quote_stream.expected_kline_total"))
    if quote_total is None and kline_total is None:
        return progress
    enriched = dict(progress)
    enriched["quote_total"] = max(int(enriched.get("quote_total") or 0), quote_total or 0)
    enriched["kline_total"] = max(int(enriched.get("kline_total") or 0), kline_total or 0)
    enriched["total_objects"] = enriched["quote_total"] + enriched["kline_total"]
    enriched["subscribed_objects"] = (
        int(enriched.get("quote_subscribed") or 0)
        + int(enriched.get("kline_subscribed") or 0)
    )
    enriched["completion_ratio"] = (
        enriched["subscribed_objects"] / enriched["total_objects"]
        if enriched["total_objects"]
        else 0.0
    )
    enriched["contract_months"] = service_state.get_value("quote_stream.contract_months") or enriched.get("contract_months")
    enriched["min_days_to_expiry"] = _int_or_none(service_state.get_value("quote_stream.min_days_to_expiry")) or 1
    return enriched



def _int_or_none(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _json_int_list(value: str | None) -> list[int]:
    if not value:
        return []
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    result: list[int] = []
    for item in payload:
        try:
            result.append(int(item))
        except (TypeError, ValueError):
            continue
    return result


def _quote_stream_payload(
    service_state: ServiceStateRepository,
    *,
    connection: sqlite3.Connection | None = None,
) -> dict:
    pids = _json_int_list(service_state.get_value("quote_stream.pids"))
    live_pids = [pid for pid in pids if _pid_is_quote_stream(pid)]
    running = (
        (service_state.get_value("quote_stream.running") or "false") == "true"
        or bool(live_pids)
    )
    progress = _quote_stream_progress(
        service_state.get_value("quote_stream.report_dir"),
        running=running,
    )
    contract_months = service_state.get_value("quote_stream.contract_months")
    min_days_to_expiry = _int_or_none(
        service_state.get_value("quote_stream.min_days_to_expiry")
    )
    worker_count = _int_or_none(service_state.get_value("quote_stream.worker_count")) or len(live_pids)
    status = _quote_stream_status_value(service_state, running=running)
    status_state = _quote_stream_state_from_value(status)
    if connection is not None and (
        running or status_state == QuoteStreamState.STARTING
    ) and int(progress.get("worker_reports") or 0) == 0:
        progress = _progress_with_cached_expected_counts(
            progress,
            service_state=service_state,
        )
    return {
        "status": status,
        "running": running,
        "message": service_state.get_value("quote_stream.message"),
        "contract_months": contract_months,
        "min_days_to_expiry": min_days_to_expiry,
        "worker_count": worker_count,
        "pids": live_pids,
        "report_dir": service_state.get_value("quote_stream.report_dir"),
        "started_at": service_state.get_value("quote_stream.started_at"),
        "finished_at": service_state.get_value("quote_stream.finished_at"),
        "progress": progress,
        "health": build_realtime_health(
            connection,
            running=running,
            progress=progress,
        ),
        "active_option_count": _active_option_count(connection),
        "contract_discovery_running": (
            service_state.get_value("quote_stream.contract_discovery_running") or "false"
        )
        == "true",
        "contract_list_ready": (
            service_state.get_value("quote_stream.contract_list_ready") or "false"
        )
        == "true",
    }


def _quote_stream_status_value(
    service_state: ServiceStateRepository,
    *,
    running: bool,
) -> str:
    if running:
        return QuoteStreamState.RUNNING.value
    return _quote_stream_state_from_value(
        service_state.get_value("quote_stream.status")
    ).value


def _quote_stream_state_from_value(
    value: object,
    *,
    allow_running: bool = False,
) -> QuoteStreamState:
    try:
        state = QuoteStreamState(str(value))
    except ValueError:
        return QuoteStreamState.STOPPED
    if state == QuoteStreamState.RUNNING and not allow_running:
        return QuoteStreamState.STOPPED
    return state


def _active_option_count(connection: sqlite3.Connection | None) -> int | None:
    if connection is None:
        return None
    try:
        row = connection.execute(
            """
            SELECT COUNT(*)
            FROM instruments
            WHERE active = 1 AND option_class IN ('CALL', 'PUT')
            """
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    return int(row[0] or 0) if row is not None else 0


def _latest_quote_update(connection: sqlite3.Connection | None) -> str | None:
    if connection is None:
        return None
    try:
        row = connection.execute("SELECT MAX(received_at) FROM quote_current").fetchone()
    except sqlite3.OperationalError:
        return None
    return None if row is None else row[0]


def _quote_stream_progress(report_dir: str | None, *, running: bool) -> dict:
    if not running:
        return _empty_quote_stream_progress(running=running)
    if not report_dir:
        return _empty_quote_stream_progress(running=running)
    directory = Path(report_dir)
    if not directory.exists():
        return _empty_quote_stream_progress(running=running)
    rows: list[dict] = []
    for path in sorted(directory.glob("worker-*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        progress = _progress_from_report(payload)
        if progress:
            rows.append(progress)
    if not rows:
        return _empty_quote_stream_progress(running=running)
    quote_subscribed = sum(_int_value(row.get("quote_subscribed")) for row in rows)
    quote_total = sum(_int_value(row.get("quote_total")) for row in rows)
    kline_subscribed = sum(_int_value(row.get("kline_subscribed")) for row in rows)
    kline_total = sum(_int_value(row.get("kline_total")) for row in rows)
    near_expiry_quote_subscribed = sum(
        _int_value(row.get("near_expiry_quote_subscribed")) for row in rows
    )
    near_expiry_quote_total = sum(
        _int_value(row.get("near_expiry_quote_total")) for row in rows
    )
    near_expiry_kline_subscribed = sum(
        _int_value(row.get("near_expiry_kline_subscribed")) for row in rows
    )
    near_expiry_kline_total = sum(
        _int_value(row.get("near_expiry_kline_total")) for row in rows
    )
    near_expiry_subscribed = near_expiry_quote_subscribed + near_expiry_kline_subscribed
    near_expiry_total = near_expiry_quote_total + near_expiry_kline_total
    near_expiry_months = max(_int_value(row.get("near_expiry_months")) for row in rows)
    contract_months = _aggregate_contract_months(rows)
    subscribed_objects = quote_subscribed + kline_subscribed
    total_objects = quote_total + kline_total
    statuses = {str(row.get("status") or "") for row in rows}
    status = "running" if running else "stopped"
    if "failed" in statuses:
        status = "failed"
    elif running and "subscribing" in statuses:
        status = "subscribing"
    updated_values = [
        str(row.get("updated_at")) for row in rows if row.get("updated_at") is not None
    ]
    wait_update_values = [
        str(row.get("last_wait_update_at"))
        for row in rows
        if row.get("last_wait_update_at") is not None
    ]
    quote_write_values = [
        str(row.get("last_quote_write_at"))
        for row in rows
        if row.get("last_quote_write_at") is not None
    ]
    tqsdk_notify = _latest_tqsdk_notify(rows)
    tqsdk_connection_status = _aggregate_tqsdk_connection_status(rows)
    tqsdk_disconnect_values = [
        str(row.get("last_tqsdk_disconnect_at"))
        for row in rows
        if row.get("last_tqsdk_disconnect_at") is not None
    ]
    tqsdk_restore_values = [
        str(row.get("last_tqsdk_restore_at"))
        for row in rows
        if row.get("last_tqsdk_restore_at") is not None
    ]
    contract_refresh_values = [
        str(row.get("last_contract_refresh_at"))
        for row in rows
        if row.get("last_contract_refresh_at") is not None
    ]
    contract_reconcile_values = [
        str(row.get("last_contract_reconcile_at"))
        for row in rows
        if row.get("last_contract_reconcile_at") is not None
    ]
    started_values = [
        str(row.get("started_at")) for row in rows if row.get("started_at") is not None
    ]
    elapsed_seconds = _elapsed_seconds(
        min(started_values) if started_values else None,
        max(updated_values) if updated_values else None,
    )
    eta = _subscription_eta(
        rows,
        quote_subscribed=quote_subscribed,
        quote_total=quote_total,
        kline_subscribed=kline_subscribed,
        kline_total=kline_total,
    )
    underlying_progress = _aggregate_underlying_progress(rows)
    return {
        "status": status,
        "worker_reports": len(rows),
        "quote_subscribed": quote_subscribed,
        "quote_total": quote_total,
        "kline_subscribed": kline_subscribed,
        "kline_total": kline_total,
        "near_expiry_months": near_expiry_months,
        "near_expiry_quote_subscribed": near_expiry_quote_subscribed,
        "near_expiry_quote_total": near_expiry_quote_total,
        "near_expiry_kline_subscribed": near_expiry_kline_subscribed,
        "near_expiry_kline_total": near_expiry_kline_total,
        "near_expiry_subscribed": near_expiry_subscribed,
        "near_expiry_total": near_expiry_total,
        "contract_months": contract_months,
        "underlying_progress": underlying_progress,
        "subscribed_objects": subscribed_objects,
        "total_objects": total_objects,
        "completion_ratio": subscribed_objects / total_objects
        if total_objects
        else 0.0,
        "started_at": min(started_values) if started_values else None,
        "updated_at": max(updated_values) if updated_values else None,
        "last_wait_update_at": max(wait_update_values) if wait_update_values else None,
        "last_quote_write_at": max(quote_write_values) if quote_write_values else None,
        "last_tqsdk_notify_at": tqsdk_notify.get("last_tqsdk_notify_at"),
        "last_tqsdk_notify_code": tqsdk_notify.get("last_tqsdk_notify_code"),
        "last_tqsdk_notify_level": tqsdk_notify.get("last_tqsdk_notify_level"),
        "last_tqsdk_notify_content": tqsdk_notify.get("last_tqsdk_notify_content"),
        "tqsdk_connection_status": tqsdk_connection_status,
        "last_tqsdk_disconnect_at": max(tqsdk_disconnect_values)
        if tqsdk_disconnect_values
        else None,
        "last_tqsdk_restore_at": max(tqsdk_restore_values)
        if tqsdk_restore_values
        else None,
        "tqsdk_notify_count": sum(_int_value(row.get("tqsdk_notify_count")) for row in rows),
        "contract_refresh_count": sum(
            _int_value(row.get("contract_refresh_count")) for row in rows
        ),
        "last_contract_refresh_at": max(contract_refresh_values)
        if contract_refresh_values
        else None,
        "last_contract_reconcile_at": max(contract_reconcile_values)
        if contract_reconcile_values
        else None,
        "contract_reconcile_added_quote_count": sum(
            _int_value(row.get("contract_reconcile_added_quote_count"))
            for row in rows
        ),
        "contract_reconcile_removed_quote_count": sum(
            _int_value(row.get("contract_reconcile_removed_quote_count"))
            for row in rows
        ),
        "contract_reconcile_added_kline_count": sum(
            _int_value(row.get("contract_reconcile_added_kline_count"))
            for row in rows
        ),
        "contract_reconcile_removed_kline_count": sum(
            _int_value(row.get("contract_reconcile_removed_kline_count"))
            for row in rows
        ),
        "moneyness_filter": _aggregate_text_value(rows, "moneyness_filter"),
        "moneyness_recalc_seconds": max(
            _int_value(row.get("moneyness_recalc_seconds")) for row in rows
        ),
        "moneyness_recalc_count": sum(
            _int_value(row.get("moneyness_recalc_count")) for row in rows
        ),
        "moneyness_kline_match_count": sum(
            _int_value(row.get("moneyness_kline_match_count")) for row in rows
        ),
        "moneyness_sticky_kline_count": sum(
            _int_value(row.get("moneyness_sticky_kline_count")) for row in rows
        ),
        "moneyness_added_kline_count": sum(
            _int_value(row.get("moneyness_added_kline_count")) for row in rows
        ),
        "moneyness_skipped_out_of_session_count": sum(
            _int_value(row.get("moneyness_skipped_out_of_session_count"))
            for row in rows
        ),
        "moneyness_skipped_missing_price_count": sum(
            _int_value(row.get("moneyness_skipped_missing_price_count"))
            for row in rows
        ),
        "last_moneyness_recalc_at": _max_text_value(
            rows,
            "last_moneyness_recalc_at",
        ),
        "current_kline_symbol": _aggregate_text_value(rows, "current_kline_symbol"),
        "current_kline_started_at": _max_text_value(rows, "current_kline_started_at"),
        "kline_subscription_timeout_seconds": max(
            _int_value(row.get("kline_subscription_timeout_seconds")) for row in rows
        ),
        "kline_subscription_timeout_count": sum(
            _int_value(row.get("kline_subscription_timeout_count")) for row in rows
        ),
        "kline_subscription_error_count": sum(
            _int_value(row.get("kline_subscription_error_count")) for row in rows
        ),
        "last_kline_subscription_error_symbol": _aggregate_text_value(
            rows,
            "last_kline_subscription_error_symbol",
        ),
        "last_kline_subscription_error": _aggregate_text_value(
            rows,
            "last_kline_subscription_error",
        ),
        "cycle_count": sum(_int_value(row.get("cycle_count")) for row in rows),
        "wait_update_count": sum(_int_value(row.get("wait_update_count")) for row in rows),
        "quotes_written": sum(_int_value(row.get("quotes_written")) for row in rows),
        "changed_quotes_written": sum(
            _int_value(row.get("changed_quotes_written")) for row in rows
        ),
        "elapsed_seconds": elapsed_seconds,
        "active_stage": eta["active_stage"],
        "stage_label": eta["stage_label"],
        "stage_average_seconds_per_object": eta["stage_average_seconds_per_object"],
        "stage_estimated_remaining_seconds": eta["stage_estimated_remaining_seconds"],
        "estimated_remaining_seconds": eta["estimated_remaining_seconds"],
        "estimated_remaining_is_total": eta["estimated_remaining_is_total"],
        "waiting_for_kline_eta": eta["waiting_for_kline_eta"],
        "average_seconds_per_object": eta["stage_average_seconds_per_object"],
    }


def _progress_from_report(payload: dict) -> dict | None:
    progress = payload.get("progress")
    if isinstance(progress, dict):
        return progress
    result = payload.get("result")
    if not isinstance(result, dict):
        return None
    quote_total = _int_value(result.get("symbol_count"))
    kline_total = _int_value(result.get("kline_symbol_count"))
    quote_subscribed = _int_value(result.get("subscribed_quote_count"), quote_total)
    kline_subscribed = _int_value(result.get("subscribed_kline_count"), kline_total)
    near_expiry_quote_total = _int_value(
        result.get("near_expiry_quote_total"),
        _int_value(result.get("near_expiry_quote_count")),
    )
    near_expiry_kline_total = _int_value(
        result.get("near_expiry_kline_total"),
        _int_value(result.get("near_expiry_kline_count")),
    )
    near_expiry_quote_subscribed = _int_value(
        result.get("near_expiry_quote_subscribed"),
        near_expiry_quote_total,
    )
    near_expiry_kline_subscribed = _int_value(
        result.get("near_expiry_kline_subscribed"),
        near_expiry_kline_total,
    )
    near_expiry_subscribed = (
        near_expiry_quote_subscribed + near_expiry_kline_subscribed
    )
    near_expiry_total = near_expiry_quote_total + near_expiry_kline_total
    total_objects = quote_total + kline_total
    subscribed_objects = quote_subscribed + kline_subscribed
    return {
        "status": str(payload.get("status") or "stopped"),
        "started_at": result.get("started_at"),
        "updated_at": result.get("finished_at"),
        "last_wait_update_at": result.get("last_wait_update_at"),
        "last_quote_write_at": result.get("last_quote_write_at"),
        "last_tqsdk_notify_at": result.get("last_tqsdk_notify_at"),
        "last_tqsdk_notify_code": result.get("last_tqsdk_notify_code"),
        "last_tqsdk_notify_level": result.get("last_tqsdk_notify_level"),
        "last_tqsdk_notify_content": result.get("last_tqsdk_notify_content"),
        "tqsdk_connection_status": result.get("tqsdk_connection_status") or "unknown",
        "last_tqsdk_disconnect_at": result.get("last_tqsdk_disconnect_at"),
        "last_tqsdk_restore_at": result.get("last_tqsdk_restore_at"),
        "tqsdk_notify_count": _int_value(result.get("tqsdk_notify_count")),
        "contract_refresh_count": _int_value(result.get("contract_refresh_count")),
        "last_contract_refresh_at": result.get("last_contract_refresh_at"),
        "last_contract_reconcile_at": result.get("last_contract_reconcile_at"),
        "contract_reconcile_added_quote_count": _int_value(
            result.get("contract_reconcile_added_quote_count")
        ),
        "contract_reconcile_removed_quote_count": _int_value(
            result.get("contract_reconcile_removed_quote_count")
        ),
        "contract_reconcile_added_kline_count": _int_value(
            result.get("contract_reconcile_added_kline_count")
        ),
        "contract_reconcile_removed_kline_count": _int_value(
            result.get("contract_reconcile_removed_kline_count")
        ),
        "cycle_count": _int_value(result.get("cycles")),
        "wait_update_count": _int_value(result.get("wait_update_count")),
        "quotes_written": _int_value(result.get("quotes_written")),
        "changed_quotes_written": _int_value(result.get("changed_quotes_written")),
        "quote_started_at": result.get("quote_started_at") or result.get("started_at"),
        "quote_finished_at": result.get("quote_finished_at"),
        "kline_started_at": result.get("kline_started_at"),
        "quote_subscribed": quote_subscribed,
        "quote_total": quote_total,
        "kline_subscribed": kline_subscribed,
        "kline_total": kline_total,
        "near_expiry_months": _int_value(result.get("near_expiry_months")),
        "contract_months": _contract_months_from_result(result),
        "near_expiry_quote_subscribed": near_expiry_quote_subscribed,
        "near_expiry_quote_total": near_expiry_quote_total,
        "near_expiry_kline_subscribed": near_expiry_kline_subscribed,
        "near_expiry_kline_total": near_expiry_kline_total,
        "near_expiry_subscribed": near_expiry_subscribed,
        "near_expiry_total": near_expiry_total,
        "subscribed_objects": subscribed_objects,
        "total_objects": total_objects,
        "completion_ratio": subscribed_objects / total_objects
        if total_objects
        else 0.0,
        "moneyness_filter": result.get("moneyness_filter"),
        "moneyness_recalc_seconds": _int_value(
            result.get("moneyness_recalc_seconds")
        ),
        "moneyness_recalc_count": _int_value(result.get("moneyness_recalc_count")),
        "moneyness_kline_match_count": _int_value(
            result.get("moneyness_kline_match_count")
        ),
        "moneyness_sticky_kline_count": _int_value(
            result.get("moneyness_sticky_kline_count")
        ),
        "moneyness_added_kline_count": _int_value(
            result.get("moneyness_added_kline_count")
        ),
        "moneyness_skipped_out_of_session_count": _int_value(
            result.get("moneyness_skipped_out_of_session_count")
        ),
        "moneyness_skipped_missing_price_count": _int_value(
            result.get("moneyness_skipped_missing_price_count")
        ),
        "last_moneyness_recalc_at": result.get("last_moneyness_recalc_at"),
        "current_kline_symbol": result.get("current_kline_symbol"),
        "current_kline_started_at": result.get("current_kline_started_at"),
        "kline_subscription_timeout_seconds": _int_value(
            result.get("kline_subscription_timeout_seconds")
        ),
        "kline_subscription_timeout_count": _int_value(
            result.get("kline_subscription_timeout_count")
        ),
        "kline_subscription_error_count": _int_value(
            result.get("kline_subscription_error_count")
        ),
        "last_kline_subscription_error_symbol": result.get(
            "last_kline_subscription_error_symbol"
        ),
        "last_kline_subscription_error": result.get("last_kline_subscription_error"),
        "underlying_progress": result.get("underlying_progress") or {},
    }


def _aggregate_underlying_progress(rows: list[dict]) -> dict:
    aggregated: dict[str, dict] = {}
    for row in rows:
        progress = row.get("underlying_progress")
        if not isinstance(progress, dict):
            continue
        for symbol, item in progress.items():
            if not isinstance(item, dict):
                continue
            key = str(item.get("underlying_symbol") or symbol)
            target = aggregated.setdefault(
                key,
                {
                    "underlying_symbol": key,
                    "quote_subscribed": 0,
                    "quote_total": 0,
                    "kline_subscribed": 0,
                    "kline_total": 0,
                    "kline_call_subscribed": 0,
                    "kline_call_total": 0,
                    "kline_put_subscribed": 0,
                    "kline_put_total": 0,
                    "subscribed_objects": 0,
                    "total_objects": 0,
                    "completion_ratio": 0.0,
                    "status": "pending",
                },
            )
            for field in (
                "quote_subscribed",
                "quote_total",
                "kline_subscribed",
                "kline_total",
                "kline_call_subscribed",
                "kline_call_total",
                "kline_put_subscribed",
                "kline_put_total",
                "subscribed_objects",
                "total_objects",
            ):
                target[field] += _int_value(item.get(field))
    for item in aggregated.values():
        total = _int_value(item.get("total_objects"))
        subscribed = _int_value(item.get("subscribed_objects"))
        item["completion_ratio"] = subscribed / total if total else 0.0
        if total > 0 and subscribed >= total:
            item["status"] = "subscribed"
        elif subscribed > 0:
            item["status"] = "subscribing"
        else:
            item["status"] = "pending"
    return dict(sorted(aggregated.items()))


def _latest_tqsdk_notify(rows: list[dict]) -> dict:
    candidates = [row for row in rows if row.get("last_tqsdk_notify_at") is not None]
    if not candidates:
        return {}
    latest = max(candidates, key=lambda row: str(row.get("last_tqsdk_notify_at")))
    return {
        "last_tqsdk_notify_at": latest.get("last_tqsdk_notify_at"),
        "last_tqsdk_notify_code": latest.get("last_tqsdk_notify_code"),
        "last_tqsdk_notify_level": latest.get("last_tqsdk_notify_level"),
        "last_tqsdk_notify_content": latest.get("last_tqsdk_notify_content"),
    }


def _aggregate_tqsdk_connection_status(rows: list[dict]) -> str:
    statuses = {str(row.get("tqsdk_connection_status") or "unknown") for row in rows}
    if "disconnected" in statuses:
        return "disconnected"
    if "reconnecting" in statuses:
        return "reconnecting"
    if "connected" in statuses:
        return "connected"
    return "unknown"


def _aggregate_contract_months(rows: list[dict]) -> str | None:
    values = {
        str(row.get("contract_months")).strip().lower()
        for row in rows
        if row.get("contract_months") is not None
    }
    if not values:
        return None
    if "all" in values:
        return "all"
    parsed = sorted(_int_value(value) for value in values if _int_value(value) > 0)
    return str(parsed[-1]) if parsed else None


def _aggregate_text_value(rows: list[dict], key: str) -> str | None:
    values = {
        str(row.get(key)).strip()
        for row in rows
        if row.get(key) is not None and str(row.get(key)).strip()
    }
    if not values:
        return None
    return ",".join(sorted(values))


def _max_text_value(rows: list[dict], key: str) -> str | None:
    values = [
        str(row.get(key)).strip()
        for row in rows
        if row.get(key) is not None and str(row.get(key)).strip()
    ]
    return max(values) if values else None


def _contract_months_from_result(result: dict) -> str | None:
    value = result.get("contract_months")
    if value is not None:
        return str(value).strip().lower()
    limit = result.get("contract_month_limit")
    if limit is None:
        return "all"
    parsed = _int_value(limit)
    return str(parsed) if parsed > 0 else None


def _empty_quote_stream_progress(*, running: bool) -> dict:
    return {
        "status": "running" if running else "stopped",
        "worker_reports": 0,
        "quote_subscribed": 0,
        "quote_total": 0,
        "kline_subscribed": 0,
        "kline_total": 0,
        "near_expiry_months": 0,
        "near_expiry_quote_subscribed": 0,
        "near_expiry_quote_total": 0,
        "near_expiry_kline_subscribed": 0,
        "near_expiry_kline_total": 0,
        "near_expiry_subscribed": 0,
        "near_expiry_total": 0,
        "contract_months": None,
        "subscribed_objects": 0,
        "total_objects": 0,
        "completion_ratio": 0.0,
        "started_at": None,
        "updated_at": None,
        "last_wait_update_at": None,
        "last_quote_write_at": None,
        "last_tqsdk_notify_at": None,
        "last_tqsdk_notify_code": None,
        "last_tqsdk_notify_level": None,
        "last_tqsdk_notify_content": None,
        "tqsdk_connection_status": "unknown",
        "last_tqsdk_disconnect_at": None,
        "last_tqsdk_restore_at": None,
        "tqsdk_notify_count": 0,
        "contract_refresh_count": 0,
        "last_contract_refresh_at": None,
        "last_contract_reconcile_at": None,
        "contract_reconcile_added_quote_count": 0,
        "contract_reconcile_removed_quote_count": 0,
        "contract_reconcile_added_kline_count": 0,
        "contract_reconcile_removed_kline_count": 0,
        "cycle_count": 0,
        "wait_update_count": 0,
        "quotes_written": 0,
        "changed_quotes_written": 0,
        "elapsed_seconds": None,
        "active_stage": "stopped" if not running else "initializing",
        "stage_label": None,
        "stage_average_seconds_per_object": None,
        "stage_estimated_remaining_seconds": None,
        "average_seconds_per_object": None,
        "estimated_remaining_seconds": None,
        "estimated_remaining_is_total": False,
        "waiting_for_kline_eta": False,
    }


def _int_value(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _elapsed_seconds(started_at: str | None, updated_at: str | None) -> float | None:
    started = _parse_datetime(started_at)
    updated = _parse_datetime(updated_at)
    if started is None or updated is None:
        return None
    return max(0.0, (updated - started).total_seconds())


def _subscription_eta(
    rows: list[dict],
    *,
    quote_subscribed: int,
    quote_total: int,
    kline_subscribed: int,
    kline_total: int,
) -> dict:
    if quote_total > 0 and quote_subscribed < quote_total:
        active_stage = "quote"
        stage_label = "Quote"
    elif kline_total > 0 and kline_subscribed < kline_total:
        active_stage = "kline"
        stage_label = "K线"
    else:
        return {
            "active_stage": "running",
            "stage_label": "实时运行",
            "stage_average_seconds_per_object": None,
            "stage_estimated_remaining_seconds": 0.0,
            "estimated_remaining_seconds": 0.0,
            "estimated_remaining_is_total": True,
            "waiting_for_kline_eta": False,
        }

    quote_average = _stage_average_seconds(
        rows,
        subscribed_key="quote_subscribed",
        started_keys=("quote_started_at", "started_at"),
        finished_keys=("quote_finished_at", "updated_at"),
    )
    kline_average = _stage_average_seconds(
        rows,
        subscribed_key="kline_subscribed",
        started_keys=("kline_started_at", "quote_finished_at", "started_at"),
        finished_keys=("updated_at",),
    )
    quote_remaining = (
        quote_average * max(0, quote_total - quote_subscribed)
        if quote_average is not None
        else None
    )
    kline_remaining = (
        kline_average * max(0, kline_total - kline_subscribed)
        if kline_average is not None
        else None
    )
    if active_stage == "quote":
        stage_average = quote_average
        stage_remaining = quote_remaining
        total_remaining = (
            quote_remaining + kline_remaining
            if quote_remaining is not None and kline_remaining is not None
            else None
        )
        waiting_for_kline_eta = kline_total > 0 and kline_remaining is None
    else:
        stage_average = kline_average
        stage_remaining = kline_remaining
        total_remaining = kline_remaining
        waiting_for_kline_eta = kline_total > 0 and kline_remaining is None

    return {
        "active_stage": active_stage,
        "stage_label": stage_label,
        "stage_average_seconds_per_object": stage_average,
        "stage_estimated_remaining_seconds": stage_remaining,
        "estimated_remaining_seconds": total_remaining,
        "estimated_remaining_is_total": total_remaining is not None,
        "waiting_for_kline_eta": waiting_for_kline_eta,
    }


def _stage_average_seconds(
    rows: list[dict],
    *,
    subscribed_key: str,
    started_keys: tuple[str, ...],
    finished_keys: tuple[str, ...],
) -> float | None:
    elapsed_total = 0.0
    subscribed_total = 0
    for row in rows:
        subscribed = _int_value(row.get(subscribed_key))
        if subscribed <= 0:
            continue
        started_at = _first_text(row, started_keys)
        finished_at = _first_text(row, finished_keys)
        elapsed = _elapsed_seconds(started_at, finished_at)
        if elapsed is None or elapsed <= 0:
            continue
        elapsed_total += elapsed
        subscribed_total += subscribed
    if subscribed_total <= 0:
        return None
    return elapsed_total / subscribed_total


def _first_text(row: dict, keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is not None:
            return str(value)
    return None


def _parse_datetime(value: str | None):
    if not value:
        return None
    try:
        from datetime import datetime

        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _pid_is_quote_stream(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        return _windows_pid_command_line_contains(pid, "option_data_manager.cli.quote_stream")
    command_line_path = Path(f"/proc/{pid}/cmdline")
    try:
        return "option_data_manager.cli.quote_stream" in command_line_path.read_text(
            encoding="utf-8",
            errors="ignore",
        )
    except OSError:
        return False


def _windows_pid_command_line_contains(pid: int, needle: str) -> bool:
    try:
        completed = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                f"(Get-CimInstance Win32_Process -Filter \"ProcessId={pid}\").CommandLine",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
    except Exception:
        return False
    return needle in (completed.stdout or "")


def create_app_from_database() -> FastAPI:
    """ASGI factory for uvicorn."""

    database_path = os.environ.get("ODM_DATABASE_PATH", DEFAULT_DATABASE_PATH)
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path, check_same_thread=False, timeout=30)
    configure_sqlite_runtime(connection, enable_wal=True)
    return create_webui_app(connection, database_path=database_path)


def main() -> None:
    """Start a local development WebUI server."""

    host = os.environ.get("ODM_WEB_HOST", "127.0.0.1")
    port = int(os.environ.get("ODM_WEB_PORT", "8765"))
    uvicorn.run(
        "option_data_manager.webui.app:create_app_from_database",
        host=host,
        port=port,
        factory=True,
        reload=False,
    )


INDEX_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>期权数据管理工具</title>
  <link
    href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
    rel="stylesheet"
    integrity="sha384-QWTKZyjpPEjISv5WaRU9Oer+R5dB8QyLZ6lGR4xjLxjUaaV+7pD1Q+KjF2S8J7h"
    crossorigin="anonymous"
  />
  <link rel="stylesheet" href="/assets/webui.css" />
</head>
<body>
  <div class="container-fluid app-shell p-0">
    <div class="row g-0 min-vh-100">
      <aside class="col-auto sidebar d-flex flex-column">
        <div class="brand d-flex align-items-center gap-2">
          <span class="brand-dot" aria-hidden="true"></span>
          <strong>期权数据管理工具</strong>
        </div>
        <nav class="nav nav-pills flex-column sidebar-nav" aria-label="主导航">
          <button class="nav-link active" type="button" data-page="overview">总览</button>
          <button class="nav-link" type="button" data-page="quote">T型报价</button>
          <button class="nav-link" type="button" data-page="runs">采集日志</button>
          <button class="nav-link" type="button" data-page="api">API</button>
          <button class="nav-link" type="button" data-page="settings">设置</button>
        </nav>
        <div class="health-pill mt-auto"><span class="status-dot" id="health-dot"></span><span id="health-text">等待数据</span></div>
      </aside>

      <main class="col workspace">
        <section id="overview" class="page active">
          <div class="hero-card card">
            <div class="hero-title-block">
              <h1>采集健康总览</h1>
              <p>按交易所、品种和标的定位当前切片覆盖缺口。</p>
            </div>
            <div class="hero-metrics">
              <div class="metric-card"><span>最新刷新</span><strong id="latest-update">--</strong></div>
              <div class="metric-card"><span>活跃期权</span><strong id="summary-options">--</strong></div>
              <div class="metric-card"><span>指标覆盖</span><strong class="good" id="summary-iv">--</strong></div>
              <div class="metric-card"><span>20日K线</span><strong class="good" id="summary-kline">正常</strong></div>
              <div class="metric-card"><span>采集分片</span><strong id="summary-batches">--</strong></div>
            </div>
          </div>

          <section class="panel card">
            <div class="panel-header">
              <div><span class="panel-title">交易所采集状态</span><span class="panel-note">覆盖率、品种数和标的数用于判断全市场缺口。</span></div>
            </div>
            <div class="exchange-cards" id="exchange-cards"></div>
          </section>

          <section class="panel card collection-panel">
            <div class="panel-header">
              <div><span class="panel-title">采集与订阅进度</span><span class="panel-note">实时订阅负责盘中 Quote/K线；IV/Greeks 由独立指标 worker 按需刷新，批量补采负责分片落库。</span></div>
            </div>
            <div class="progress-cards" id="collection-progress"></div>
            <div class="collection-failures notice warn mt-3" id="collection-failures" hidden></div>
          </section>

          <section class="panel card">
            <div class="panel-header">
              <div><span class="panel-title">标的汇总表</span><span class="panel-note" id="underlying-scope-note">一行一个订阅范围内标的，进入 T型报价查看 CALL/PUT 全链。</span></div>
              <div class="btn-group btn-group-sm" role="group" aria-label="状态筛选">
                <button class="btn btn-light active" type="button" id="show-all">全部</button>
                <button class="btn btn-outline-warning" type="button" id="show-issues">只看缺口</button>
              </div>
            </div>
            <div class="exchange-tabs" id="exchange-tabs" role="tablist" aria-label="交易所筛选"></div>
            <div class="table-responsive table-shell">
              <table class="table table-sm align-middle summary-table underlying-table">
                <colgroup><col class="underlying-exchange"><col class="underlying-product"><col class="underlying-symbol"><col class="underlying-expiry"><col class="underlying-count"><col class="underlying-count"><col class="underlying-count"><col class="underlying-ratio"><col class="underlying-ratio"><col class="underlying-kline"><col class="underlying-time"><col class="underlying-status"><col class="underlying-status"><col class="underlying-action"></colgroup>
                <thead><tr><th>交易所</th><th>品种</th><th>标的合约</th><th>到期月</th><th>剩余天数</th><th>CALL</th><th>PUT</th><th>Quote</th><th>IV</th><th>20D K线</th><th>行情时间</th><th>交易时间</th><th>状态</th><th>操作</th></tr></thead>
                <tbody id="underlying-rows"></tbody>
              </table>
              <div class="empty-state d-none" id="overview-empty">
                当前实时订阅还没有刷新出本轮行情切片。请先启动合约管理器和实时订阅，或等待订阅完成后刷新。
              </div>
            </div>
          </section>
        </section>

        <section id="quote" class="page">
          <div class="hero-card card">
            <div class="hero-title-block">
              <h1 id="quote-title">T型报价</h1>
              <p>标的合约 · T型报价 · SQLite 当前切片</p>
            </div>
            <div class="hero-metrics">
              <div class="metric-card"><span>实时行情时间</span><strong id="quote-book-time">--</strong></div>
              <div class="metric-card"><span>活跃期权</span><strong id="quote-active-options">--</strong></div>
              <div class="metric-card"><span>指标覆盖</span><strong class="good" id="quote-iv-coverage">--</strong></div>
              <div class="metric-card"><span>实时状态</span><strong class="warn" id="quote-kline-status">未订阅</strong></div>
            </div>
          </div>

          <section class="quote-shell card">
            <div class="quote-toolbar d-flex align-items-center gap-3">
              <label class="form-label mb-0">交易所</label>
              <select class="form-select form-select-sm" id="exchange-select"></select>
              <label class="form-label mb-0">品种</label>
              <select class="form-select form-select-sm" id="product-select"></select>
              <label class="form-label mb-0">到期月</label>
              <select class="form-select form-select-sm" id="expiry-select"></select>
              <span class="expiry-days">剩余到期天数 <strong id="toolbar-expiry-days">--</strong></span>
              <span>交易时间 <strong id="toolbar-session-state">--</strong></span>
              <span class="ms-auto">行情时间 <strong id="toolbar-book-time">--</strong></span>
            </div>
            <div class="quote-table-wrap">
              <table class="table table-sm mb-0 quote-table">
                <colgroup>
                  <col class="option-col"><col class="option-col"><col class="option-col"><col class="option-col"><col class="option-col">
                  <col class="option-col"><col class="option-col"><col class="option-col"><col class="option-col"><col class="option-col">
                  <col class="central-col">
                  <col class="option-col"><col class="option-col"><col class="option-col"><col class="option-col"><col class="option-col">
                  <col class="option-col"><col class="option-col"><col class="option-col"><col class="option-col"><col class="option-col">
                </colgroup>
                <thead id="quote-head"></thead>
                <tbody id="quote-rows"></tbody>
              </table>
            </div>
          </section>
        </section>

        <section id="runs" class="page">
          <div class="hero-card card"><div class="hero-title-block"><h1>日志与诊断</h1><p>追踪采集运行、API/设置/后台刷新事件和可重试异常。</p></div><div class="hero-metrics"><div class="metric-card"><span>最近运行</span><strong id="latest-run">--</strong></div><div class="metric-card"><span>采集异常</span><strong id="latest-errors">--</strong></div><div class="metric-card"><span>系统日志</span><strong id="latest-service-logs">--</strong></div></div></div>
          <section class="panel card"><div class="panel-header"><span class="panel-title">系统日志</span><span class="panel-note">记录本地 API、设置保存、后台刷新窗口等事件，不写入明文密码或完整 API Key。</span></div><div id="service-logs-list" class="log-list"></div></section>
          <section class="panel card"><div class="panel-header"><span class="panel-title">采集运行记录</span></div><div id="runs-list" class="log-list"></div></section>
          <section class="panel card"><div class="panel-header"><span class="panel-title">最近采集异常</span></div><div id="run-errors-list" class="log-list"></div></section>
        </section>

        <section id="api" class="page">
          <div class="hero-card card">
            <div class="hero-title-block"><h1>本地 API</h1><p>本机只读数据接口、文档入口和接入状态。</p></div>
            <div class="hero-metrics">
              <div class="metric-card"><span>服务状态</span><strong id="api-health-status">检查中</strong></div>
              <div class="metric-card"><span>认证</span><strong id="api-auth-status">--</strong></div>
              <div class="metric-card"><span>请求数</span><strong id="api-request-count">--</strong></div>
              <div class="metric-card"><span>平均延迟</span><strong id="api-latency">--</strong></div>
            </div>
          </div>
          <section class="panel card">
            <div class="panel-header"><span class="panel-title">接入信息</span><span class="panel-note">API Key 在设置页管理；这里保留调用入口和运行状态。</span></div>
            <div class="api-access-grid">
              <div class="api-access-card">
                <span>Base URL</span>
                <strong class="mono" id="api-base-url">--</strong>
                <small>当前 WebUI 同源挂载本地 API。</small>
              </div>
              <div class="api-access-card">
                <span>Swagger UI</span>
                <a class="btn btn-sm btn-primary" href="/docs" target="_blank" rel="noreferrer">打开文档</a>
                <small>用于查看和试调完整接口。</small>
              </div>
              <div class="api-access-card">
                <span>OpenAPI Schema</span>
                <a class="btn btn-sm btn-light" href="/openapi.json" target="_blank" rel="noreferrer">查看 JSON</a>
                <small>供外部监控平台生成客户端。</small>
              </div>
              <div class="api-access-card">
                <span>API Key</span>
                <button class="btn btn-sm btn-light" type="button" id="manage-api-keys">管理 Key</button>
                <small id="api-key-hint">认证状态加载中。</small>
              </div>
            </div>
          </section>
          <section class="panel card">
            <div class="panel-header"><span class="panel-title">调用方式</span><span class="panel-note">只读接口默认面向本机集成，开启认证后需传 Bearer Token。</span></div>
            <div class="api-callout">
              <div>
                <span>认证头</span>
                <code id="api-auth-header">Authorization: Bearer &lt;API_KEY&gt;</code>
              </div>
              <div>
                <span>当前绑定</span>
                <code id="api-bind-info">--</code>
              </div>
            </div>
          </section>
          <section class="panel card">
            <div class="panel-header"><span class="panel-title">常用端点</span><span class="panel-note">监控平台优先从这些只读接口取当前切片和运行状态。</span></div>
            <div class="table-responsive table-shell">
              <table class="table table-sm align-middle summary-table api-endpoint-table">
                <colgroup><col class="api-method"><col class="api-path"><col class="api-purpose"><col class="api-action"></colgroup>
                <thead><tr><th>方法</th><th>路径</th><th>用途</th><th>操作</th></tr></thead>
                <tbody id="api-endpoint-rows"></tbody>
              </table>
            </div>
          </section>
          <section class="panel card">
            <div class="panel-header"><span class="panel-title">运行摘要</span><span class="panel-note">用于判断 API 是否可作为外部监控平台的数据出口。</span></div>
            <div class="api-runtime-grid">
              <div><span>数据库</span><strong class="mono" id="api-database-path">--</strong></div>
              <div><span>活跃期权</span><strong id="api-active-options">--</strong></div>
              <div><span>最新切片</span><strong id="api-latest-update">--</strong></div>
              <div><span>错误数</span><strong id="api-error-count">--</strong></div>
            </div>
            <div class="notice mt-3 good" id="api-page-message">API 页面加载中。</div>
          </section>
        </section>

        <section id="settings" class="page">
          <div class="hero-card card"><div class="hero-title-block"><h1>设置</h1><p>TQSDK、SQLite 和本地 API 设置入口。</p></div><div class="hero-metrics"><div class="metric-card"><span>凭据状态</span><strong class="good">已隔离</strong></div></div></div>
          <section class="panel card">
            <div class="panel-header"><span class="panel-title">TQSDK 凭据</span><span class="panel-note">密码保存后脱敏显示，不写入日志或页面状态。</span></div>
            <div class="settings-grid">
              <label>账号<input class="form-control form-control-sm" id="setting-account" autocomplete="username" /></label>
              <label>密码<input class="form-control form-control-sm" id="setting-password" type="password" autocomplete="current-password" placeholder="输入新密码后保存" /></label>
              <button class="btn btn-sm btn-primary" type="button" id="save-credentials">保存凭据</button>
              <button class="btn btn-sm btn-light" type="button" id="test-credentials">测试连接</button>
            </div>
            <div class="notice mt-3" id="settings-message">设置加载中。</div>
          </section>
          <section class="panel card">
            <div class="panel-header"><span class="panel-title">本地 API</span><span class="panel-note">默认只绑定本机，API Key 强制认证可配置。</span></div>
            <div class="settings-grid">
              <label>Bind<input class="form-control form-control-sm" id="setting-api-bind" /></label>
              <label>Port<input class="form-control form-control-sm" id="setting-api-port" type="number" min="1" max="65535" /></label>
              <label>刷新间隔秒<input class="form-control form-control-sm" id="setting-refresh-interval" type="number" min="1" /></label>
              <label>分片大小<input class="form-control form-control-sm" id="setting-batch-size" type="number" min="1" /></label>
              <label>等待轮数<input class="form-control form-control-sm" id="setting-wait-cycles" type="number" min="0" /></label>
              <label>后台窗口分片<input class="form-control form-control-sm" id="setting-max-batches" type="number" min="1" /></label>
              <label class="checkline"><input class="form-check-input" id="setting-auth-required" type="checkbox" /> 强制 API Key</label>
              <div class="settings-actions">
                <button class="btn btn-sm btn-primary" type="button" id="save-runtime-settings">保存运行设置</button>
                <button class="btn btn-sm btn-warning" type="button" id="trigger-refresh">手动刷新</button>
              </div>
            </div>
          </section>
          <section class="panel card">
            <div class="panel-header"><span class="panel-title">合约管理器</span><span class="panel-note">独立维护本地合约列表；实时订阅只消费它维护好的结果。</span></div>
            <div class="settings-grid">
              <label>刷新间隔秒<input class="form-control form-control-sm" id="setting-contract-manager-refresh-interval" type="number" min="60" /></label>
              <div class="settings-actions contract-manager-actions">
                <button class="btn btn-sm btn-primary" type="button" id="start-contract-manager">启动合约管理器</button>
                <button class="btn btn-sm btn-light" type="button" id="stop-contract-manager">停止合约管理器</button>
              </div>
            </div>
            <div class="notice mt-3" id="contract-manager-message">合约管理器状态加载中。</div>
          </section>
          <section class="panel card">
            <div class="panel-header"><span class="panel-title">实时 Quote/指标 Worker</span><span class="panel-note">常驻 Quote 订阅分片负责行情刷新；指标 worker 异步刷新 IV/Greeks，不阻塞 Quote 采集。</span></div>
            <div class="settings-grid">
              <label>Worker 数<select class="form-select form-select-sm" id="setting-quote-workers"><option value="1">1 个 worker</option><option value="2">2 个 worker</option><option value="3">3 个 worker</option><option value="4">4 个 worker</option></select></label>
              <label>订阅分片大小<input class="form-control form-control-sm" id="setting-quote-shard-size" type="number" min="1" /></label>
              <label>K线批量大小<input class="form-control form-control-sm" id="setting-kline-batch-size" type="number" min="1" /></label>
              <label>K线窗口天数<input class="form-control form-control-sm" id="setting-kline-data-length" type="number" min="1" /></label>
              <label>K线单合约超时秒<input class="form-control form-control-sm" id="setting-kline-timeout-seconds" type="number" min="1" /></label>
              <label>近月标记数量<input class="form-control form-control-sm" id="setting-near-expiry-months" type="number" min="1" /></label>
              <label>订阅合约月份<select class="form-select form-select-sm" id="setting-contract-months"><option value="1">1 个</option><option value="2">2 个</option><option value="3">3 个</option><option value="all">全部</option></select></label>
              <label>最小剩余天数<input class="form-control form-control-sm" id="setting-min-days-to-expiry" type="number" min="0" /></label>
              <label>虚实度重算秒<input class="form-control form-control-sm" id="setting-moneyness-recalc-seconds" type="number" min="1" /></label>
              <label>最大 symbols<input class="form-control form-control-sm" id="setting-quote-max-symbols" type="number" min="1" placeholder="留空为全量" /></label>
              <label>指标最小间隔秒<input class="form-control form-control-sm" id="setting-metrics-min-interval" type="number" min="1" /></label>
              <label>标的全链间隔秒<input class="form-control form-control-sm" id="setting-metrics-chain-interval" type="number" min="1" /></label>
              <label class="checkline"><input class="form-check-input" id="setting-prioritize-near-expiry" type="checkbox" /> 近月优先订阅</label>
              <label class="checkline"><input class="form-check-input setting-moneyness" value="otm" type="checkbox" /> 虚值K线</label>
              <label class="checkline"><input class="form-check-input setting-moneyness" value="atm" type="checkbox" /> 平值K线</label>
              <label class="checkline"><input class="form-check-input setting-moneyness" value="itm" type="checkbox" /> 实值K线</label>
              <div class="settings-actions quote-stream-actions">
                <button class="btn btn-sm btn-primary" type="button" id="start-quote-stream">启动实时订阅</button>
                <button class="btn btn-sm btn-light" type="button" id="stop-quote-stream">停止实时订阅</button>
              </div>
            </div>
            <div class="notice mt-3" id="quote-stream-message">实时订阅状态加载中。</div>
          </section>
          <section class="panel card" id="settings-api-key-panel">
            <div class="panel-header"><span class="panel-title">API Key</span><span class="panel-note">完整 Key 只在创建时显示。</span></div>
            <div class="settings-grid">
              <label>名称<input class="form-control form-control-sm" id="api-key-name" placeholder="local-monitor" /></label>
              <label>权限范围<select class="form-select form-select-sm" id="api-key-scope"><option value="read">只读访问</option></select></label>
              <button class="btn btn-sm btn-primary" type="button" id="create-api-key">创建 Key</button>
            </div>
            <div class="notice mt-3" id="api-key-message">API Key 创建后只显示一次完整密钥。</div>
            <div class="table-responsive table-shell mt-3">
              <table class="table table-sm align-middle summary-table api-key-table">
                <colgroup><col class="api-key-id"><col class="api-key-name"><col class="api-key-fingerprint"><col class="api-key-scope"><col class="api-key-status"><col class="api-key-used"><col class="api-key-actions"></colgroup>
                <thead><tr><th>ID</th><th>名称</th><th>指纹</th><th>权限范围</th><th>状态</th><th>最后使用</th><th>操作</th></tr></thead>
                <tbody id="api-key-rows"></tbody>
              </table>
            </div>
          </section>
        </section>
      </main>
    </div>
  </div>

  <div class="drawer-backdrop" id="drawer-backdrop"></div>
  <aside class="drawer" id="detail-drawer" aria-hidden="true">
    <button class="btn btn-sm btn-light float-end" id="close-drawer" type="button">关闭</button>
    <h3 id="drawer-title">行权价详情</h3>
    <p class="muted">CALL/PUT 合约、Quote 原始字段、Greeks、IV、20日K线切片与最近采集日志。</p>
    <div id="drawer-body"></div>
  </aside>
  <script>window.__ODM_INITIAL_STATE__ = __ODM_INITIAL_STATE__;</script>
  <script src="/assets/webui.js"></script>
</body>
</html>
"""


WEBUI_CSS = r"""
:root {
  --panel: #fffdf8;
  --panel-2: #fff7f3;
  --line: #eaded6;
  --line-2: #f2e6d5;
  --text: #3f3033;
  --muted: #7c6a6a;
  --sakura: #f2c6d4;
  --brand: #8a4246;
  --matcha: #e4f1e0;
  --aqua: #2b6e6e;
  --warn: #9b641f;
  --mono: "SFMono-Regular", "Cascadia Mono", Consolas, Menlo, monospace;
  --sans: Inter, "Segoe UI", "Microsoft YaHei", system-ui, sans-serif;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  color: var(--text);
  font: 13px/1.45 var(--sans);
  background:
    linear-gradient(90deg, rgba(138, 66, 70, .07) 1px, transparent 1px),
    linear-gradient(180deg, rgba(138, 66, 70, .055) 1px, transparent 1px),
    #fbfaf3;
  background-size: 36px 36px;
  letter-spacing: 0;
}
.app-shell { min-width: 1180px; }
.app-shell > .row {
  display: grid;
  grid-template-columns: clamp(216px, 11.5vw, 246px) minmax(0, 1fr);
  min-height: 100vh;
  margin: 0;
}
.app-shell > .row > .sidebar,
.app-shell > .row > .workspace {
  width: auto;
  max-width: none;
  padding-left: 0;
  padding-right: 0;
}
.sidebar {
  position: sticky;
  top: 0;
  width: clamp(216px, 11.5vw, 246px);
  height: 100vh;
  padding: 22px 16px;
  border-right: 1px solid rgba(138, 66, 70, .16);
  background: var(--panel);
}
.brand {
  padding: 0 4px 10px;
  border-bottom: 1px solid rgba(138, 66, 70, .13);
  color: #4b292c;
}
.brand-dot {
  width: 22px;
  height: 22px;
  border-radius: 4px;
  background: var(--sakura);
  box-shadow: 0 0 0 1px rgba(138, 66, 70, .16);
}
.sidebar-nav { gap: 6px; margin-top: 22px; }
.sidebar .nav-link {
  width: 100%;
  height: 38px;
  padding: 0 12px;
  border: 1px solid transparent;
  border-radius: 4px;
  color: #6e5758;
  text-align: left;
  font-weight: 700;
}
.sidebar .nav-link:hover { background: rgba(242, 230, 213, .72); color: #4b292c; }
.sidebar .nav-link.active { color: #fffdf8; background: var(--brand); border-color: var(--brand); }
.health-pill {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  height: 28px;
  border: 1px solid rgba(59, 143, 143, .28);
  border-radius: 4px;
  background: var(--matcha);
  color: var(--aqua);
  font-weight: 800;
}
.status-dot { width: 7px; height: 7px; border-radius: 50%; background: currentColor; }
.status-dot.warn { color: var(--warn); }
.workspace { min-width: 0; padding: clamp(18px, 2vw, 34px) !important; }
.page { display: none; }
.page.active { display: block; }
.card, .panel, .hero-card, .quote-shell {
  border: 1px solid rgba(138, 66, 70, .14);
  border-radius: 4px;
  background: var(--panel);
  box-shadow: none;
}
.hero-card {
  min-height: 116px;
  display: grid;
  grid-template-columns: minmax(360px, .9fr) minmax(520px, 1.35fr);
  overflow: hidden;
  margin-bottom: 18px;
  border-color: var(--brand);
}
.hero-title-block {
  display: flex;
  flex-direction: column;
  justify-content: center;
  padding: 20px 22px;
  background: var(--brand);
  color: #fffdf8;
}
.hero-title-block h1 { margin: 0 0 6px; font-size: 23px; font-weight: 800; }
.hero-title-block p { margin: 0; color: rgba(255, 253, 248, .82); }
.hero-metrics {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(118px, 1fr));
  gap: 12px;
  align-items: center;
  padding: 20px 22px;
}
.metric-card {
  min-height: 58px;
  padding: 10px 12px;
  border: 1px solid rgba(138, 66, 70, .13);
  border-radius: 4px;
  background: var(--panel);
}
.metric-card span { display: block; color: var(--muted); font-size: 11px; font-weight: 700; }
.metric-card strong { display: block; margin-top: 4px; color: var(--text); font: 800 13px/1.2 var(--mono); }
.good { color: var(--aqua) !important; }
.warn { color: var(--warn) !important; }
.bad { color: var(--brand) !important; }
.panel { padding: 17px 18px; margin-bottom: 16px; }
.panel-header { display: flex; justify-content: space-between; align-items: center; gap: 14px; margin-bottom: 14px; }
.panel-title { font-size: 15px; font-weight: 800; }
.panel-note { color: var(--muted); font-size: 12px; margin-left: 8px; }
.exchange-cards { display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; }
.collection-panel {
  margin-top: 22px;
  margin-bottom: 20px;
  padding: 0;
  border: 0;
  background: transparent;
}
.collection-panel .panel-header {
  margin-bottom: 10px;
  padding: 0 2px;
}
.progress-cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 12px; }
.settings-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(160px, 1fr));
  gap: 14px 18px;
  align-items: end;
}
.settings-grid label {
  display: flex;
  flex-direction: column;
  gap: 7px;
  color: var(--muted);
  font-size: 12px;
}
.settings-grid input,
.settings-grid select {
  min-height: 30px;
}
.settings-grid button {
  min-height: 32px;
}
.settings-actions {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
  align-items: end;
}
.quote-stream-actions {
  grid-column: 1 / -1;
  grid-template-columns: repeat(2, minmax(160px, 1fr));
}
.contract-manager-actions {
  grid-column: span 3;
  grid-template-columns: repeat(2, minmax(160px, 1fr));
}
.settings-grid button:disabled,
.settings-grid input:disabled,
.settings-grid select:disabled {
  cursor: not-allowed;
  opacity: .58;
}
.settings-grid .checkline {
  display: flex;
  flex-direction: row;
  align-items: center;
  gap: 8px;
  min-height: 32px;
  color: var(--ink);
}
.settings-grid + .notice,
.settings-grid + .table-shell { margin-top: 18px; }
.api-access-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 14px;
}
.api-access-card,
.api-runtime-grid > div {
  min-height: 86px;
  padding: 13px 14px;
  border: 1px solid rgba(138, 66, 70, .13);
  border-radius: 4px;
  background: rgba(255, 253, 248, .94);
}
.api-access-card span,
.api-runtime-grid span,
.api-callout span {
  display: block;
  margin-bottom: 7px;
  color: var(--muted);
  font-size: 11px;
  font-weight: 800;
}
.api-access-card strong,
.api-runtime-grid strong {
  display: block;
  min-width: 0;
  overflow: hidden;
  color: var(--text);
  text-overflow: ellipsis;
  white-space: nowrap;
}
.api-access-card small {
  display: block;
  margin-top: 8px;
  color: var(--muted);
  font-size: 11px;
}
.api-access-card .btn {
  min-height: 30px;
  min-width: 104px;
}
.api-callout {
  display: grid;
  grid-template-columns: minmax(320px, .9fr) minmax(320px, 1.1fr);
  gap: 14px;
}
.api-callout > div {
  padding: 13px 14px;
  border: 1px solid rgba(138, 66, 70, .13);
  border-radius: 4px;
  background: #fffdf8;
}
.api-callout code {
  display: block;
  min-height: 30px;
  padding: 7px 9px;
  overflow: hidden;
  border: 1px solid var(--line);
  border-radius: 4px;
  background: #fbfaf3;
  color: var(--text);
  text-overflow: ellipsis;
  white-space: nowrap;
}
.api-runtime-grid {
  display: grid;
  grid-template-columns: 1.4fr repeat(3, minmax(0, .8fr));
  gap: 14px;
}
.api-endpoint-table col.api-method { width: 9%; }
.api-endpoint-table col.api-path { width: 25%; }
.api-endpoint-table col.api-purpose { width: 48%; }
.api-endpoint-table col.api-action { width: 18%; }
.api-endpoint-table code {
  font-family: var(--mono);
  font-weight: 800;
}
.exchange-card {
  min-height: 74px;
  padding: 12px 14px;
  border: 1px solid rgba(138, 66, 70, .13);
  border-radius: 4px;
  background: var(--panel);
}
.exchange-card strong { font-size: 16px; margin-right: 10px; }
.exchange-card div:last-child { color: var(--aqua); font-size: 12px; margin-top: 5px; }
.progress-card {
  min-height: 70px;
  padding: 11px 13px;
  border: 1px solid rgba(138, 66, 70, .13);
  border-radius: 4px;
  background: #fffdf8;
}
.progress-card.good {
  border-color: rgba(59,143,143,.22);
  background: #f4fbf1;
}
.progress-card.warn {
  border-color: rgba(163,108,46,.24);
  background: #fffaf0;
}
.progress-card.bad {
  border-color: rgba(138,66,70,.28);
  background: #fff3f4;
}
.progress-card span { display: block; color: var(--muted); font-size: 11px; font-weight: 800; }
.progress-card strong { display: block; margin-top: 5px; font: 800 16px/1.2 var(--mono); color: var(--text); }
.progress-card small { display: block; margin-top: 4px; color: var(--muted); font-size: 11px; }
.collection-failures[hidden] {
  display: none !important;
}
.stream-progress-card {
  grid-column: 1 / -1;
}
.stream-stage {
  display: inline-flex;
  align-items: center;
  min-height: 22px;
  padding: 0 8px;
  border-radius: 4px;
  background: rgba(59,143,143,.10);
  color: var(--aqua);
  font-size: 11px;
  font-weight: 900;
}
.stream-progress-line {
  display: grid;
  grid-template-columns: minmax(90px, max-content) 1fr minmax(82px, max-content);
  align-items: center;
  gap: 12px;
  margin-top: 10px;
}
.stream-progress-track {
  position: relative;
  height: 10px;
  border-radius: 999px;
  background: rgba(234, 222, 214, .78);
}
.stream-progress-fill {
  height: 100%;
  width: 0%;
  border-radius: inherit;
  background: linear-gradient(90deg, var(--aqua), #7da95e);
  transition: width .25s ease;
}
.stream-progress-marker {
  position: absolute;
  top: 0;
  bottom: 0;
  width: 2px;
  background: var(--brand);
  transform: translateX(-50%);
  z-index: 3;
  pointer-events: none;
}
.stream-progress-meta {
  color: var(--muted);
  font: 800 11px/1.2 var(--mono);
  text-align: right;
}
.stream-split-grid {
  display: grid;
  grid-template-columns: 1fr;
  gap: 10px 14px;
  margin-top: 12px;
}
.stream-split-title {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  color: var(--muted);
  font-size: 11px;
  font-weight: 800;
}
@media (max-width: 720px) {
  .stream-split-grid {
    grid-template-columns: 1fr;
  }
}
.exchange-tabs {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  padding: 0 16px 12px;
}
.exchange-tab {
  min-height: 30px;
  padding: 0 12px;
  border: 1px solid var(--line);
  border-radius: 4px;
  background: rgba(255, 253, 248, .92);
  color: var(--muted);
  font-size: 12px;
  font-weight: 800;
}
.exchange-tab.active {
  border-color: rgba(138, 66, 70, .34);
  background: var(--brand);
  color: #fff;
}
.table-shell, .quote-table-wrap {
  border: 1px solid var(--line);
  border-radius: 4px;
  background: var(--panel);
}
.table { width: 100%; table-layout: fixed; margin: 0; }
.table th, .table td {
  height: 34px;
  padding: 0 10px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  vertical-align: middle;
  border-bottom: 1px solid var(--line);
}
.table th { color: #5f4645; background: var(--line-2); font-size: 11px; font-weight: 800; }
.table td { color: var(--text); background: rgba(255, 253, 248, .98); }
.summary-table tbody tr:nth-child(even) td { background: #f7fbf3; }
.summary-table tbody tr.clickable:hover td { background: rgba(242, 198, 212, .22); }
.summary-table td.cell-right { text-align: right; }
.summary-table td.cell-center { text-align: center; }
.summary-table .empty-row td {
  height: 54px;
  padding: 0 16px;
  color: var(--muted);
  text-align: center;
  background: rgba(255, 253, 248, .78);
  border-bottom: 0;
}
.underlying-table col.underlying-exchange { width: 7%; }
.underlying-table col.underlying-product { width: 7%; }
.underlying-table col.underlying-symbol { width: 10%; }
.underlying-table col.underlying-expiry { width: 7%; }
.underlying-table col.underlying-count { width: 6%; }
.underlying-table col.underlying-ratio { width: 8%; }
.underlying-table col.underlying-kline { width: 13%; }
.underlying-table col.underlying-time { width: 13%; }
.underlying-table col.underlying-status { width: 8%; }
.underlying-table col.underlying-action { width: 7%; }
.api-key-table {
  border-collapse: separate;
  border-spacing: 0;
}
.api-key-table col.api-key-id { width: 5%; }
.api-key-table col.api-key-name { width: 11%; }
.api-key-table col.api-key-fingerprint { width: 49%; }
.api-key-table col.api-key-scope { width: 9%; }
.api-key-table col.api-key-status { width: 7%; }
.api-key-table col.api-key-used { width: 9%; }
.api-key-table col.api-key-actions { width: 10%; }
.api-key-table th,
.api-key-table td {
  padding: 0 14px;
  border-right: 1px solid rgba(234, 222, 214, .75);
}
.api-key-table th:last-child,
.api-key-table td:last-child {
  border-right: 0;
}
.api-key-fingerprint-text {
  display: block;
  font-size: 10px;
  line-height: 1.15;
  white-space: nowrap;
}
.api-key-actions-cell {
  display: flex;
  align-items: center;
  gap: 8px;
}
.table .btn.table-action {
  min-width: auto;
  height: 24px;
  padding: 0 8px;
  font-size: 11px;
}
.mono, .num { font-family: var(--mono); font-weight: 800; }
.tag {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 48px;
  height: 22px;
  padding: 0 8px;
  border-radius: 4px;
  border: 1px solid rgba(138, 66, 70, .12);
  background: #f7f1ed;
  font-size: 11px;
  font-weight: 800;
}
.tag.good { background: var(--matcha); color: var(--aqua); }
.tag.warn { background: #fff2dc; color: var(--warn); }
.tag.bad { background: #fae3e6; color: var(--brand); }
.status-cell { display: flex; flex-direction: column; align-items: flex-start; gap: 3px; }
.cell-center .status-cell { align-items: center; }
.status-subline { font-size: 11px; line-height: 1; color: var(--muted); white-space: nowrap; }
.quote-shell { overflow: hidden; }
.quote-toolbar {
  min-height: 54px;
  padding: 10px 18px;
  border-bottom: 1px solid var(--line);
  background: var(--panel);
  flex-wrap: wrap;
}
.quote-toolbar .form-select { width: 106px; border-color: rgba(138, 66, 70, .18); background-color: var(--panel); }
.quote-table-wrap { overflow: visible; min-height: clamp(430px, 52vh, 620px); }
.quote-table { width: 100%; min-width: 100%; border-collapse: collapse; table-layout: fixed; }
.quote-table col.option-col { width: 4.6%; }
.quote-table col.central-col { width: 8%; }
.quote-table th, .quote-table td {
  height: 32px;
  padding: 0;
  text-align: center;
  font-size: clamp(10px, .66vw, 12px);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.quote-table th { height: 34px; color: #5f4645; background: var(--line-2); font: 800 11px/1 var(--sans); }
.quote-table .market-strip th { height: 56px; background: var(--panel); border-bottom-color: #eaded6; }
.quote-table .call-title { color: #b06c6f; text-align: left; padding-left: 20px; font-size: 15px; }
.quote-table .put-title { color: #3b8f8f; text-align: right; padding-right: 20px; font-size: 15px; }
.quote-table .under-bid,
.quote-table .under-ask { position: relative; overflow: visible; background: var(--panel); }
.quote-table .under-bid .tag,
.quote-table .under-ask .tag {
  position: absolute;
  top: 50%;
  width: max-content;
  max-width: none;
  height: 22px;
  padding: 0 6px;
  font-size: 11px;
  line-height: 22px;
}
.quote-table .under-bid .tag { right: 4px; transform: translateY(-50%); }
.quote-table .under-ask .tag { left: 4px; transform: translateY(-50%); }
.quote-table .under-last {
  background: var(--panel);
  color: var(--text);
  font-family: var(--mono);
  border-left: 1px solid var(--line);
  border-right: 1px solid var(--line);
}
.quote-table .under-last span { color: var(--muted); font: 700 11px/1.1 var(--sans); }
.quote-table .call-side.itm { background: #fff1ed; }
.quote-table .call-side.otm { background: #fffaf6; }
.quote-table .put-side.itm { background: #eaf5e8; }
.quote-table .put-side.otm { background: #f8fcf5; }
.quote-table .call-side.atm-neutral,
.quote-table .put-side.atm-neutral { background: var(--panel); }
.quote-table .strike-cell {
  color: var(--text);
  background: var(--panel);
  font: 900 12px/1 var(--mono);
  border-left: 1px solid var(--line);
  border-right: 1px solid var(--line);
}
.strike-cell.atm {
  color: var(--text) !important;
  background: var(--panel) !important;
  border-color: #d8b0a6 !important;
  box-shadow: inset 0 0 0 2px rgba(138, 66, 70, .28);
}
.iv { color: #7657ff; }
.price-bid { color: var(--aqua); }
.price-ask { color: var(--brand); }
.latest-price { font-family: var(--mono); font-weight: 900; }
.latest-price.latest-up { animation: latestUpFade 1.6s ease-out; }
.latest-price.latest-down { animation: latestDownFade 1.6s ease-out; }
@keyframes latestUpFade { 0% { color: var(--brand); } 72% { color: var(--brand); } 100% { color: var(--text); } }
@keyframes latestDownFade { 0% { color: var(--aqua); } 72% { color: var(--aqua); } 100% { color: var(--text); } }
.bar-cell { position: relative; overflow: hidden; font-family: var(--mono); font-weight: 800; }
.bar-cell .bar {
  position: absolute;
  top: 8px;
  bottom: 8px;
  z-index: 0;
  max-width: calc(100% - 4px);
  background: #c3e4d4;
}
.bar-cell.ask .bar { background: var(--sakura); }
.bar-cell.call.quantity .bar { left: 2px; }
.bar-cell.put.quantity .bar { right: 2px; }
.bar-cell.call.depth .bar { right: 2px; }
.bar-cell.put.depth .bar { left: 2px; }
.bar-cell .value { position: relative; z-index: 1; }
.log-list { display: grid; gap: 8px; }
.log-row { border: 1px solid var(--line); border-radius: 4px; padding: 12px; background: var(--panel); }
.notice {
  padding: 12px 14px;
  border: 1px solid rgba(163, 108, 46, .22);
  border-radius: 4px;
  background: #fff2dc;
  color: var(--warn);
}
.notice.warn { border-color: rgba(163, 108, 46, .22); background: #fff2dc; color: var(--warn); }
.notice.good { border-color: rgba(59,143,143,.25); background: var(--matcha); color: var(--aqua); }
.notice.bad { border-color: rgba(138,66,70,.25); background: #fae3e6; color: var(--brand); }
.empty-state {
  padding: 18px;
  color: var(--muted);
  font-size: 13px;
  border-top: 1px solid var(--line);
  background: rgba(255,255,255,.45);
}
.drawer-backdrop { position: fixed; inset: 0; display: none; background: rgba(63,48,51,.24); z-index: 40; }
.drawer-backdrop.open { display: block; }
.drawer {
  position: fixed;
  top: 0;
  right: 0;
  width: 460px;
  height: 100vh;
  padding: 20px;
  overflow: auto;
  border-left: 1px solid rgba(138, 66, 70, .16);
  background: var(--panel);
  z-index: 50;
  transform: translateX(100%);
  transition: transform .18s ease;
}
.drawer.open { transform: translateX(0); }
.drawer h3 { margin: 18px 0 8px; font-size: 18px; }
.muted { color: var(--muted); }
.kv { display: grid; grid-template-columns: 130px 1fr; gap: 8px; padding: 10px 0; border-bottom: 1px solid var(--line); }
"""


WEBUI_JS = r"""
const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

const QuoteStreamStatus = Object.freeze({
  BLOCKED: "blocked",
  STARTING: "starting",
  RUNNING: "running",
  STOPPED: "stopped",
});

const state = {
  overview: null,
  quote: null,
  settings: null,
  selectedUnderlying: null,
  selectedExchange: "ALL",
  showIssuesOnly: false,
  previousLatest: new Map(),
  streamEta: {
    lastSample: null,
    estimate: null,
  },
  quoteStreamRefreshBusy: false,
  overviewRefreshBusy: false,
};

const exchangeNames = {
  DCE: "大商所",
  CZCE: "郑商所",
  SHFE: "上期所",
  INE: "能源中心",
  GFEX: "广期所",
};

const productNames = {
  a: "黄大豆1号",
  ad: "铝合金",
  ag: "白银",
  al: "铝",
  ao: "氧化铝",
  ap: "苹果",
  au: "黄金",
  b: "黄大豆2号",
  bc: "国际铜",
  br: "丁二烯橡胶",
  bu: "沥青",
  bz: "苯乙烯",
  c: "玉米",
  cf: "棉花",
  cj: "红枣",
  cs: "玉米淀粉",
  cu: "铜",
  eb: "苯乙烯",
  eg: "乙二醇",
  fg: "玻璃",
  fu: "燃料油",
  i: "铁矿石",
  jd: "鸡蛋",
  jm: "焦煤",
  l: "聚乙烯",
  lc: "碳酸锂",
  lg: "原木",
  lh: "生猪",
  m: "豆粕",
  ma: "甲醇",
  ni: "镍",
  nr: "20号胶",
  oi: "菜籽油",
  op: "氧化铝",
  p: "棕榈油",
  pb: "铅",
  pd: "工业硅",
  pf: "短纤",
  pg: "液化石油气",
  pk: "花生",
  pl: "瓶片",
  pp: "聚丙烯",
  pr: "对二甲苯",
  ps: "多晶硅",
  pt: "铂",
  px: "对二甲苯",
  rb: "螺纹钢",
  rm: "菜粕",
  ru: "橡胶",
  sa: "纯碱",
  sc: "原油",
  sf: "硅铁",
  sh: "烧碱",
  si: "工业硅",
  sm: "锰硅",
  sn: "锡",
  sp: "纸浆",
  sr: "白糖",
  ta: "PTA",
  ur: "尿素",
  v: "聚氯乙烯",
  y: "豆油",
  zc: "动力煤",
  zn: "锌",
};

const escapeHtml = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;",
}[char]));

function productLabel(product) {
  const key = String(product ?? "").toLowerCase();
  return productNames[key] ?? product ?? "--";
}

function productLabelForRow(row) {
  return row?.product_display_name ?? productLabel(row?.product_id);
}

function localizeStatusMessage(message) {
  if (!message) return null;
  const text = String(message);
  const known = {
    "Refresh stopped because the local service restarted.": "本地服务已重启，本次刷新已停止。",
    "Background refresh started.": "后台刷新已启动。",
    "Refresh is already running in the background.": "后台刷新正在运行。",
    "Refresh started in the background. The WebUI will update as batches finish.": "后台刷新已启动，分片完成后页面会自动更新。",
    "Collection refresh window started.": "采集刷新窗口已启动。",
    "Refresh stopped because no active collection plan exists.": "没有可执行的采集计划，刷新已停止。",
    "Full-market refresh completed.": "全市场刷新已完成。",
    "Refresh paused after three windows without progress.": "连续三个窗口无进展，刷新已暂停。",
    "TQSDK credentials are not configured.": "TQSDK 凭据尚未配置。",
  };
  if (known[text]) return known[text];
  if (text.startsWith("Refresh failed:")) return `刷新失败：${text.slice("Refresh failed:".length).trim()}`;
  return text;
}

function fmtNum(value, digits = 0, dash = "--") {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return dash;
  const fixed = Number(value).toFixed(digits);
  if (digits === 0) return fixed;
  return fixed;
}

function fmtMaybeTrim(value, digits, trim) {
  const rendered = fmtNum(value, digits);
  return trim ? rendered.replace(/(\.\d*?[1-9])0+$/, "$1").replace(/\.0+$/, "") : rendered;
}

function fmtPct(value) {
  if (value === null || value === undefined) return "--";
  return `${(Number(value) * 100).toFixed(1)}%`;
}

function fmtDaysToExpiry(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
  return `${Number(value)}天`;
}

function fmtDateTime(value) {
  if (!value) return "--";
  const text = String(value);
  if (text.includes("T") && /(?:Z|[+-]\d{2}:?\d{2})$/.test(text)) {
    const date = new Date(text);
    if (!Number.isNaN(date.getTime())) {
      const parts = new Intl.DateTimeFormat("zh-CN", {
        timeZone: "Asia/Hong_Kong",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
      }).formatToParts(date).reduce((acc, part) => {
        acc[part.type] = part.value;
        return acc;
      }, {});
      return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`;
    }
  }
  return text
    .replace("T", " ")
    .replace(/(\.\d+)(?=(Z|[+-]\d{2}:?\d{2})?$)/, "")
    .replace(/(Z|[+-]\d{2}:?\d{2})$/, "");
}

function fmtDuration(seconds) {
  if (seconds === null || seconds === undefined || Number.isNaN(Number(seconds))) return "--";
  const total = Math.max(0, Math.round(Number(seconds)));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = total % 60;
  if (hours > 0) return `${hours}小时${minutes}分${secs}秒`;
  if (minutes > 0) return `${minutes}分${secs}秒`;
  return `${secs}秒`;
}

function fmtAvgDuration(seconds) {
  if (seconds === null || seconds === undefined || Number.isNaN(Number(seconds))) return "--";
  const value = Number(seconds);
  if (value >= 10) return `${value.toFixed(1)}秒/对象`;
  if (value >= 1) return `${value.toFixed(2)}秒/对象`;
  return `${Math.round(value * 1000)}毫秒/对象`;
}

function fmtStreamStage(progress, running) {
  if (progress?.refreshingContracts) return "正在刷新合约列表";
  if (!running) return "未运行";
  if (!progress || progress.workerReports === 0) return "初始化";
  if (progress.quoteTotal === 0 && progress.klineTotal === 0) return "初始化";
  if (progress.quoteSubscribed < progress.quoteTotal) return "Quote订阅中";
  if (progress.klineSubscribed < progress.klineTotal) return "K线订阅中";
  return "实时运行中";
}

function symbolParts(symbol) {
  const [exchange = "--", instrument = symbol ?? "--"] = String(symbol ?? "--").split(".");
  const product = instrument.match(/^[A-Za-z]+/)?.[0] ?? "--";
  const expiry = instrument.match(/(\d{3,4})/)?.[1] ?? "--";
  return { exchange, product, productName: productLabel(product), expiry };
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url} ${response.status}`);
  return response.json();
}

async function init() {
  bindNavigation();
  bindControls();
  await loadSettings();
  const initialPage = location.hash.slice(1) || "overview";
  state.overview = window.__ODM_INITIAL_STATE__?.overview ?? emptyOverview();
  showPage(initialPage, { updateHash: false });
  renderOverview();
  if (window.__ODM_INITIAL_STATE__?.overview) {
    state.selectedUnderlying = window.__ODM_INITIAL_STATE__.selectedUnderlying;
    state.quote = window.__ODM_INITIAL_STATE__.quote;
    observeStreamProgress(quoteStreamProgress(state.overview.quote_stream));
    renderSelectors();
    if (state.quote) renderQuote({ animate: false });
    await renderRuns();
  } else if (!["settings", "api"].includes(initialPage)) {
    refreshOverview();
    renderRuns();
  }
  setInterval(refreshCurrentQuote, 2500);
  setInterval(refreshOverview, 30000);
  setInterval(refreshQuoteStreamStatusForOverview, 2500);
  setInterval(() => {
    if (state.overview) renderCollectionProgress();
  }, 1000);
  setInterval(() => { if ($("#runs").classList.contains("active")) renderRuns(); }, 10000);
}

function bindNavigation() {
  $$(".nav-link[data-page]").forEach((tab) => {
    tab.addEventListener("click", () => showPage(tab.dataset.page));
  });
  window.addEventListener("hashchange", () => showPage(location.hash.slice(1), { updateHash: false }));
}

function showPage(page, options = {}) {
  const id = $(`#${page}`) ? page : "overview";
  $$(".page").forEach((node) => node.classList.toggle("active", node.id === id));
  $$(".nav-link[data-page]").forEach((node) => node.classList.toggle("active", node.dataset.page === id));
  if (options.updateHash !== false) history.replaceState(null, "", `#${id}`);
  if (id === "overview" && !state.overview) refreshOverview();
  if (id === "runs") renderRuns();
  if (id === "api") renderApiPage();
}

function emptyOverview() {
  return {
    summary: {
      active_options: 0,
      active_underlyings: 0,
      active_quote_symbols: 0,
      option_quote_rows: 0,
      quote_symbol_rows: 0,
      quote_symbol_market_rows: 0,
      option_kline_symbols: 0,
      metrics_rows: 0,
      rows_with_greeks: 0,
      rows_with_iv: 0,
      acquisition_errors: 0,
      acquisition_runs: 0,
      latest_update: null,
      latest_quote_update: null,
      quote_coverage: 0,
      greeks_coverage: 0,
      iv_coverage: 0,
      kline_coverage: 0,
    },
    collection: {
      active_batches: 0,
      success_batches: 0,
      pending_batches: 0,
      failed_batches: 0,
      running_batches: 0,
      planned_underlyings: 0,
      completion_ratio: 0,
      recent_failures: [],
    },
    collection_groups: [],
    exchanges: [],
    underlyings: [],
    refresh: { running: false, message: null },
    quote_stream: {
      running: false,
      message: "实时订阅状态加载中。",
      contract_months: $("#setting-contract-months")?.value ?? "2",
      worker_count: 0,
      pids: [],
      progress: null,
      active_option_count: null,
      contract_discovery_running: false,
      contract_list_ready: false,
    },
  };
}

function bindControls() {
  $("#show-all").addEventListener("click", () => {
    state.showIssuesOnly = false;
    $("#show-all").classList.add("active");
    $("#show-issues").classList.remove("active");
    renderUnderlyingRows();
  });
  $("#show-issues").addEventListener("click", () => {
    state.showIssuesOnly = true;
    $("#show-issues").classList.add("active");
    $("#show-all").classList.remove("active");
    renderUnderlyingRows();
  });
  ["exchange-select", "product-select", "expiry-select"].forEach((id) => {
    $(`#${id}`).addEventListener("change", () => onSelectorChange(id));
  });
  $("#close-drawer").addEventListener("click", closeDrawer);
  $("#drawer-backdrop").addEventListener("click", closeDrawer);
  $("#save-credentials").addEventListener("click", saveCredentials);
  $("#test-credentials").addEventListener("click", testCredentials);
  $("#save-runtime-settings").addEventListener("click", saveRuntimeSettings);
  $("#trigger-refresh").addEventListener("click", triggerRefresh);
  $("#start-contract-manager").addEventListener("click", startContractManager);
  $("#stop-contract-manager").addEventListener("click", stopContractManager);
  $("#start-quote-stream").addEventListener("click", startQuoteStream);
  $("#stop-quote-stream").addEventListener("click", stopQuoteStream);
  $("#create-api-key").addEventListener("click", createApiKey);
  $("#manage-api-keys").addEventListener("click", () => {
    showPage("settings");
    setTimeout(() => $("#settings-api-key-panel")?.scrollIntoView({ block: "start", behavior: "smooth" }), 30);
  });
}

async function loadSettings() {
  try {
    const settings = await fetchJson("/api/settings");
    state.settings = settings;
    $("#setting-account").value = settings.tqsdk.account ?? "";
    $("#setting-api-bind").value = settings.api.bind ?? "127.0.0.1";
    $("#setting-api-port").value = settings.api.port ?? 8770;
    $("#setting-auth-required").checked = Boolean(settings.api.auth_required);
    $("#setting-refresh-interval").value = settings.collection.refresh_interval_seconds ?? 30;
    $("#setting-batch-size").value = settings.collection.option_batch_size ?? 20;
    $("#setting-wait-cycles").value = settings.collection.wait_cycles ?? 1;
    $("#setting-max-batches").value = settings.collection.max_batches ?? 100;
    $("#setting-contract-manager-refresh-interval").value = settings.contract_manager?.refresh_interval_seconds ?? 3600;
    $("#setting-quote-workers").value = settings.quote_stream?.workers ?? 1;
    $("#setting-quote-shard-size").value = settings.quote_stream?.quote_shard_size ?? 1000;
    $("#setting-kline-batch-size").value = settings.quote_stream?.kline_batch_size ?? 1;
    $("#setting-kline-data-length").value = settings.quote_stream?.kline_data_length ?? 3;
    $("#setting-kline-timeout-seconds").value = settings.quote_stream?.kline_subscription_timeout_seconds ?? 30;
    $("#setting-near-expiry-months").value = settings.quote_stream?.near_expiry_months ?? 2;
    $("#setting-contract-months").value = settings.quote_stream?.contract_months ?? "2";
    $("#setting-min-days-to-expiry").value = settings.quote_stream?.min_days_to_expiry ?? 1;
    $("#setting-moneyness-recalc-seconds").value = settings.quote_stream?.moneyness_recalc_seconds ?? 30;
    const moneyness = new Set(String(settings.quote_stream?.moneyness_filter ?? "atm,itm,otm").split(",").map((item) => item.trim()));
    $$(".setting-moneyness").forEach((checkbox) => {
      checkbox.checked = moneyness.has(checkbox.value);
    });
    $("#setting-prioritize-near-expiry").checked = settings.quote_stream?.prioritize_near_expiry ?? true;
    $("#setting-quote-max-symbols").value = settings.quote_stream?.max_symbols ?? "";
    $("#setting-metrics-min-interval").value = settings.metrics?.min_interval_seconds ?? 30;
    $("#setting-metrics-chain-interval").value = settings.metrics?.underlying_chain_interval_seconds ?? 30;
    renderContractManagerStatus(settings.contract_manager?.status);
    renderQuoteStreamStatus(settings.quote_stream?.status);
    setNotice(
      "#settings-message",
      settings.tqsdk.password_configured ? "TQSDK 密码已配置。" : "TQSDK 密码尚未配置。",
      settings.tqsdk.password_configured ? "good" : "warn",
    );
    await loadApiKeys();
  } catch (error) {
    setNotice("#settings-message", `设置加载失败：${error.message}`, "bad");
  }
}

async function renderApiPage() {
  if (!$("#api").classList.contains("active")) return;
  try {
    const [status, settings] = await Promise.all([
      fetchJson("/api/webui/api-summary"),
      state.settings ? Promise.resolve(state.settings) : fetchJson("/api/settings"),
    ]);
    state.settings = settings;
    const api = status.api ?? {};
    const summary = status.summary ?? {};
    const authRequired = Boolean(settings.api?.auth_required ?? api.auth_required);
    const baseUrl = window.location.origin;
    const bind = settings.api?.bind ?? api.bind ?? "127.0.0.1";
    const port = settings.api?.port ?? api.port ?? window.location.port;
    setText("#api-health-status", status.status === "empty" ? "空库可用" : "正常");
    setTone("#api-health-status", status.status === "error" ? "bad" : status.status === "empty" ? "warn" : "good");
    setText("#api-auth-status", authRequired ? "需要 Key" : "本机免 Key");
    setTone("#api-auth-status", authRequired ? "warn" : "good");
    setText("#api-request-count", fmtNum(api.request_count));
    setText("#api-latency", `${fmtMaybeTrim(api.average_latency_ms, 1, true)} ms`);
    setText("#api-base-url", baseUrl);
    setText("#api-key-hint", authRequired ? "已开启强制认证，请在设置页创建或管理 Key。" : "当前未强制认证；如需外部集成可先创建 Key 再开启认证。");
    setText("#api-auth-header", authRequired ? "Authorization: Bearer <API_KEY>" : "当前未强制要求；开启后使用 Authorization: Bearer <API_KEY>");
    setText("#api-bind-info", `${bind}:${port}`);
    setText("#api-database-path", status.database_path ?? "--");
    setText("#api-active-options", fmtNum(summary.active_options));
    setText("#api-latest-update", fmtDateTime(summary.latest_quote_update));
    setText("#api-error-count", fmtNum(api.error_count));
    renderApiEndpointRows(authRequired);
    setNotice("#api-page-message", "API 状态已更新。API Key 管理保留在设置页，接入方从这里查看调用入口。", "good");
  } catch (error) {
    setText("#api-health-status", "不可用");
    setTone("#api-health-status", "bad");
    setNotice("#api-page-message", `API 状态加载失败：${error.message}`, "bad");
  }
}

function setText(selector, value) {
  const element = $(selector);
  if (element) element.textContent = value ?? "--";
}

function setTone(selector, tone) {
  const element = $(selector);
  if (!element) return;
  element.classList.remove("good", "warn", "bad");
  if (tone) element.classList.add(tone);
}

function renderApiEndpointRows(authRequired) {
  const endpoints = [
    ["GET", "/api/health", "服务健康检查和数据库连通性。", "/api/health"],
    ["GET", "/api/status", "汇总 API 请求统计、采集状态、实时订阅状态和数据覆盖。", "/api/status"],
    ["GET", "/api/quote-stream", "读取实时 Quote/K线 worker 状态和按标的聚合的订阅进度。", "/api/quote-stream"],
    ["GET", "/api/underlyings", "当前订阅范围内标的列表，适合监控平台做标的导航。", "/api/underlyings"],
    ["GET", "/api/options?underlying=...", "按标的读取期权链合约元数据。", "/api/options"],
    ["GET", "/api/options/{symbol}/quote", "读取单个期权合约当前 Quote 切片。", null],
  ];
  $("#api-endpoint-rows").innerHTML = endpoints.map(([method, path, purpose, href]) => `
    <tr>
      <td><span class="tag good">${method}</span></td>
      <td><code>${escapeHtml(path)}</code></td>
      <td>${escapeHtml(purpose)}${authRequired ? " 需要 Bearer Token。" : ""}</td>
      <td>${href ? `<a class="btn btn-sm btn-light table-action" href="${href}" target="_blank" rel="noreferrer">打开</a>` : `<span class="muted">填入 symbol 后调用</span>`}</td>
    </tr>
  `).join("");
}

async function saveCredentials() {
  const account = $("#setting-account").value.trim();
  const password = $("#setting-password").value;
  if (!account || !password) {
    setNotice("#settings-message", "账号和新密码都不能为空。", "warn");
    return;
  }
  await fetchJsonWithBody("/api/settings/tqsdk-credentials", "PUT", { account, password });
  $("#setting-password").value = "";
  setNotice("#settings-message", "TQSDK 凭据已保存。", "good");
  await loadSettings();
}

async function testCredentials() {
  const result = await fetchJsonWithBody("/api/settings/test-tqsdk-connection", "POST", {});
  const tone = result.status === "ok" ? "good" : result.status === "failed" ? "bad" : "warn";
  setNotice("#settings-message", result.message, tone);
}

async function saveRuntimeSettings() {
  const updates = [
    ["api.bind", $("#setting-api-bind").value.trim() || "127.0.0.1"],
    ["api.port", $("#setting-api-port").value || "8770"],
    ["api.auth_required", $("#setting-auth-required").checked ? "true" : "false"],
    ["collection.refresh_interval_seconds", $("#setting-refresh-interval").value || "30"],
    ["collection.option_batch_size", $("#setting-batch-size").value || "20"],
    ["collection.wait_cycles", $("#setting-wait-cycles").value || "1"],
    ["collection.max_batches", $("#setting-max-batches").value || "100"],
    ["contract_manager.refresh_interval_seconds", $("#setting-contract-manager-refresh-interval").value || "3600"],
    ["quote_stream.workers", $("#setting-quote-workers").value || "1"],
    ["quote_stream.quote_shard_size", $("#setting-quote-shard-size").value || "1000"],
    ["quote_stream.kline_batch_size", $("#setting-kline-batch-size").value || "1"],
    ["quote_stream.kline_data_length", $("#setting-kline-data-length").value || "3"],
    ["quote_stream.kline_subscription_timeout_seconds", $("#setting-kline-timeout-seconds").value || "30"],
    ["quote_stream.near_expiry_months", $("#setting-near-expiry-months").value || "2"],
    ["quote_stream.contract_months", $("#setting-contract-months").value || "2"],
    ["quote_stream.min_days_to_expiry", $("#setting-min-days-to-expiry").value || "1"],
    ["quote_stream.moneyness_filter", selectedMoneynessFilter()],
    ["quote_stream.moneyness_recalc_seconds", $("#setting-moneyness-recalc-seconds").value || "30"],
    ["quote_stream.prioritize_near_expiry", $("#setting-prioritize-near-expiry").checked ? "true" : "false"],
    ["quote_stream.max_symbols", $("#setting-quote-max-symbols").value || ""],
    ["metrics.min_interval_seconds", $("#setting-metrics-min-interval").value || "30"],
    ["metrics.underlying_chain_interval_seconds", $("#setting-metrics-chain-interval").value || "30"],
  ];
  for (const [key, value] of updates) {
    await fetchJsonWithBody(`/api/settings/${encodeURIComponent(key)}`, "PUT", { value });
  }
  setNotice("#settings-message", "运行设置已保存。", "good");
  await loadSettings();
}

async function triggerRefresh() {
  setNotice("#settings-message", "正在触发采集刷新。", "warn");
  const result = await fetchJsonWithBody("/api/refresh", "POST", {});
  setNotice("#settings-message", `${result.message} 报告：${result.report_path}`, result.status === "started" ? "good" : "warn");
  await refreshOverview();
}

function selectedMoneynessFilter() {
  const selected = $$(".setting-moneyness")
    .filter((checkbox) => checkbox.checked)
    .map((checkbox) => checkbox.value);
  return selected.length ? selected.join(",") : "atm,itm,otm";
}

async function startContractManager() {
  setContractManagerControls({ running: true, busy: true });
  setNotice("#contract-manager-message", "正在启动合约管理器并刷新合约列表。", "warn");
  try {
    await saveRuntimeSettings();
    const result = await fetchJsonWithBody("/api/contract-manager/start", "POST", {});
    renderContractManagerStatus(result);
    const quoteStatus = await fetchJson("/api/quote-stream");
    renderQuoteStreamStatus(quoteStatus);
  } catch (error) {
    setNotice("#contract-manager-message", `合约管理器启动失败：${error.message}`, "bad");
    setContractManagerControls({ running: false });
  }
}

async function stopContractManager() {
  setContractManagerControls({ running: true, busy: true });
  setNotice("#contract-manager-message", "正在停止合约管理器。", "warn");
  try {
    const result = await fetchJsonWithBody("/api/contract-manager/stop", "POST", {});
    renderContractManagerStatus(result);
    const quoteStatus = await fetchJson("/api/quote-stream");
    renderQuoteStreamStatus(quoteStatus);
  } catch (error) {
    setNotice("#contract-manager-message", `合约管理器停止失败：${error.message}`, "bad");
    setContractManagerControls({ running: true });
  }
}

async function startQuoteStream() {
  setQuoteStreamControls({ running: true, busy: true });
  setNotice("#quote-stream-message", "正在启动实时订阅 worker。", "warn");
  try {
    await saveRuntimeSettings();
    const payload = {
      workers: Number($("#setting-quote-workers").value || 1),
      quote_shard_size: Number($("#setting-quote-shard-size").value || 1000),
      kline_batch_size: Number($("#setting-kline-batch-size").value || 1),
      contract_months: $("#setting-contract-months").value || "2",
    };
    const maxSymbols = $("#setting-quote-max-symbols").value;
    if (maxSymbols) payload.max_symbols = Number(maxSymbols);
    const result = await fetchJsonWithBody("/api/quote-stream/start", "POST", payload);
    renderQuoteStreamStatus(result);
    await refreshOverview();
  } catch (error) {
    setNotice("#quote-stream-message", `实时订阅启动失败：${error.message}`, "bad");
    setQuoteStreamControls({ running: false, contractListReady: false });
  }
}

async function stopQuoteStream() {
  setQuoteStreamControls({ running: true, busy: true });
  setNotice("#quote-stream-message", "正在停止实时订阅 worker。", "warn");
  try {
    const result = await fetchJsonWithBody("/api/quote-stream/stop", "POST", {});
    renderQuoteStreamStatus(result);
    await refreshOverview();
  } catch (error) {
    setNotice("#quote-stream-message", `实时订阅停止失败：${error.message}`, "bad");
    setQuoteStreamControls({ running: true });
  }
}

function renderQuoteStreamStatus(status) {
  if (!status) {
    setNotice("#quote-stream-message", "实时订阅状态暂不可用。", "warn");
    setQuoteStreamControls({ running: false });
    return;
  }
  const configuredWorkers = Number($("#setting-quote-workers")?.value || status.worker_count || 1);
  const starting = isQuoteStreamStarting(status);
  const discovering = isContractDiscoveryRunning(status);
  const hasContracts = status.active_option_count === null || status.active_option_count === undefined || Number(status.active_option_count) > 0;
  const contractListReady = Boolean(status.contract_list_ready);
  const stateText = discovering
    ? "正在拉取合约列表"
    : starting
    ? "实时订阅启动中"
    : status.running
    ? "实时订阅运行中"
    : "实时订阅未运行";
  const workerText = status.running
    ? `正在运行 ${fmtNum(status.worker_count || configuredWorkers)} 个独立 worker 进程`
    : discovering
    ? "拉取完成后可启动实时订阅"
    : starting
    ? `准备启动 ${fmtNum(configuredWorkers)} 个独立 worker 进程`
    : `配置为启动 ${fmtNum(configuredWorkers)} 个独立 worker 进程`;
  const contractText = hasContracts
    ? contractListReady
      ? `合约列表已准备：${fmtNum(status.active_option_count)} 个可订阅期权合约`
      : `合约管理器未就绪，无法订阅行情。当前本地库已有 ${fmtNum(status.active_option_count)} 个历史合约，请先启动合约管理器。`
    : "合约管理器尚未刷新出可订阅合约，无法订阅行情。";
  const message = status.message ? `上次状态：${status.message}` : "等待启动。";
  const reports = status.report_dir ? `报告目录：${status.report_dir}` : "";
  const health = status.health ? `网络健康：${status.health.label ?? status.health.status}` : "";
  const moneyness = fmtMoneynessHint(status);
  const klineGuard = fmtKlineGuardHint(status);
  const notify = fmtTqsdkNotifyHint(status);
  const tone = status.health?.tone === "bad" ? "bad" : status.running ? "good" : status.status === "blocked" || !hasContracts || !contractListReady ? "bad" : "warn";
  setNotice("#quote-stream-message", [stateText, workerText, contractText, health, moneyness, klineGuard, notify, message, reports].filter(Boolean).join(" · "), tone);
  setQuoteStreamControls({
    running: Boolean(status.running),
    busy: starting || discovering,
    discovering,
    hasContracts,
    contractListReady,
  });
}

function renderContractManagerStatus(status) {
  if (!status) {
    setNotice("#contract-manager-message", "合约管理器状态暂不可用。", "warn");
    setContractManagerControls({ running: false });
    return;
  }
  const stateText = status.refresh_running
    ? "合约管理器刷新中"
    : status.healthy
    ? "合约管理器正常"
    : status.running
    ? "合约管理器运行中"
    : "合约管理器未运行";
  const contractText = `合约列表：${fmtNum(status.active_option_count)} 个可订阅期权合约`;
  const heartbeat = status.last_heartbeat_at ? `心跳：${fmtDateTime(status.last_heartbeat_at)}` : "";
  const success = status.last_success_at ? `最近成功：${fmtDateTime(status.last_success_at)}` : "";
  const interval = `刷新间隔：${fmtNum(status.refresh_interval_seconds)} 秒`;
  const tone = status.healthy ? "good" : status.running ? "warn" : "bad";
  setNotice(
    "#contract-manager-message",
    [stateText, contractText, interval, heartbeat, success, status.message].filter(Boolean).join(" · "),
    tone,
  );
  setContractManagerControls({ running: Boolean(status.running), busy: Boolean(status.refresh_running) });
}

function setContractManagerControls({ running, busy = false }) {
  $("#start-contract-manager").disabled = Boolean(running || busy);
  $("#stop-contract-manager").disabled = Boolean(!running && !busy);
  $("#setting-contract-manager-refresh-interval").disabled = Boolean(running || busy);
}

function isQuoteStreamStarting(quoteStream) {
  return !quoteStream?.running
    && !quoteStream?.contract_discovery_running
    && quoteStream?.status === QuoteStreamStatus.STARTING;
}

function isContractDiscoveryRunning(quoteStream) {
  return Boolean(quoteStream?.contract_discovery_running);
}

function setQuoteStreamControls({ running, busy = false, discovering = false, hasContracts = true, contractListReady = false }) {
  $("#start-quote-stream").disabled = Boolean(running || busy || discovering || !hasContracts || !contractListReady);
  $("#stop-quote-stream").disabled = Boolean(!running || busy);
  [
    "setting-quote-workers",
    "setting-quote-shard-size",
    "setting-kline-batch-size",
    "setting-kline-data-length",
    "setting-kline-timeout-seconds",
    "setting-near-expiry-months",
    "setting-contract-months",
    "setting-min-days-to-expiry",
    "setting-moneyness-recalc-seconds",
    "setting-quote-max-symbols",
    "setting-metrics-min-interval",
    "setting-metrics-chain-interval",
    "setting-prioritize-near-expiry",
  ].forEach((id) => {
    $(`#${id}`).disabled = Boolean(running || busy);
  });
  $$(".setting-moneyness").forEach((checkbox) => {
    checkbox.disabled = Boolean(running || busy);
  });
}

function setNotice(selector, text, tone = "warn") {
  const element = typeof selector === "string" ? $(selector) : selector;
  if (!element) return;
  element.textContent = text;
  element.classList.remove("good", "warn", "bad");
  element.classList.add(tone);
}

async function loadApiKeys() {
  const keys = await fetchJson("/api/api-keys");
  $("#api-key-rows").innerHTML = keys.length ? keys.map((key) => `<tr>
    <td>${key.key_id}</td><td>${escapeHtml(key.name)}</td><td class="mono" title="${escapeHtml(key.fingerprint)}"><span class="api-key-fingerprint-text">${escapeHtml(key.fingerprint)}</span></td>
    <td>${escapeHtml(apiScopeLabel(key.scope))}</td><td>${key.enabled ? "启用" : "停用"}</td><td>${escapeHtml(fmtDateTime(key.last_used_at))}</td>
    <td><div class="api-key-actions-cell">
      <button class="btn btn-sm btn-light table-action" type="button" data-copy="${escapeHtml(key.fingerprint)}">复制</button>
      <button class="btn btn-sm btn-light table-action" type="button" data-delete-key="${key.key_id}">删除</button>
    </div></td>
  </tr>`).join("") : `<tr class="empty-row"><td colspan="7">暂无 API Key</td></tr>`;
  $$("#api-key-rows [data-copy]").forEach((button) => {
    button.addEventListener("click", () => copyText(button.dataset.copy, "#api-key-message"));
  });
  $$("#api-key-rows [data-delete-key]").forEach((button) => {
    button.addEventListener("click", () => deleteApiKey(button.dataset.deleteKey));
  });
}

async function createApiKey() {
  const name = $("#api-key-name").value.trim();
  const scope = $("#api-key-scope").value.trim() || "read";
  if (!name) {
    setNotice("#settings-message", "API Key 名称不能为空。", "warn");
    return;
  }
  const created = await fetchJsonWithBody("/api/api-keys", "POST", { name, scope });
  setApiKeySecretNotice(created.secret);
  await loadApiKeys();
}

function apiScopeLabel(scope) {
  return scope === "read" ? "只读访问" : scope;
}

function setApiKeySecretNotice(secret) {
  const element = $("#api-key-message");
  element.classList.remove("good", "warn", "bad");
  element.classList.add("good");
  element.innerHTML = `
    <span>新 API Key：<strong class="mono">${escapeHtml(secret)}</strong></span>
    <button class="btn btn-sm btn-light table-action" type="button" id="copy-new-api-key">复制</button>
  `;
  $("#copy-new-api-key").addEventListener("click", () => copyText(secret, "#api-key-message"));
}

async function deleteApiKey(keyId) {
  const confirmed = window.confirm("删除后这个 API Key 将无法继续使用，确定删除吗？");
  if (!confirmed) return;
  await fetchJsonWithBody(`/api/api-keys/${encodeURIComponent(keyId)}`, "DELETE", {});
  setNotice("#api-key-message", "API Key 已删除。", "good");
  await loadApiKeys();
}

async function copyText(value, noticeSelector) {
  try {
    await navigator.clipboard.writeText(value ?? "");
    setNotice(noticeSelector, "已复制。", "good");
  } catch {
    setNotice(noticeSelector, "复制失败，请手动选中文本复制。", "bad");
  }
}

async function fetchJsonWithBody(url, method, body) {
  const options = { method, headers: { "Content-Type": "application/json" } };
  if (body !== undefined) options.body = JSON.stringify(body);
  const response = await fetch(url, {
    ...options,
  });
  if (!response.ok) throw new Error(`${url} ${response.status}`);
  return response.json();
}

function renderOverview() {
  const summary = state.overview.summary;
  $("#latest-update").textContent = fmtDateTime(summary.latest_update ?? state.overview.latest_run?.finished_at);
  $("#summary-options").textContent = fmtNum(summary.active_options);
  $("#summary-iv").textContent = `IV ${fmtPct(summary.iv_coverage)} · Greeks ${fmtPct(summary.greeks_coverage)}`;
  $("#summary-kline").textContent = summary.kline_coverage >= 0.999 ? "正常" : "补齐中";
  $("#summary-batches").textContent = fmtCollectionProgress(state.overview.collection);
  updateHealthPill(summary);
  renderExchangeCards();
  renderCollectionProgress();
  renderExchangeTabs();
  renderUnderlyingRows();
}

function updateHealthPill(summary) {
  const hasData = Number(summary.active_options) > 0;
  $("#health-text").textContent = hasData ? "数据已加载" : "等待采集";
  $("#health-dot").classList.toggle("warn", !hasData);
}

function renderExchangeCards() {
  const exchanges = state.overview.exchanges?.length ? state.overview.exchanges : [];
  $("#exchange-cards").innerHTML = exchanges.slice(0, 5).map((group) => {
    const quoteCoverage = group.quote_coverage ?? 0;
    const ivCoverage = group.iv_coverage ?? 0;
    const cls = quoteCoverage < 0.95 ? "bad" : ivCoverage < 0.98 ? "warn" : "";
    return `<div class="exchange-card ${cls}">
      <strong>${escapeHtml(exchangeNames[group.exchange_id] ?? group.exchange_id)}</strong><span>${fmtNum(group.product_count)}品种 / ${fmtNum(group.underlying_count)}标的</span>
      <div>Quote ${fmtPct(quoteCoverage)} · IV ${fmtPct(ivCoverage)}</div>
    </div>`;
  }).join("");
}

function fmtCollectionProgress(progress) {
  if (!progress || !progress.active_batches) return "--";
  return `${fmtNum(progress.success_batches)} / ${fmtNum(progress.active_batches)}`;
}

function fmtQuoteStreamValue(quoteStream) {
  if (isQuoteStreamStarting(quoteStream)) return "启动中";
  if (!quoteStream?.running) return "空闲";
  const workers = Number(quoteStream.worker_count ?? 0);
  return workers > 0 ? `${fmtNum(workers)} worker` : "运行中";
}

function fmtQuoteStreamHint(quoteStream) {
  const message = localizeStatusMessage(quoteStream?.message);
  const pids = quoteStream?.pids ?? [];
  if (quoteStream?.running && pids.length) return `PID ${pids.join(", ")}`;
  if (message) return message;
  if (quoteStream?.finished_at) return fmtDateTime(quoteStream.finished_at);
  return quoteStream?.running ? "实时订阅已启动" : "等待启动";
}

function fmtRealtimeHealthValue(quoteStream) {
  const health = quoteStream?.health;
  if (!health) return "--";
  return health.label ?? health.status ?? "--";
}

function fmtRealtimeHealthHint(quoteStream) {
  const health = quoteStream?.health;
  if (!health) return "等待健康检测。";
  const notify = fmtTqsdkNotifyHint(quoteStream);
  const contractManager = fmtContractManagerHint(quoteStream);
  return [health.message ?? "等待健康检测。", contractManager, notify].filter(Boolean).join(" · ");
}

function realtimeHealthTone(quoteStream) {
  const tone = quoteStream?.health?.tone;
  if (tone === "bad" || tone === "warn" || tone === "good") return tone;
  return "";
}

function fmtTqsdkNotifyHint(quoteStream) {
  const progress = quoteStream?.progress ?? {};
  const content = progress.last_tqsdk_notify_content;
  if (!content) return "";
  const status = progress.tqsdk_connection_status;
  const statusLabels = {
    connected: "连接正常",
    reconnecting: "重连中",
    disconnected: "已断开",
  };
  const label = statusLabels[status] ? `TQSDK ${statusLabels[status]}` : "TQSDK 通知";
  return `${label}：${content}`;
}

function fmtContractManagerHint(quoteStream) {
  const progress = quoteStream?.progress ?? {};
  const refreshCount = Number(progress.contract_refresh_count ?? 0);
  const added = Number(progress.contract_reconcile_added_quote_count ?? 0)
    + Number(progress.contract_reconcile_added_kline_count ?? 0);
  const removed = Number(progress.contract_reconcile_removed_quote_count ?? 0)
    + Number(progress.contract_reconcile_removed_kline_count ?? 0);
  if (!refreshCount && !added && !removed) return "";
  const timeText = progress.last_contract_refresh_at ? fmtDateTime(progress.last_contract_refresh_at) : "--";
  return `合约管理 ${fmtNum(refreshCount)}次 · 增${fmtNum(added)} / 减${fmtNum(removed)} · ${timeText}`;
}

function fmtMoneynessHint(quoteStream) {
  const progress = quoteStream?.progress ?? {};
  const recalcCount = Number(progress.moneyness_recalc_count ?? 0);
  const matchCount = Number(progress.moneyness_kline_match_count ?? 0);
  const stickyCount = Number(progress.moneyness_sticky_kline_count ?? 0);
  const addedCount = Number(progress.moneyness_added_kline_count ?? 0);
  if (!recalcCount && !matchCount && !stickyCount && !addedCount) return "";
  const skippedSession = Number(progress.moneyness_skipped_out_of_session_count ?? 0);
  const skippedPrice = Number(progress.moneyness_skipped_missing_price_count ?? 0);
  return `虚实度K线 命中${fmtNum(matchCount)} / sticky${fmtNum(stickyCount)} / 新增${fmtNum(addedCount)} / 跳过${fmtNum(skippedSession + skippedPrice)}`;
}

function fmtKlineGuardHint(quoteStream) {
  const progress = quoteStream?.progress ?? {};
  const timeoutCount = Number(progress.kline_subscription_timeout_count ?? 0);
  const errorCount = Number(progress.kline_subscription_error_count ?? 0);
  const current = progress.current_kline_symbol;
  if (!timeoutCount && !errorCount && !current) return "";
  const currentText = current ? `当前K线 ${current}` : "";
  const errorText = timeoutCount || errorCount ? `K线跳过 超时${fmtNum(timeoutCount)} / 失败${fmtNum(errorCount)}` : "";
  return [currentText, errorText].filter(Boolean).join(" · ");
}

function fmtBatchRefreshHint(refresh) {
  const message = localizeStatusMessage(refresh?.message);
  if (refresh?.running) return message ?? "批量补采正在按分片窗口推进。";
  if (message?.includes("本地服务已重启")) {
    return "本地服务重启后，上一次批量补采已停止；实时订阅单独运行。";
  }
  if (refresh?.finished_at) return `上次批量补采结束：${fmtDateTime(refresh.finished_at)}`;
  return "当前没有批量补采任务；实时订阅看上方进度。";
}

function quoteStreamProgress(quoteStream) {
  const progress = quoteStream?.progress ?? {};
  const total = Number(progress.total_objects ?? 0);
  const subscribed = Number(progress.subscribed_objects ?? 0);
  const ratio = total > 0
    ? Math.min(1, Math.max(0, Number(progress.completion_ratio ?? subscribed / total)))
    : 0;
  return {
    total,
    subscribed,
    ratio,
    quoteSubscribed: Number(progress.quote_subscribed ?? 0),
    quoteTotal: Number(progress.quote_total ?? 0),
    klineSubscribed: Number(progress.kline_subscribed ?? 0),
    klineTotal: Number(progress.kline_total ?? 0),
    nearExpiryMonths: Number(progress.near_expiry_months ?? 0),
    nearExpirySubscribed: Number(progress.near_expiry_subscribed ?? 0),
    nearExpiryTotal: Number(progress.near_expiry_total ?? 0),
    nearExpiryQuoteSubscribed: Number(progress.near_expiry_quote_subscribed ?? 0),
    nearExpiryQuoteTotal: Number(progress.near_expiry_quote_total ?? 0),
    nearExpiryKlineSubscribed: Number(progress.near_expiry_kline_subscribed ?? 0),
    nearExpiryKlineTotal: Number(progress.near_expiry_kline_total ?? 0),
    contractMonths: progress.contract_months ?? quoteStream?.contract_months ?? null,
    moneynessFilter: progress.moneyness_filter ?? null,
    moneynessRecalcCount: Number(progress.moneyness_recalc_count ?? 0),
    moneynessMatchCount: Number(progress.moneyness_kline_match_count ?? 0),
    moneynessStickyCount: Number(progress.moneyness_sticky_kline_count ?? 0),
    moneynessAddedCount: Number(progress.moneyness_added_kline_count ?? 0),
    moneynessSkippedOutOfSession: Number(progress.moneyness_skipped_out_of_session_count ?? 0),
    moneynessSkippedMissingPrice: Number(progress.moneyness_skipped_missing_price_count ?? 0),
    lastMoneynessRecalcAt: progress.last_moneyness_recalc_at,
    currentKlineSymbol: progress.current_kline_symbol,
    currentKlineStartedAt: progress.current_kline_started_at,
    klineTimeoutSeconds: Number(progress.kline_subscription_timeout_seconds ?? 0),
    klineTimeoutCount: Number(progress.kline_subscription_timeout_count ?? 0),
    klineErrorCount: Number(progress.kline_subscription_error_count ?? 0),
    lastKlineErrorSymbol: progress.last_kline_subscription_error_symbol,
    lastKlineError: progress.last_kline_subscription_error,
    refreshingContracts: Boolean(quoteStream?.contract_discovery_running),
    workerReports: Number(progress.worker_reports ?? 0),
    updatedAt: progress.updated_at,
    elapsedSeconds: progress.elapsed_seconds,
    averageSecondsPerObject: progress.average_seconds_per_object,
    estimatedRemainingSeconds: progress.estimated_remaining_seconds,
    activeStage: progress.active_stage,
    stageLabel: progress.stage_label,
    stageAverageSecondsPerObject: progress.stage_average_seconds_per_object,
    stageEstimatedRemainingSeconds: progress.stage_estimated_remaining_seconds,
    estimatedRemainingIsTotal: Boolean(progress.estimated_remaining_is_total),
    waitingForKlineEta: Boolean(progress.waiting_for_kline_eta),
    status: progress.status ?? quoteStream?.status ?? (quoteStream?.running ? QuoteStreamStatus.RUNNING : QuoteStreamStatus.STOPPED),
  };
}

function activeStreamStage(progress) {
  if (!progress) return null;
  if (progress.quoteTotal > 0 && progress.quoteSubscribed < progress.quoteTotal) return "quote";
  if (progress.klineTotal > 0 && progress.klineSubscribed < progress.klineTotal) return "kline";
  return null;
}

function streamStageCounters(progress, stage) {
  if (stage === "quote") {
    return { subscribed: progress.quoteSubscribed, total: progress.quoteTotal, label: "Quote" };
  }
  if (stage === "kline") {
    return { subscribed: progress.klineSubscribed, total: progress.klineTotal, label: "K线对象" };
  }
  return { subscribed: progress.subscribed, total: progress.total, label: "订阅" };
}

function observeStreamProgress(progress) {
  const stage = progress.activeStage || activeStreamStage(progress);
  if (!stage || stage === "running" || stage === "stopped") {
    state.streamEta.lastSample = null;
    state.streamEta.estimate = null;
    return;
  }
  state.streamEta.estimate = {
    stage,
    stageLabel: progress.stageLabel || streamStageCounters(progress, stage).label,
    totalRemainingSeconds: progress.estimatedRemainingIsTotal ? progress.estimatedRemainingSeconds : null,
    stageRemainingSeconds: progress.stageEstimatedRemainingSeconds,
    stageAverageSecondsPerObject: progress.stageAverageSecondsPerObject,
    waitingForKlineEta: progress.waitingForKlineEta,
    estimatedAtMs: Date.now(),
  };
}

function liveCountdown(value, estimatedAtMs) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return null;
  const elapsedSeconds = (Date.now() - estimatedAtMs) / 1000;
  return Math.max(0, Number(value) - elapsedSeconds);
}

function liveStreamEta() {
  const estimate = state.streamEta.estimate;
  if (!estimate) return null;
  return liveCountdown(estimate.totalRemainingSeconds, estimate.estimatedAtMs);
}

function liveStageEta() {
  const estimate = state.streamEta.estimate;
  if (!estimate) return null;
  return liveCountdown(estimate.stageRemainingSeconds, estimate.estimatedAtMs);
}

function streamEtaSummary(progress) {
  const stage = progress.activeStage || activeStreamStage(progress);
  if (!stage || stage === "running") return "订阅对象已建立，进入实时运行。";
  const counters = streamStageCounters(progress, stage);
  const estimate = state.streamEta.estimate?.stage === stage ? state.streamEta.estimate : null;
  const totalRemaining = liveStreamEta();
  const stageRemaining = liveStageEta();
  if (!estimate) {
    return `${counters.label}速度校准中`;
  }
  const speedText = estimate.stageAverageSecondsPerObject === null || estimate.stageAverageSecondsPerObject === undefined
    ? `${counters.label}速度校准中`
    : `${counters.label}平均 ${fmtAvgDuration(estimate.stageAverageSecondsPerObject)}`;
  if (totalRemaining !== null) {
    return `${speedText} · 总预计剩余 ${fmtDuration(totalRemaining)}`;
  }
  if (stageRemaining !== null && estimate.waitingForKlineEta) {
    return `${speedText} · ${counters.label}剩余 ${fmtDuration(stageRemaining)} · 总剩余待K线速度校准`;
  }
  if (stageRemaining !== null) {
    return `${speedText} · ${counters.label}剩余 ${fmtDuration(stageRemaining)}`;
  }
  return speedText;
}

function streamNearExpiryKlineMarker(progress) {
  if (!progress || progress.klineTotal <= 0 || progress.nearExpiryKlineTotal <= 0) return "";
  if (progress.contractMonths !== "all") return "";
  const markerRatio = Math.min(1, Math.max(0, progress.nearExpiryKlineTotal / progress.klineTotal));
  const months = progress.nearExpiryMonths || 2;
  const title = `近${months}月K线对象 ${fmtNum(progress.nearExpiryKlineSubscribed)} / ${fmtNum(progress.nearExpiryKlineTotal)}`;
  return `<div class="stream-progress-marker" title="${escapeHtml(title)}" style="left: ${escapeHtml((markerRatio * 100).toFixed(2))}%"></div>`;
}

function streamNearExpirySummary(progress) {
  if (!progress || progress.contractMonths !== "all") return "";
  const months = progress.nearExpiryMonths || 2;
  return ` · 近${fmtNum(months)}月K线对象 ${fmtNum(progress.nearExpiryKlineSubscribed)}/${fmtNum(progress.nearExpiryKlineTotal)} · 近${fmtNum(months)}月Quote ${fmtNum(progress.nearExpiryQuoteSubscribed)}/${fmtNum(progress.nearExpiryQuoteTotal)}`;
}

function streamContractScopeText(progress) {
  const value = String(progress?.contractMonths ?? "").trim().toLowerCase();
  if (value === "all") return "订阅范围：全部合约";
  const months = Number(value);
  if (Number.isFinite(months) && months > 0) return `订阅范围：近${fmtNum(months)}个月合约`;
  return "订阅范围：按当前设置";
}

function streamMoneynessSummary(progress) {
  const filter = moneynessFilterLabel(progress?.moneynessFilter);
  const match = Number(progress?.moneynessMatchCount ?? 0);
  const sticky = Number(progress?.moneynessStickyCount ?? 0);
  const added = Number(progress?.moneynessAddedCount ?? 0);
  const skipped = Number(progress?.moneynessSkippedOutOfSession ?? 0)
    + Number(progress?.moneynessSkippedMissingPrice ?? 0);
  const timeout = Number(progress?.klineTimeoutCount ?? 0);
  const errors = Number(progress?.klineErrorCount ?? 0);
  const guard = timeout || errors ? `，订阅跳过 超时 ${fmtNum(timeout)} / 失败 ${fmtNum(errors)}` : "";
  return `虚实度K线：${filter}，当前命中 ${fmtNum(match)}，日内累计 ${fmtNum(sticky)}，新增 ${fmtNum(added)}，跳过 ${fmtNum(skipped)}${guard}`;
}

function moneynessFilterLabel(value) {
  const labels = { otm: "虚值", atm: "平值", itm: "实值" };
  const items = String(value || "atm,itm,otm")
    .split(",")
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean);
  const unique = [...new Set(items)];
  if (!unique.length || unique.length >= 3) return "全选";
  return unique.map((item) => labels[item] ?? item).join("+");
}

function renderCollectionProgress() {
  const summary = state.overview.summary ?? {};
  const progress = state.overview.collection ?? {};
  const refresh = state.overview.refresh ?? {};
  const quoteStream = state.overview.quote_stream ?? {};
  const active = Number(progress.active_batches ?? 0);
  const pending = Number(progress.pending_batches ?? 0);
  const failed = Number(progress.failed_batches ?? 0);
  const complete = active > 0 && Number(progress.success_batches ?? 0) >= active;
  const streamProgress = quoteStreamProgress(quoteStream);
  const quoteRatio = streamProgress.quoteTotal > 0
    ? Math.min(1, Math.max(0, streamProgress.quoteSubscribed / streamProgress.quoteTotal))
    : 0;
  const klineRatio = streamProgress.klineTotal > 0
    ? Math.min(1, Math.max(0, streamProgress.klineSubscribed / streamProgress.klineTotal))
    : 0;
  const streamStage = fmtStreamStage(streamProgress, quoteStream.running);
  const cards = [
    ["总分片", fmtNum(active), `${fmtNum(progress.planned_underlyings ?? 0)} 标的`, ""],
    ["已完成", fmtNum(progress.success_batches ?? 0), fmtPct(progress.completion_ratio ?? 0), complete ? "good" : ""],
    ["待采集", fmtNum(pending), "等待窗口执行", pending > 0 ? "warn" : ""],
    ["失败待重试", fmtNum(failed), "不会丢失进度", failed > 0 ? "bad" : "good"],
    [
      "批量补采",
      refresh.running ? "运行中" : "未运行",
      fmtBatchRefreshHint(refresh),
      refresh.running ? "warn" : "",
    ],
    [
      "实时 Quote",
      fmtQuoteStreamValue(quoteStream),
      fmtQuoteStreamHint(quoteStream),
      quoteStream.running ? "warn" : "",
    ],
    [
      "开盘网络",
      fmtRealtimeHealthValue(quoteStream),
      fmtRealtimeHealthHint(quoteStream),
      realtimeHealthTone(quoteStream),
    ],
    ["最新数据写入", fmtDateTime(summary.latest_update), summary.latest_update ? "Quote/指标/K线最新落库" : "暂无写入", ""],
    ["最近批量分片更新", fmtDateTime(progress.latest_batch_update), progress.latest_batch_update ? "仅批量采集进度" : "暂无更新", ""],
  ];
  const streamProgressHtml = `
    <div class="progress-card stream-progress-card ${quoteStream.running || streamProgress.refreshingContracts ? "warn" : ""}">
      <span>实时订阅对象进度</span>
      <strong>${escapeHtml(streamStage)}</strong>
      <div class="stream-split-grid">
        <div>
          <div class="stream-split-title"><span>Quote</span><span>${escapeHtml(fmtPct(quoteRatio))}</span></div>
          <div class="stream-progress-line">
            <small>${escapeHtml(fmtNum(streamProgress.quoteSubscribed))} / ${escapeHtml(fmtNum(streamProgress.quoteTotal))}</small>
            <div class="stream-progress-track" aria-hidden="true">
              <div class="stream-progress-fill" style="width: ${escapeHtml((quoteRatio * 100).toFixed(1))}%"></div>
            </div>
            <small class="stream-progress-meta">${escapeHtml(streamProgress.quoteSubscribed >= streamProgress.quoteTotal && streamProgress.quoteTotal > 0 ? "完成" : "订阅中")}</small>
          </div>
        </div>
        <div>
          <div class="stream-split-title"><span>K线对象</span><span>${escapeHtml(fmtPct(klineRatio))}</span></div>
          <div class="stream-progress-line">
            <small>${escapeHtml(fmtNum(streamProgress.klineSubscribed))} / ${escapeHtml(fmtNum(streamProgress.klineTotal))}</small>
            <div class="stream-progress-track" aria-hidden="true">
              <div class="stream-progress-fill" style="width: ${escapeHtml((klineRatio * 100).toFixed(1))}%"></div>
              ${streamNearExpiryKlineMarker(streamProgress)}
            </div>
            <small class="stream-progress-meta">${escapeHtml(streamProgress.klineSubscribed >= streamProgress.klineTotal && streamProgress.klineTotal > 0 ? "完成" : "订阅中")}</small>
          </div>
        </div>
      </div>
      <small>${escapeHtml(streamContractScopeText(streamProgress))} · ${escapeHtml(streamMoneynessSummary(streamProgress))} · ${escapeHtml(fmtNum(streamProgress.workerReports))} worker报告${escapeHtml(streamNearExpirySummary(streamProgress))} · 订阅总用时 ${escapeHtml(fmtDuration(streamProgress.elapsedSeconds))} · ${escapeHtml(streamEtaSummary(streamProgress))}</small>
    </div>
  `;
  $("#collection-progress").innerHTML = streamProgressHtml + cards.map(([label, value, hint, tone]) => `
    <div class="progress-card ${escapeHtml(tone)}">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(value)}</strong>
      <small>${escapeHtml(hint)}</small>
    </div>
  `).join("");
  const failures = progress.recent_failures ?? [];
  const failureNotice = $("#collection-failures");
  failureNotice.hidden = failures.length === 0;
  failureNotice.innerHTML = failures.length
    ? `最近失败分片：${failures.map((item) => `${escapeHtml(item.underlying_symbol)}#${escapeHtml(item.batch_index)} ${escapeHtml(item.last_error ?? item.status)}`).join("；")}`
    : "";
}

function renderExchangeTabs() {
  const rows = scopedUnderlyingRows();
  const exchanges = unique(rows.map((row) => row.exchange_id));
  if (!exchanges.includes(state.selectedExchange) && state.selectedExchange !== "ALL") {
    state.selectedExchange = "ALL";
  }
  const tabs = [
    { value: "ALL", label: "全部", count: rows.length },
    ...exchanges.map((exchange) => ({
      value: exchange,
      label: exchangeNames[exchange] ?? exchange,
      count: rows.filter((row) => row.exchange_id === exchange).length,
    })),
  ];
  $("#exchange-tabs").innerHTML = tabs.map((tab) => `
    <button class="exchange-tab ${tab.value === state.selectedExchange ? "active" : ""}" type="button" role="tab" data-exchange="${escapeHtml(tab.value)}">
      ${escapeHtml(tab.label)} ${escapeHtml(fmtNum(tab.count))}
    </button>
  `).join("");
  $$("#exchange-tabs .exchange-tab").forEach((tab) => {
    tab.addEventListener("click", () => {
      state.selectedExchange = tab.dataset.exchange ?? "ALL";
      renderExchangeTabs();
      renderUnderlyingRows();
    });
  });
}

function renderUnderlyingRows() {
  const scopeRows = scopedUnderlyingRows();
  const rows = scopeRows.filter((row) => {
    const exchangeMatch = state.selectedExchange === "ALL" || row.exchange_id === state.selectedExchange;
    const issueMatch = !state.showIssuesOnly || !["正常", "已订阅"].includes(row.status);
    return exchangeMatch && issueMatch;
  });
  const scopeText = streamContractScopeText(quoteStreamProgress(state.overview?.quote_stream));
  $("#underlying-scope-note").textContent = `${scopeText.replace("订阅范围：", "按")}显示；一行一个标的，进入 T型报价查看 CALL/PUT 全链。`;
  $("#overview-empty").classList.toggle("d-none", rows.length !== 0);
  $("#underlying-rows").innerHTML = rows.map((row) => {
    const statusClass = ["正常", "已订阅"].includes(row.status)
      ? "good"
      : ["数据缺口", "待订阅", "订阅中"].includes(row.status)
      ? "warn"
      : "bad";
    const subscriptionLines = subscriptionProgressLines(row);
    const sessionTone = tradingSessionTone(row.trading_session_state);
    return `<tr class="clickable" data-underlying="${escapeHtml(row.underlying_symbol)}">
      <td>${escapeHtml(exchangeNames[row.exchange_id] ?? row.exchange_id)}</td>
      <td>${escapeHtml(productLabelForRow(row))}</td>
      <td class="mono">${escapeHtml(row.underlying_symbol)}</td>
      <td class="mono">${escapeHtml(row.expiry_month ?? "--")}</td>
      <td class="mono cell-right">${fmtDaysToExpiry(row.days_to_expiry)}</td>
      <td class="mono cell-right">${fmtNum(overviewSideCount(row, "CALL"))}</td>
      <td class="mono cell-right">${fmtNum(overviewSideCount(row, "PUT"))}</td>
      <td class="good cell-right">${fmtPct(row.quote_coverage)}</td>
      <td class="${row.iv_coverage < 0.98 ? "warn" : "good"} cell-right">${fmtPct(row.iv_coverage)}</td>
      <td class="${row.kline_count ? "good" : "warn"} cell-center">${row.kline_count ? escapeHtml(fmtDateTime(row.display_kline_time ?? row.latest_kline_time)) : "缺口"}</td>
      <td class="mono cell-center">${escapeHtml(fmtDateTime(row.display_market_time ?? row.latest_quote_time))}</td>
      <td class="cell-center"><span class="tag ${sessionTone}">${escapeHtml(row.trading_session_label ?? "--")}</span></td>
      <td class="cell-center"><div class="status-cell"><span class="tag ${statusClass}">${escapeHtml(row.status)}</span>${subscriptionLines}</div></td>
      <td class="cell-center"><button class="btn btn-sm btn-light" type="button">进入T型</button></td>
    </tr>`;
  }).join("");
  $$("#underlying-rows tr").forEach((row) => row.addEventListener("click", async () => {
    state.selectedUnderlying = row.dataset.underlying;
    renderSelectors();
    await loadQuote(state.selectedUnderlying, { animate: false });
    showPage("quote");
  }));
}

function overviewSideCount(row, side) {
  const klineTotal = Number(row.subscription_kline_total ?? 0);
  if (klineTotal <= 0) return side === "CALL" ? row.call_count : row.put_count;
  const sideTotal =
    Number(row.subscription_kline_call_total ?? 0)
    + Number(row.subscription_kline_put_total ?? 0);
  if (sideTotal <= 0) return side === "CALL" ? row.call_count : row.put_count;
  const key = side === "CALL" ? "subscription_kline_call_total" : "subscription_kline_put_total";
  return Number(row[key] ?? 0);
}

function subscriptionProgressLines(row) {
  const quoteTotal = Number(row.subscription_quote_total ?? 0);
  const quoteSubscribed = Number(row.subscription_quote_subscribed ?? 0);
  const klineTotal = Number(row.subscription_kline_total ?? 0);
  const klineSubscribed = Number(row.subscription_kline_subscribed ?? 0);
  if (quoteTotal <= 0 && klineTotal <= 0) return "";
  const quoteText = quoteTotal > 0
    ? `Quote ${fmtNum(quoteSubscribed)}/${fmtNum(quoteTotal)}`
    : "Quote --";
  const klineText = klineTotal > 0
    ? `K线 ${fmtNum(klineSubscribed)}/${fmtNum(klineTotal)}`
    : "K线 0/0";
  return `
    <span class="status-subline mono">${escapeHtml(quoteText)}</span>
    <span class="status-subline mono">${escapeHtml(klineText)}</span>
  `;
}

function selectorRows() {
  const quoteSelectors = state.quote?.selectors?.map((row) => ({
    ...row,
    underlying_symbol: row.underlying_symbol ?? row.symbol,
  })) ?? [];
  return quoteSelectors.length ? quoteSelectors : scopedUnderlyingRows();
}

function scopedUnderlyingRows() {
  return filterRowsByContractScope(
    state.overview?.underlyings ?? [],
    currentContractScopeValue(),
  );
}

function currentContractScopeValue() {
  return String(
    state.overview?.quote_stream?.progress?.contract_months
      ?? state.overview?.quote_stream?.contract_months
      ?? $("#setting-contract-months")?.value
      ?? "2",
  ).trim().toLowerCase();
}

function filterRowsByContractScope(rows, scopeValue) {
  if (scopeValue === "all") return rows;
  const limit = Number(scopeValue);
  if (!Number.isFinite(limit) || limit < 1) return rows;
  const grouped = new Map();
  for (const row of rows) {
    const groupKey = `${row.exchange_id ?? ""}|${row.product_id ?? ""}`;
    if (!grouped.has(groupKey)) grouped.set(groupKey, []);
    grouped.get(groupKey).push(row);
  }
  const allowed = new Set();
  for (const groupRows of grouped.values()) {
    [...groupRows]
      .sort((left, right) => compareContractRows(left, right))
      .slice(0, limit)
      .forEach((row) => allowed.add(row.underlying_symbol));
  }
  return rows.filter((row) => allowed.has(row.underlying_symbol));
}

function compareContractRows(left, right) {
  const leftKey = contractMonthKey(left.underlying_symbol ?? left.expiry_month);
  const rightKey = contractMonthKey(right.underlying_symbol ?? right.expiry_month);
  if (leftKey[0] !== rightKey[0]) return leftKey[0] - rightKey[0];
  if (leftKey[1] !== rightKey[1]) return leftKey[1] - rightKey[1];
  return String(left.underlying_symbol ?? "").localeCompare(String(right.underlying_symbol ?? ""));
}

function contractMonthKey(symbol) {
  const match = String(symbol ?? "").match(/(\d{3,4})/);
  if (!match) return [9999, 99];
  const digits = match[1];
  const year = digits.length === 3 ? 2020 + Number(digits.slice(0, 1)) : 2000 + Number(digits.slice(0, 2));
  const month = digits.length === 3 ? Number(digits.slice(1, 3)) : Number(digits.slice(2, 4));
  if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) return [9999, 99];
  return [year, month];
}

function renderSelectors() {
  const rows = selectorRows();
  const current = rows.find((row) => row.underlying_symbol === state.selectedUnderlying) ?? rows[0];
  if (!current) return;
  const exchangeRows = rows.filter((row) => row.exchange_id === current.exchange_id);
  const productRows = exchangeRows.filter((row) => row.product_id === current.product_id);
  fillSelect($("#exchange-select"), unique(rows.map((row) => row.exchange_id)), current.exchange_id, (value) => exchangeNames[value] ?? value);
  fillSelect($("#product-select"), unique(exchangeRows.map((row) => row.product_id)), current.product_id, (value) => productLabel(productRows.find((row) => row.product_id === value)?.product_display_name ?? value));
  fillSelectOptions(
    $("#expiry-select"),
    productRows.map((row) => ({
      value: row.underlying_symbol,
      label: `${row.expiry_month ?? row.underlying_symbol}${row.days_to_expiry === null || row.days_to_expiry === undefined ? "" : `（${fmtDaysToExpiry(row.days_to_expiry)}）`}`,
    })),
    current.underlying_symbol,
  );
}

function unique(values) {
  return [...new Set(values.filter(Boolean))].sort();
}

function fillSelect(select, values, selected, labelFn = (value) => value) {
  select.innerHTML = values.map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(labelFn(value))}</option>`).join("");
  select.value = selected;
}

function fillSelectOptions(select, options, selected) {
  const seen = new Set();
  const uniqueOptions = [];
  for (const option of options) {
    if (seen.has(option.value)) continue;
    seen.add(option.value);
    uniqueOptions.push(option);
  }
  uniqueOptions.sort((left, right) => String(left.label).localeCompare(String(right.label)));
  select.innerHTML = uniqueOptions
    .map((option) => `<option value="${escapeHtml(option.value)}">${escapeHtml(option.label)}</option>`)
    .join("");
  select.value = selected;
}

async function onSelectorChange(changedId) {
  const rows = selectorRows();
  const exchange = $("#exchange-select").value;
  const product = $("#product-select").value;
  const selectedUnderlying = $("#expiry-select").value;
  let current;
  if (changedId === "expiry-select") {
    current = rows.find((row) => row.underlying_symbol === selectedUnderlying);
  } else if (changedId === "product-select") {
    current = rows.find((row) => row.exchange_id === exchange && row.product_id === product);
  } else if (changedId === "exchange-select") {
    current = rows.find((row) => row.exchange_id === exchange);
  }
  current = current ??
    rows.find((row) => row.exchange_id === exchange && row.product_id === product && row.underlying_symbol === selectedUnderlying) ??
    rows.find((row) => row.exchange_id === exchange && row.product_id === product) ??
    rows.find((row) => row.exchange_id === exchange) ??
    rows[0];
  if (current) {
    state.selectedUnderlying = current.underlying_symbol;
    await loadQuote(current.underlying_symbol, { animate: false });
    renderSelectors();
  }
}

async function loadQuote(underlying, options = {}) {
  state.selectedUnderlying = underlying;
  state.quote = await fetchJson(`/api/webui/tquote?selectors=false&underlying=${encodeURIComponent(underlying)}`);
  renderQuote(options);
}

async function refreshCurrentQuote() {
  if (!state.selectedUnderlying || !$("#quote").classList.contains("active")) return;
  try {
    state.quote = await fetchJson(`/api/webui/tquote?selectors=false&underlying=${encodeURIComponent(state.selectedUnderlying)}`);
    renderQuote({ animate: true });
  } catch {
    // Keep the last rendered quote visible if the local API is temporarily unavailable.
  }
}

async function refreshOverview() {
  if (!$("#overview").classList.contains("active")) return;
  if (state.overviewRefreshBusy) return;
  state.overviewRefreshBusy = true;
  try {
    state.overview = await fetchJson("/api/webui/overview?limit=500");
    observeStreamProgress(quoteStreamProgress(state.overview.quote_stream));
    const scopedRows = scopedUnderlyingRows();
    if (
      !state.selectedUnderlying ||
      !scopedRows.some((row) => row.underlying_symbol === state.selectedUnderlying)
    ) {
      state.selectedUnderlying = scopedRows[0]?.underlying_symbol ?? null;
      if (state.selectedUnderlying) {
        await loadQuote(state.selectedUnderlying, { animate: false });
      }
    }
    renderOverview();
    renderSelectors();
  } catch {
    // Keep the existing selector universe visible if the local API is temporarily unavailable.
  } finally {
    state.overviewRefreshBusy = false;
  }
}

async function refreshQuoteStreamStatusForOverview() {
  if (!state.overview || state.quoteStreamRefreshBusy) return;
  state.quoteStreamRefreshBusy = true;
  try {
    const status = await fetchJson("/api/quote-stream");
    state.overview.quote_stream = status;
    observeStreamProgress(quoteStreamProgress(status));
    renderCollectionProgress();
    if ($("#settings").classList.contains("active")) {
      renderQuoteStreamStatus(status);
      const managerStatus = await fetchJson("/api/contract-manager");
      renderContractManagerStatus(managerStatus);
    }
  } catch {
    // Keep the existing stream status visible until the next overview refresh.
  } finally {
    state.quoteStreamRefreshBusy = false;
  }
}

function renderQuote(options = {}) {
  const data = state.quote;
  const underlying = data.underlying ?? {};
  const parts = symbolParts(underlying.symbol);
  const activeOptions = data.strikes.reduce((count, row) => count + (row.CALL ? 1 : 0) + (row.PUT ? 1 : 0), 0);
  const subscribedOptions = data.strikes.reduce((count, row) => count + (isSubscribedQuoteOption(row.CALL) ? 1 : 0) + (isSubscribedQuoteOption(row.PUT) ? 1 : 0), 0);
  const hasIv = (option) => isSubscribedQuoteOption(option) && option?.iv !== null && option?.iv !== undefined;
  const withIv = data.strikes.reduce((count, row) => count + (hasIv(row.CALL) ? 1 : 0) + (hasIv(row.PUT) ? 1 : 0), 0);
  const hasGreeks = (option) => ["delta", "gamma", "theta", "vega", "rho"].some((key) => option?.[key] !== null && option?.[key] !== undefined);
  const hasSubscribedGreeks = (option) => isSubscribedQuoteOption(option) && hasGreeks(option);
  const withGreeks = data.strikes.reduce((count, row) => count + (hasSubscribedGreeks(row.CALL) ? 1 : 0) + (hasSubscribedGreeks(row.PUT) ? 1 : 0), 0);
  const withKline = data.strikes.reduce((count, row) => count + (isSubscribedQuoteOption(row.CALL) && row.CALL?.has_kline ? 1 : 0) + (isSubscribedQuoteOption(row.PUT) && row.PUT?.has_kline ? 1 : 0), 0);
  $("#quote-title").textContent = `${underlying.symbol ?? "T型报价"} ${underlying.product_display_name ?? parts.productName}期货`;
  const realtimeFresh = Boolean(underlying.realtime_subscribed);
  const displayTime = realtimeFresh
    ? (underlying.realtime_latest_source_datetime ?? underlying.realtime_latest_quote_received_at)
    : null;
  $("#quote-book-time").textContent = fmtDateTime(displayTime);
  $("#toolbar-book-time").textContent = fmtDateTime(displayTime);
  $("#toolbar-expiry-days").textContent = fmtDaysToExpiry(underlying.days_to_expiry);
  $("#toolbar-session-state").textContent = underlying.trading_session_label ?? "--";
  $("#quote-active-options").textContent = fmtNum(activeOptions);
  $("#quote-iv-coverage").textContent = subscribedOptions ? `IV ${fmtPct(withIv / subscribedOptions)} · Greeks ${fmtPct(withGreeks / subscribedOptions)}` : "--";
  const realtimeStatus = realtimeFresh
    ? subscribedOptions && withKline >= subscribedOptions
      ? "正常"
      : "K线补齐中"
    : "未订阅";
  const realtimeTone = !realtimeFresh
    ? "warn"
    : subscribedOptions && withKline >= subscribedOptions
    ? "good"
    : "warn";
  const sessionLabel = underlying.trading_session_label ? ` · ${underlying.trading_session_label}` : "";
  $("#quote-kline-status").textContent = `${realtimeStatus}${sessionLabel}`;
  $("#quote-kline-status").classList.remove("good", "warn", "bad");
  $("#quote-kline-status").classList.add(realtimeTone);
  $("#quote-head").innerHTML = `<tr class="market-strip">
    <th class="call-title" colspan="9">看涨期权 CALL</th>
    <th class="under-bid"><span class="tag good">买价 ${fmtNum(underlying.bid_price1, 0)}</span></th>
    <th class="under-last"><span>标的最新</span><br>${fmtNum(underlying.last_price, 0)}</th>
    <th class="under-ask"><span class="tag bad">卖价 ${fmtNum(underlying.ask_price1, 0)}</span></th>
    <th class="put-title" colspan="9">看跌期权 PUT</th>
  </tr>
  <tr>
    <th>成交量</th><th>持仓量</th><th>IV</th><th>Vega</th><th>Theta</th><th>Gamma</th><th>Delta</th><th>最新价</th><th>卖价</th><th>买价</th>
    <th>行权价</th>
    <th>买价</th><th>卖价</th><th>最新价</th><th>Delta</th><th>Gamma</th><th>Theta</th><th>Vega</th><th>IV</th><th>持仓量</th><th>成交量</th>
  </tr>`;
  const maxima = sideMaxima(data.strikes);
  $("#quote-rows").innerHTML = data.strikes.map((row) => quoteRow(row, data, maxima)).join("");
  $$("#quote-rows tr").forEach((row) => row.addEventListener("click", () => openDetail(JSON.parse(row.dataset.payload))));
  applyLatestAnimation(options.animate !== false);
}

function isSubscribedQuoteOption(option) {
  return Boolean(option) && option.moneyness_in_subscription_scope !== false;
}

function tradingSessionTone(value) {
  if (value === "in_session") return "good";
  if (value === "out_of_session") return "warn";
  return "";
}

function sideMaxima(rows) {
  const maxima = {
    CALL: { volume: 0, open_interest: 0, bid_volume1: 0, ask_volume1: 0 },
    PUT: { volume: 0, open_interest: 0, bid_volume1: 0, ask_volume1: 0 },
  };
  for (const row of rows) {
    for (const side of ["CALL", "PUT"]) {
      const option = row[side];
      if (!option) continue;
      for (const key of Object.keys(maxima[side])) {
        maxima[side][key] = Math.max(maxima[side][key], Number(option[key]) || 0);
      }
    }
  }
  return maxima;
}

function optionMoneynessClass(option) {
  const moneyness = String(option?.moneyness || "").toLowerCase();
  if (moneyness === "atm") return "atm-neutral";
  if (moneyness === "itm") return "itm";
  return "otm";
}

function quoteRow(row, data, maxima) {
  const payload = JSON.stringify({ strike: row.strike_price, call: row.CALL, put: row.PUT }).replace(/"/g, "&quot;");
  const atm = row.is_atm ? " atm" : "";
  return `<tr class="clickable" data-payload="${payload}">
    ${callCells(row.CALL, row.strike_price, data, maxima)}
    <td class="strike-cell${atm}">${fmtNum(row.strike_price, 0)}</td>
    ${putCells(row.PUT, row.strike_price, data, maxima)}
  </tr>`;
}

function callCells(option, strike, data, maxima) {
  const tone = `call-side ${optionMoneynessClass(option)}`;
  return [
    barCell(option, { displayKey: "volume", max: maxima.CALL.volume, side: "call", kind: "quantity", className: tone }),
    barCell(option, { displayKey: "open_interest", max: maxima.CALL.open_interest, side: "call", kind: "quantity", className: tone }),
    textCell(option?.iv, `${tone} iv`, 3),
    textCell(option?.vega, tone, 3),
    textCell(option?.theta, tone, 3),
    textCell(option?.gamma, tone, 3),
    textCell(option?.delta, tone, 3),
    textCell(option?.last_price, `${tone} latest-price`, 3, true, option?.symbol),
    barCell(option, { displayKey: "ask_price1", energyKey: "ask_volume1", max: maxima.CALL.ask_volume1, side: "call", kind: "depth", className: `${tone} ask`, digits: 3, priceClass: "price-ask" }),
    barCell(option, { displayKey: "bid_price1", energyKey: "bid_volume1", max: maxima.CALL.bid_volume1, side: "call", kind: "depth", className: tone, digits: 3, priceClass: "price-bid" }),
  ].join("");
}

function putCells(option, strike, data, maxima) {
  const tone = `put-side ${optionMoneynessClass(option)}`;
  return [
    barCell(option, { displayKey: "bid_price1", energyKey: "bid_volume1", max: maxima.PUT.bid_volume1, side: "put", kind: "depth", className: tone, digits: 3, priceClass: "price-bid" }),
    barCell(option, { displayKey: "ask_price1", energyKey: "ask_volume1", max: maxima.PUT.ask_volume1, side: "put", kind: "depth", className: `${tone} ask`, digits: 3, priceClass: "price-ask" }),
    textCell(option?.last_price, `${tone} latest-price`, 3, true, option?.symbol),
    textCell(option?.delta, tone, 3),
    textCell(option?.gamma, tone, 3),
    textCell(option?.theta, tone, 3),
    textCell(option?.vega, tone, 3),
    textCell(option?.iv, `${tone} iv`, 3),
    barCell(option, { displayKey: "open_interest", max: maxima.PUT.open_interest, side: "put", kind: "quantity", className: tone }),
    barCell(option, { displayKey: "volume", max: maxima.PUT.volume, side: "put", kind: "quantity", className: tone }),
  ].join("");
}

function textCell(value, className = "", digits = 0, trim = false, symbol = "") {
  const dataAttrs = symbol ? ` data-symbol="${escapeHtml(symbol)}" data-value="${escapeHtml(value ?? "")}"` : "";
  return `<td class="${className}"${dataAttrs}>${fmtMaybeTrim(value, digits, trim)}</td>`;
}

function energyWidth(value, max) {
  const numeric = Number(value) || 0;
  const maximum = Number(max) || 0;
  if (!numeric || !maximum) return 0;
  return Math.max(1, Math.round((numeric / maximum) * 100));
}

function barCell(option, config) {
  const value = option?.[config.displayKey];
  const energyValue = option?.[config.energyKey ?? config.displayKey];
  const pct = energyWidth(energyValue, config.max);
  const depthAttr = config.energyKey ? ` data-order-qty="${escapeHtml(energyValue ?? "")}"` : "";
  const priceClass = config.priceClass ?? "";
  const bar = pct ? `<span class="bar" style="width:${pct}%"></span>` : "";
  return `<td class="bar-cell ${config.className} ${config.side} ${config.kind ?? "quantity"}"${depthAttr}>
    ${bar}<span class="value ${priceClass}">${fmtMaybeTrim(value, config.digits ?? 0, (config.digits ?? 0) > 0)}</span>
  </td>`;
}

function applyLatestAnimation(animate) {
  $$(".latest-price[data-symbol]").forEach((cell) => {
    const symbol = cell.dataset.symbol;
    const current = Number(cell.dataset.value);
    const previous = state.previousLatest.get(symbol);
    cell.classList.remove("latest-up", "latest-down");
    if (animate && previous !== undefined && Number.isFinite(current) && current !== previous) {
      void cell.offsetWidth;
      cell.classList.add(current > previous ? "latest-up" : "latest-down");
    }
    if (Number.isFinite(current)) state.previousLatest.set(symbol, current);
  });
}

async function renderRuns() {
  try {
    const data = await fetchJson("/api/webui/runs?limit=30");
    $("#latest-run").textContent = fmtDateTime(data.runs[0]?.finished_at ?? data.runs[0]?.started_at);
    $("#latest-errors").textContent = fmtNum(data.errors.length);
    $("#latest-service-logs").textContent = fmtNum(data.service_logs?.length ?? 0);
    $("#service-logs-list").innerHTML = (data.service_logs ?? []).length
      ? data.service_logs.map((log) => `<div class="log-row log-${escapeHtml(log.level)}">
        <strong class="tag ${log.level === "error" ? "bad" : log.level === "warning" ? "warn" : "good"}">${escapeHtml(log.level)}</strong>
        <span class="mono">${escapeHtml(fmtDateTime(log.created_at))}</span>
        <span class="muted">${escapeHtml(log.category)}</span>
        <div>${escapeHtml(log.message)}</div>
      </div>`).join("")
      : `<div class="empty-state">暂无系统日志。触发刷新、保存设置或测试连接后会出现在这里。</div>`;
    $("#runs-list").innerHTML = data.runs.length
      ? data.runs.map((run) => `<div class="log-row">
        <strong>${escapeHtml(run.status)}</strong>
        <span class="mono">${escapeHtml(fmtDateTime(run.finished_at ?? run.started_at))}</span>
        <div class="muted">${escapeHtml(run.message ?? "")}</div>
      </div>`).join("")
      : `<div class="empty-state">暂无采集运行记录。</div>`;
    $("#run-errors-list").innerHTML = data.errors.length
      ? data.errors.slice(0, 20).map((error) => `<div class="log-row">
        <strong class="tag bad">${escapeHtml(error.error_type)}</strong>
        <span class="mono">${escapeHtml(fmtDateTime(error.created_at))}</span>
        <span class="muted">${escapeHtml(error.stage)} ${escapeHtml(error.symbol ?? "")}</span>
        <div>${escapeHtml(error.message)}</div>
      </div>`).join("")
      : `<div class="empty-state">暂无采集异常。</div>`;
  } catch {
    $("#service-logs-list").innerHTML = `<div class="notice">系统日志暂不可用。</div>`;
    $("#runs-list").innerHTML = `<div class="notice">采集日志暂不可用。</div>`;
  }
}

function openDetail(payload) {
  $("#drawer-title").textContent = `行权价 ${fmtNum(payload.strike, 0)} 详情`;
  $("#drawer-body").innerHTML = [
    ["CALL 合约", payload.call?.symbol ?? "--"],
    ["PUT 合约", payload.put?.symbol ?? "--"],
    ["CALL Delta", fmtNum(payload.call?.delta, 3)],
    ["PUT Delta", fmtNum(payload.put?.delta, 3)],
    ["IV", `CALL ${fmtNum(payload.call?.iv, 3)} / PUT ${fmtNum(payload.put?.iv, 3)}`],
    ["K线", payload.call?.has_kline || payload.put?.has_kline ? "20D 完整" : "缺口"],
  ].map(([label, value]) => `<div class="kv"><span class="muted">${escapeHtml(label)}</span><span class="mono">${escapeHtml(value)}</span></div>`).join("");
  $("#drawer-backdrop").classList.add("open");
  $("#detail-drawer").classList.add("open");
  $("#detail-drawer").setAttribute("aria-hidden", "false");
}

function closeDrawer() {
  $("#drawer-backdrop").classList.remove("open");
  $("#detail-drawer").classList.remove("open");
  $("#detail-drawer").setAttribute("aria-hidden", "true");
}

window.__odmTestHooks = {
  renderQuote,
  applyLatestAnimation,
  state,
};

init().catch((error) => {
  document.body.innerHTML = `<main class="p-4"><div class="notice">WebUI 加载失败：${escapeHtml(error.message)}</div></main>`;
});
"""


if __name__ == "__main__":
    main()
