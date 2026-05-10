"""Unified local FastAPI application."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from pydantic import BaseModel
import uvicorn

from option_data_manager.acquisition import AcquisitionRepository
from option_data_manager.api_keys import ApiKeyRecord, ApiKeyRepository
from option_data_manager.collection_state import CollectionStateRepository
from option_data_manager.klines import KlineRepository
from option_data_manager.option_metrics import OptionMetricsRepository
from option_data_manager.quotes import QuoteRepository
from option_data_manager.realtime_health import build_realtime_health
from option_data_manager.settings import (
    PlainTextProtector,
    SettingsRepository,
    TQSDK_ACCOUNT_KEY,
    TQSDK_PASSWORD_KEY,
    default_secret_protector,
)
from option_data_manager.source_quality import SourceQualityRepository
from option_data_manager.service_state import ServiceLogRepository, ServiceStateRepository
from option_data_manager.tqsdk_connection import create_tqsdk_api_with_retries
from option_data_manager.webui.read_model import (
    WebuiReadModel,
    _days_to_expiry,
)


DEFAULT_DATABASE_PATH = "data/option-data-current.sqlite3"
API_AUTH_REQUIRED_KEY = "api.auth_required"
API_BIND_KEY = "api.bind"
API_PORT_KEY = "api.port"
REFRESH_INTERVAL_KEY = "collection.refresh_interval_seconds"
OPTION_BATCH_SIZE_KEY = "collection.option_batch_size"
WAIT_CYCLES_KEY = "collection.wait_cycles"
DEFAULT_BACKGROUND_MAX_BATCHES = "100"
QUOTE_STREAM_WORKERS_KEY = "quote_stream.workers"
QUOTE_STREAM_SHARD_SIZE_KEY = "quote_stream.quote_shard_size"
QUOTE_STREAM_KLINE_BATCH_SIZE_KEY = "quote_stream.kline_batch_size"
QUOTE_STREAM_MAX_SYMBOLS_KEY = "quote_stream.max_symbols"
QUOTE_STREAM_KLINE_DATA_LENGTH_KEY = "quote_stream.kline_data_length"
QUOTE_STREAM_PRIORITIZE_NEAR_EXPIRY_KEY = "quote_stream.prioritize_near_expiry"
QUOTE_STREAM_NEAR_EXPIRY_MONTHS_KEY = "quote_stream.near_expiry_months"
METRICS_MIN_INTERVAL_SECONDS_KEY = "metrics.min_interval_seconds"
METRICS_UNDERLYING_CHAIN_INTERVAL_SECONDS_KEY = "metrics.underlying_chain_interval_seconds"
DEFAULT_QUOTE_STREAM_WORKERS = "1"
DEFAULT_QUOTE_STREAM_SHARD_SIZE = "1000"
DEFAULT_QUOTE_STREAM_KLINE_BATCH_SIZE = "1"
DEFAULT_QUOTE_STREAM_KLINE_DATA_LENGTH = "3"
DEFAULT_QUOTE_STREAM_PRIORITIZE_NEAR_EXPIRY = "true"
DEFAULT_QUOTE_STREAM_NEAR_EXPIRY_MONTHS = "2"
DEFAULT_METRICS_MIN_INTERVAL_SECONDS = "30"


class TqsdkCredentialUpdate(BaseModel):
    """Request body for saving TQSDK credentials."""

    account: str
    password: str


class SettingUpdate(BaseModel):
    """Generic safe setting update."""

    value: str


class ApiKeyCreateRequest(BaseModel):
    """Request body for creating a local API key."""

    name: str
    scope: str = "read"


class ApiKeyCreatedResponse(BaseModel):
    """API key creation response. The secret is shown once only."""

    key_id: int
    name: str
    fingerprint: str
    scope: str
    enabled: bool
    created_at: str
    last_used_at: str | None
    revoked_at: str | None
    secret: str


class ApiKeyResponse(BaseModel):
    """Safe API key metadata response."""

    key_id: int
    name: str
    fingerprint: str
    scope: str
    enabled: bool
    created_at: str
    last_used_at: str | None
    revoked_at: str | None


class ApiKeyEnabledUpdate(BaseModel):
    """Enable/disable an API key."""

    enabled: bool


class RefreshResponse(BaseModel):
    """Result of a synchronous local refresh trigger."""

    status: str
    report_path: str
    message: str


class QuoteStreamStartRequest(BaseModel):
    """Optional runtime overrides for quote stream workers."""

    workers: int | None = None
    quote_shard_size: int | None = None
    kline_batch_size: int | None = None
    max_symbols: int | None = None
    discover: bool = False


class QuoteStreamResponse(BaseModel):
    """Current quote stream worker status."""

    status: str
    running: bool
    message: str | None
    worker_count: int
    pids: list[int]
    report_dir: str | None
    started_at: str | None
    finished_at: str | None
    progress: dict[str, Any] | None = None
    health: dict[str, Any] | None = None


def create_app(
    connection: sqlite3.Connection,
    *,
    database_path: str | None = None,
    protector: Any | None = None,
) -> FastAPI:
    """Create the unified local API application."""

    connection.row_factory = sqlite3.Row
    _ensure_runtime_tables(connection, protector=protector)
    settings = SettingsRepository(connection, protector or _default_protector())
    api_keys = ApiKeyRepository(connection)
    service_state = ServiceStateRepository(connection)
    service_logs = ServiceLogRepository(connection)
    read_model = WebuiReadModel(connection)
    refresh_lock = threading.Lock()
    refresh_worker: dict[str, threading.Thread | None] = {"thread": None}
    quote_stream_lock = threading.Lock()
    quote_stream_processes: dict[str, list[Any]] = {"processes": []}
    metrics_worker_processes: dict[str, list[Any]] = {"processes": []}
    if (service_state.get_value("collection.refresh_running") or "false") == "true":
        service_state.set_value("collection.refresh_running", "false")
        service_state.set_value(
            "collection.refresh_message",
            "Refresh stopped because the local service restarted.",
        )
    if (service_state.get_value("quote_stream.running") or "false") == "true":
        service_state.set_value("quote_stream.running", "false")
        service_state.set_value(
            "quote_stream.message",
            "本地服务重启后，实时订阅 worker 未自动接管。",
        )
    if (service_state.get_value("metrics_worker.running") or "false") == "true":
        service_state.set_value("metrics_worker.running", "false")
        service_state.set_value(
            "metrics_worker.message",
            "本地服务重启后，指标刷新 worker 未自动接管。",
        )

    app = FastAPI(title="Option Data Manager API")
    app.state.service_state = service_state
    app.state.quote_stream_processes = quote_stream_processes
    app.state.metrics_worker_processes = metrics_worker_processes

    @app.middleware("http")
    async def record_metrics(request: Request, call_next: Any) -> Any:
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception as exc:
            if request.url.path.startswith("/api/"):
                service_state.record_request(
                    path=request.url.path,
                    method=request.method,
                    status_code=500,
                    latency_ms=(time.perf_counter() - started) * 1000,
                )
                service_logs.append(
                    level="error",
                    category="api",
                    message="Unhandled API request error.",
                    context={
                        "path": request.url.path,
                        "method": request.method,
                        "error_type": type(exc).__name__,
                    },
                )
            raise
        if request.url.path.startswith("/api/"):
            service_state.record_request(
                path=request.url.path,
                method=request.method,
                status_code=response.status_code,
                latency_ms=(time.perf_counter() - started) * 1000,
            )
            if response.status_code >= 500:
                service_logs.append(
                    level="error",
                    category="api",
                    message="API request returned a server error.",
                    context={
                        "path": request.url.path,
                        "method": request.method,
                        "status_code": response.status_code,
                    },
                )
        return response

    def require_auth(
        request: Request,
        authorization: str | None = Header(default=None),
        x_api_key: str | None = Header(default=None),
    ) -> ApiKeyRecord | None:
        if not _auth_required(settings):
            return None
        if (
            request.method == "POST"
            and request.url.path == "/api/api-keys"
            and not api_keys.list_keys()
        ):
            return None
        secret = _extract_api_key(authorization, x_api_key)
        if not secret:
            raise HTTPException(status_code=401, detail="API key required.")
        record = api_keys.verify(secret)
        if record is None:
            raise HTTPException(status_code=403, detail="Invalid API key.")
        return record

    @app.get("/")
    def index() -> dict[str, str]:
        return {
            "service": "Option Data Manager",
            "docs": "/docs",
            "health": "/api/health",
            "status": "/api/status",
        }

    @app.get("/api/health")
    def health(_: ApiKeyRecord | None = Depends(require_auth)) -> dict[str, Any]:
        return {
            "status": "ok",
            "database_connected": True,
            "database_path": database_path,
            "time": datetime.now(UTC).isoformat(),
        }

    @app.get("/api/status")
    def status(_: ApiKeyRecord | None = Depends(require_auth)) -> dict[str, Any]:
        overview = read_model.overview(limit=500)
        api_summary = service_state.api_summary()
        latest_run = AcquisitionRepository(connection).list_runs(limit=1)
        return {
            "status": _overall_status(overview, latest_run),
            "database_path": database_path,
            "summary": overview["summary"],
            "collection": overview["collection"],
            "refresh": _refresh_status(service_state),
            "quote_stream": _quote_stream_status(
                service_state,
                quote_stream_processes,
                connection=connection,
            ),
            "latest_run": latest_run[0].__dict__ if latest_run else None,
            "api": {
                "bind": settings.get_value(API_BIND_KEY) or "127.0.0.1",
                "port": int(settings.get_value(API_PORT_KEY) or "8770"),
                "auth_required": _auth_required(settings),
                "request_count": api_summary.request_count,
                "error_count": api_summary.error_count,
                "average_latency_ms": round(api_summary.average_latency_ms, 3),
            },
        }

    @app.post("/api/refresh", response_model=RefreshResponse)
    def refresh(_: ApiKeyRecord | None = Depends(require_auth)) -> RefreshResponse:
        report_path = Path("docs/qa/sdk-contract-reports/latest-collection-command-report.md")
        with refresh_lock:
            worker = refresh_worker.get("thread")
            if worker is not None and worker.is_alive():
                service_logs.append(
                    level="info",
                    category="collection",
                    message=(
                        "Refresh trigger ignored because a background refresh "
                        "is already running."
                    ),
                )
                return RefreshResponse(
                    status="running",
                    report_path=str(report_path),
                    message="Refresh is already running in the background.",
                )
            args = _collection_args(
                database_path=database_path or DEFAULT_DATABASE_PATH,
                report_path=report_path,
                settings=settings,
            )
            started_at = datetime.now(UTC).isoformat()
            service_state.set_value("collection.refresh_running", "true")
            service_state.set_value("collection.refresh_started_at", started_at)
            service_state.set_value("collection.refresh_finished_at", None)
            service_state.set_value("collection.refresh_message", "Background refresh started.")
            service_logs.append(
                level="info",
                category="collection",
                message="Background refresh started.",
                context={"report_path": str(report_path)},
            )
            worker = threading.Thread(
                target=_run_refresh_until_complete,
                kwargs={
                    "args": args,
                    "database_path": database_path or DEFAULT_DATABASE_PATH,
                    "started_at": started_at,
                },
                daemon=True,
            )
            refresh_worker["thread"] = worker
            worker.start()
        return RefreshResponse(
            status="started",
            report_path=str(report_path),
            message="Refresh started in the background. The WebUI will update as batches finish.",
        )

    @app.get("/api/quote-stream", response_model=QuoteStreamResponse)
    def quote_stream_status(
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> QuoteStreamResponse:
        return QuoteStreamResponse(
            **_quote_stream_status(
                service_state,
                quote_stream_processes,
                connection=connection,
            )
        )

    @app.post("/api/quote-stream/start", response_model=QuoteStreamResponse)
    def start_quote_stream(
        payload: QuoteStreamStartRequest | None = None,
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> QuoteStreamResponse:
        request = payload or QuoteStreamStartRequest()
        with quote_stream_lock:
            status = _quote_stream_status(
                service_state,
                quote_stream_processes,
                connection=connection,
            )
            if status["running"]:
                return QuoteStreamResponse(**status)
            account = settings.get_value(TQSDK_ACCOUNT_KEY)
            password = settings.get_secret(TQSDK_PASSWORD_KEY)
            if not account or not password:
                service_logs.append(
                    level="warning",
                    category="quote_stream",
                    message="实时订阅启动被阻止：TQSDK 凭据缺失。",
                )
                return QuoteStreamResponse(
                    status="blocked",
                    running=False,
                    message="TQSDK 凭据尚未配置。",
                    worker_count=0,
                    pids=[],
                    report_dir=None,
                    started_at=None,
                    finished_at=None,
                )
            worker_count = request.workers or _positive_int_setting(
                settings.get_value(QUOTE_STREAM_WORKERS_KEY),
                int(DEFAULT_QUOTE_STREAM_WORKERS),
            )
            quote_shard_size = request.quote_shard_size or _positive_int_setting(
                settings.get_value(QUOTE_STREAM_SHARD_SIZE_KEY),
                int(DEFAULT_QUOTE_STREAM_SHARD_SIZE),
            )
            kline_batch_size = request.kline_batch_size or _positive_int_setting(
                settings.get_value(QUOTE_STREAM_KLINE_BATCH_SIZE_KEY),
                int(DEFAULT_QUOTE_STREAM_KLINE_BATCH_SIZE),
            )
            max_symbols = request.max_symbols
            if max_symbols is None:
                max_symbols = _optional_positive_int(
                    settings.get_value(QUOTE_STREAM_MAX_SYMBOLS_KEY)
                )
            metrics_min_interval_seconds = _positive_int_setting(
                settings.get_value(METRICS_MIN_INTERVAL_SECONDS_KEY),
                int(DEFAULT_METRICS_MIN_INTERVAL_SECONDS),
            )
            kline_data_length = _positive_int_setting(
                settings.get_value(QUOTE_STREAM_KLINE_DATA_LENGTH_KEY),
                int(DEFAULT_QUOTE_STREAM_KLINE_DATA_LENGTH),
            )
            prioritize_near_expiry = _bool_setting(
                settings.get_value(QUOTE_STREAM_PRIORITIZE_NEAR_EXPIRY_KEY),
                DEFAULT_QUOTE_STREAM_PRIORITIZE_NEAR_EXPIRY == "true",
            )
            near_expiry_months = _positive_int_setting(
                settings.get_value(QUOTE_STREAM_NEAR_EXPIRY_MONTHS_KEY),
                int(DEFAULT_QUOTE_STREAM_NEAR_EXPIRY_MONTHS),
            )
            underlying_chain_interval_seconds = _positive_int_setting(
                settings.get_value(METRICS_UNDERLYING_CHAIN_INTERVAL_SECONDS_KEY),
                int(DEFAULT_METRICS_MIN_INTERVAL_SECONDS),
            )
            if worker_count < 1:
                raise HTTPException(status_code=400, detail="workers must be positive.")
            if quote_shard_size < 1:
                raise HTTPException(
                    status_code=400,
                    detail="quote_shard_size must be positive.",
                )
            if kline_batch_size < 1:
                raise HTTPException(
                    status_code=400,
                    detail="kline_batch_size must be positive.",
                )
            if max_symbols is not None and max_symbols < 1:
                raise HTTPException(
                    status_code=400,
                    detail="max_symbols must be positive when provided.",
                )
            report_dir = Path("docs/qa/live-evidence/quote-stream-runtime")
            report_dir.mkdir(parents=True, exist_ok=True)
            processes = _start_quote_stream_processes(
                database_path=database_path or DEFAULT_DATABASE_PATH,
                report_dir=report_dir,
                worker_count=worker_count,
                quote_shard_size=quote_shard_size,
                kline_batch_size=kline_batch_size,
                kline_data_length=kline_data_length,
                prioritize_near_expiry=prioritize_near_expiry,
                near_expiry_months=near_expiry_months,
                max_symbols=max_symbols,
                discover=request.discover,
                metrics_dirty_min_interval_seconds=metrics_min_interval_seconds,
                underlying_chain_dirty_interval_seconds=underlying_chain_interval_seconds,
            )
            metrics_processes = _start_metrics_worker_processes(
                database_path=database_path or DEFAULT_DATABASE_PATH,
                report_dir=report_dir,
                min_interval_seconds=metrics_min_interval_seconds,
            )
            started_at = datetime.now(UTC).isoformat()
            quote_stream_processes["processes"] = processes
            metrics_worker_processes["processes"] = metrics_processes
            service_state.set_value("quote_stream.running", "true")
            service_state.set_value("quote_stream.started_at", started_at)
            service_state.set_value("quote_stream.finished_at", None)
            service_state.set_value("quote_stream.worker_count", str(worker_count))
            service_state.set_value(
                "quote_stream.pids",
                json.dumps([int(process.pid) for process in processes]),
            )
            service_state.set_value("quote_stream.report_dir", str(report_dir))
            service_state.set_value("quote_stream.message", "实时订阅 worker 已启动。")
            service_state.set_value("metrics_worker.running", "true")
            service_state.set_value("metrics_worker.started_at", started_at)
            service_state.set_value("metrics_worker.finished_at", None)
            service_state.set_value(
                "metrics_worker.pids",
                json.dumps([int(process.pid) for process in metrics_processes]),
            )
            service_state.set_value("metrics_worker.report_dir", str(report_dir))
            service_state.set_value("metrics_worker.message", "指标刷新 worker 已启动。")
            service_logs.append(
                level="info",
                category="quote_stream",
                message="实时订阅 worker 已启动。",
                context={
                    "worker_count": worker_count,
                    "quote_shard_size": quote_shard_size,
                    "kline_data_length": kline_data_length,
                    "prioritize_near_expiry": prioritize_near_expiry,
                    "near_expiry_months": near_expiry_months,
                    "max_symbols_configured": max_symbols is not None,
                    "discover": request.discover,
                    "report_dir": str(report_dir),
                    "metrics_min_interval_seconds": metrics_min_interval_seconds,
                    "underlying_chain_interval_seconds": underlying_chain_interval_seconds,
                },
            )
            return QuoteStreamResponse(
                **_quote_stream_status(
                    service_state,
                    quote_stream_processes,
                    connection=connection,
                )
            )

    @app.post("/api/quote-stream/stop", response_model=QuoteStreamResponse)
    def stop_quote_stream(
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> QuoteStreamResponse:
        with quote_stream_lock:
            _request_quote_stream_stop_files(service_state)
            processes = quote_stream_processes.get("processes", [])
            persisted_pids = _json_int_list(service_state.get_value("quote_stream.pids"))
            persisted_metrics_pids = _json_int_list(
                service_state.get_value("metrics_worker.pids")
            )
            attached_pids = {int(process.pid) for process in processes}
            attached_metrics_pids = {
                int(process.pid)
                for process in metrics_worker_processes.get("processes", [])
            }
            stopped = 0
            for process in processes:
                if process.poll() is None:
                    stopped += 1
            for process in processes:
                if process.poll() is None:
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.terminate()
                if process.poll() is None:
                    process.terminate()
                if process.poll() is None:
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        process.kill()
            for pid in persisted_pids:
                if pid in attached_pids:
                    continue
                if _pid_is_quote_stream(pid):
                    _terminate_pid(pid)
                    stopped += 1
            metrics_stopped = _stop_attached_processes(
                metrics_worker_processes.get("processes", [])
            )
            for pid in persisted_metrics_pids:
                if pid in attached_metrics_pids:
                    continue
                if _pid_is_metrics_worker(pid):
                    _terminate_pid(pid)
                    metrics_stopped += 1
            finished_at = datetime.now(UTC).isoformat()
            quote_stream_processes["processes"] = []
            metrics_worker_processes["processes"] = []
            service_state.set_value("quote_stream.running", "false")
            service_state.set_value("quote_stream.finished_at", finished_at)
            service_state.set_value("quote_stream.pids", "[]")
            service_state.set_value("metrics_worker.running", "false")
            service_state.set_value("metrics_worker.finished_at", finished_at)
            service_state.set_value("metrics_worker.pids", "[]")
            service_state.set_value(
                "quote_stream.message",
                f"已请求停止 {stopped} 个实时订阅 worker 和 {metrics_stopped} 个指标 worker。",
            )
            service_state.set_value(
                "metrics_worker.message",
                f"已请求停止 {metrics_stopped} 个指标刷新 worker。",
            )
            service_logs.append(
                level="info",
                category="quote_stream",
                message="实时订阅 worker 已停止。",
                context={"stopped_workers": stopped},
            )
            return QuoteStreamResponse(
                **_quote_stream_status(
                    service_state,
                    quote_stream_processes,
                    connection=connection,
                )
            )

    @app.get("/api/exchanges")
    def exchanges(_: ApiKeyRecord | None = Depends(require_auth)) -> list[dict[str, Any]]:
        return read_model.overview(limit=500)["exchanges"]

    @app.get("/api/products")
    def products(
        exchange: str | None = None,
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            """
            SELECT
                exchange_id,
                product_id,
                COUNT(DISTINCT underlying_symbol) AS underlying_count,
                COUNT(*) AS option_count
            FROM instruments
            WHERE active = 1
              AND option_class IN ('CALL', 'PUT')
              AND (? IS NULL OR exchange_id = ?)
            GROUP BY exchange_id, product_id
            ORDER BY exchange_id, product_id
            """,
            (exchange, exchange),
        ).fetchall()
        return [dict(row) for row in rows]

    @app.get("/api/underlyings")
    def underlyings(_: ApiKeyRecord | None = Depends(require_auth)) -> list[dict[str, Any]]:
        return read_model.overview(limit=500)["underlyings"]

    @app.get("/api/options")
    def options(
        underlying: str | None = None,
        limit: int = Query(default=500, ge=1, le=5000),
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            """
            SELECT *
            FROM instruments
            WHERE option_class IN ('CALL', 'PUT')
              AND (? IS NULL OR underlying_symbol = ?)
            ORDER BY underlying_symbol, option_class, strike_price, symbol
            LIMIT ?
            """,
            (underlying, underlying, limit),
        ).fetchall()
        return [_option_api_row(row) for row in rows]

    @app.get("/api/options/{symbol}/quote")
    def option_quote(
        symbol: str,
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> dict[str, Any]:
        row = connection.execute(
            "SELECT * FROM quote_current WHERE symbol = ?",
            (symbol,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Quote not found.")
        return dict(row)

    @app.get("/api/options/{symbol}/klines")
    def option_klines(
        symbol: str,
        days: int = Query(default=20, ge=1, le=120),
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            """
            SELECT *
            FROM kline_20d_current
            WHERE symbol = ?
            ORDER BY bar_datetime DESC
            LIMIT ?
            """,
            (symbol, days),
        ).fetchall()
        return [dict(row) for row in rows]

    @app.get("/api/acquisition-runs")
    def acquisition_runs(
        limit: int = Query(default=50, ge=1, le=500),
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> list[dict[str, Any]]:
        return [run.__dict__ for run in AcquisitionRepository(connection).list_runs(limit=limit)]

    @app.get("/api/acquisition-errors")
    def acquisition_errors(
        run_id: int | None = None,
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> list[dict[str, Any]]:
        repository = AcquisitionRepository(connection)
        if run_id is not None:
            return [error.__dict__ for error in repository.list_errors_for_run(run_id)]
        rows = connection.execute(
            """
            SELECT *
            FROM acquisition_errors
            ORDER BY error_id DESC
            LIMIT 200
            """
        ).fetchall()
        return [dict(row) for row in rows]

    @app.get("/api/logs")
    def service_log_entries(
        limit: int = Query(default=100, ge=1, le=500),
        category: str | None = None,
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> list[dict[str, Any]]:
        return [
            record.__dict__
            for record in service_logs.list_logs(limit=limit, category=category)
        ]

    @app.get("/api/settings")
    def get_settings(_: ApiKeyRecord | None = Depends(require_auth)) -> dict[str, Any]:
        account = settings.get_value(TQSDK_ACCOUNT_KEY)
        return {
            "tqsdk": {
                "account": account,
                "password_configured": settings.get_secret(TQSDK_PASSWORD_KEY) is not None,
            },
            "api": {
                "bind": settings.get_value(API_BIND_KEY) or "127.0.0.1",
                "port": int(settings.get_value(API_PORT_KEY) or "8770"),
                "auth_required": _auth_required(settings),
            },
            "collection": {
                "refresh_interval_seconds": int(settings.get_value(REFRESH_INTERVAL_KEY) or "30"),
                "option_batch_size": int(settings.get_value(OPTION_BATCH_SIZE_KEY) or "20"),
                "wait_cycles": int(settings.get_value(WAIT_CYCLES_KEY) or "1"),
                "max_underlyings": int(settings.get_value("collection.max_underlyings") or "1000000"),
                "max_batches": int(
                    settings.get_value("collection.max_batches")
                    or DEFAULT_BACKGROUND_MAX_BATCHES
                ),
                "auto_retry": (settings.get_value("collection.auto_retry") or "true") == "true",
                "kline_backfill": (settings.get_value("collection.kline_backfill") or "true") == "true",
                "inactive_handling": settings.get_value("collection.inactive_handling") or "mark_inactive",
                "sqlite_path": database_path or DEFAULT_DATABASE_PATH,
            },
            "quote_stream": {
                "workers": _positive_int_setting(
                    settings.get_value(QUOTE_STREAM_WORKERS_KEY),
                    int(DEFAULT_QUOTE_STREAM_WORKERS),
                ),
                "quote_shard_size": _positive_int_setting(
                    settings.get_value(QUOTE_STREAM_SHARD_SIZE_KEY),
                    int(DEFAULT_QUOTE_STREAM_SHARD_SIZE),
                ),
                "kline_batch_size": _positive_int_setting(
                    settings.get_value(QUOTE_STREAM_KLINE_BATCH_SIZE_KEY),
                    int(DEFAULT_QUOTE_STREAM_KLINE_BATCH_SIZE),
                ),
                "max_symbols": _optional_positive_int(
                    settings.get_value(QUOTE_STREAM_MAX_SYMBOLS_KEY)
                ),
                "kline_data_length": _positive_int_setting(
                    settings.get_value(QUOTE_STREAM_KLINE_DATA_LENGTH_KEY),
                    int(DEFAULT_QUOTE_STREAM_KLINE_DATA_LENGTH),
                ),
                "prioritize_near_expiry": _bool_setting(
                    settings.get_value(QUOTE_STREAM_PRIORITIZE_NEAR_EXPIRY_KEY),
                    DEFAULT_QUOTE_STREAM_PRIORITIZE_NEAR_EXPIRY == "true",
                ),
                "near_expiry_months": _positive_int_setting(
                    settings.get_value(QUOTE_STREAM_NEAR_EXPIRY_MONTHS_KEY),
                    int(DEFAULT_QUOTE_STREAM_NEAR_EXPIRY_MONTHS),
                ),
                "status": _quote_stream_status(
                    service_state,
                    quote_stream_processes,
                    connection=connection,
                ),
            },
            "metrics": {
                "min_interval_seconds": _positive_int_setting(
                    settings.get_value(METRICS_MIN_INTERVAL_SECONDS_KEY),
                    int(DEFAULT_METRICS_MIN_INTERVAL_SECONDS),
                ),
                "underlying_chain_interval_seconds": _positive_int_setting(
                    settings.get_value(METRICS_UNDERLYING_CHAIN_INTERVAL_SECONDS_KEY),
                    int(DEFAULT_METRICS_MIN_INTERVAL_SECONDS),
                ),
            },
            "safe_metadata": [item.__dict__ for item in settings.list_metadata()],
        }

    @app.put("/api/settings/tqsdk-credentials")
    def save_tqsdk_credentials(
        payload: TqsdkCredentialUpdate,
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> dict[str, Any]:
        account = payload.account.strip()
        if not account:
            raise HTTPException(status_code=400, detail="TQSDK account must not be empty.")
        if not payload.password:
            raise HTTPException(status_code=400, detail="TQSDK password must not be empty.")
        settings.set_value(TQSDK_ACCOUNT_KEY, account)
        settings.set_secret(TQSDK_PASSWORD_KEY, payload.password)
        service_logs.append(
            level="info",
            category="settings",
            message="TQSDK credentials updated.",
            context={"account_configured": True, "password_configured": True},
        )
        return {"account": account, "password_configured": True}

    @app.post("/api/settings/test-tqsdk-connection")
    def test_tqsdk_connection(_: ApiKeyRecord | None = Depends(require_auth)) -> dict[str, Any]:
        account = settings.get_value(TQSDK_ACCOUNT_KEY)
        password = settings.get_secret(TQSDK_PASSWORD_KEY)
        if not account or not password:
            service_logs.append(
                level="warning",
                category="settings",
                message="TQSDK connection test blocked because credentials are missing.",
            )
            return {"status": "blocked", "message": "TQSDK credentials are not configured."}
        if os.environ.get("ODM_ENABLE_TQSDK_CONNECTION_TEST") != "1":
            service_logs.append(
                level="info",
                category="settings",
                message="TQSDK connection test skipped by local safety setting.",
            )
            return {
                "status": "skipped",
                "message": (
                    "Live TQSDK test is disabled in the WebUI. "
                    "Run odm-test-tqsdk for the standalone credential check."
                ),
            }
        try:
            api = create_tqsdk_api_with_retries(account, password)
            api.close()
        except Exception as exc:
            service_logs.append(
                level="error",
                category="settings",
                message="TQSDK connection test failed.",
                context={"error_type": type(exc).__name__},
            )
            return {"status": "failed", "message": f"{type(exc).__name__}: {exc}"}
        service_logs.append(
            level="info",
            category="settings",
            message="TQSDK connection test completed.",
        )
        return {"status": "ok", "message": "TQSDK connection test completed."}

    @app.put("/api/settings/{key}")
    def save_setting(
        key: str,
        payload: SettingUpdate,
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> dict[str, Any]:
        if key in {TQSDK_PASSWORD_KEY, "password", "secret"}:
            raise HTTPException(status_code=400, detail="Use the credential endpoint for secrets.")
        settings.set_value(key, payload.value)
        service_logs.append(
            level="info",
            category="settings",
            message="Runtime setting updated.",
            context={"key": key},
        )
        return {"key": key, "updated": True}

    @app.get("/api/api-keys", response_model=list[ApiKeyResponse])
    def list_api_keys(_: ApiKeyRecord | None = Depends(require_auth)) -> list[ApiKeyResponse]:
        return [_api_key_response(record) for record in api_keys.list_keys()]

    @app.post("/api/api-keys", response_model=ApiKeyCreatedResponse)
    def create_api_key(
        payload: ApiKeyCreateRequest,
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> ApiKeyCreatedResponse:
        created = api_keys.create_key(name=payload.name, scope=payload.scope)
        service_logs.append(
            level="info",
            category="security",
            message="Local API key created.",
            context={
                "key_id": created.record.key_id,
                "name": payload.name,
                "scope": payload.scope,
            },
        )
        return ApiKeyCreatedResponse(
            **created.record.__dict__,
            secret=created.secret,
        )

    @app.put("/api/api-keys/{key_id}", response_model=ApiKeyResponse)
    def update_api_key(
        key_id: int,
        payload: ApiKeyEnabledUpdate,
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> ApiKeyResponse:
        try:
            return _api_key_response(api_keys.set_enabled(key_id, payload.enabled))
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/api-keys/{key_id}/revoke", response_model=ApiKeyResponse)
    def revoke_api_key(
        key_id: int,
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> ApiKeyResponse:
        try:
            return _api_key_response(api_keys.revoke(key_id))
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.delete("/api/api-keys/{key_id}")
    def delete_api_key(
        key_id: int,
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> dict[str, Any]:
        try:
            api_keys.delete_key(key_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        service_logs.append(
            level="info",
            category="security",
            message="Local API key deleted.",
            context={"key_id": key_id},
        )
        return {"key_id": key_id, "deleted": True}

    return app


def create_app_from_database() -> FastAPI:
    """ASGI factory used by uvicorn."""

    database_path = os.environ.get("ODM_DATABASE_PATH", DEFAULT_DATABASE_PATH)
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path, check_same_thread=False)
    return create_app(connection, database_path=database_path)


def main() -> None:
    """Start the local API service."""

    database_path = os.environ.get("ODM_DATABASE_PATH", DEFAULT_DATABASE_PATH)
    connection = sqlite3.connect(database_path)
    settings = SettingsRepository(connection, _default_protector())
    host = os.environ.get("ODM_API_HOST", settings.get_value(API_BIND_KEY) or "127.0.0.1")
    port = int(os.environ.get("ODM_API_PORT", settings.get_value(API_PORT_KEY) or "8770"))
    connection.close()
    uvicorn.run(
        "option_data_manager.api.app:create_app_from_database",
        host=host,
        port=port,
        factory=True,
        reload=False,
    )


def _ensure_runtime_tables(connection: sqlite3.Connection, *, protector: Any | None) -> None:
    QuoteRepository(connection)
    KlineRepository(connection)
    OptionMetricsRepository(connection)
    AcquisitionRepository(connection)
    CollectionStateRepository(connection)
    SourceQualityRepository(connection)
    SettingsRepository(connection, protector or _default_protector())
    ApiKeyRepository(connection)
    ServiceStateRepository(connection)
    ServiceLogRepository(connection)


def _default_protector() -> Any:
    if os.environ.get("ODM_SECRET_PROTECTOR") == "plaintext":
        return PlainTextProtector()
    return default_secret_protector()


def _auth_required(settings: SettingsRepository) -> bool:
    return (settings.get_value(API_AUTH_REQUIRED_KEY) or "false").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _extract_api_key(authorization: str | None, x_api_key: str | None) -> str | None:
    if x_api_key:
        return x_api_key.strip()
    if authorization and authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return None


def _api_key_response(record: ApiKeyRecord) -> ApiKeyResponse:
    return ApiKeyResponse(**record.__dict__)


def _collection_args(
    *,
    database_path: str,
    report_path: Path,
    settings: SettingsRepository,
) -> list[str]:
    return [
        "--database",
        database_path,
        "--report",
        str(report_path),
        "--option-batch-size",
        settings.get_value(OPTION_BATCH_SIZE_KEY) or "20",
        "--wait-cycles",
        settings.get_value(WAIT_CYCLES_KEY) or "1",
        "--max-underlyings",
        settings.get_value("collection.max_underlyings") or "1000000",
        "--max-batches",
        settings.get_value("collection.max_batches") or DEFAULT_BACKGROUND_MAX_BATCHES,
    ]


def _start_quote_stream_processes(
    *,
    database_path: str,
    report_dir: Path,
    worker_count: int,
    quote_shard_size: int,
    kline_batch_size: int,
    kline_data_length: int,
    prioritize_near_expiry: bool,
    near_expiry_months: int,
    max_symbols: int | None,
    discover: bool,
    metrics_dirty_min_interval_seconds: int,
    underlying_chain_dirty_interval_seconds: int,
) -> list[subprocess.Popen[Any]]:
    processes: list[subprocess.Popen[Any]] = []
    startupinfo = _hidden_startupinfo()
    creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    for stale_stop_file in report_dir.glob("worker-*.stop"):
        stale_stop_file.unlink(missing_ok=True)
    for stale_report_file in report_dir.glob("worker-*.json"):
        stale_report_file.unlink(missing_ok=True)
    for worker_index in range(worker_count):
        report_path = report_dir / f"worker-{worker_index:02d}-of-{worker_count:02d}.json"
        stop_path = report_dir / f"worker-{worker_index:02d}-of-{worker_count:02d}.stop"
        stop_path.unlink(missing_ok=True)
        command = [
            sys.executable,
            "-m",
            "option_data_manager.cli.quote_stream",
            "--database",
            database_path,
            "--report",
            str(report_path),
            "--worker-index",
            str(worker_index),
            "--worker-count",
            str(worker_count),
            "--quote-shard-size",
            str(quote_shard_size),
            "--kline-batch-size",
            str(kline_batch_size),
            "--kline-data-length",
            str(kline_data_length),
            "--near-expiry-months",
            str(near_expiry_months),
            "--metrics-dirty-min-interval-seconds",
            str(metrics_dirty_min_interval_seconds),
            "--underlying-chain-dirty-interval-seconds",
            str(underlying_chain_dirty_interval_seconds),
            "--stop-file",
            str(stop_path),
        ]
        if max_symbols is not None:
            command.extend(["--max-symbols", str(max_symbols)])
        if not prioritize_near_expiry:
            command.append("--no-prioritize-near-expiry")
        if not discover:
            command.append("--no-discover")
        processes.append(
            subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                startupinfo=startupinfo,
                creationflags=creationflags,
            )
        )
    return processes


def _start_metrics_worker_processes(
    *,
    database_path: str,
    report_dir: Path,
    min_interval_seconds: int,
) -> list[subprocess.Popen[Any]]:
    startupinfo = _hidden_startupinfo()
    creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    report_path = report_dir / "metrics-worker.json"
    report_path.unlink(missing_ok=True)
    command = [
        sys.executable,
        "-m",
        "option_data_manager.cli.metrics_worker",
        "--database",
        database_path,
        "--report",
        str(report_path),
        "--min-interval-seconds",
        str(min_interval_seconds),
        "--retry-delay-seconds",
        str(min_interval_seconds),
    ]
    return [
        subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            startupinfo=startupinfo,
            creationflags=creationflags,
        )
    ]


def _hidden_startupinfo() -> subprocess.STARTUPINFO | None:
    if os.name != "nt":
        return None
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    return startupinfo


def _request_quote_stream_stop_files(service_state: ServiceStateRepository) -> None:
    report_dir = service_state.get_value("quote_stream.report_dir")
    if not report_dir:
        return
    directory = Path(report_dir)
    worker_count = _int_or_none(service_state.get_value("quote_stream.worker_count")) or 0
    if worker_count:
        for worker_index in range(worker_count):
            path = directory / f"worker-{worker_index:02d}-of-{worker_count:02d}.stop"
            path.write_text("stop requested\n", encoding="utf-8")
    for path in directory.glob("worker-*.stop"):
        path.write_text("stop requested\n", encoding="utf-8")


def _quote_stream_status(
    service_state: ServiceStateRepository,
    process_table: dict[str, list[Any]],
    *,
    connection: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    processes = process_table.get("processes", [])
    attached_pids = [int(process.pid) for process in processes if process.poll() is None]
    persisted_pids = _json_int_list(service_state.get_value("quote_stream.pids"))
    live_persisted_pids = [
        pid for pid in persisted_pids if pid not in attached_pids and _pid_is_quote_stream(pid)
    ]
    live_pids = attached_pids or live_persisted_pids
    running = bool(live_pids)
    if running:
        if (service_state.get_value("quote_stream.running") or "false") != "true":
            service_state.set_value("quote_stream.running", "true")
            service_state.set_value(
                "quote_stream.message",
                "实时订阅 worker 仍在运行。",
            )
    elif processes or (service_state.get_value("quote_stream.running") or "false") == "true":
        process_table["processes"] = []
        service_state.set_value("quote_stream.running", "false")
        service_state.set_value("quote_stream.finished_at", datetime.now(UTC).isoformat())
        service_state.set_value("quote_stream.message", "实时订阅 worker 已退出。")
    worker_count = _int_or_none(service_state.get_value("quote_stream.worker_count")) or (
        len(live_pids) if live_pids else len(persisted_pids)
    )
    status = "running" if running else "stopped"
    progress = _quote_stream_progress(
        service_state.get_value("quote_stream.report_dir"),
        running=running,
    )
    health = build_realtime_health(
        connection,
        running=running,
        progress=progress,
    )
    return {
        "status": status,
        "running": running,
        "message": service_state.get_value("quote_stream.message"),
        "worker_count": worker_count,
        "pids": live_pids,
        "report_dir": service_state.get_value("quote_stream.report_dir"),
        "started_at": service_state.get_value("quote_stream.started_at"),
        "finished_at": service_state.get_value("quote_stream.finished_at"),
        "progress": progress,
        "health": health,
    }


def _quote_stream_progress(report_dir: str | None, *, running: bool) -> dict[str, Any]:
    if not running:
        return _empty_quote_stream_progress(running=running)
    if not report_dir:
        return _empty_quote_stream_progress(running=running)
    directory = Path(report_dir)
    if not directory.exists():
        return _empty_quote_stream_progress(running=running)
    progress_rows: list[dict[str, Any]] = []
    for path in sorted(directory.glob("worker-*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        progress = _progress_from_report(payload)
        if progress:
            progress_rows.append(progress)
    if not progress_rows:
        return _empty_quote_stream_progress(running=running)
    quote_subscribed = sum(_int_value(row.get("quote_subscribed")) for row in progress_rows)
    quote_total = sum(_int_value(row.get("quote_total")) for row in progress_rows)
    kline_subscribed = sum(_int_value(row.get("kline_subscribed")) for row in progress_rows)
    kline_total = sum(_int_value(row.get("kline_total")) for row in progress_rows)
    near_expiry_quote_subscribed = sum(
        _int_value(row.get("near_expiry_quote_subscribed")) for row in progress_rows
    )
    near_expiry_quote_total = sum(
        _int_value(row.get("near_expiry_quote_total")) for row in progress_rows
    )
    near_expiry_kline_subscribed = sum(
        _int_value(row.get("near_expiry_kline_subscribed")) for row in progress_rows
    )
    near_expiry_kline_total = sum(
        _int_value(row.get("near_expiry_kline_total")) for row in progress_rows
    )
    near_expiry_subscribed = near_expiry_quote_subscribed + near_expiry_kline_subscribed
    near_expiry_total = near_expiry_quote_total + near_expiry_kline_total
    near_expiry_months = max(
        _int_value(row.get("near_expiry_months")) for row in progress_rows
    )
    subscribed_objects = quote_subscribed + kline_subscribed
    total_objects = quote_total + kline_total
    statuses = {str(row.get("status") or "") for row in progress_rows}
    status = "running" if running else "stopped"
    if "failed" in statuses:
        status = "failed"
    elif running and "subscribing" in statuses:
        status = "subscribing"
    updated_values = [
        str(row.get("updated_at"))
        for row in progress_rows
        if row.get("updated_at") is not None
    ]
    wait_update_values = [
        str(row.get("last_wait_update_at"))
        for row in progress_rows
        if row.get("last_wait_update_at") is not None
    ]
    quote_write_values = [
        str(row.get("last_quote_write_at"))
        for row in progress_rows
        if row.get("last_quote_write_at") is not None
    ]
    tqsdk_notify = _latest_tqsdk_notify(progress_rows)
    tqsdk_connection_status = _aggregate_tqsdk_connection_status(progress_rows)
    tqsdk_disconnect_values = [
        str(row.get("last_tqsdk_disconnect_at"))
        for row in progress_rows
        if row.get("last_tqsdk_disconnect_at") is not None
    ]
    tqsdk_restore_values = [
        str(row.get("last_tqsdk_restore_at"))
        for row in progress_rows
        if row.get("last_tqsdk_restore_at") is not None
    ]
    started_values = [
        str(row.get("started_at"))
        for row in progress_rows
        if row.get("started_at") is not None
    ]
    elapsed_seconds = _elapsed_seconds(
        min(started_values) if started_values else None,
        max(updated_values) if updated_values else None,
    )
    eta = _subscription_eta(
        progress_rows,
        quote_subscribed=quote_subscribed,
        quote_total=quote_total,
        kline_subscribed=kline_subscribed,
        kline_total=kline_total,
    )
    return {
        "status": status,
        "worker_reports": len(progress_rows),
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
        "tqsdk_notify_count": sum(
            _int_value(row.get("tqsdk_notify_count")) for row in progress_rows
        ),
        "cycle_count": sum(_int_value(row.get("cycle_count")) for row in progress_rows),
        "wait_update_count": sum(
            _int_value(row.get("wait_update_count")) for row in progress_rows
        ),
        "quotes_written": sum(_int_value(row.get("quotes_written")) for row in progress_rows),
        "changed_quotes_written": sum(
            _int_value(row.get("changed_quotes_written")) for row in progress_rows
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


def _progress_from_report(payload: dict[str, Any]) -> dict[str, Any] | None:
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
    near_expiry_total = near_expiry_quote_total + near_expiry_kline_total
    near_expiry_subscribed = (
        near_expiry_quote_subscribed + near_expiry_kline_subscribed
    )
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
        "cycle_count": _int_value(result.get("cycles")),
        "wait_update_count": _int_value(result.get("wait_update_count")),
        "quotes_written": _int_value(result.get("quotes_written")),
        "changed_quotes_written": _int_value(result.get("changed_quotes_written")),
        "quote_started_at": result.get("quote_started_at") or result.get("started_at"),
        "quote_finished_at": result.get("quote_finished_at"),
        "kline_started_at": result.get("kline_started_at"),
        "worker_index": result.get("worker_index"),
        "worker_count": result.get("worker_count"),
        "quote_subscribed": quote_subscribed,
        "quote_total": quote_total,
        "kline_subscribed": kline_subscribed,
        "kline_total": kline_total,
        "near_expiry_months": _int_value(result.get("near_expiry_months")),
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
    }


def _latest_tqsdk_notify(rows: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = [
        row for row in rows if row.get("last_tqsdk_notify_at") is not None
    ]
    if not candidates:
        return {}
    latest = max(candidates, key=lambda row: str(row.get("last_tqsdk_notify_at")))
    return {
        "last_tqsdk_notify_at": latest.get("last_tqsdk_notify_at"),
        "last_tqsdk_notify_code": latest.get("last_tqsdk_notify_code"),
        "last_tqsdk_notify_level": latest.get("last_tqsdk_notify_level"),
        "last_tqsdk_notify_content": latest.get("last_tqsdk_notify_content"),
    }


def _aggregate_tqsdk_connection_status(rows: list[dict[str, Any]]) -> str:
    statuses = {
        str(row.get("tqsdk_connection_status") or "unknown") for row in rows
    }
    if "disconnected" in statuses:
        return "disconnected"
    if "reconnecting" in statuses:
        return "reconnecting"
    if "connected" in statuses:
        return "connected"
    return "unknown"


def _empty_quote_stream_progress(*, running: bool) -> dict[str, Any]:
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


def _int_value(value: Any, default: int = 0) -> int:
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
    rows: list[dict[str, Any]],
    *,
    quote_subscribed: int,
    quote_total: int,
    kline_subscribed: int,
    kline_total: int,
) -> dict[str, Any]:
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
    rows: list[dict[str, Any]],
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


def _first_text(row: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is not None:
            return str(value)
    return None


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
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


def _pid_is_metrics_worker(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        return _windows_pid_command_line_contains(
            pid,
            "option_data_manager.cli.metrics_worker",
        )
    command_line_path = Path(f"/proc/{pid}/cmdline")
    try:
        return "option_data_manager.cli.metrics_worker" in command_line_path.read_text(
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


def _terminate_pid(pid: int) -> None:
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        return
    try:
        os.kill(pid, 15)
    except Exception:
        return


def _stop_attached_processes(processes: list[Any]) -> int:
    stopped = 0
    for process in processes:
        if process.poll() is None:
            stopped += 1
            process.terminate()
    for process in processes:
        if process.poll() is None:
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
    return stopped


def _json_int_list(value: str | None) -> list[int]:
    if not value:
        return []
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    integers: list[int] = []
    for item in payload:
        try:
            integers.append(int(item))
        except (TypeError, ValueError):
            continue
    return integers


def _positive_int_setting(value: str | None, default: int) -> int:
    parsed = _optional_positive_int(value)
    return default if parsed is None else parsed


def _bool_setting(value: str | None, default: bool) -> bool:
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _optional_positive_int(value: str | None) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _run_refresh_until_complete(
    *,
    args: list[str],
    database_path: str,
    started_at: str,
) -> None:
    from option_data_manager.cli.collect_market import main as collect_main

    state_connection = sqlite3.connect(database_path, check_same_thread=False)
    state_connection.row_factory = sqlite3.Row
    state = ServiceStateRepository(state_connection)
    logs = ServiceLogRepository(state_connection)
    stalled_windows = 0
    previous_remaining: int | None = None
    window_count = 0
    try:
        while True:
            window_count += 1
            state.set_value("collection.refresh_window_count", str(window_count))
            state.set_value(
                "collection.refresh_message",
                f"Collecting window {window_count}.",
            )
            logs.append(
                level="info",
                category="collection",
                message="Collection refresh window started.",
                context={"window_count": window_count},
            )
            exit_code = collect_main(args)
            state.set_value("collection.last_refresh_exit_code", str(exit_code))
            if exit_code != 0:
                message = _refresh_failure_message(exit_code)
                state.set_value(
                    "collection.refresh_message",
                    message,
                )
                logs.append(
                    level="error",
                    category="collection",
                    message=message,
                    context={"window_count": window_count, "exit_code": exit_code},
                )
                break
            progress = WebuiReadModel(state_connection).overview(limit=1)["collection"]
            active_batches = int(progress["active_batches"])
            remaining_batches = int(progress["remaining_batches"])
            state.set_value("collection.refresh_remaining_batches", str(remaining_batches))
            if active_batches == 0:
                message = "Refresh stopped because no active collection plan exists."
                state.set_value(
                    "collection.refresh_message",
                    message,
                )
                logs.append(level="warning", category="collection", message=message)
                break
            if remaining_batches == 0:
                message = "Full-market refresh completed."
                state.set_value(
                    "collection.refresh_message",
                    message,
                )
                logs.append(
                    level="info",
                    category="collection",
                    message=message,
                    context={"window_count": window_count},
                )
                break
            if previous_remaining is not None and remaining_batches >= previous_remaining:
                stalled_windows += 1
            else:
                stalled_windows = 0
            previous_remaining = remaining_batches
            if stalled_windows >= 3:
                message = "Refresh paused after three windows without progress."
                state.set_value(
                    "collection.refresh_message",
                    message,
                )
                logs.append(
                    level="warning",
                    category="collection",
                    message=message,
                    context={
                        "window_count": window_count,
                        "remaining_batches": remaining_batches,
                    },
                )
                break
    except Exception as exc:
        state.set_value("collection.last_refresh_exit_code", "1")
        message = f"Refresh failed: {type(exc).__name__}: {exc}"
        state.set_value(
            "collection.refresh_message",
            message,
        )
        logs.append(
            level="error",
            category="collection",
            message=message,
            context={"error_type": type(exc).__name__},
        )
    finally:
        state.set_value("collection.refresh_running", "false")
        state.set_value("collection.refresh_started_at", started_at)
        state.set_value("collection.refresh_finished_at", datetime.now(UTC).isoformat())
        state_connection.close()


def _refresh_status(service_state: ServiceStateRepository) -> dict[str, Any]:
    return {
        "running": (service_state.get_value("collection.refresh_running") or "false")
        == "true",
        "started_at": service_state.get_value("collection.refresh_started_at"),
        "finished_at": service_state.get_value("collection.refresh_finished_at"),
        "message": service_state.get_value("collection.refresh_message"),
        "window_count": _int_or_none(
            service_state.get_value("collection.refresh_window_count")
        ),
        "remaining_batches": _int_or_none(
            service_state.get_value("collection.refresh_remaining_batches")
        ),
        "last_exit_code": _int_or_none(
            service_state.get_value("collection.last_refresh_exit_code")
        ),
    }


def _refresh_failure_message(exit_code: int) -> str:
    if exit_code == 2:
        return "Refresh blocked: TQSDK credentials are not configured."
    return f"Refresh failed with exit code {exit_code}; see the collection report."


def _int_or_none(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _option_api_row(row: sqlite3.Row) -> dict[str, Any]:
    data = dict(row)
    data["days_to_expire_datetime"] = _days_to_expiry(data.get("expire_datetime"))
    data["days_to_last_exercise_datetime"] = _days_to_expiry(
        data.get("last_exercise_datetime")
    )
    return data


def _overall_status(overview: dict[str, Any], latest_run: list[Any]) -> str:
    if latest_run and latest_run[0].status == "failed":
        return "failed"
    summary = overview["summary"]
    if summary["active_options"] == 0:
        return "empty"
    if summary["acquisition_errors"]:
        return "partial_failure"
    return "ok"


if __name__ == "__main__":
    main()
