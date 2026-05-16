"""Unified local FastAPI application."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
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
from option_data_manager.cli.collect_market import _discover_and_persist_market
from option_data_manager.api_keys import ApiKeyRecord, ApiKeyRepository
from option_data_manager.collection_state import CollectionStateRepository
from option_data_manager.klines import KlineRepository
from option_data_manager.option_metrics import OptionMetricsRepository
from option_data_manager.quote_streamer import (
    DEFAULT_KLINE_SUBSCRIPTION_TIMEOUT_SECONDS,
    DEFAULT_MONEYNESS_FILTER,
    DEFAULT_MONEYNESS_RECALC_SECONDS,
    expected_subscription_counts,
)
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
from option_data_manager.sqlite_runtime import configure_sqlite_runtime
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
QUOTE_STREAM_CONTRACT_MONTHS_KEY = "quote_stream.contract_months"
QUOTE_STREAM_MIN_DAYS_TO_EXPIRY_KEY = "quote_stream.min_days_to_expiry"
QUOTE_STREAM_MONEYNESS_FILTER_KEY = "quote_stream.moneyness_filter"
QUOTE_STREAM_MONEYNESS_RECALC_SECONDS_KEY = "quote_stream.moneyness_recalc_seconds"
QUOTE_STREAM_KLINE_SUBSCRIPTION_TIMEOUT_SECONDS_KEY = (
    "quote_stream.kline_subscription_timeout_seconds"
)
CONTRACT_MANAGER_REFRESH_INTERVAL_SECONDS_KEY = "contract_manager.refresh_interval_seconds"
METRICS_MIN_INTERVAL_SECONDS_KEY = "metrics.min_interval_seconds"
METRICS_UNDERLYING_CHAIN_INTERVAL_SECONDS_KEY = "metrics.underlying_chain_interval_seconds"
DEFAULT_QUOTE_STREAM_WORKERS = "1"
DEFAULT_QUOTE_STREAM_SHARD_SIZE = "1000"
DEFAULT_QUOTE_STREAM_KLINE_BATCH_SIZE = "1"
DEFAULT_QUOTE_STREAM_KLINE_DATA_LENGTH = "3"
DEFAULT_QUOTE_STREAM_PRIORITIZE_NEAR_EXPIRY = "true"
DEFAULT_QUOTE_STREAM_NEAR_EXPIRY_MONTHS = "2"
DEFAULT_QUOTE_STREAM_CONTRACT_MONTHS = "2"
DEFAULT_QUOTE_STREAM_MIN_DAYS_TO_EXPIRY = "1"
DEFAULT_QUOTE_STREAM_MONEYNESS_FILTER = DEFAULT_MONEYNESS_FILTER
DEFAULT_QUOTE_STREAM_MONEYNESS_RECALC_SECONDS = str(DEFAULT_MONEYNESS_RECALC_SECONDS)
DEFAULT_QUOTE_STREAM_KLINE_SUBSCRIPTION_TIMEOUT_SECONDS = str(
    int(DEFAULT_KLINE_SUBSCRIPTION_TIMEOUT_SECONDS)
)
DEFAULT_CONTRACT_MANAGER_REFRESH_INTERVAL_SECONDS = "3600"
DEFAULT_METRICS_MIN_INTERVAL_SECONDS = "30"


class QuoteStreamState(StrEnum):
    """Stable states for the realtime quote-stream lifecycle."""

    BLOCKED = "blocked"
    STARTING = "starting"
    RUNNING = "running"
    STOPPED = "stopped"


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


class ContractManagerResponse(BaseModel):
    """Current contract manager status."""

    status: str
    running: bool
    healthy: bool
    refresh_running: bool
    message: str | None
    active_option_count: int
    refresh_interval_seconds: int
    started_at: str | None = None
    stopped_at: str | None = None
    last_heartbeat_at: str | None = None
    last_success_at: str | None = None
    last_error_at: str | None = None


class QuoteStreamStartRequest(BaseModel):
    """Optional runtime overrides for quote stream workers."""

    workers: int | None = None
    quote_shard_size: int | None = None
    kline_batch_size: int | None = None
    contract_months: str | None = None
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
    active_option_count: int | None = None
    contract_discovery_running: bool = False
    contract_list_ready: bool = False
    contract_months: str | None = None
    min_days_to_expiry: int | None = None


def create_app(
    connection: sqlite3.Connection,
    *,
    database_path: str | None = None,
    protector: Any | None = None,
) -> FastAPI:
    """Create the unified local API application."""

    connection.row_factory = sqlite3.Row
    secret_protector = protector or _default_protector()
    _ensure_runtime_tables(connection, protector=secret_protector)
    settings = SettingsRepository(connection, secret_protector)
    api_keys = ApiKeyRepository(connection)
    service_state = ServiceStateRepository(connection)
    service_logs = ServiceLogRepository(connection)
    read_model = WebuiReadModel(connection)
    refresh_lock = threading.Lock()
    refresh_worker: dict[str, threading.Thread | None] = {"thread": None}
    quote_stream_lock = threading.Lock()
    contract_manager_lock = threading.Lock()
    contract_manager_stop_event = threading.Event()
    settings_write_lock = threading.Lock()
    quote_stream_start_worker: dict[str, threading.Thread | None] = {"thread": None}
    contract_manager_worker: dict[str, threading.Thread | None] = {"thread": None}
    quote_stream_processes: dict[str, list[Any]] = {"processes": []}
    metrics_worker_processes: dict[str, list[Any]] = {"processes": []}
    service_state.set_value("quote_stream.contract_discovery_running", "false")
    service_state.set_value("quote_stream.contract_list_ready", "false")
    service_state.set_value("quote_stream.status", QuoteStreamState.BLOCKED.value)
    service_state.set_value(
        "quote_stream.message",
        "合约管理器未运行，实时订阅无法启动。",
    )
    if (service_state.get_value("contract_manager.running") or "false") == "true":
        service_state.set_value("contract_manager.running", "false")
        service_state.set_value("contract_manager.refresh_running", "false")
        service_state.set_value(
            "contract_manager.message",
            "本地服务重启后，合约管理器未自动接管。",
        )
    if (service_state.get_value("collection.refresh_running") or "false") == "true":
        service_state.set_value("collection.refresh_running", "false")
        service_state.set_value(
            "collection.refresh_message",
            "Refresh stopped because the local service restarted.",
        )
    if (service_state.get_value("quote_stream.running") or "false") == "true":
        service_state.set_value("quote_stream.running", "false")
        service_state.set_value("quote_stream.status", QuoteStreamState.STOPPED.value)
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
    app.state.contract_manager_worker = contract_manager_worker

    @app.middleware("http")
    async def record_metrics(request: Request, call_next: Any) -> Any:
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception as exc:
            if request.url.path.startswith("/api/"):
                _safe_record_request(
                    service_state,
                    path=request.url.path,
                    method=request.method,
                    status_code=500,
                    latency_ms=(time.perf_counter() - started) * 1000,
                )
                _safe_append_service_log(
                    service_logs,
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
            _safe_record_request(
                service_state,
                path=request.url.path,
                method=request.method,
                status_code=response.status_code,
                latency_ms=(time.perf_counter() - started) * 1000,
            )
            if response.status_code >= 500:
                _safe_append_service_log(
                    service_logs,
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

    @app.get("/api/contract-manager", response_model=ContractManagerResponse)
    def contract_manager_status(
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> ContractManagerResponse:
        return ContractManagerResponse(
            **_contract_manager_status(
                service_state,
                contract_manager_worker,
                connection=connection,
                settings=settings,
            )
        )

    @app.post("/api/contract-manager/start", response_model=ContractManagerResponse)
    def start_contract_manager(
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> ContractManagerResponse:
        with contract_manager_lock:
            worker = contract_manager_worker.get("thread")
            if worker is not None and worker.is_alive():
                return ContractManagerResponse(
                    **_contract_manager_status(
                        service_state,
                        contract_manager_worker,
                        connection=connection,
                        settings=settings,
                    )
                )
            account = settings.get_value(TQSDK_ACCOUNT_KEY)
            password, password_error = _safe_get_secret(settings, TQSDK_PASSWORD_KEY)
            if password_error is not None:
                message = "TQSDK 凭据无法在当前 WSL 用户下读取，请在设置页重新保存密码。"
                service_state.set_value("contract_manager.message", message)
                service_state.set_value("contract_manager.running", "false")
                return ContractManagerResponse(
                    **_contract_manager_status(
                        service_state,
                        contract_manager_worker,
                        connection=connection,
                        settings=settings,
                    )
                )
            if not account or not password:
                message = "TQSDK 凭据尚未配置，无法启动合约管理器。"
                service_state.set_value("contract_manager.message", message)
                service_state.set_value("contract_manager.running", "false")
                return ContractManagerResponse(
                    **_contract_manager_status(
                        service_state,
                        contract_manager_worker,
                        connection=connection,
                        settings=settings,
                    )
                )
            refresh_interval_seconds = _positive_int_setting(
                settings.get_value(CONTRACT_MANAGER_REFRESH_INTERVAL_SECONDS_KEY),
                int(DEFAULT_CONTRACT_MANAGER_REFRESH_INTERVAL_SECONDS),
            )
            contract_manager_stop_event.clear()
            service_state.set_value("contract_manager.running", "true")
            service_state.set_value("contract_manager.refresh_running", "true")
            service_state.set_value("contract_manager.started_at", datetime.now(UTC).isoformat())
            service_state.set_value("contract_manager.stopped_at", None)
            service_state.set_value("contract_manager.message", "合约管理器正在刷新合约列表。")
            service_state.set_value("quote_stream.contract_list_ready", "false")
            service_logs.append(
                level="info",
                category="contract_manager",
                message="合约管理器已启动。",
                context={"refresh_interval_seconds": refresh_interval_seconds},
            )
            worker = threading.Thread(
                target=_run_contract_manager,
                kwargs={
                    "connection": connection,
                    "account": account,
                    "password": password,
                    "service_state": service_state,
                    "service_logs": service_logs,
                    "stop_event": contract_manager_stop_event,
                    "refresh_interval_seconds": refresh_interval_seconds,
                },
                daemon=True,
            )
            contract_manager_worker["thread"] = worker
            worker.start()
            return ContractManagerResponse(
                **_contract_manager_status(
                    service_state,
                    contract_manager_worker,
                    connection=connection,
                    settings=settings,
                )
            )

    @app.post("/api/contract-manager/stop", response_model=ContractManagerResponse)
    def stop_contract_manager(
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> ContractManagerResponse:
        with contract_manager_lock:
            contract_manager_stop_event.set()
            worker = contract_manager_worker.get("thread")
            if worker is None or not worker.is_alive():
                stopped_at = datetime.now(UTC).isoformat()
                service_state.set_value("contract_manager.running", "false")
                service_state.set_value("contract_manager.refresh_running", "false")
                service_state.set_value("contract_manager.stopped_at", stopped_at)
                service_state.set_value("contract_manager.message", "合约管理器已停止。")
                service_state.set_value("quote_stream.contract_list_ready", "false")
            else:
                service_state.set_value("contract_manager.message", "合约管理器正在停止。")
            return ContractManagerResponse(
                **_contract_manager_status(
                    service_state,
                    contract_manager_worker,
                    connection=connection,
                    settings=settings,
                )
            )

    @app.post("/api/quote-stream/start", response_model=QuoteStreamResponse)
    def start_quote_stream(
        payload: QuoteStreamStartRequest | None = None,
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> QuoteStreamResponse:
        request = payload or QuoteStreamStartRequest()
        with quote_stream_lock:
            starting_worker = quote_stream_start_worker.get("thread")
            if starting_worker is not None and starting_worker.is_alive():
                return QuoteStreamResponse(
                    **_quote_stream_status(
                        service_state,
                        quote_stream_processes,
                        connection=connection,
                    )
                )
            status = _quote_stream_status(
                service_state,
                quote_stream_processes,
                connection=connection,
            )
            if status["running"]:
                return QuoteStreamResponse(**status)
            account = settings.get_value(TQSDK_ACCOUNT_KEY)
            password, password_error = _safe_get_secret(settings, TQSDK_PASSWORD_KEY)
            if password_error is not None:
                service_state.set_value("quote_stream.status", QuoteStreamState.BLOCKED.value)
                service_state.set_value("quote_stream.running", "false")
                service_logs.append(
                    level="warning",
                    category="quote_stream",
                    message="实时订阅启动被阻止：TQSDK 凭据无法读取。",
                    context={"error_type": type(password_error).__name__},
                )
                return QuoteStreamResponse(
                    status=QuoteStreamState.BLOCKED.value,
                    running=False,
                    message="TQSDK 凭据无法在当前 WSL 用户下读取，请在设置页重新保存密码。",
                    worker_count=0,
                    pids=[],
                    report_dir=None,
                    started_at=None,
                    finished_at=None,
                )
            if not account or not password:
                service_state.set_value("quote_stream.status", QuoteStreamState.BLOCKED.value)
                service_state.set_value("quote_stream.running", "false")
                service_logs.append(
                    level="warning",
                    category="quote_stream",
                    message="实时订阅启动被阻止：TQSDK 凭据缺失。",
                )
                return QuoteStreamResponse(
                    status=QuoteStreamState.BLOCKED.value,
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
            contract_months = _contract_months_setting(
                request.contract_months
                if request.contract_months is not None
                else settings.get_value(QUOTE_STREAM_CONTRACT_MONTHS_KEY),
                DEFAULT_QUOTE_STREAM_CONTRACT_MONTHS,
            )
            min_days_to_expiry = _non_negative_int_setting(
                settings.get_value(QUOTE_STREAM_MIN_DAYS_TO_EXPIRY_KEY),
                int(DEFAULT_QUOTE_STREAM_MIN_DAYS_TO_EXPIRY),
            )
            moneyness_filter = (
                settings.get_value(QUOTE_STREAM_MONEYNESS_FILTER_KEY)
                or DEFAULT_QUOTE_STREAM_MONEYNESS_FILTER
            )
            moneyness_recalc_seconds = _positive_int_setting(
                settings.get_value(QUOTE_STREAM_MONEYNESS_RECALC_SECONDS_KEY),
                int(DEFAULT_QUOTE_STREAM_MONEYNESS_RECALC_SECONDS),
            )
            kline_subscription_timeout_seconds = _positive_int_setting(
                settings.get_value(
                    QUOTE_STREAM_KLINE_SUBSCRIPTION_TIMEOUT_SECONDS_KEY
                ),
                int(DEFAULT_QUOTE_STREAM_KLINE_SUBSCRIPTION_TIMEOUT_SECONDS),
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
            active_options = _active_option_count(connection)
            manager_status = _contract_manager_status(
                service_state,
                contract_manager_worker,
                connection=connection,
                settings=settings,
            )
            if active_options < 1 or not manager_status["healthy"]:
                message = "合约管理器未正常运行，无法启动实时订阅。请先启动合约管理器并等待合约列表刷新成功。"
                service_state.set_value("quote_stream.status", QuoteStreamState.BLOCKED.value)
                service_state.set_value("quote_stream.running", "false")
                service_state.set_value("quote_stream.message", message)
                service_state.set_value("quote_stream.pids", "[]")
                service_logs.append(
                    level="warning",
                    category="quote_stream",
                    message="实时订阅启动被阻止：合约管理器未就绪。",
                )
                return QuoteStreamResponse(
                    status=QuoteStreamState.BLOCKED.value,
                    running=False,
                    message=message,
                    worker_count=worker_count,
                    pids=[],
                    report_dir=_safe_get_service_value(service_state, "quote_stream.report_dir"),
                    started_at=_safe_get_service_value(service_state, "quote_stream.started_at"),
                    finished_at=_safe_get_service_value(service_state, "quote_stream.finished_at"),
                    progress=_empty_quote_stream_progress(running=False),
                    health=build_realtime_health(connection, running=False, progress={}),
                    active_option_count=active_options,
                    contract_discovery_running=False,
                    contract_list_ready=bool(manager_status["healthy"]),
                )
            started_at = datetime.now(UTC).isoformat()
            configured_report_dir = _safe_get_service_value(
                service_state,
                "quote_stream.report_dir",
            )
            report_dir = (
                Path(configured_report_dir)
                if configured_report_dir
                else Path("docs/qa/live-evidence/quote-stream-runtime")
            )
            report_dir.mkdir(parents=True, exist_ok=True)
            service_state.set_value("quote_stream.status", QuoteStreamState.STARTING.value)
            service_state.set_value("quote_stream.running", "false")
            service_state.set_value("quote_stream.started_at", started_at)
            service_state.set_value("quote_stream.finished_at", None)
            service_state.set_value("quote_stream.worker_count", str(worker_count))
            service_state.set_value("quote_stream.contract_months", contract_months)
            service_state.set_value(
                "quote_stream.min_days_to_expiry",
                str(min_days_to_expiry),
            )
            service_state.set_value(
                "quote_stream.max_symbols",
                str(max_symbols) if max_symbols is not None else None,
            )
            _store_quote_stream_expected_counts(service_state, None)
            service_state.set_value("quote_stream.pids", "[]")
            service_state.set_value("quote_stream.report_dir", str(report_dir))
            service_state.set_value(
                "quote_stream.message",
                "正在启动实时订阅 worker。",
            )
            service_logs.append(
                level="info",
                category="quote_stream",
                message="实时订阅后台启动已开始。",
                context={
                    "worker_count": worker_count,
                    "quote_shard_size": quote_shard_size,
                    "kline_data_length": kline_data_length,
                    "prioritize_near_expiry": prioritize_near_expiry,
                    "near_expiry_months": near_expiry_months,
                    "contract_months": contract_months,
                    "min_days_to_expiry": min_days_to_expiry,
                    "moneyness_filter": moneyness_filter,
                    "moneyness_recalc_seconds": moneyness_recalc_seconds,
                    "kline_subscription_timeout_seconds": (
                        kline_subscription_timeout_seconds
                    ),
                    "max_symbols_configured": max_symbols is not None,
                    "discover_requested": False,
                    "report_dir": str(report_dir),
                    "metrics_min_interval_seconds": metrics_min_interval_seconds,
                    "underlying_chain_interval_seconds": underlying_chain_interval_seconds,
                },
            )
            worker = threading.Thread(
                target=_run_quote_stream_start,
                kwargs={
                    "connection": connection,
                    "account": account,
                    "password": password,
                    "force_discovery": False,
                    "service_state": service_state,
                    "service_logs": service_logs,
                    "quote_stream_processes": quote_stream_processes,
                    "metrics_worker_processes": metrics_worker_processes,
                    "database_path": database_path or DEFAULT_DATABASE_PATH,
                    "report_dir": report_dir,
                    "worker_count": worker_count,
                    "quote_shard_size": quote_shard_size,
                    "kline_batch_size": kline_batch_size,
                    "kline_data_length": kline_data_length,
                    "prioritize_near_expiry": prioritize_near_expiry,
                    "near_expiry_months": near_expiry_months,
                    "contract_months": contract_months,
                    "min_days_to_expiry": min_days_to_expiry,
                    "moneyness_filter": moneyness_filter,
                    "moneyness_recalc_seconds": moneyness_recalc_seconds,
                    "kline_subscription_timeout_seconds": (
                        kline_subscription_timeout_seconds
                    ),
                    "max_symbols": max_symbols,
                    "metrics_dirty_min_interval_seconds": metrics_min_interval_seconds,
                    "underlying_chain_dirty_interval_seconds": underlying_chain_interval_seconds,
                    "quote_stream_lock": quote_stream_lock,
                },
                daemon=True,
            )
            quote_stream_start_worker["thread"] = worker
            worker.start()
            return QuoteStreamResponse(
                status=QuoteStreamState.STARTING.value,
                running=False,
                message="正在启动实时订阅 worker。",
                worker_count=worker_count,
                pids=[],
                report_dir=str(report_dir),
                started_at=started_at,
                finished_at=None,
                progress={
                    **_empty_quote_stream_progress(running=False),
                    "status": "starting",
                    "contract_months": contract_months,
                    "min_days_to_expiry": min_days_to_expiry,
                },
                health=build_realtime_health(connection, running=False, progress={}),
                active_option_count=active_options,
                contract_discovery_running=False,
                contract_list_ready=bool(manager_status["healthy"]),
                contract_months=contract_months,
                min_days_to_expiry=min_days_to_expiry,
            )

    @app.post("/api/quote-stream/discover-contracts", response_model=QuoteStreamResponse)
    def discover_quote_stream_contracts(
        _: ApiKeyRecord | None = Depends(require_auth),
    ) -> QuoteStreamResponse:
        with contract_manager_lock:
            manager_worker = contract_manager_worker.get("thread")
            if manager_worker is not None and manager_worker.is_alive():
                return QuoteStreamResponse(
                    **_quote_stream_status(
                        service_state,
                        quote_stream_processes,
                        connection=connection,
                    )
                )
            if _quote_stream_status(
                service_state,
                quote_stream_processes,
                connection=connection,
            )["running"]:
                return QuoteStreamResponse(
                    **_quote_stream_status(
                        service_state,
                        quote_stream_processes,
                        connection=connection,
                    )
                )
            account = settings.get_value(TQSDK_ACCOUNT_KEY)
            password, password_error = _safe_get_secret(settings, TQSDK_PASSWORD_KEY)
            if password_error is not None:
                message = "TQSDK 凭据无法在当前 WSL 用户下读取，请在设置页重新保存密码。"
                service_state.set_value("contract_manager.message", message)
                return QuoteStreamResponse(
                    status=QuoteStreamState.BLOCKED.value,
                    running=False,
                    message=message,
                    worker_count=0,
                    pids=[],
                    report_dir=_safe_get_service_value(service_state, "quote_stream.report_dir"),
                    started_at=_safe_get_service_value(service_state, "quote_stream.started_at"),
                    finished_at=_safe_get_service_value(service_state, "quote_stream.finished_at"),
                    progress=_empty_quote_stream_progress(running=False),
                    health=build_realtime_health(connection, running=False, progress={}),
                    active_option_count=_active_option_count(connection),
                    contract_discovery_running=False,
                )
            if not account or not password:
                message = "TQSDK 凭据尚未配置，无法启动合约管理器。"
                service_state.set_value("contract_manager.message", message)
                return QuoteStreamResponse(
                    status=QuoteStreamState.BLOCKED.value,
                    running=False,
                    message=message,
                    worker_count=0,
                    pids=[],
                    report_dir=_safe_get_service_value(service_state, "quote_stream.report_dir"),
                    started_at=_safe_get_service_value(service_state, "quote_stream.started_at"),
                    finished_at=_safe_get_service_value(service_state, "quote_stream.finished_at"),
                    progress=_empty_quote_stream_progress(running=False),
                    health=build_realtime_health(connection, running=False, progress={}),
                    active_option_count=_active_option_count(connection),
                    contract_discovery_running=False,
                )
            refresh_interval_seconds = _positive_int_setting(
                settings.get_value(CONTRACT_MANAGER_REFRESH_INTERVAL_SECONDS_KEY),
                int(DEFAULT_CONTRACT_MANAGER_REFRESH_INTERVAL_SECONDS),
            )
            contract_manager_stop_event.clear()
            service_state.set_value("contract_manager.running", "true")
            service_state.set_value("contract_manager.refresh_running", "true")
            service_state.set_value("contract_manager.started_at", datetime.now(UTC).isoformat())
            service_state.set_value("contract_manager.stopped_at", None)
            service_state.set_value("contract_manager.message", "合约管理器正在刷新合约列表。")
            service_state.set_value("quote_stream.contract_discovery_running", "true")
            service_state.set_value("quote_stream.contract_list_ready", "false")
            service_state.set_value("quote_stream.running", "false")
            service_state.set_value("quote_stream.status", QuoteStreamState.STOPPED.value)
            service_state.set_value("quote_stream.message", "合约管理器正在刷新合约列表。")
            service_logs.append(
                level="info",
                category="contract_manager",
                message="合约管理器已启动。",
            )
            worker = threading.Thread(
                target=_run_contract_manager,
                kwargs={
                    "connection": connection,
                    "account": account,
                    "password": password,
                    "service_state": service_state,
                    "service_logs": service_logs,
                    "stop_event": contract_manager_stop_event,
                    "refresh_interval_seconds": refresh_interval_seconds,
                },
                daemon=True,
            )
            contract_manager_worker["thread"] = worker
            worker.start()
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
            persisted_pids = _json_int_list(
                _safe_get_service_value(service_state, "quote_stream.pids")
            )
            persisted_metrics_pids = _json_int_list(
                _safe_get_service_value(service_state, "metrics_worker.pids")
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
            _safe_set_service_value(service_state, "quote_stream.running", "false")
            _safe_set_service_value(
                service_state,
                "quote_stream.status",
                QuoteStreamState.STOPPED.value,
            )
            _safe_set_service_value(service_state, "quote_stream.finished_at", finished_at)
            _safe_set_service_value(service_state, "quote_stream.pids", "[]")
            _safe_set_service_value(service_state, "metrics_worker.running", "false")
            _safe_set_service_value(service_state, "metrics_worker.finished_at", finished_at)
            _safe_set_service_value(service_state, "metrics_worker.pids", "[]")
            stop_message = (
                f"已请求停止 {stopped} 个实时订阅 worker 和 {metrics_stopped} 个指标 worker。"
            )
            _safe_set_service_value(
                service_state,
                "quote_stream.message",
                stop_message,
            )
            _safe_set_service_value(
                service_state,
                "metrics_worker.message",
                f"已请求停止 {metrics_stopped} 个指标刷新 worker。",
            )
            _safe_append_service_log(
                service_logs,
                level="info",
                category="quote_stream",
                message="实时订阅 worker 已停止。",
                context={"stopped_workers": stopped},
            )
            payload = _quote_stream_status(
                service_state,
                quote_stream_processes,
                connection=connection,
            )
            payload["message"] = stop_message
            payload["finished_at"] = payload["finished_at"] or finished_at
            return QuoteStreamResponse(**payload)

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
        password, password_error = _safe_get_secret(settings, TQSDK_PASSWORD_KEY)
        return {
            "tqsdk": {
                "account": account,
                "password_configured": password is not None,
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
            "contract_manager": {
                "refresh_interval_seconds": _positive_int_setting(
                    settings.get_value(CONTRACT_MANAGER_REFRESH_INTERVAL_SECONDS_KEY),
                    int(DEFAULT_CONTRACT_MANAGER_REFRESH_INTERVAL_SECONDS),
                ),
                "status": _contract_manager_status(
                    service_state,
                    contract_manager_worker,
                    connection=connection,
                    settings=settings,
                ),
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
                "contract_months": _contract_months_setting(
                    settings.get_value(QUOTE_STREAM_CONTRACT_MONTHS_KEY),
                    DEFAULT_QUOTE_STREAM_CONTRACT_MONTHS,
                ),
                "min_days_to_expiry": _non_negative_int_setting(
                    settings.get_value(QUOTE_STREAM_MIN_DAYS_TO_EXPIRY_KEY),
                    int(DEFAULT_QUOTE_STREAM_MIN_DAYS_TO_EXPIRY),
                ),
                "moneyness_filter": settings.get_value(
                    QUOTE_STREAM_MONEYNESS_FILTER_KEY
                )
                or DEFAULT_QUOTE_STREAM_MONEYNESS_FILTER,
                "moneyness_recalc_seconds": _positive_int_setting(
                    settings.get_value(QUOTE_STREAM_MONEYNESS_RECALC_SECONDS_KEY),
                    int(DEFAULT_QUOTE_STREAM_MONEYNESS_RECALC_SECONDS),
                ),
                "kline_subscription_timeout_seconds": _positive_int_setting(
                    settings.get_value(
                        QUOTE_STREAM_KLINE_SUBSCRIPTION_TIMEOUT_SECONDS_KEY
                    ),
                    int(DEFAULT_QUOTE_STREAM_KLINE_SUBSCRIPTION_TIMEOUT_SECONDS),
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
        with settings_write_lock:
            writer, writer_connection = _settings_writer(
                settings,
                database_path=database_path,
                protector=secret_protector,
            )
            try:
                writer.set_value(TQSDK_ACCOUNT_KEY, account)
                writer.set_secret(TQSDK_PASSWORD_KEY, payload.password)
            finally:
                if writer_connection is not None:
                    writer_connection.close()
        _safe_append_service_log(
            service_logs,
            level="info",
            category="settings",
            message="TQSDK credentials updated.",
            context={"account_configured": True, "password_configured": True},
        )
        return {"account": account, "password_configured": True}

    @app.post("/api/settings/test-tqsdk-connection")
    def test_tqsdk_connection(_: ApiKeyRecord | None = Depends(require_auth)) -> dict[str, Any]:
        account = settings.get_value(TQSDK_ACCOUNT_KEY)
        password, password_error = _safe_get_secret(settings, TQSDK_PASSWORD_KEY)
        if password_error is not None:
            service_logs.append(
                level="warning",
                category="settings",
                message="TQSDK connection test blocked because credentials cannot be read.",
                context={"error_type": type(password_error).__name__},
            )
            return {
                "status": "blocked",
                "message": "TQSDK credentials cannot be read in this WSL runtime; please save the password again.",
            }
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
        with settings_write_lock:
            writer, writer_connection = _settings_writer(
                settings,
                database_path=database_path,
                protector=secret_protector,
            )
            try:
                writer.set_value(key, payload.value)
            finally:
                if writer_connection is not None:
                    writer_connection.close()
        _safe_append_service_log(
            service_logs,
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
    configure_sqlite_runtime(connection, enable_wal=True)
    return create_app(connection, database_path=database_path)


def main() -> None:
    """Start the local API service."""

    database_path = os.environ.get("ODM_DATABASE_PATH", DEFAULT_DATABASE_PATH)
    connection = sqlite3.connect(database_path)
    configure_sqlite_runtime(connection)
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


def _safe_get_secret(
    settings: SettingsRepository,
    key: str,
) -> tuple[str | None, Exception | None]:
    try:
        return (settings.get_secret(key), None)
    except (ValueError, OSError) as exc:
        return (None, exc)


def _settings_writer(
    fallback_settings: SettingsRepository,
    *,
    database_path: str | None,
    protector: Any,
) -> tuple[SettingsRepository, sqlite3.Connection | None]:
    if not database_path or database_path == ":memory:":
        return (fallback_settings, None)
    connection = sqlite3.connect(database_path, timeout=30)
    configure_sqlite_runtime(connection)
    return (SettingsRepository(connection, protector), connection)


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


def _ensure_quote_stream_universe(
    connection: sqlite3.Connection,
    *,
    account: str,
    password: str,
    force_discovery: bool,
    service_state: ServiceStateRepository,
    service_logs: ServiceLogRepository,
    log_category: str = "quote_stream",
    update_quote_stream_message: bool = True,
) -> tuple[bool, str, bool]:
    """Ensure realtime subscriptions have an active contract universe."""

    existing_active_options = _active_option_count(connection)
    if existing_active_options > 0 and not force_discovery:
        message = f"使用已有合约列表启动实时订阅，当前 {existing_active_options} 个可订阅期权合约。"
        if update_quote_stream_message:
            service_state.set_value("quote_stream.message", message)
        service_logs.append(
            level="info",
            category=log_category,
            message="实时订阅使用已有合约列表启动。",
            context={"active_options": existing_active_options},
        )
        return (True, message, False)
    reason = (
        "手动刷新合约列表"
        if force_discovery
        else "启动前刷新合约列表"
        if existing_active_options > 0
        else "首次启动前初始化合约列表"
    )
    if update_quote_stream_message:
        service_state.set_value("quote_stream.message", f"实时订阅启动前正在{reason}。")
    service_logs.append(
        level="info",
        category=log_category,
        message="实时订阅启动前初始化合约列表。",
        context={"force_discovery": force_discovery},
    )
    api = None
    try:
        api = create_tqsdk_api_with_retries(account, password)
        discovery = _discover_and_persist_market(api, connection)
    except Exception as exc:
        message = f"实时订阅启动前合约初始化失败：{type(exc).__name__}: {exc}"
        if update_quote_stream_message:
            service_state.set_value("quote_stream.message", message)
        service_logs.append(
            level="error",
            category=log_category,
            message="实时订阅启动前合约初始化失败。",
            context={"error_type": type(exc).__name__},
        )
        return (False, message, True)
    finally:
        if api is not None:
            close = getattr(api, "close", None)
            if callable(close):
                close()
    active_options = _active_option_count(connection)
    if active_options < 1:
        message = "实时订阅启动前未发现可订阅期权合约，请检查 TQSDK 合约查询权限或交易所数据。"
        if update_quote_stream_message:
            service_state.set_value("quote_stream.message", message)
        service_logs.append(
            level="warning",
            category=log_category,
            message="实时订阅启动前未发现可订阅期权合约。",
            context={
                "discovery_returned": discovery is not None,
                "option_symbol_count": getattr(discovery, "option_symbol_count", 0)
                if discovery is not None
                else 0,
            },
        )
        return (False, message, True)
    service_logs.append(
        level="info",
        category=log_category,
        message="实时订阅启动前合约列表初始化完成。",
        context={
            "active_options": active_options,
            "discovered_options": getattr(discovery, "option_symbol_count", 0)
            if discovery is not None
            else None,
            "discovered_underlyings": getattr(discovery, "underlying_count", 0)
            if discovery is not None
            else None,
        },
    )
    return (True, f"已初始化 {active_options} 个可订阅期权合约。", True)


def _run_quote_stream_start(
    *,
    connection: sqlite3.Connection,
    account: str,
    password: str,
    force_discovery: bool,
    service_state: ServiceStateRepository,
    service_logs: ServiceLogRepository,
    quote_stream_processes: dict[str, list[Any]],
    metrics_worker_processes: dict[str, list[Any]],
    database_path: str,
    report_dir: Path,
    worker_count: int,
    quote_shard_size: int,
    kline_batch_size: int,
    kline_data_length: int,
    prioritize_near_expiry: bool,
    near_expiry_months: int,
    contract_months: str,
    min_days_to_expiry: int,
    moneyness_filter: str,
    moneyness_recalc_seconds: int,
    kline_subscription_timeout_seconds: int,
    max_symbols: int | None,
    metrics_dirty_min_interval_seconds: int,
    underlying_chain_dirty_interval_seconds: int,
    quote_stream_lock: threading.Lock,
) -> None:
    try:
        universe_ready, universe_message, discovery_ran = _ensure_quote_stream_universe(
            connection,
            account=account,
            password=password,
            force_discovery=force_discovery,
            service_state=service_state,
            service_logs=service_logs,
        )
        if not universe_ready:
            with quote_stream_lock:
                _store_quote_stream_expected_counts(service_state, None)
                _safe_set_service_value(
                    service_state,
                    "quote_stream.status",
                    QuoteStreamState.BLOCKED.value,
                )
                _safe_set_service_value(service_state, "quote_stream.running", "false")
                _safe_set_service_value(
                    service_state,
                    "quote_stream.finished_at",
                    datetime.now(UTC).isoformat(),
                )
                _safe_set_service_value(service_state, "quote_stream.message", universe_message)
                _safe_set_service_value(service_state, "quote_stream.pids", "[]")
            return
        expected_counts = _calculate_expected_subscription_counts(
            connection,
            worker_count=worker_count,
            contract_months=contract_months,
            min_days_to_expiry=min_days_to_expiry,
            max_symbols=max_symbols,
            moneyness_filter=moneyness_filter,
        )
        with quote_stream_lock:
            _store_quote_stream_expected_counts(service_state, expected_counts)
        processes = _start_quote_stream_processes(
            database_path=database_path,
            report_dir=report_dir,
            worker_count=worker_count,
            quote_shard_size=quote_shard_size,
            kline_batch_size=kline_batch_size,
            kline_data_length=kline_data_length,
            prioritize_near_expiry=prioritize_near_expiry,
            near_expiry_months=near_expiry_months,
            contract_months=contract_months,
            min_days_to_expiry=min_days_to_expiry,
            moneyness_filter=moneyness_filter,
            moneyness_recalc_seconds=moneyness_recalc_seconds,
            kline_subscription_timeout_seconds=kline_subscription_timeout_seconds,
            max_symbols=max_symbols,
            discover=False,
            metrics_dirty_min_interval_seconds=metrics_dirty_min_interval_seconds,
            underlying_chain_dirty_interval_seconds=underlying_chain_dirty_interval_seconds,
        )
        metrics_processes = _start_metrics_worker_processes(
            database_path=database_path,
            report_dir=report_dir,
            min_interval_seconds=metrics_dirty_min_interval_seconds,
        )
    except Exception as exc:
        message = f"实时订阅后台启动失败：{type(exc).__name__}: {exc}"
        with quote_stream_lock:
            _store_quote_stream_expected_counts(service_state, None)
            _safe_set_service_value(
                service_state,
                "quote_stream.status",
                QuoteStreamState.BLOCKED.value,
            )
            _safe_set_service_value(service_state, "quote_stream.running", "false")
            _safe_set_service_value(
                service_state,
                "quote_stream.finished_at",
                datetime.now(UTC).isoformat(),
            )
            _safe_set_service_value(service_state, "quote_stream.message", message)
            _safe_set_service_value(service_state, "quote_stream.pids", "[]")
        _safe_append_service_log(
            service_logs,
            level="error",
            category="quote_stream",
            message="实时订阅后台启动失败。",
            context={"error_type": type(exc).__name__},
        )
        return
    started_at = datetime.now(UTC).isoformat()
    start_message = (
        "已初始化合约列表并启动实时订阅 worker。"
        if discovery_ran
        else "实时订阅 worker 已启动。"
    )
    with quote_stream_lock:
        quote_stream_processes["processes"] = processes
        metrics_worker_processes["processes"] = metrics_processes
        _safe_set_service_value(
            service_state,
            "quote_stream.status",
            QuoteStreamState.RUNNING.value,
        )
        _safe_set_service_value(service_state, "quote_stream.running", "true")
        _safe_set_service_value(service_state, "quote_stream.started_at", started_at)
        _safe_set_service_value(service_state, "quote_stream.finished_at", None)
        _safe_set_service_value(service_state, "quote_stream.worker_count", str(worker_count))
        _safe_set_service_value(service_state, "quote_stream.contract_months", contract_months)
        _safe_set_service_value(
            service_state,
            "quote_stream.min_days_to_expiry",
            str(min_days_to_expiry),
        )
        _safe_set_service_value(
            service_state,
            "quote_stream.max_symbols",
            str(max_symbols) if max_symbols is not None else None,
        )
        _safe_set_service_value(
            service_state,
            "quote_stream.pids",
            json.dumps([int(process.pid) for process in processes]),
        )
        _safe_set_service_value(service_state, "quote_stream.report_dir", str(report_dir))
        _safe_set_service_value(service_state, "quote_stream.message", start_message)
        _safe_set_service_value(service_state, "metrics_worker.running", "true")
        _safe_set_service_value(service_state, "metrics_worker.started_at", started_at)
        _safe_set_service_value(service_state, "metrics_worker.finished_at", None)
        _safe_set_service_value(
            service_state,
            "metrics_worker.pids",
            json.dumps([int(process.pid) for process in metrics_processes]),
        )
        _safe_set_service_value(service_state, "metrics_worker.report_dir", str(report_dir))
        _safe_set_service_value(service_state, "metrics_worker.message", "指标刷新 worker 已启动。")
    _safe_append_service_log(
        service_logs,
        level="info",
        category="quote_stream",
        message=start_message,
        context={
            "worker_count": worker_count,
            "quote_shard_size": quote_shard_size,
            "kline_data_length": kline_data_length,
            "prioritize_near_expiry": prioritize_near_expiry,
            "near_expiry_months": near_expiry_months,
            "contract_months": contract_months,
            "min_days_to_expiry": min_days_to_expiry,
            "moneyness_filter": moneyness_filter,
            "moneyness_recalc_seconds": moneyness_recalc_seconds,
            "kline_subscription_timeout_seconds": kline_subscription_timeout_seconds,
            "max_symbols_configured": max_symbols is not None,
            "discover_requested": force_discovery,
            "discovery_ran": discovery_ran,
            "report_dir": str(report_dir),
            "metrics_min_interval_seconds": metrics_dirty_min_interval_seconds,
            "underlying_chain_interval_seconds": underlying_chain_dirty_interval_seconds,
        },
    )


def _run_contract_manager(
    *,
    connection: sqlite3.Connection,
    account: str,
    password: str,
    service_state: ServiceStateRepository,
    service_logs: ServiceLogRepository,
    stop_event: threading.Event,
    refresh_interval_seconds: int,
) -> None:
    while not stop_event.is_set():
        now = datetime.now(UTC).isoformat()
        service_state.set_value("contract_manager.running", "true")
        service_state.set_value("contract_manager.refresh_running", "true")
        service_state.set_value("contract_manager.last_heartbeat_at", now)
        service_state.set_value("contract_manager.message", "合约管理器正在刷新合约列表。")
        service_state.set_value("quote_stream.contract_discovery_running", "true")
        try:
            universe_ready, universe_message, _discovery_ran = _ensure_quote_stream_universe(
                connection,
                account=account,
                password=password,
                force_discovery=True,
                service_state=service_state,
                service_logs=service_logs,
                log_category="contract_manager",
                update_quote_stream_message=False,
            )
            active_options = _active_option_count(connection)
            success = universe_ready and active_options > 0
            message = (
                f"合约管理器正常运行，当前 {active_options} 个可订阅期权合约。"
                if success
                else universe_message
            )
            finished_at = datetime.now(UTC).isoformat()
            service_state.set_value("contract_manager.refresh_running", "false")
            service_state.set_value("contract_manager.last_heartbeat_at", finished_at)
            service_state.set_value("contract_manager.message", message)
            service_state.set_value("quote_stream.contract_discovery_running", "false")
            service_state.set_value("quote_stream.contract_list_ready", "true" if success else "false")
            service_state.set_value("quote_stream.message", message)
            service_state.set_value(
                "quote_stream.contract_list_ready_at",
                finished_at if success else None,
            )
            if success:
                service_state.set_value("contract_manager.last_success_at", finished_at)
            else:
                service_state.set_value("contract_manager.last_error_at", finished_at)
            service_logs.append(
                level="info" if success else "warning",
                category="contract_manager",
                message="合约管理器刷新完成。" if success else "合约管理器刷新未完成。",
                context={"active_options": active_options},
            )
        except Exception as exc:
            failed_at = datetime.now(UTC).isoformat()
            message = f"合约管理器刷新失败：{type(exc).__name__}: {exc}"
            service_state.set_value("contract_manager.refresh_running", "false")
            service_state.set_value("contract_manager.last_heartbeat_at", failed_at)
            service_state.set_value("contract_manager.last_error_at", failed_at)
            service_state.set_value("contract_manager.message", message)
            service_state.set_value("quote_stream.contract_discovery_running", "false")
            service_state.set_value("quote_stream.contract_list_ready", "false")
            service_state.set_value("quote_stream.message", message)
            service_logs.append(
                level="error",
                category="contract_manager",
                message="合约管理器刷新失败。",
                context={"error_type": type(exc).__name__},
            )
        if stop_event.wait(max(1, refresh_interval_seconds)):
            break
    stopped_at = datetime.now(UTC).isoformat()
    service_state.set_value("contract_manager.running", "false")
    service_state.set_value("contract_manager.refresh_running", "false")
    service_state.set_value("contract_manager.stopped_at", stopped_at)
    service_state.set_value("contract_manager.message", "合约管理器已停止。")
    service_state.set_value("quote_stream.contract_discovery_running", "false")
    service_state.set_value("quote_stream.contract_list_ready", "false")


def _active_option_count(connection: sqlite3.Connection) -> int:
    try:
        row = connection.execute(
            """
            SELECT COUNT(*)
            FROM instruments
            WHERE active = 1
              AND option_class IN ('CALL', 'PUT')
            """
        ).fetchone()
    except sqlite3.Error:
        return 0
    if row is None:
        return 0
    return int(row[0])


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
    contract_months: str,
    min_days_to_expiry: int,
    moneyness_filter: str,
    moneyness_recalc_seconds: int,
    kline_subscription_timeout_seconds: int,
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
            "--contract-months",
            contract_months,
            "--min-days-to-expiry",
            str(min_days_to_expiry),
            "--moneyness-filter",
            moneyness_filter,
            "--moneyness-recalc-seconds",
            str(moneyness_recalc_seconds),
            "--kline-subscription-timeout-seconds",
            str(kline_subscription_timeout_seconds),
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
    report_dir = _safe_get_service_value(service_state, "quote_stream.report_dir")
    if not report_dir:
        return
    directory = Path(report_dir)
    worker_count = _int_or_none(
        _safe_get_service_value(service_state, "quote_stream.worker_count")
    ) or 0
    if worker_count:
        for worker_index in range(worker_count):
            path = directory / f"worker-{worker_index:02d}-of-{worker_count:02d}.stop"
            path.write_text("stop requested\n", encoding="utf-8")
    for path in directory.glob("worker-*.stop"):
        path.write_text("stop requested\n", encoding="utf-8")


def _contract_manager_status(
    service_state: ServiceStateRepository,
    worker_ref: dict[str, threading.Thread | None],
    *,
    connection: sqlite3.Connection | None,
    settings: SettingsRepository,
) -> dict[str, Any]:
    thread = worker_ref.get("thread")
    thread_alive = thread is not None and thread.is_alive()
    persisted_running = _bool_service_state(service_state, "contract_manager.running")
    if persisted_running and not thread_alive:
        _safe_set_service_value(service_state, "contract_manager.running", "false")
        _safe_set_service_value(service_state, "contract_manager.refresh_running", "false")
        _safe_set_service_value(
            service_state,
            "contract_manager.message",
            "本地服务重启后，合约管理器未自动接管。",
        )
        persisted_running = False
    running = bool(thread_alive and persisted_running)
    refresh_running = running and _bool_service_state(
        service_state,
        "contract_manager.refresh_running",
    )
    active_option_count = _active_option_count(connection) if connection is not None else 0
    last_success_at = _safe_get_service_value(service_state, "contract_manager.last_success_at")
    healthy = bool(running and active_option_count > 0 and last_success_at)
    status = (
        "refreshing"
        if refresh_running
        else "running"
        if healthy
        else "starting"
        if running
        else "stopped"
    )
    return {
        "status": status,
        "running": running,
        "healthy": healthy,
        "refresh_running": refresh_running,
        "message": _safe_get_service_value(service_state, "contract_manager.message")
        or ("合约管理器正常运行。" if running else "合约管理器未运行。"),
        "active_option_count": active_option_count,
        "refresh_interval_seconds": _positive_int_setting(
            settings.get_value(CONTRACT_MANAGER_REFRESH_INTERVAL_SECONDS_KEY),
            int(DEFAULT_CONTRACT_MANAGER_REFRESH_INTERVAL_SECONDS),
        ),
        "started_at": _safe_get_service_value(service_state, "contract_manager.started_at"),
        "stopped_at": _safe_get_service_value(service_state, "contract_manager.stopped_at"),
        "last_heartbeat_at": _safe_get_service_value(service_state, "contract_manager.last_heartbeat_at"),
        "last_success_at": last_success_at,
        "last_error_at": _safe_get_service_value(service_state, "contract_manager.last_error_at"),
    }


def _quote_stream_status(
    service_state: ServiceStateRepository,
    process_table: dict[str, list[Any]],
    *,
    connection: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    processes = process_table.get("processes", [])
    attached_pids = [int(process.pid) for process in processes if process.poll() is None]
    persisted_pids = _json_int_list(
        _safe_get_service_value(service_state, "quote_stream.pids")
    )
    live_persisted_pids = [
        pid for pid in persisted_pids if pid not in attached_pids and _pid_is_quote_stream(pid)
    ]
    live_pids = attached_pids or live_persisted_pids
    running = bool(live_pids)
    stored_status = _quote_stream_state(service_state)
    persisted_running = _safe_get_service_value(service_state, "quote_stream.running")
    if running:
        if (persisted_running or "false") != "true":
            _safe_set_service_value(
                service_state,
                "quote_stream.status",
                QuoteStreamState.RUNNING.value,
            )
            _safe_set_service_value(service_state, "quote_stream.running", "true")
            _safe_set_service_value(
                service_state,
                "quote_stream.message",
                "实时订阅 worker 仍在运行。",
            )
    elif processes or (persisted_running or "false") == "true":
        process_table["processes"] = []
        stored_status = QuoteStreamState.STOPPED
        _safe_set_service_value(
            service_state,
            "quote_stream.status",
            QuoteStreamState.STOPPED.value,
        )
        _safe_set_service_value(service_state, "quote_stream.running", "false")
        _safe_set_service_value(
            service_state,
            "quote_stream.finished_at",
            datetime.now(UTC).isoformat(),
        )
        _safe_set_service_value(
            service_state,
            "quote_stream.message",
            "实时订阅 worker 已退出。",
        )
    elif stored_status == QuoteStreamState.RUNNING:
        stored_status = QuoteStreamState.STOPPED
        _safe_set_service_value(
            service_state,
            "quote_stream.status",
            QuoteStreamState.STOPPED.value,
        )
    worker_count = _int_or_none(
        _safe_get_service_value(service_state, "quote_stream.worker_count")
    ) or (
        len(live_pids) if live_pids else len(persisted_pids)
    )
    status = QuoteStreamState.RUNNING if running else stored_status
    report_dir = _safe_get_service_value(service_state, "quote_stream.report_dir")
    progress = _quote_stream_progress(
        report_dir,
        running=running,
    )
    contract_months = _safe_get_service_value(
        service_state,
        "quote_stream.contract_months",
    )
    min_days_to_expiry = _int_or_none(
        _safe_get_service_value(service_state, "quote_stream.min_days_to_expiry")
    )
    if (
        (running or status == QuoteStreamState.STARTING)
        and int(progress.get("worker_reports") or 0) == 0
    ):
        progress = _progress_with_cached_expected_counts(
            progress,
            service_state=service_state,
        )
    health = build_realtime_health(
        connection,
        running=running,
        progress=progress,
    )
    active_option_count = _active_option_count(connection) if connection is not None else None
    contract_discovery_running = (
        _safe_get_service_value(service_state, "quote_stream.contract_discovery_running")
        or "false"
    ) == "true"
    contract_list_ready = _bool_service_state(
        service_state,
        "quote_stream.contract_list_ready",
    )
    return {
        "status": status.value,
        "running": running,
        "message": _safe_get_service_value(service_state, "quote_stream.message")
        or ("实时订阅 worker 仍在运行。" if running else "实时订阅未运行。"),
        "worker_count": worker_count,
        "pids": live_pids,
        "report_dir": report_dir,
        "started_at": _safe_get_service_value(service_state, "quote_stream.started_at"),
        "finished_at": _safe_get_service_value(service_state, "quote_stream.finished_at"),
        "progress": progress,
        "health": health,
        "active_option_count": active_option_count,
        "contract_discovery_running": contract_discovery_running,
        "contract_list_ready": contract_list_ready,
        "contract_months": contract_months,
        "min_days_to_expiry": min_days_to_expiry,
    }


def _safe_get_service_value(
    service_state: ServiceStateRepository,
    key: str,
    default: str | None = None,
) -> str | None:
    try:
        return service_state.get_value(key)
    except sqlite3.OperationalError as exc:
        if _is_sqlite_busy(exc):
            return default
        raise


def _quote_stream_state(service_state: ServiceStateRepository) -> QuoteStreamState:
    value = _safe_get_service_value(service_state, "quote_stream.status")
    try:
        return QuoteStreamState(value or QuoteStreamState.STOPPED.value)
    except ValueError:
        return QuoteStreamState.STOPPED


def _calculate_expected_subscription_counts(
    connection: sqlite3.Connection,
    *,
    worker_count: int,
    contract_months: str | None,
    min_days_to_expiry: int | None,
    max_symbols: int | None,
    moneyness_filter: str | None = None,
) -> dict[str, int]:
    contract_month_limit = None
    if (contract_months or "").strip().lower() != "all":
        contract_month_limit = _positive_int_setting(
            contract_months,
            int(DEFAULT_QUOTE_STREAM_CONTRACT_MONTHS),
        )
    return expected_subscription_counts(
        connection,
        worker_count=max(worker_count, 1),
        max_symbols=max_symbols,
        contract_month_limit=contract_month_limit,
        min_days_to_expiry=(
            min_days_to_expiry
            if min_days_to_expiry is not None
            else int(DEFAULT_QUOTE_STREAM_MIN_DAYS_TO_EXPIRY)
        ),
        moneyness_filter=moneyness_filter or DEFAULT_QUOTE_STREAM_MONEYNESS_FILTER,
    )


def _store_quote_stream_expected_counts(
    service_state: ServiceStateRepository,
    counts: dict[str, int] | None,
) -> None:
    for key in ("quote_total", "kline_total", "total_objects"):
        _safe_set_service_value(
            service_state,
            f"quote_stream.expected_{key}",
            str(counts[key]) if counts is not None else None,
        )


def _progress_with_cached_expected_counts(
    progress: dict[str, Any],
    *,
    service_state: ServiceStateRepository,
) -> dict[str, Any]:
    quote_total = _int_or_none(
        _safe_get_service_value(service_state, "quote_stream.expected_quote_total")
    )
    kline_total = _int_or_none(
        _safe_get_service_value(service_state, "quote_stream.expected_kline_total")
    )
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
    enriched["contract_months"] = _safe_get_service_value(
        service_state,
        "quote_stream.contract_months",
    ) or enriched.get("contract_months")
    enriched["min_days_to_expiry"] = (
        _int_or_none(
            _safe_get_service_value(service_state, "quote_stream.min_days_to_expiry")
        )
        or int(DEFAULT_QUOTE_STREAM_MIN_DAYS_TO_EXPIRY)
    )
    return enriched


def _bool_service_state(service_state: ServiceStateRepository, key: str) -> bool:
    return (_safe_get_service_value(service_state, key) or "false").lower() == "true"


def _safe_set_service_value(
    service_state: ServiceStateRepository,
    key: str,
    value: str | None,
) -> bool:
    try:
        service_state.set_value(key, value)
    except (sqlite3.OperationalError, SystemError) as exc:
        if _is_sqlite_retryable(exc):
            return False
        raise
    return True


def _safe_append_service_log(
    service_logs: ServiceLogRepository,
    *,
    level: str,
    category: str,
    message: str,
    context: dict[str, Any] | None = None,
) -> bool:
    try:
        service_logs.append(
            level=level,
            category=category,
            message=message,
            context=context,
        )
    except (sqlite3.OperationalError, SystemError) as exc:
        if _is_sqlite_retryable(exc):
            return False
        raise
    return True


def _safe_record_request(
    service_state: ServiceStateRepository,
    *,
    path: str,
    method: str,
    status_code: int,
    latency_ms: float,
) -> bool:
    try:
        service_state.record_request(
            path=path,
            method=method,
            status_code=status_code,
            latency_ms=latency_ms,
        )
    except (sqlite3.OperationalError, SystemError) as exc:
        if _is_sqlite_retryable(exc):
            return False
        raise
    return True


def _is_sqlite_busy(exc: sqlite3.OperationalError) -> bool:
    message = str(exc).lower()
    return "database is locked" in message or "database is busy" in message


def _is_sqlite_retryable(exc: sqlite3.OperationalError | SystemError) -> bool:
    message = str(exc).lower()
    if isinstance(exc, SystemError):
        return (
            "sqlite3.connection" in message
            or "commit" in message
            or "error return without exception set" in message
        )
    return (
        _is_sqlite_busy(exc)
        or "cannot commit - no transaction is active" in message
        or "cannot rollback - no transaction is active" in message
    )


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
    contract_months = _aggregate_contract_months(progress_rows)
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
    contract_refresh_values = [
        str(row.get("last_contract_refresh_at"))
        for row in progress_rows
        if row.get("last_contract_refresh_at") is not None
    ]
    contract_reconcile_values = [
        str(row.get("last_contract_reconcile_at"))
        for row in progress_rows
        if row.get("last_contract_reconcile_at") is not None
    ]
    moneyness_recalc_values = [
        str(row.get("last_moneyness_recalc_at"))
        for row in progress_rows
        if row.get("last_moneyness_recalc_at") is not None
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
    underlying_progress = _aggregate_underlying_progress(progress_rows)
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
        "tqsdk_notify_count": sum(
            _int_value(row.get("tqsdk_notify_count")) for row in progress_rows
        ),
        "contract_refresh_count": sum(
            _int_value(row.get("contract_refresh_count")) for row in progress_rows
        ),
        "last_contract_refresh_at": max(contract_refresh_values)
        if contract_refresh_values
        else None,
        "last_contract_reconcile_at": max(contract_reconcile_values)
        if contract_reconcile_values
        else None,
        "contract_reconcile_added_quote_count": sum(
            _int_value(row.get("contract_reconcile_added_quote_count"))
            for row in progress_rows
        ),
        "contract_reconcile_removed_quote_count": sum(
            _int_value(row.get("contract_reconcile_removed_quote_count"))
            for row in progress_rows
        ),
        "contract_reconcile_added_kline_count": sum(
            _int_value(row.get("contract_reconcile_added_kline_count"))
            for row in progress_rows
        ),
        "contract_reconcile_removed_kline_count": sum(
            _int_value(row.get("contract_reconcile_removed_kline_count"))
            for row in progress_rows
        ),
        "moneyness_filter": _aggregate_text_value(progress_rows, "moneyness_filter"),
        "moneyness_recalc_seconds": max(
            _int_value(row.get("moneyness_recalc_seconds")) for row in progress_rows
        ),
        "moneyness_recalc_count": sum(
            _int_value(row.get("moneyness_recalc_count")) for row in progress_rows
        ),
        "moneyness_kline_match_count": sum(
            _int_value(row.get("moneyness_kline_match_count")) for row in progress_rows
        ),
        "moneyness_sticky_kline_count": sum(
            _int_value(row.get("moneyness_sticky_kline_count")) for row in progress_rows
        ),
        "moneyness_added_kline_count": sum(
            _int_value(row.get("moneyness_added_kline_count")) for row in progress_rows
        ),
        "moneyness_skipped_out_of_session_count": sum(
            _int_value(row.get("moneyness_skipped_out_of_session_count"))
            for row in progress_rows
        ),
        "moneyness_skipped_missing_price_count": sum(
            _int_value(row.get("moneyness_skipped_missing_price_count"))
            for row in progress_rows
        ),
        "last_moneyness_recalc_at": max(moneyness_recalc_values)
        if moneyness_recalc_values
        else None,
        "current_kline_symbol": _aggregate_text_value(
            progress_rows,
            "current_kline_symbol",
        ),
        "current_kline_started_at": max(
            str(row.get("current_kline_started_at"))
            for row in progress_rows
            if row.get("current_kline_started_at") is not None
        )
        if any(row.get("current_kline_started_at") is not None for row in progress_rows)
        else None,
        "kline_subscription_timeout_seconds": max(
            _int_value(row.get("kline_subscription_timeout_seconds"))
            for row in progress_rows
        ),
        "kline_subscription_timeout_count": sum(
            _int_value(row.get("kline_subscription_timeout_count"))
            for row in progress_rows
        ),
        "kline_subscription_error_count": sum(
            _int_value(row.get("kline_subscription_error_count"))
            for row in progress_rows
        ),
        "last_kline_subscription_error_symbol": _aggregate_text_value(
            progress_rows,
            "last_kline_subscription_error_symbol",
        ),
        "last_kline_subscription_error": _aggregate_text_value(
            progress_rows,
            "last_kline_subscription_error",
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
        "worker_index": result.get("worker_index"),
        "worker_count": result.get("worker_count"),
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
        "underlying_progress": result.get("underlying_progress") or {},
        "moneyness_filter": result.get("moneyness_filter"),
        "moneyness_recalc_seconds": _int_value(result.get("moneyness_recalc_seconds")),
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
    }


def _aggregate_underlying_progress(
    rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    aggregated: dict[str, dict[str, Any]] = {}
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


def _aggregate_contract_months(rows: list[dict[str, Any]]) -> str | None:
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


def _aggregate_text_value(rows: list[dict[str, Any]], key: str) -> str | None:
    values = {
        str(row.get(key)).strip()
        for row in rows
        if row.get(key) is not None and str(row.get(key)).strip()
    }
    if not values:
        return None
    return ",".join(sorted(values))


def _contract_months_from_result(result: dict[str, Any]) -> str | None:
    value = result.get("contract_months")
    if value is not None:
        return str(value).strip().lower()
    limit = result.get("contract_month_limit")
    if limit is None:
        return "all"
    parsed = _int_value(limit)
    return str(parsed) if parsed > 0 else None


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
        "moneyness_filter": None,
        "moneyness_recalc_seconds": 0,
        "moneyness_recalc_count": 0,
        "moneyness_kline_match_count": 0,
        "moneyness_sticky_kline_count": 0,
        "moneyness_added_kline_count": 0,
        "moneyness_skipped_out_of_session_count": 0,
        "moneyness_skipped_missing_price_count": 0,
        "last_moneyness_recalc_at": None,
        "current_kline_symbol": None,
        "current_kline_started_at": None,
        "kline_subscription_timeout_seconds": 0,
        "kline_subscription_timeout_count": 0,
        "kline_subscription_error_count": 0,
        "last_kline_subscription_error_symbol": None,
        "last_kline_subscription_error": None,
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


def _non_negative_int_setting(value: str | None, default: int) -> int:
    if value is None or str(value).strip() == "":
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed >= 0 else default


def _bool_setting(value: str | None, default: bool) -> bool:
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _contract_months_setting(value: str | None, default: str) -> str:
    raw = str(value if value is not None else default).strip().lower()
    return raw if raw in {"1", "2", "3", "all"} else default


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
    configure_sqlite_runtime(state_connection)
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
