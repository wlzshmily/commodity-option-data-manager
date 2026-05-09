"""Unified local FastAPI application."""

from __future__ import annotations

from datetime import UTC, datetime
import os
from pathlib import Path
import sqlite3
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
from option_data_manager.webui.read_model import WebuiReadModel


DEFAULT_DATABASE_PATH = "data/option-data-current.sqlite3"
API_AUTH_REQUIRED_KEY = "api.auth_required"
API_BIND_KEY = "api.bind"
API_PORT_KEY = "api.port"
REFRESH_INTERVAL_KEY = "collection.refresh_interval_seconds"
OPTION_BATCH_SIZE_KEY = "collection.option_batch_size"
WAIT_CYCLES_KEY = "collection.wait_cycles"
DEFAULT_BACKGROUND_MAX_BATCHES = "100"


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
    if (service_state.get_value("collection.refresh_running") or "false") == "true":
        service_state.set_value("collection.refresh_running", "false")
        service_state.set_value(
            "collection.refresh_message",
            "Refresh stopped because the local service restarted.",
        )

    app = FastAPI(title="Option Data Manager API")

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
        return [dict(row) for row in rows]

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
