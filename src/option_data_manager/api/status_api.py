"""Read-only FastAPI endpoints for current acquisition status."""

from __future__ import annotations

from pathlib import Path
import os
import sqlite3
from typing import Any

from fastapi import FastAPI, Query
from pydantic import BaseModel

from option_data_manager.acquisition import AcquisitionRepository
from option_data_manager.klines import KlineRepository
from option_data_manager.option_metrics import OptionMetricsRepository
from option_data_manager.quotes import QuoteRepository


DEFAULT_DATABASE_PATH = "data/option-data-current.sqlite3"


class ApiIndex(BaseModel):
    """Root response for humans opening the local API."""

    service: str
    docs: str
    health: str
    status: str


class HealthResponse(BaseModel):
    """Simple health response."""

    status: str
    database_connected: bool


class CurrentSliceCounts(BaseModel):
    """Current table counts for dashboard/API consumers."""

    quote_rows: int
    kline_rows: int
    metrics_rows: int
    acquisition_runs: int
    acquisition_errors: int


class AcquisitionRunResponse(BaseModel):
    """Safe acquisition run response."""

    run_id: int
    started_at: str
    finished_at: str | None
    trigger: str
    status: str
    symbols_discovered: int
    quotes_written: int
    kline_rows_written: int
    metrics_written: int
    error_count: int
    message: str | None


class AcquisitionErrorResponse(BaseModel):
    """Safe acquisition error response."""

    error_id: int
    run_id: int | None
    symbol: str | None
    stage: str
    error_type: str
    message: str
    created_at: str
    retryable: bool


class StatusResponse(BaseModel):
    """Current acquisition health summary."""

    status: str
    database_path: str | None
    counts: CurrentSliceCounts
    latest_run: AcquisitionRunResponse | None


def create_status_app(
    connection: sqlite3.Connection,
    *,
    database_path: str | None = None,
) -> FastAPI:
    """Create a read-only status API bound to one SQLite connection."""

    connection.row_factory = sqlite3.Row
    _ensure_known_tables(connection)
    app = FastAPI(title="Option Data Manager API")

    @app.get("/", response_model=ApiIndex)
    def index() -> ApiIndex:
        return ApiIndex(
            service="Option Data Manager",
            docs="/docs",
            health="/api/health",
            status="/api/status",
        )

    @app.get("/api/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(status="ok", database_connected=True)

    @app.get("/api/status", response_model=StatusResponse)
    def status() -> StatusResponse:
        return _status_response(connection, database_path=database_path)

    @app.get("/api/acquisition-runs", response_model=list[AcquisitionRunResponse])
    def acquisition_runs(
        limit: int = Query(default=20, ge=1, le=200),
    ) -> list[AcquisitionRunResponse]:
        repository = AcquisitionRepository(connection)
        return [
            AcquisitionRunResponse(**run.__dict__)
            for run in repository.list_runs(limit=limit)
        ]

    @app.get("/api/acquisition-errors", response_model=list[AcquisitionErrorResponse])
    def acquisition_errors(
        run_id: int | None = None,
    ) -> list[AcquisitionErrorResponse]:
        if run_id is None:
            rows = connection.execute(
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
                LIMIT 200
                """
            ).fetchall()
            return [_error_response_from_row(row) for row in rows]

        repository = AcquisitionRepository(connection)
        return [
            AcquisitionErrorResponse(
                error_id=error.error_id,
                run_id=error.run_id,
                symbol=error.symbol,
                stage=error.stage,
                error_type=error.error_type,
                message=error.message,
                created_at=error.created_at,
                retryable=error.retryable,
            )
            for error in repository.list_errors_for_run(run_id)
        ]

    return app


def create_app_from_database() -> FastAPI:
    """ASGI factory for local uvicorn/FastAPI inspection."""

    database_path = os.environ.get("ODM_DATABASE_PATH", DEFAULT_DATABASE_PATH)
    connection = sqlite3.connect(database_path, check_same_thread=False)
    return create_status_app(connection, database_path=database_path)


def _ensure_known_tables(connection: sqlite3.Connection) -> None:
    QuoteRepository(connection)
    KlineRepository(connection)
    OptionMetricsRepository(connection)
    AcquisitionRepository(connection)


def _status_response(
    connection: sqlite3.Connection,
    *,
    database_path: str | None,
) -> StatusResponse:
    latest_run = AcquisitionRepository(connection).list_runs(limit=1)
    response_status = "ok"
    if latest_run:
        if latest_run[0].status == "failed":
            response_status = "failed"
        elif latest_run[0].status == "partial_failure" or latest_run[0].error_count:
            response_status = "partial_failure"

    return StatusResponse(
        status=response_status,
        database_path=database_path,
        counts=CurrentSliceCounts(
            quote_rows=_count_rows(connection, "quote_current"),
            kline_rows=_count_rows(connection, "kline_20d_current"),
            metrics_rows=_count_rows(connection, "option_source_metrics_current"),
            acquisition_runs=_count_rows(connection, "acquisition_runs"),
            acquisition_errors=_count_rows(connection, "acquisition_errors"),
        ),
        latest_run=(
            AcquisitionRunResponse(**latest_run[0].__dict__)
            if latest_run
            else None
        ),
    )


def _count_rows(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])


def _error_response_from_row(row: sqlite3.Row) -> AcquisitionErrorResponse:
    data: dict[str, Any] = dict(row)
    data["retryable"] = bool(data["retryable"])
    return AcquisitionErrorResponse(**data)
