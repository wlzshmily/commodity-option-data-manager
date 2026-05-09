"""Acquisition run and error persistence."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import json
import sqlite3
from typing import Any

from .source_values import json_safe
from .storage import Migration, apply_migrations


ACQUISITION_MIGRATION = Migration(
    500,
    "create acquisition runs and errors",
    (
        """
        CREATE TABLE IF NOT EXISTS acquisition_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            trigger TEXT NOT NULL,
            status TEXT NOT NULL,
            symbols_discovered INTEGER NOT NULL DEFAULT 0,
            quotes_written INTEGER NOT NULL DEFAULT 0,
            kline_rows_written INTEGER NOT NULL DEFAULT 0,
            metrics_written INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            message TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS acquisition_errors (
            error_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            symbol TEXT,
            stage TEXT NOT NULL,
            error_type TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL,
            retryable INTEGER NOT NULL CHECK (retryable IN (0, 1)),
            raw_context_json TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES acquisition_runs(run_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_acquisition_errors_run_id
        ON acquisition_errors(run_id)
        """,
    ),
)


RUN_STATUSES = frozenset(
    {
        "running",
        "success",
        "partial_source_unavailable",
        "partial_failure",
        "failed",
    }
)


@dataclass(frozen=True)
class AcquisitionRunRecord:
    """One acquisition refresh cycle."""

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


@dataclass(frozen=True)
class AcquisitionErrorRecord:
    """One acquisition failure or warning."""

    error_id: int
    run_id: int | None
    symbol: str | None
    stage: str
    error_type: str
    message: str
    created_at: str
    retryable: bool
    raw_context_json: str


class AcquisitionRepository:
    """SQLite repository for acquisition run evidence."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        connection.row_factory = sqlite3.Row
        self._connection = connection
        apply_migrations(connection, (ACQUISITION_MIGRATION,))

    def start_run(
        self,
        *,
        trigger: str,
        started_at: str | None = None,
        message: str | None = None,
    ) -> AcquisitionRunRecord:
        """Create a running acquisition record."""

        cleaned_trigger = _required_text(trigger, "Acquisition trigger")
        actual_started_at = started_at or datetime.now(UTC).isoformat()
        cursor = self._connection.execute(
            """
            INSERT INTO acquisition_runs (
                started_at,
                trigger,
                status,
                message
            )
            VALUES (?, ?, 'running', ?)
            """,
            (actual_started_at, cleaned_trigger, message),
        )
        self._connection.commit()
        return self.get_run(int(cursor.lastrowid))  # type: ignore[return-value]

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        finished_at: str | None = None,
        symbols_discovered: int = 0,
        quotes_written: int = 0,
        kline_rows_written: int = 0,
        metrics_written: int = 0,
        error_count: int = 0,
        message: str | None = None,
    ) -> AcquisitionRunRecord:
        """Mark a run as finished with explicit counters."""

        _validate_run_status(status)
        if status == "running":
            raise ValueError("Finished acquisition run status cannot be running.")
        actual_finished_at = finished_at or datetime.now(UTC).isoformat()
        counters = (
            symbols_discovered,
            quotes_written,
            kline_rows_written,
            metrics_written,
            error_count,
        )
        if any(counter < 0 for counter in counters):
            raise ValueError("Acquisition counters must not be negative.")

        self._connection.execute(
            """
            UPDATE acquisition_runs
            SET
                finished_at = ?,
                status = ?,
                symbols_discovered = ?,
                quotes_written = ?,
                kline_rows_written = ?,
                metrics_written = ?,
                error_count = ?,
                message = ?
            WHERE run_id = ?
            """,
            (
                actual_finished_at,
                status,
                symbols_discovered,
                quotes_written,
                kline_rows_written,
                metrics_written,
                error_count,
                message,
                run_id,
            ),
        )
        self._connection.commit()
        record = self.get_run(run_id)
        if record is None:
            raise ValueError("Acquisition run does not exist.")
        return record

    def get_run(self, run_id: int) -> AcquisitionRunRecord | None:
        """Return one acquisition run by id."""

        row = self._connection.execute(
            """
            SELECT
                run_id,
                started_at,
                finished_at,
                trigger,
                status,
                symbols_discovered,
                quotes_written,
                kline_rows_written,
                metrics_written,
                error_count,
                message
            FROM acquisition_runs
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
        if row is None:
            return None
        return AcquisitionRunRecord(**dict(row))

    def list_runs(self, *, limit: int = 50) -> list[AcquisitionRunRecord]:
        """Return latest acquisition runs."""

        if limit < 1:
            raise ValueError("Acquisition run list limit must be positive.")
        rows = self._connection.execute(
            """
            SELECT
                run_id,
                started_at,
                finished_at,
                trigger,
                status,
                symbols_discovered,
                quotes_written,
                kline_rows_written,
                metrics_written,
                error_count,
                message
            FROM acquisition_runs
            ORDER BY run_id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [AcquisitionRunRecord(**dict(row)) for row in rows]

    def finish_stale_running_runs(
        self,
        *,
        started_before: str,
        finished_at: str | None = None,
        message: str = "Run marked failed because the worker stopped before finishing.",
    ) -> int:
        """Mark old running records as failed after interrupted workers."""

        cutoff = _required_text(started_before, "Stale running cutoff")
        actual_finished_at = finished_at or datetime.now(UTC).isoformat()
        cursor = self._connection.execute(
            """
            UPDATE acquisition_runs
            SET
                finished_at = ?,
                status = 'failed',
                error_count = CASE WHEN error_count > 0 THEN error_count ELSE 1 END,
                message = ?
            WHERE status = 'running'
              AND started_at < ?
            """,
            (actual_finished_at, message, cutoff),
        )
        self._connection.commit()
        return int(cursor.rowcount)

    def record_error(
        self,
        *,
        stage: str,
        error_type: str,
        message: str,
        run_id: int | None = None,
        symbol: str | None = None,
        created_at: str | None = None,
        retryable: bool = True,
        context: dict[str, Any] | None = None,
    ) -> AcquisitionErrorRecord:
        """Record one acquisition error."""

        payload = json.dumps(
            json_safe(context or {}),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        cursor = self._connection.execute(
            """
            INSERT INTO acquisition_errors (
                run_id,
                symbol,
                stage,
                error_type,
                message,
                created_at,
                retryable,
                raw_context_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                symbol.strip() if symbol is not None and symbol.strip() else None,
                _required_text(stage, "Acquisition error stage"),
                _required_text(error_type, "Acquisition error type"),
                _required_text(message, "Acquisition error message"),
                created_at or datetime.now(UTC).isoformat(),
                1 if retryable else 0,
                payload,
            ),
        )
        self._connection.commit()
        return self.get_error(int(cursor.lastrowid))  # type: ignore[return-value]

    def get_error(self, error_id: int) -> AcquisitionErrorRecord | None:
        """Return one acquisition error by id."""

        row = self._connection.execute(
            """
            SELECT
                error_id,
                run_id,
                symbol,
                stage,
                error_type,
                message,
                created_at,
                retryable,
                raw_context_json
            FROM acquisition_errors
            WHERE error_id = ?
            """,
            (error_id,),
        ).fetchone()
        if row is None:
            return None
        return _error_from_row(row)

    def list_errors_for_run(self, run_id: int) -> list[AcquisitionErrorRecord]:
        """Return errors associated with one run."""

        rows = self._connection.execute(
            """
            SELECT
                error_id,
                run_id,
                symbol,
                stage,
                error_type,
                message,
                created_at,
                retryable,
                raw_context_json
            FROM acquisition_errors
            WHERE run_id = ?
            ORDER BY error_id
            """,
            (run_id,),
        ).fetchall()
        return [_error_from_row(row) for row in rows]


def _error_from_row(row: sqlite3.Row) -> AcquisitionErrorRecord:
    data = dict(row)
    data["retryable"] = bool(data["retryable"])
    return AcquisitionErrorRecord(**data)


def _validate_run_status(status: str) -> None:
    if status not in RUN_STATUSES:
        raise ValueError("Unsupported acquisition run status.")


def _required_text(value: str, field_name: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{field_name} must not be empty.")
    return cleaned
