"""Small service state and API metric repositories."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
import sqlite3
from typing import Any

from .storage import Migration, apply_migrations
from .sqlite_runtime import is_sqlite_retryable


SERVICE_STATE_MIGRATION = Migration(
    920,
    "create service state and api metrics",
    (
        """
        CREATE TABLE IF NOT EXISTS service_state (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS api_request_metrics (
            metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT NOT NULL,
            method TEXT NOT NULL,
            status_code INTEGER NOT NULL,
            latency_ms REAL NOT NULL,
            created_at TEXT NOT NULL
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_api_request_metrics_created
        ON api_request_metrics(created_at)
        """,
    ),
)


SERVICE_LOG_MIGRATION = Migration(
    921,
    "create local service logs",
    (
        """
        CREATE TABLE IF NOT EXISTS service_logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            level TEXT NOT NULL,
            category TEXT NOT NULL,
            message TEXT NOT NULL,
            context_json TEXT NOT NULL DEFAULT '{}'
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_service_logs_created
        ON service_logs(created_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_service_logs_category
        ON service_logs(category, created_at)
        """,
    ),
)


@dataclass(frozen=True)
class ServiceLogRecord:
    """One local application event safe for WebUI diagnostics."""

    log_id: int
    created_at: str
    level: str
    category: str
    message: str
    context_json: str


@dataclass(frozen=True)
class ApiMetricSummary:
    """Aggregate local API request metrics."""

    request_count: int
    error_count: int
    average_latency_ms: float


class ServiceStateRepository:
    """SQLite service state and local metric helper."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        connection.row_factory = sqlite3.Row
        self._connection = connection
        apply_migrations(connection, (SERVICE_STATE_MIGRATION, SERVICE_LOG_MIGRATION))

    def set_value(self, key: str, value: str | None) -> None:
        now = datetime.now(UTC).isoformat()
        try:
            self._connection.execute(
                """
                INSERT INTO service_state (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (key, value, now),
            )
            self._connection.commit()
        except sqlite3.DatabaseError:
            self._connection.rollback()
            raise

    def get_value(self, key: str) -> str | None:
        row = self._connection.execute(
            "SELECT value FROM service_state WHERE key = ?",
            (key,),
        ).fetchone()
        return None if row is None else row["value"]

    def record_request(
        self,
        *,
        path: str,
        method: str,
        status_code: int,
        latency_ms: float,
    ) -> None:
        try:
            self._connection.execute(
                """
                INSERT INTO api_request_metrics (
                    path, method, status_code, latency_ms, created_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    path,
                    method,
                    status_code,
                    latency_ms,
                    datetime.now(UTC).isoformat(),
                ),
            )
            self._connection.commit()
        except sqlite3.DatabaseError:
            self._connection.rollback()
            raise

    def cleanup_request_metrics(
        self,
        *,
        retention_days: int,
        max_rows: int,
        now: datetime | None = None,
    ) -> dict[str, int]:
        """Delete old API metric rows by age and total row cap."""

        cutoff = (now or datetime.now(UTC)) - timedelta(days=max(0, retention_days))
        before = self._count_rows("api_request_metrics")
        deleted_by_age = self._connection.execute(
            "DELETE FROM api_request_metrics WHERE created_at < ?",
            (cutoff.isoformat(),),
        ).rowcount
        deleted_by_cap = 0
        if max_rows > 0:
            deleted_by_cap = self._connection.execute(
                """
                DELETE FROM api_request_metrics
                WHERE metric_id NOT IN (
                    SELECT metric_id
                    FROM api_request_metrics
                    ORDER BY metric_id DESC
                    LIMIT ?
                )
                """,
                (max_rows,),
            ).rowcount
        self._connection.commit()
        after = self._count_rows("api_request_metrics")
        return {
            "before": before,
            "after": after,
            "deleted_by_age": max(0, int(deleted_by_age or 0)),
            "deleted_by_cap": max(0, int(deleted_by_cap or 0)),
        }

    def api_summary(self) -> ApiMetricSummary:
        row = self._connection.execute(
            """
            SELECT
                COUNT(*) AS request_count,
                SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS error_count,
                AVG(latency_ms) AS average_latency_ms
            FROM api_request_metrics
            """
        ).fetchone()
        return ApiMetricSummary(
            request_count=int(row["request_count"] or 0),
            error_count=int(row["error_count"] or 0),
            average_latency_ms=float(row["average_latency_ms"] or 0.0),
        )

    def _count_rows(self, table: str) -> int:
        return int(self._connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


class ServiceLogRepository:
    """SQLite-backed local service log repository."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        connection.row_factory = sqlite3.Row
        self._connection = connection
        apply_migrations(connection, (SERVICE_STATE_MIGRATION, SERVICE_LOG_MIGRATION))

    def append(
        self,
        *,
        level: str,
        category: str,
        message: str,
        context: dict[str, Any] | None = None,
    ) -> ServiceLogRecord:
        """Persist one safe diagnostic event and return it."""

        normalized_level = level.strip().lower() or "info"
        if normalized_level not in {"debug", "info", "warning", "error"}:
            raise ValueError("Unsupported service log level.")
        cleaned_category = _required_text(category, "Service log category")
        cleaned_message = _required_text(message, "Service log message")
        created_at = datetime.now(UTC).isoformat()
        try:
            cursor = self._connection.execute(
                """
                INSERT INTO service_logs (
                    created_at, level, category, message, context_json
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    created_at,
                    normalized_level,
                    cleaned_category,
                    cleaned_message,
                    _context_json(context),
                ),
            )
            self._connection.commit()
        except sqlite3.DatabaseError as exc:
            self._connection.rollback()
            if is_sqlite_retryable(exc):
                return ServiceLogRecord(
                    log_id=0,
                    created_at=created_at,
                    level=normalized_level,
                    category=cleaned_category,
                    message=cleaned_message,
                    context_json=_context_json(context),
                )
            raise
        return ServiceLogRecord(
            log_id=int(cursor.lastrowid),
            created_at=created_at,
            level=normalized_level,
            category=cleaned_category,
            message=cleaned_message,
            context_json=_context_json(context),
        )

    def list_logs(
        self,
        *,
        limit: int = 100,
        category: str | None = None,
    ) -> list[ServiceLogRecord]:
        """Return recent local service logs newest first."""

        safe_limit = max(1, min(limit, 500))
        if category:
            rows = self._connection.execute(
                """
                SELECT log_id, created_at, level, category, message, context_json
                FROM service_logs
                WHERE category = ?
                ORDER BY log_id DESC
                LIMIT ?
                """,
                (category, safe_limit),
            ).fetchall()
        else:
            rows = self._connection.execute(
                """
                SELECT log_id, created_at, level, category, message, context_json
                FROM service_logs
                ORDER BY log_id DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
        return [ServiceLogRecord(**dict(row)) for row in rows]

    def cleanup_logs(
        self,
        *,
        retention_days: int,
        max_rows: int,
        now: datetime | None = None,
    ) -> dict[str, int]:
        """Delete old service log rows by age and total row cap."""

        cutoff = (now or datetime.now(UTC)) - timedelta(days=max(0, retention_days))
        before = self._count_rows("service_logs")
        deleted_by_age = self._connection.execute(
            "DELETE FROM service_logs WHERE created_at < ?",
            (cutoff.isoformat(),),
        ).rowcount
        deleted_by_cap = 0
        if max_rows > 0:
            deleted_by_cap = self._connection.execute(
                """
                DELETE FROM service_logs
                WHERE log_id NOT IN (
                    SELECT log_id
                    FROM service_logs
                    ORDER BY log_id DESC
                    LIMIT ?
                )
                """,
                (max_rows,),
            ).rowcount
        self._connection.commit()
        after = self._count_rows("service_logs")
        return {
            "before": before,
            "after": after,
            "deleted_by_age": max(0, int(deleted_by_age or 0)),
            "deleted_by_cap": max(0, int(deleted_by_cap or 0)),
        }

    def _count_rows(self, table: str) -> int:
        return int(self._connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _context_json(context: dict[str, Any] | None) -> str:
    return json.dumps(context or {}, ensure_ascii=False, sort_keys=True, default=str)


def _required_text(value: str, label: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{label} must not be empty.")
    return cleaned
