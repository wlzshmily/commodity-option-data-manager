"""Small service state and API metric repositories."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import sqlite3

from .storage import Migration, apply_migrations


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
        apply_migrations(connection, (SERVICE_STATE_MIGRATION,))

    def set_value(self, key: str, value: str | None) -> None:
        now = datetime.now(UTC).isoformat()
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

