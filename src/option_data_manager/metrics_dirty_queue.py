"""Dirty queue for asynchronous option metrics refresh."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import sqlite3

from .storage import Migration, apply_migrations


METRICS_DIRTY_QUEUE_MIGRATION = Migration(
    700,
    "create metrics dirty queue",
    (
        """
        CREATE TABLE IF NOT EXISTS metrics_dirty_queue (
            symbol TEXT PRIMARY KEY,
            underlying_symbol TEXT NOT NULL,
            reason TEXT NOT NULL,
            first_dirty_at TEXT NOT NULL,
            last_dirty_at TEXT NOT NULL,
            next_attempt_at TEXT NOT NULL,
            dirty_count INTEGER NOT NULL,
            attempt_count INTEGER NOT NULL,
            status TEXT NOT NULL,
            processing_started_at TEXT,
            last_error_type TEXT,
            last_error_message TEXT,
            updated_at TEXT NOT NULL
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_metrics_dirty_queue_due
        ON metrics_dirty_queue(status, next_attempt_at)
        """,
        """
        CREATE TABLE IF NOT EXISTS metrics_chain_dirty_state (
            underlying_symbol TEXT PRIMARY KEY,
            last_marked_at TEXT NOT NULL
        )
        """,
    ),
)


@dataclass(frozen=True)
class MetricsDirtyTask:
    """One option metrics refresh task claimed by a worker."""

    symbol: str
    underlying_symbol: str
    reason: str
    dirty_count: int
    attempt_count: int


class MetricsDirtyQueueRepository:
    """SQLite-backed queue for coalesced IV/Greeks refresh tasks."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        connection.row_factory = sqlite3.Row
        self._connection = connection
        apply_migrations(connection, (METRICS_DIRTY_QUEUE_MIGRATION,))

    def mark_option_dirty(
        self,
        *,
        symbol: str,
        underlying_symbol: str,
        reason: str,
        dirty_at: str,
    ) -> None:
        """Mark one option as needing metrics refresh, coalescing repeats."""

        self._connection.execute(
            """
            INSERT INTO metrics_dirty_queue (
                symbol,
                underlying_symbol,
                reason,
                first_dirty_at,
                last_dirty_at,
                next_attempt_at,
                dirty_count,
                attempt_count,
                status,
                processing_started_at,
                last_error_type,
                last_error_message,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 1, 0, 'pending', NULL, NULL, NULL, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                underlying_symbol = excluded.underlying_symbol,
                reason = excluded.reason,
                last_dirty_at = excluded.last_dirty_at,
                next_attempt_at = MIN(metrics_dirty_queue.next_attempt_at, excluded.next_attempt_at),
                dirty_count = metrics_dirty_queue.dirty_count + 1,
                status = 'pending',
                processing_started_at = NULL,
                updated_at = excluded.updated_at
            """,
            (
                symbol,
                underlying_symbol,
                reason,
                dirty_at,
                dirty_at,
                dirty_at,
                dirty_at,
            ),
        )
        self._connection.commit()

    def mark_underlying_chain_dirty(
        self,
        *,
        underlying_symbol: str,
        reason: str,
        dirty_at: str,
        min_interval_seconds: int,
    ) -> int:
        """Mark active options for one underlying, limited by chain interval."""

        last_marked = self._connection.execute(
            """
            SELECT last_marked_at
            FROM metrics_chain_dirty_state
            WHERE underlying_symbol = ?
            """,
            (underlying_symbol,),
        ).fetchone()
        if last_marked is not None and not _elapsed(
            str(last_marked["last_marked_at"]),
            dirty_at,
            min_interval_seconds,
        ):
            return 0

        rows = self._connection.execute(
            """
            SELECT symbol
            FROM instruments
            WHERE active = 1
              AND underlying_symbol = ?
              AND option_class IN ('CALL', 'PUT')
            ORDER BY symbol
            """,
            (underlying_symbol,),
        ).fetchall()
        for row in rows:
            self.mark_option_dirty(
                symbol=str(row["symbol"]),
                underlying_symbol=underlying_symbol,
                reason=reason,
                dirty_at=dirty_at,
            )
        self._connection.execute(
            """
            INSERT INTO metrics_chain_dirty_state (underlying_symbol, last_marked_at)
            VALUES (?, ?)
            ON CONFLICT(underlying_symbol) DO UPDATE SET
                last_marked_at = excluded.last_marked_at
            """,
            (underlying_symbol, dirty_at),
        )
        self._connection.commit()
        return len(rows)

    def claim_due_tasks(self, *, now: str, limit: int) -> list[MetricsDirtyTask]:
        """Claim due pending tasks for one worker process."""

        if limit < 1:
            raise ValueError("Metrics dirty queue claim limit must be positive.")
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            rows = self._connection.execute(
                """
                SELECT symbol, underlying_symbol, reason, dirty_count, attempt_count
                FROM metrics_dirty_queue
                WHERE status = 'pending'
                  AND next_attempt_at <= ?
                ORDER BY next_attempt_at, last_dirty_at, symbol
                LIMIT ?
                """,
                (now, limit),
            ).fetchall()
            symbols = [str(row["symbol"]) for row in rows]
            if symbols:
                placeholders = ",".join("?" for _ in symbols)
                self._connection.execute(
                    f"""
                    UPDATE metrics_dirty_queue
                    SET status = 'running',
                        processing_started_at = ?,
                        updated_at = ?
                    WHERE symbol IN ({placeholders})
                    """,
                    (now, now, *symbols),
                )
        except Exception:
            self._connection.rollback()
            raise
        else:
            self._connection.commit()
        return [
            MetricsDirtyTask(
                symbol=str(row["symbol"]),
                underlying_symbol=str(row["underlying_symbol"]),
                reason=str(row["reason"]),
                dirty_count=int(row["dirty_count"]),
                attempt_count=int(row["attempt_count"]),
            )
            for row in rows
        ]

    def postpone_recently_refreshed(
        self,
        *,
        task: MetricsDirtyTask,
        next_attempt_at: str,
        now: str,
    ) -> None:
        """Return a claimed task to pending when its metrics are still fresh."""

        self._connection.execute(
            """
            UPDATE metrics_dirty_queue
            SET status = 'pending',
                processing_started_at = NULL,
                next_attempt_at = ?,
                updated_at = ?
            WHERE symbol = ?
            """,
            (next_attempt_at, now, task.symbol),
        )
        self._connection.commit()

    def complete_task(self, task: MetricsDirtyTask) -> None:
        """Remove a completed dirty task."""

        self._connection.execute(
            "DELETE FROM metrics_dirty_queue WHERE symbol = ?",
            (task.symbol,),
        )
        self._connection.commit()

    def fail_task(
        self,
        task: MetricsDirtyTask,
        *,
        error_type: str,
        message: str,
        retry_at: str,
        now: str,
    ) -> None:
        """Record a failed attempt without losing the coalesced task."""

        self._connection.execute(
            """
            UPDATE metrics_dirty_queue
            SET status = 'pending',
                processing_started_at = NULL,
                attempt_count = attempt_count + 1,
                next_attempt_at = ?,
                last_error_type = ?,
                last_error_message = ?,
                updated_at = ?
            WHERE symbol = ?
            """,
            (retry_at, error_type, message, now, task.symbol),
        )
        self._connection.commit()

    def pending_count(self) -> int:
        """Return pending task count."""

        return int(
            self._connection.execute(
                "SELECT COUNT(*) FROM metrics_dirty_queue WHERE status = 'pending'"
            ).fetchone()[0]
        )


def next_time_after(seconds: int, *, now: str | None = None) -> str:
    """Return an ISO timestamp seconds after now."""

    base = _parse_datetime(now) if now else datetime.now(UTC)
    return (base + timedelta(seconds=max(seconds, 0))).isoformat()


def _elapsed(start: str, end: str, seconds: int) -> bool:
    return (_parse_datetime(end) - _parse_datetime(start)).total_seconds() >= seconds


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
