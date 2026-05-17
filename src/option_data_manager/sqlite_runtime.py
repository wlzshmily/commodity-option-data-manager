"""Runtime SQLite connection tuning and retry helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import sqlite3
import threading
import time
from typing import Callable, TypeVar


DEFAULT_BUSY_TIMEOUT_MS = 30_000
DEFAULT_WAL_AUTOCHECKPOINT_PAGES = 1_000
DEFAULT_JOURNAL_SIZE_LIMIT_BYTES = 64 * 1024 * 1024

_T = TypeVar("_T")


def configure_sqlite_runtime(
    connection: sqlite3.Connection,
    *,
    busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
    enable_wal: bool = False,
    wal_autocheckpoint_pages: int = DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
    journal_size_limit_bytes: int = DEFAULT_JOURNAL_SIZE_LIMIT_BYTES,
    enable_incremental_vacuum: bool = False,
) -> None:
    """Apply pragmas used by long-lived API and worker processes."""

    connection.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
    if enable_incremental_vacuum:
        connection.execute("PRAGMA auto_vacuum = INCREMENTAL")
    if enable_wal:
        connection.execute("PRAGMA journal_mode = WAL").fetchone()
        connection.execute("PRAGMA synchronous = NORMAL")
        connection.execute(f"PRAGMA wal_autocheckpoint = {int(wal_autocheckpoint_pages)}")
        connection.execute(f"PRAGMA journal_size_limit = {int(journal_size_limit_bytes)}")


class SQLiteRetryTracker:
    """In-process counter for retryable SQLite contention symptoms."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._retry_count = 0
        self._last_retry_at: str | None = None
        self._last_error: str | None = None

    def record_retry(self, exc: BaseException) -> None:
        with self._lock:
            self._retry_count += 1
            self._last_retry_at = datetime.now(UTC).isoformat()
            self._last_error = f"{type(exc).__name__}: {exc}"

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "retry_count": self._retry_count,
                "last_retry_at": self._last_retry_at,
                "last_error": self._last_error,
            }


@dataclass(frozen=True)
class SQLiteRetryPolicy:
    """Bounded retry policy for short SQLite operations."""

    attempts: int = 3
    initial_delay_seconds: float = 0.05
    backoff_multiplier: float = 2.0


def run_with_sqlite_retries(
    operation: Callable[[], _T],
    *,
    policy: SQLiteRetryPolicy | None = None,
    tracker: SQLiteRetryTracker | None = None,
) -> _T:
    """Run a short SQLite operation with bounded retry/backoff."""

    retry_policy = policy or SQLiteRetryPolicy()
    attempts = max(1, retry_policy.attempts)
    delay_seconds = max(0.0, retry_policy.initial_delay_seconds)
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except (sqlite3.DatabaseError, SystemError) as exc:
            if not is_sqlite_retryable(exc) or attempt >= attempts:
                raise
            if tracker is not None:
                tracker.record_retry(exc)
            if delay_seconds:
                time.sleep(delay_seconds)
            delay_seconds *= max(1.0, retry_policy.backoff_multiplier)
    raise RuntimeError("unreachable SQLite retry state")


def is_sqlite_busy(exc: sqlite3.DatabaseError) -> bool:
    message = str(exc).lower()
    return "database is locked" in message or "database is busy" in message


def is_sqlite_retryable(exc: sqlite3.DatabaseError | SystemError) -> bool:
    message = str(exc).lower()
    if isinstance(exc, SystemError):
        return (
            "sqlite3.connection" in message
            or "commit" in message
            or "error return without exception set" in message
        )
    return (
        is_sqlite_busy(exc)
        or "cannot commit - no transaction is active" in message
        or "cannot rollback - no transaction is active" in message
    )
