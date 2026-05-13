"""Runtime SQLite connection tuning."""

from __future__ import annotations

import sqlite3


def configure_sqlite_runtime(
    connection: sqlite3.Connection,
    *,
    busy_timeout_ms: int = 30_000,
    enable_wal: bool = False,
) -> None:
    """Apply pragmas used by long-lived API and worker processes."""

    connection.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
    if enable_wal:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = NORMAL")
