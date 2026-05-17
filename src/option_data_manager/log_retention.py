"""Retention cleanup for low-value runtime telemetry."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
import sqlite3
from typing import Any

from .service_state import ServiceLogRepository, ServiceStateRepository


DEFAULT_RUNTIME_LOG_MAX_BYTES = 512 * 1024 * 1024


@dataclass(frozen=True)
class TelemetryRetentionPolicy:
    """Bounded retention policy for local telemetry rows and stale log files."""

    service_log_retention_days: int = 14
    service_log_max_rows: int = 20_000
    api_metric_retention_days: int = 7
    api_metric_max_rows: int = 50_000
    runtime_log_retention_days: int = 14
    runtime_log_max_bytes: int = DEFAULT_RUNTIME_LOG_MAX_BYTES
    incremental_vacuum_pages: int = 128


DEFAULT_TELEMETRY_RETENTION_POLICY = TelemetryRetentionPolicy()


def cleanup_telemetry(
    connection: sqlite3.Connection,
    *,
    policy: TelemetryRetentionPolicy = DEFAULT_TELEMETRY_RETENTION_POLICY,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Clean bounded SQLite telemetry tables and reclaim small chunks of space."""

    started_at = datetime.now(UTC)
    cleanup_now = now or started_at
    try:
        service_state = ServiceStateRepository(connection)
        service_logs = ServiceLogRepository(connection)
        service_logs_result = service_logs.cleanup_logs(
            retention_days=policy.service_log_retention_days,
            max_rows=policy.service_log_max_rows,
            now=cleanup_now,
        )
        api_metrics_result = service_state.cleanup_request_metrics(
            retention_days=policy.api_metric_retention_days,
            max_rows=policy.api_metric_max_rows,
            now=cleanup_now,
        )
        vacuum_pages = _incremental_vacuum(connection, policy.incremental_vacuum_pages)
    except sqlite3.DatabaseError as exc:
        connection.rollback()
        return {
            "status": "failed",
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "error": f"{type(exc).__name__}: {exc}",
        }
    return {
        "status": "ok",
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "service_logs": service_logs_result,
        "api_request_metrics": api_metrics_result,
        "incremental_vacuum_pages": vacuum_pages,
    }


def cleanup_runtime_log_files(
    log_dir: Path,
    *,
    policy: TelemetryRetentionPolicy = DEFAULT_TELEMETRY_RETENTION_POLICY,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Delete stale rotated log artifacts while protecting active runtime files."""

    started_at = datetime.now(UTC)
    if not log_dir.exists():
        return {
            "status": "ok",
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "deleted_files": 0,
            "deleted_bytes": 0,
            "remaining_bytes": 0,
        }
    cleanup_now = now or started_at
    cutoff = cleanup_now - timedelta(days=max(0, policy.runtime_log_retention_days))
    candidates = sorted(
        _cleanup_candidates(log_dir),
        key=lambda item: item.stat().st_mtime if item.exists() else 0,
    )
    deleted_files = 0
    deleted_bytes = 0
    for path in candidates:
        if _mtime_datetime(path) >= cutoff:
            continue
        size = _safe_size(path)
        if _delete_file(path):
            deleted_files += 1
            deleted_bytes += size

    remaining = _log_tree_size(log_dir)
    if remaining > policy.runtime_log_max_bytes:
        for path in candidates:
            if not path.exists():
                continue
            size = _safe_size(path)
            if _delete_file(path):
                deleted_files += 1
                deleted_bytes += size
                remaining = max(0, remaining - size)
            if remaining <= policy.runtime_log_max_bytes:
                break

    return {
        "status": "ok",
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "deleted_files": deleted_files,
        "deleted_bytes": deleted_bytes,
        "remaining_bytes": remaining,
    }


def _incremental_vacuum(connection: sqlite3.Connection, pages: int) -> int:
    if pages <= 0:
        return 0
    connection.execute(f"PRAGMA incremental_vacuum({int(pages)})")
    return int(pages)


def _cleanup_candidates(log_dir: Path) -> list[Path]:
    suffixes = {".log", ".old", ".bak", ".gz", ".zip"}
    result: list[Path] = []
    for path in log_dir.rglob("*"):
        if not path.is_file():
            continue
        name = path.name
        if _protected_runtime_file(name):
            continue
        if path.suffix in suffixes or ".log." in name:
            result.append(path)
    return result


def _protected_runtime_file(name: str) -> bool:
    if name.endswith(".out.log") or name.endswith(".err.log"):
        return True
    protected_suffixes = (
        ".pid",
        ".stop",
        ".sqlite",
        ".sqlite3",
        ".sqlite3-wal",
        ".sqlite3-shm",
        ".db",
        ".db-wal",
        ".db-shm",
        ".json",
    )
    return name.endswith(protected_suffixes)


def _safe_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _mtime_datetime(path: Path) -> datetime:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except OSError:
        return datetime.now(UTC)


def _delete_file(path: Path) -> bool:
    try:
        path.unlink()
    except OSError:
        return False
    return True


def _log_tree_size(log_dir: Path) -> int:
    total = 0
    for path in log_dir.rglob("*"):
        if path.is_file() and not _protected_runtime_file(path.name):
            total += _safe_size(path)
    return total
