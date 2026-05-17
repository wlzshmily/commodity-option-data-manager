"""Resource snapshots for operator-facing health checks."""

from __future__ import annotations

from pathlib import Path
import os
import shutil
from typing import Any


def build_resource_snapshot(
    *,
    database_path: str | None,
    telemetry_database_path: str | None = None,
    runtime_log_dirs: list[Path] | None = None,
) -> dict[str, Any]:
    """Collect lightweight disk, memory, CPU, SQLite, and log-size signals."""

    base_path = _existing_parent(database_path) or Path.cwd()
    disk = _disk_snapshot(base_path)
    sqlite_files = _sqlite_file_snapshot(database_path, telemetry_database_path)
    runtime_logs = _runtime_log_snapshot(runtime_log_dirs or [base_path])
    return {
        "disk": disk,
        "memory": _memory_snapshot(),
        "cpu": _cpu_snapshot(),
        "sqlite": sqlite_files,
        "runtime_logs": runtime_logs,
    }


def default_telemetry_database_path(database_path: str | None) -> str | None:
    """Return the default sidecar telemetry database path for a core DB path."""

    override = os.environ.get("ODM_TELEMETRY_DATABASE_PATH")
    if override:
        return override
    if not database_path or database_path == ":memory:":
        return None
    path = Path(database_path)
    return str(path.with_name("option-data-telemetry.sqlite3"))


def _disk_snapshot(path: Path) -> dict[str, Any]:
    try:
        usage = shutil.disk_usage(path)
    except OSError as exc:
        return {"status": "unknown", "path": str(path), "error": str(exc)}
    free_percent = (usage.free / usage.total * 100.0) if usage.total else 0.0
    return {
        "status": "ok",
        "path": str(path),
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "free_percent": round(free_percent, 3),
    }


def _memory_snapshot() -> dict[str, Any]:
    meminfo = _linux_meminfo()
    if not meminfo:
        return {"status": "unknown"}
    total = meminfo.get("MemTotal", 0)
    available = meminfo.get("MemAvailable", 0)
    available_percent = (available / total * 100.0) if total else 0.0
    return {
        "status": "ok",
        "total_bytes": total,
        "available_bytes": available,
        "available_percent": round(available_percent, 3),
    }


def _cpu_snapshot() -> dict[str, Any]:
    cpu_count = os.cpu_count() or 1
    try:
        load_1m, load_5m, load_15m = os.getloadavg()
    except OSError:
        return {"status": "unknown", "cpu_count": cpu_count}
    return {
        "status": "ok",
        "cpu_count": cpu_count,
        "load_1m": round(load_1m, 3),
        "load_5m": round(load_5m, 3),
        "load_15m": round(load_15m, 3),
        "load_5m_ratio": round(load_5m / cpu_count, 3),
    }


def _sqlite_file_snapshot(
    database_path: str | None,
    telemetry_database_path: str | None,
) -> dict[str, Any]:
    entries = []
    for label, path_text in (
        ("core", database_path),
        ("telemetry", telemetry_database_path),
    ):
        if not path_text or path_text == ":memory:":
            continue
        path = Path(path_text)
        entries.append(
            {
                "label": label,
                "path": str(path),
                "database_size_bytes": _size(path),
                "wal_size_bytes": _size(Path(f"{path}-wal")),
                "shm_size_bytes": _size(Path(f"{path}-shm")),
            }
        )
    return {
        "databases": entries,
        "total_database_size_bytes": sum(
            int(item["database_size_bytes"]) for item in entries
        ),
        "total_wal_size_bytes": sum(int(item["wal_size_bytes"]) for item in entries),
        "total_shm_size_bytes": sum(int(item["shm_size_bytes"]) for item in entries),
    }


def _runtime_log_snapshot(runtime_log_dirs: list[Path]) -> dict[str, Any]:
    total_bytes = 0
    file_count = 0
    scanned_dirs: list[str] = []
    for directory in runtime_log_dirs:
        if not directory.exists():
            continue
        scanned_dirs.append(str(directory))
        for path in directory.rglob("*"):
            if not path.is_file() or _protected_runtime_file(path.name):
                continue
            if not _looks_like_log_artifact(path.name):
                continue
            file_count += 1
            total_bytes += _size(path)
    return {
        "directories": scanned_dirs,
        "file_count": file_count,
        "total_bytes": total_bytes,
    }


def _existing_parent(path_text: str | None) -> Path | None:
    if not path_text or path_text == ":memory:":
        return None
    path = Path(path_text)
    return path if path.is_dir() else path.parent


def _linux_meminfo() -> dict[str, int]:
    path = Path("/proc/meminfo")
    if not path.exists():
        return {}
    result: dict[str, int] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}
    for line in lines:
        key, _, value = line.partition(":")
        parts = value.strip().split()
        if not parts:
            continue
        try:
            result[key] = int(parts[0]) * 1024
        except ValueError:
            continue
    return result


def _protected_runtime_file(name: str) -> bool:
    if name.endswith(".out.log") or name.endswith(".err.log"):
        return False
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


def _looks_like_log_artifact(name: str) -> bool:
    suffixes = (".log", ".old", ".bak", ".gz", ".zip")
    return name.endswith(suffixes) or ".log." in name


def _size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0
