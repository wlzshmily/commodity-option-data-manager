"""Runtime metadata for diagnostics and smoke tests."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys


@dataclass(frozen=True)
class RuntimeInfo:
    """Small immutable snapshot of the Python runtime."""

    python_version: str
    executable: str
    package_root: Path


def get_runtime_info() -> RuntimeInfo:
    """Return basic runtime information without touching external services."""

    return RuntimeInfo(
        python_version=sys.version.split()[0],
        executable=sys.executable,
        package_root=Path(__file__).resolve().parent,
    )
