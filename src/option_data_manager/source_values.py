"""Shared source-value normalization helpers."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date, datetime
from decimal import Decimal
import math
from typing import Any


def convert_tqsdk_time(input_time: Any) -> Any:
    """Convert TQSDK numeric time values through the SDK helper."""

    from tqsdk.tafunc import time_to_datetime

    return time_to_datetime(input_time)


def normalize_datetime(
    value: Any,
    time_converter: Callable[[Any], Any],
) -> str | None:
    """Normalize source time values into stable text for SQLite."""

    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, int | float):
        if finite_float(value) is None:
            return None
        converted = time_converter(value)
        return normalize_datetime(converted, time_converter)
    return str(value)


def finite_float(value: Any) -> float | None:
    """Return a finite float or None for values SQLite should store as NULL."""

    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, Decimal):
        value = float(value)
    if isinstance(value, int | float):
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None
    return None


def json_safe(value: Any) -> Any:
    """Return a JSON-serializable value without NaN or infinity."""

    if value is None or isinstance(value, str | bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [json_safe(item) for item in value]
    return str(value)
