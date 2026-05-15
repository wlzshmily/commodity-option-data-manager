"""Trading-session helpers for product/contract-level runtime decisions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
import json
from typing import Any, Mapping


SESSION_IN = "in_session"
SESSION_OUT = "out_of_session"
SESSION_UNKNOWN = "unknown"
DEFAULT_QUOTE_ACTIVITY_FRESH_SECONDS = 5 * 60


@dataclass(frozen=True)
class TradingSessionState:
    """Normalized trading-session state for one contract/product profile."""

    state: str
    has_night: bool
    day: tuple[tuple[str, str], ...]
    night: tuple[tuple[str, str], ...]


def trading_session_state_from_payload(
    raw_payload_json: str | None,
    *,
    now: datetime | None = None,
    quote_source_datetime: str | None = None,
    quote_fresh_seconds: int = DEFAULT_QUOTE_ACTIVITY_FRESH_SECONDS,
) -> TradingSessionState:
    """Return whether the quote payload's own trading-time profile is active."""

    current_now = _local_naive(now or datetime.now())
    profile = trading_time_profile_from_payload(raw_payload_json)
    quote_is_live = _quote_source_is_fresh(
        quote_source_datetime,
        now=current_now,
        fresh_seconds=quote_fresh_seconds,
    )
    if profile is None:
        return TradingSessionState(
            state=SESSION_IN if quote_is_live else SESSION_UNKNOWN,
            has_night=False,
            day=(),
            night=(),
        )
    day_segments = _normalize_segments(profile.get("day"))
    night_segments = _normalize_segments(profile.get("night"))
    if not day_segments and not night_segments:
        return TradingSessionState(
            state=SESSION_IN if quote_is_live else SESSION_UNKNOWN,
            has_night=False,
            day=(),
            night=(),
        )
    current = current_now.time()
    in_session = any(_time_in_segment(current, segment) for segment in day_segments)
    in_session = in_session or any(
        _time_in_segment(current, segment) for segment in night_segments
    )
    return TradingSessionState(
        state=SESSION_IN if in_session or quote_is_live else SESSION_OUT,
        has_night=bool(night_segments),
        day=tuple((_format_time(start), _format_time(end)) for start, end in day_segments),
        night=tuple(
            (_format_time(start), _format_time(end)) for start, end in night_segments
        ),
    )


def trading_time_profile_from_payload(
    raw_payload_json: str | None,
) -> Mapping[str, Any] | None:
    """Extract a `trading_time` mapping from a quote payload JSON string."""

    if not raw_payload_json:
        return None
    try:
        payload = json.loads(raw_payload_json)
    except json.JSONDecodeError:
        return None
    trading_time = payload.get("trading_time")
    return trading_time if isinstance(trading_time, Mapping) else None


def normalize_trading_time_object(value: Any) -> Any:
    """Convert a TQSDK TradingTime-like object into JSON-safe mappings."""

    if value is None:
        return None
    if isinstance(value, Mapping):
        return {
            "day": _json_safe_segments(value.get("day")),
            "night": _json_safe_segments(value.get("night")),
        }
    day = getattr(value, "day", None)
    night = getattr(value, "night", None)
    if day is None and night is None:
        return value
    return {
        "day": _json_safe_segments(day),
        "night": _json_safe_segments(night),
    }


def _json_safe_segments(value: Any) -> list[list[str]]:
    segments = _normalize_segments(value)
    return [[_format_time(start), _format_time(end)] for start, end in segments]


def _normalize_segments(value: Any) -> list[tuple[time, time]]:
    if not value:
        return []
    segments: list[tuple[time, time]] = []
    for item in value:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
        start = _parse_time(item[0])
        end = _parse_time(item[1])
        if start is None or end is None:
            continue
        segments.append((start, end))
    return segments


def _parse_time(value: Any) -> time | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if " " in text:
        text = text.rsplit(" ", maxsplit=1)[-1]
    if "T" in text:
        text = text.rsplit("T", maxsplit=1)[-1]
    text = text.split("+", maxsplit=1)[0].split("Z", maxsplit=1)[0]
    for fmt in ("%H:%M:%S.%f", "%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(text[:15], fmt).time()
        except ValueError:
            continue
    return None


def _quote_source_is_fresh(
    value: str | None,
    *,
    now: datetime,
    fresh_seconds: int,
) -> bool:
    quote_datetime = _parse_datetime(value)
    if quote_datetime is None:
        return False
    age_seconds = abs((now - quote_datetime).total_seconds())
    return age_seconds <= fresh_seconds


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    return _local_naive(parsed)


def _local_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone().replace(tzinfo=None)


def _time_in_segment(current: time, segment: tuple[time, time]) -> bool:
    start, end = segment
    if start <= end:
        return start <= current <= end
    return current >= start or current <= end


def _format_time(value: time) -> str:
    return value.strftime("%H:%M:%S")
