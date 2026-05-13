"""Realtime quote-stream health checks for trading-session operation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from functools import lru_cache
import sqlite3
from typing import Any
from zoneinfo import ZoneInfo


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
HEARTBEAT_STALE_SECONDS = 90
WAIT_UPDATE_STALE_SECONDS = 120
QUOTE_WRITE_STALE_SECONDS = 180
OPEN_GRACE_SECONDS = 90


@dataclass(frozen=True)
class TradingSession:
    """A local China futures trading-session window."""

    label: str
    start: datetime
    end: datetime


def build_realtime_health(
    connection: sqlite3.Connection | None,
    *,
    running: bool,
    progress: dict[str, Any] | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a conservative realtime network health status.

    The checker only raises suspicious states during a known trading-session
    window and after the database has seen market evidence for the current
    local session day. This avoids holiday and post-close false alarms.
    """

    current = _local_now(now)
    progress = progress or {}
    session = current_trading_session(current)
    if not running:
        return {
            "checked_at": current.isoformat(),
            "session_active": session is not None,
            "session_label": session.label if session else None,
            "latest_market_source_datetime": None,
            "latest_market_received_at": None,
            "last_report_at": progress.get("updated_at"),
            "last_wait_update_at": progress.get("last_wait_update_at"),
            "last_quote_write_at": progress.get("last_quote_write_at"),
            "last_tqsdk_notify_at": progress.get("last_tqsdk_notify_at"),
            "last_tqsdk_notify_code": progress.get("last_tqsdk_notify_code"),
            "last_tqsdk_notify_level": progress.get("last_tqsdk_notify_level"),
            "last_tqsdk_notify_content": progress.get("last_tqsdk_notify_content"),
            "tqsdk_connection_status": progress.get("tqsdk_connection_status"),
            "last_tqsdk_disconnect_at": progress.get("last_tqsdk_disconnect_at"),
            "last_tqsdk_restore_at": progress.get("last_tqsdk_restore_at"),
            "status": "idle",
            "label": "未运行",
            "message": "实时订阅未运行。",
            "tone": "warn",
        }
    latest_quote = _latest_future_quote(connection) if connection is not None else None
    base = {
        "checked_at": current.isoformat(),
        "session_active": session is not None,
        "session_label": session.label if session else None,
        "latest_market_source_datetime": latest_quote.get("source_datetime")
        if latest_quote
        else None,
        "latest_market_received_at": latest_quote.get("received_at")
        if latest_quote
        else None,
        "last_report_at": progress.get("updated_at"),
        "last_wait_update_at": progress.get("last_wait_update_at"),
        "last_quote_write_at": progress.get("last_quote_write_at"),
        "last_tqsdk_notify_at": progress.get("last_tqsdk_notify_at"),
        "last_tqsdk_notify_code": progress.get("last_tqsdk_notify_code"),
        "last_tqsdk_notify_level": progress.get("last_tqsdk_notify_level"),
        "last_tqsdk_notify_content": progress.get("last_tqsdk_notify_content"),
        "tqsdk_connection_status": progress.get("tqsdk_connection_status"),
        "last_tqsdk_disconnect_at": progress.get("last_tqsdk_disconnect_at"),
        "last_tqsdk_restore_at": progress.get("last_tqsdk_restore_at"),
    }
    subscribed_objects = _int_value(progress.get("subscribed_objects"))
    total_objects = _int_value(progress.get("total_objects"))
    active_stage = str(progress.get("active_stage") or "")
    if (
        str(progress.get("status") or "") in {
            "initializing",
            "subscribing",
            "quote_subscribing",
            "kline_subscribing",
        }
        or active_stage in {"initializing", "quote", "kline"}
        or (total_objects > 0 and subscribed_objects < total_objects)
    ):
        return {
            **base,
            "status": "subscribing",
            "label": "订阅建立中",
            "message": "正在建立 Quote/K线对象，暂不做网络异常判断。",
            "tone": "warn",
        }
    if session is None:
        return {
            **base,
            "status": "session_closed",
            "label": "休市观察",
            "message": "当前不在默认交易时段内，网络异常检测不报警。",
            "tone": "good",
        }
    seconds_since_open = (current - session.start).total_seconds()
    if seconds_since_open < OPEN_GRACE_SECONDS:
        return {
            **base,
            "status": "opening_grace",
            "label": "开盘宽限",
            "message": f"{session.label}刚开始，等待行情链路稳定。",
            "tone": "warn",
            "seconds_since_session_open": max(0, seconds_since_open),
        }
    if not _has_current_session_market_evidence(latest_quote, current):
        return {
            **base,
            "status": "awaiting_market_evidence",
            "label": "等待交易证据",
            "message": "当前时段尚未看到今日市场时间推进；按节假日或休市处理，不报警。",
            "tone": "good",
            "seconds_since_session_open": max(0, seconds_since_open),
        }

    connection_status = str(progress.get("tqsdk_connection_status") or "unknown")
    if connection_status in {"disconnected", "reconnecting"}:
        content = str(progress.get("last_tqsdk_notify_content") or "").strip()
        label = "TQSDK 断线" if connection_status == "disconnected" else "TQSDK 重连中"
        message = content or "TQSDK 已报告行情网络连接异常，等待 SDK 自动重连恢复。"
        return {
            **base,
            "status": f"tqsdk_{connection_status}",
            "label": label,
            "message": message,
            "tone": "bad",
            "seconds_since_session_open": max(0, seconds_since_open),
            "last_tqsdk_notify_age_seconds": _age_seconds(
                progress.get("last_tqsdk_notify_at"),
                current,
            ),
        }

    report_age = _age_seconds(progress.get("updated_at"), current)
    wait_age = _age_seconds(progress.get("last_wait_update_at"), current)
    quote_age = _age_seconds(
        _latest_datetime_value(
            progress.get("last_quote_write_at"),
            (latest_quote or {}).get("received_at"),
        ),
        current,
    )
    if report_age is None or report_age > HEARTBEAT_STALE_SECONDS:
        return {
            **base,
            "status": "worker_heartbeat_stale",
            "label": "worker 心跳停滞",
            "message": "worker 进程仍显示运行，但健康报告长时间未更新。",
            "tone": "bad",
            "last_report_age_seconds": report_age,
        }
    if quote_age is None or quote_age > QUOTE_WRITE_STALE_SECONDS:
        return {
            **base,
            "status": "quote_write_stale",
            "label": "行情写入停滞",
            "message": "TQSDK 有更新心跳，但本地 Quote 长时间没有写入新数据。",
            "tone": "warn",
            "last_quote_write_age_seconds": quote_age,
        }
    if wait_age is None or wait_age > WAIT_UPDATE_STALE_SECONDS:
        return {
            **base,
            "status": "ok",
            "label": "链路正常",
            "message": "本地 Quote 仍在写入当前交易时段行情；忽略滞后的 wait_update 统计字段。",
            "tone": "good",
            "last_report_age_seconds": report_age,
            "last_wait_update_age_seconds": wait_age,
            "last_quote_write_age_seconds": quote_age,
        }
    return {
        **base,
        "status": "ok",
        "label": "链路正常",
        "message": "交易时段内 TQSDK 更新和本地 Quote 写入都在推进。",
        "tone": "good",
        "last_report_age_seconds": report_age,
        "last_wait_update_age_seconds": wait_age,
        "last_quote_write_age_seconds": quote_age,
    }


def current_trading_session(
    now: datetime | None = None,
    *,
    is_holiday: Callable[[datetime.date], bool] | None = None,
) -> TradingSession | None:
    """Return the broad commodity futures session containing ``now``."""

    current = _local_now(now)
    current_time = current.time()
    weekday = current.weekday()
    holiday_checker = is_holiday or _is_chinese_holiday
    day_windows = (
        ("上午盘", time(9, 0), time(10, 15)),
        ("上午盘", time(10, 30), time(11, 30)),
        ("下午盘", time(13, 30), time(15, 0)),
    )
    if weekday <= 4:
        for label, start_time, end_time in day_windows:
            if start_time <= current_time <= end_time:
                return TradingSession(
                    label=label,
                    start=datetime.combine(current.date(), start_time, SHANGHAI_TZ),
                    end=datetime.combine(current.date(), end_time, SHANGHAI_TZ),
                )
    if (
        weekday <= 4
        and current_time >= time(21, 0)
        and not _is_pre_holiday_no_night(
            current.date(),
            is_holiday=holiday_checker,
        )
    ):
        return TradingSession(
            label="夜盘",
            start=datetime.combine(current.date(), time(21, 0), SHANGHAI_TZ),
            end=datetime.combine(
                current.date() + timedelta(days=1),
                time(2, 30),
                SHANGHAI_TZ,
            ),
        )
    previous_date = current.date() - timedelta(days=1)
    if (
        current_time <= time(2, 30)
        and previous_date.weekday() <= 4
        and not _is_pre_holiday_no_night(previous_date, is_holiday=holiday_checker)
    ):
        return TradingSession(
            label="夜盘",
            start=datetime.combine(
                previous_date,
                time(21, 0),
                SHANGHAI_TZ,
            ),
            end=datetime.combine(current.date(), time(2, 30), SHANGHAI_TZ),
        )
    return None


def _is_pre_holiday_no_night(
    trading_date: datetime.date,
    *,
    is_holiday: Callable[[datetime.date], bool],
) -> bool:
    """Return whether the night session after ``trading_date`` should be quiet.

    Normal weekends do not cancel Friday night. A statutory holiday immediately
    after the current trading day, including after a weekend gap, does.
    """

    for offset in range(1, 15):
        candidate = trading_date + timedelta(days=offset)
        if is_holiday(candidate):
            return True
        if candidate.weekday() < 5:
            return False
    return False


@lru_cache(maxsize=512)
def _is_chinese_holiday(day: datetime.date) -> bool:
    """Best-effort statutory holiday lookup from TQSDK's holiday table."""

    try:
        import tqsdk.calendar as tq_calendar

        tq_calendar._init_chinese_rest_days()
        rest_days = tq_calendar.rest_days_df
        if rest_days is None:
            return False
        return bool((rest_days["date"].dt.date == day).any())
    except Exception:
        return False


def _latest_future_quote(connection: sqlite3.Connection) -> dict[str, Any] | None:
    try:
        row = connection.execute(
            """
            SELECT
                q.symbol,
                q.source_datetime,
                q.received_at
            FROM quote_current q
            JOIN instruments i ON i.symbol = q.symbol
            WHERE i.active = 1
              AND i.ins_class = 'FUTURE'
              AND q.source_datetime IS NOT NULL
            ORDER BY q.source_datetime DESC, q.received_at DESC
            LIMIT 1
            """
        ).fetchone()
    except sqlite3.Error:
        return None
    return dict(row) if row is not None else None


def _has_current_session_market_evidence(
    quote: dict[str, Any] | None,
    now: datetime,
) -> bool:
    if not quote:
        return False
    source_time = _parse_datetime(quote.get("source_datetime"))
    if source_time is None:
        return False
    return source_time.astimezone(SHANGHAI_TZ).date() == now.date()


def _age_seconds(value: Any, now: datetime) -> float | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    return max(0.0, (now - parsed.astimezone(SHANGHAI_TZ)).total_seconds())


def _latest_datetime_value(*values: Any) -> Any:
    parsed_values: list[tuple[datetime, Any]] = []
    for value in values:
        parsed = _parse_datetime(value)
        if parsed is not None:
            parsed_values.append((parsed.astimezone(UTC), value))
    if not parsed_values:
        return None
    return max(parsed_values, key=lambda item: item[0])[1]


def _int_value(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed


def _local_now(now: datetime | None) -> datetime:
    current = now or datetime.now(SHANGHAI_TZ)
    if current.tzinfo is None:
        return current.replace(tzinfo=SHANGHAI_TZ)
    return current.astimezone(SHANGHAI_TZ)
