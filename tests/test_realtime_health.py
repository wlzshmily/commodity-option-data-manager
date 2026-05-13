from datetime import datetime
import sqlite3

from option_data_manager.instruments import (
    InstrumentRepository,
    normalize_option_chain_discovery,
)
from option_data_manager.quotes import QuoteRecord, QuoteRepository
from option_data_manager.realtime_health import build_realtime_health
from option_data_manager.realtime_health import current_trading_session


def test_realtime_health_ignores_session_hours_without_market_evidence() -> None:
    connection = sqlite3.connect(":memory:")
    now = datetime.fromisoformat("2026-05-11T09:10:00+08:00")

    health = build_realtime_health(
        connection,
        running=True,
        progress={
            "status": "running",
            "updated_at": "2026-05-11T09:09:50+08:00",
            "last_wait_update_at": "2026-05-11T09:05:00+08:00",
        },
        now=now,
    )

    assert health["status"] == "awaiting_market_evidence"
    assert health["tone"] == "good"


def test_realtime_health_detects_stale_wait_update_after_market_opens() -> None:
    connection = sqlite3.connect(":memory:")
    _insert_future_quote(
        connection,
        source_datetime="2026-05-11T09:08:00+08:00",
        received_at="2026-05-11T09:08:05+08:00",
    )

    health = build_realtime_health(
        connection,
        running=True,
        progress={
            "status": "running",
            "updated_at": "2026-05-11T09:09:55+08:00",
            "last_wait_update_at": "2026-05-11T09:05:00+08:00",
            "last_quote_write_at": "2026-05-11T09:08:05+08:00",
        },
        now=datetime.fromisoformat("2026-05-11T09:10:00+08:00"),
    )

    assert health["status"] == "wait_update_stale"
    assert health["tone"] == "bad"


def test_realtime_health_does_not_alert_before_subscription_setup_finishes() -> None:
    connection = sqlite3.connect(":memory:")
    _insert_future_quote(
        connection,
        source_datetime="2026-05-11T21:08:00+08:00",
        received_at="2026-05-11T21:08:05+08:00",
    )

    health = build_realtime_health(
        connection,
        running=True,
        progress={
            "status": "running",
            "active_stage": "kline",
            "updated_at": "2026-05-11T21:09:55+08:00",
            "subscribed_objects": 100,
            "total_objects": 200,
            "last_wait_update_at": None,
            "last_quote_write_at": None,
        },
        now=datetime.fromisoformat("2026-05-11T21:10:00+08:00"),
    )

    assert health["status"] == "subscribing"
    assert health["label"] == "订阅建立中"
    assert health["tone"] == "warn"


def test_realtime_health_stays_quiet_after_close() -> None:
    connection = sqlite3.connect(":memory:")
    _insert_future_quote(
        connection,
        source_datetime="2026-05-11T14:58:00+08:00",
        received_at="2026-05-11T14:58:05+08:00",
    )

    health = build_realtime_health(
        connection,
        running=True,
        progress={
            "status": "running",
            "updated_at": "2026-05-11T15:40:00+08:00",
            "last_wait_update_at": "2026-05-11T14:58:00+08:00",
        },
        now=datetime.fromisoformat("2026-05-11T15:40:00+08:00"),
    )

    assert health["status"] == "session_closed"
    assert health["tone"] == "good"


def test_realtime_health_reports_ok_when_updates_are_fresh() -> None:
    connection = sqlite3.connect(":memory:")
    _insert_future_quote(
        connection,
        source_datetime="2026-05-11T09:09:00+08:00",
        received_at="2026-05-11T09:09:02+08:00",
    )

    health = build_realtime_health(
        connection,
        running=True,
        progress={
            "status": "running",
            "updated_at": "2026-05-11T09:09:55+08:00",
            "last_wait_update_at": "2026-05-11T09:09:40+08:00",
            "last_quote_write_at": "2026-05-11T09:09:45+08:00",
        },
        now=datetime.fromisoformat("2026-05-11T09:10:00+08:00"),
    )

    assert health["status"] == "ok"
    assert health["tone"] == "good"


def test_realtime_health_uses_tqsdk_disconnect_notification() -> None:
    connection = sqlite3.connect(":memory:")
    _insert_future_quote(
        connection,
        source_datetime="2026-05-11T09:09:00+08:00",
        received_at="2026-05-11T09:09:02+08:00",
    )

    health = build_realtime_health(
        connection,
        running=True,
        progress={
            "status": "running",
            "updated_at": "2026-05-11T09:09:55+08:00",
            "last_wait_update_at": "2026-05-11T09:09:40+08:00",
            "last_quote_write_at": "2026-05-11T09:09:45+08:00",
            "tqsdk_connection_status": "disconnected",
            "last_tqsdk_notify_at": "2026-05-11T09:09:50+08:00",
            "last_tqsdk_notify_content": "与行情服务器的网络连接断开",
        },
        now=datetime.fromisoformat("2026-05-11T09:10:00+08:00"),
    )

    assert health["status"] == "tqsdk_disconnected"
    assert health["label"] == "TQSDK 断线"
    assert health["tone"] == "bad"
    assert "网络连接断开" in health["message"]


def test_friday_night_is_a_possible_night_session_before_normal_weekend() -> None:
    session = current_trading_session(
        datetime.fromisoformat("2026-05-15T21:30:00+08:00"),
        is_holiday=lambda day: False,
    )

    assert session is not None
    assert session.label == "夜盘"


def test_friday_late_night_extends_into_saturday_morning() -> None:
    session = current_trading_session(
        datetime.fromisoformat("2026-05-16T01:00:00+08:00"),
        is_holiday=lambda day: False,
    )

    assert session is not None
    assert session.label == "夜盘"
    assert session.start.date().isoformat() == "2026-05-15"


def test_last_trading_day_before_holiday_has_no_night_session() -> None:
    holidays = {datetime.fromisoformat("2026-05-18T00:00:00+08:00").date()}
    session = current_trading_session(
        datetime.fromisoformat("2026-05-15T21:30:00+08:00"),
        is_holiday=lambda day: day in holidays,
    )

    assert session is None


def _insert_future_quote(
    connection: sqlite3.Connection,
    *,
    source_datetime: str,
    received_at: str,
) -> None:
    InstrumentRepository(connection).upsert_instruments(
        normalize_option_chain_discovery(
            underlying_symbol="DCE.a2601",
            call_symbols=("DCE.a2601C100",),
            put_symbols=("DCE.a2601P100",),
            last_seen_at="2026-05-11T00:00:00+08:00",
        )
    )
    QuoteRepository(connection).upsert_quote(
        QuoteRecord(
            symbol="DCE.a2601",
            source_datetime=source_datetime,
            received_at=received_at,
            last_price=100.0,
            ask_price1=100.2,
            bid_price1=99.8,
            ask_volume1=1.0,
            bid_volume1=1.0,
            volume=10.0,
            open_interest=20.0,
            open_price=100.0,
            high_price=101.0,
            low_price=99.0,
            close_price=100.0,
            average_price=100.0,
            price_tick=1.0,
            volume_multiple=10.0,
            raw_payload_json="{}",
        )
    )
