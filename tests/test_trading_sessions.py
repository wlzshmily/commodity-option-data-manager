from datetime import datetime, timedelta

from option_data_manager.trading_sessions import (
    SESSION_IN,
    SESSION_OUT,
    SESSION_UNKNOWN,
    normalize_trading_time_object,
    trading_session_state_from_payload,
)


def test_trading_session_state_handles_day_and_night_segments() -> None:
    payload = (
        '{"trading_time":{"day":[["09:00:00","10:15:00"]],'
        '"night":[["21:00:00","23:00:00"]]}}'
    )

    day = trading_session_state_from_payload(
        payload,
        now=datetime(2026, 5, 14, 9, 30),
    )
    night = trading_session_state_from_payload(
        payload,
        now=datetime(2026, 5, 14, 21, 30),
    )
    closed = trading_session_state_from_payload(
        payload,
        now=datetime(2026, 5, 14, 16, 0),
    )

    assert day.state == SESSION_IN
    assert night.state == SESSION_IN
    assert closed.state == SESSION_OUT
    assert day.has_night is True
    assert day.night == (("21:00:00", "23:00:00"),)


def test_trading_session_state_handles_cross_midnight_night_segment() -> None:
    payload = '{"trading_time":{"day":[],"night":[["21:00:00","02:30:00"]]}}'

    assert (
        trading_session_state_from_payload(
            payload,
            now=datetime(2026, 5, 15, 1, 30),
        ).state
        == SESSION_IN
    )
    assert (
        trading_session_state_from_payload(
            payload,
            now=datetime(2026, 5, 15, 3, 30),
        ).state
        == SESSION_OUT
    )


def test_fresh_quote_datetime_overrides_missing_night_segment() -> None:
    now = datetime(2026, 5, 14, 23, 42)
    payload = (
        '{"trading_time":{"day":[["09:00:00","10:15:00"],'
        '["10:30:00","11:30:00"],["13:30:00","15:00:00"]],"night":[]}}'
    )

    state = trading_session_state_from_payload(
        payload,
        now=now,
        quote_source_datetime=(now - timedelta(seconds=10)).isoformat(sep=" "),
    )

    assert state.state == SESSION_IN


def test_stale_quote_datetime_does_not_override_closed_profile() -> None:
    now = datetime(2026, 5, 14, 23, 42)
    payload = (
        '{"trading_time":{"day":[["09:00:00","10:15:00"],'
        '["10:30:00","11:30:00"],["13:30:00","15:00:00"]],"night":[]}}'
    )

    state = trading_session_state_from_payload(
        payload,
        now=now,
        quote_source_datetime="2026-05-14 19:04:00.000000",
    )

    assert state.state == SESSION_OUT


def test_trading_session_state_is_unknown_when_profile_missing() -> None:
    state = trading_session_state_from_payload("{}", now=datetime(2026, 5, 14, 9, 30))

    assert state.state == SESSION_UNKNOWN


def test_normalize_trading_time_object_handles_tqsdk_like_objects() -> None:
    class TradingTime:
        day = [["09:00:00", "10:15:00"]]
        night = []

    assert normalize_trading_time_object(TradingTime()) == {
        "day": [["09:00:00", "10:15:00"]],
        "night": [],
    }
