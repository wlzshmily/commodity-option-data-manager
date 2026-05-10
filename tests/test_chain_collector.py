import option_data_manager.chain_collector as chain_collector
from option_data_manager.chain_collector import (
    collect_persisted_option_chain,
    _quote_refs,
    _wait_updates,
)
from option_data_manager.instruments import InstrumentRecord
from option_data_manager.instruments import InstrumentRepository
from option_data_manager.klines import KlineRecord, KlineRepository


def test_quote_refs_prefers_batch_quote_subscription() -> None:
    calls: list[object] = []

    class FakeApi:
        def get_quote_list(self, symbols: list[str]) -> list[str]:
            calls.append(("list", tuple(symbols)))
            return [f"quote:{symbol}" for symbol in symbols]

        def get_quote(self, symbol: str) -> str:
            calls.append(("single", symbol))
            return f"single:{symbol}"

    refs = _quote_refs(
        FakeApi(),
        "CZCE.AP610",
        [
            _instrument("CZCE.AP610C8000"),
            _instrument("CZCE.AP610P8000"),
        ],
    )

    assert refs == {
        "CZCE.AP610": "quote:CZCE.AP610",
        "CZCE.AP610C8000": "quote:CZCE.AP610C8000",
        "CZCE.AP610P8000": "quote:CZCE.AP610P8000",
    }
    assert calls == [("list", ("CZCE.AP610", "CZCE.AP610C8000", "CZCE.AP610P8000"))]


def test_quote_refs_falls_back_to_single_quote_subscription() -> None:
    calls: list[str] = []

    class FakeApi:
        def get_quote(self, symbol: str) -> str:
            calls.append(symbol)
            return f"single:{symbol}"

    refs = _quote_refs(FakeApi(), "CZCE.AP610", [_instrument("CZCE.AP610C8000")])

    assert refs == {
        "CZCE.AP610": "single:CZCE.AP610",
        "CZCE.AP610C8000": "single:CZCE.AP610C8000",
    }
    assert calls == ["CZCE.AP610", "CZCE.AP610C8000"]


def test_wait_updates_uses_absolute_deadline(monkeypatch) -> None:
    deadlines: list[float] = []

    class FakeApi:
        def wait_update(self, *, deadline: float) -> bool:
            deadlines.append(deadline)
            return True

    monkeypatch.setattr(chain_collector.time, "time", lambda: 1000.0)

    _wait_updates(FakeApi(), wait_cycles=2)

    assert deadlines == [1001.0, 1001.0]


def test_chain_collector_splices_three_day_klines_with_cached_twenty_days() -> None:
    import sqlite3

    connection = sqlite3.connect(":memory:")
    InstrumentRepository(connection).upsert_instruments([_instrument("CZCE.AP610C8000")])
    kline_repo = KlineRepository(connection)
    kline_repo.replace_symbol_klines(
        "CZCE.AP610C8000",
        [_kline("CZCE.AP610C8000", day) for day in range(1, 21)],
    )
    kline_repo.replace_symbol_klines(
        "CZCE.AP610",
        [_kline("CZCE.AP610", day) for day in range(1, 21)],
    )

    class FakeApi:
        def __init__(self) -> None:
            self.kline_lengths: list[int] = []

        def get_quote_list(self, symbols: list[str]) -> list[dict[str, object]]:
            return [
                {
                    "datetime": "2026-05-10T00:00:00+00:00",
                    "last_price": 100.0,
                }
                for _ in symbols
            ]

        def get_kline_serial(
            self,
            symbol: str,
            *,
            duration_seconds: int,
            data_length: int,
        ) -> list[dict[str, object]]:
            self.kline_lengths.append(data_length)
            return [_single_kline_row(day, symbol) for day in range(19, 22)]

        def wait_update(self, *, deadline: float) -> bool:
            return True

        def query_option_greeks(self, symbol: str) -> None:
            return None

    iv_rows: list[dict[str, object]] = []

    def fake_iv_calculator(klines: object, quote: object) -> dict[str, list[float]]:
        iv_rows.extend(klines.to_dict("records"))  # type: ignore[attr-defined]
        return {"impv": [0.2]}

    api = FakeApi()
    result = collect_persisted_option_chain(
        api,
        connection,
        underlying_symbol="CZCE.AP610",
        option_symbols=("CZCE.AP610C8000",),
        wait_cycles=1,
        iv_calculator=fake_iv_calculator,
    )

    assert api.kline_lengths == [3, 3]
    assert result.kline_rows_written == 40
    assert len(iv_rows) == 20
    assert iv_rows[0]["datetime"] == "2026-05-02"
    assert iv_rows[-1]["datetime"] == "2026-05-21"
    assert iv_rows[-1]["duration"] == 86400
    assert iv_rows[-1]["close"] == 121.0
    assert iv_rows[-1]["close1"] == 221.0
    assert len(kline_repo.get_klines("CZCE.AP610C8000")) == 20
    assert kline_repo.get_klines("CZCE.AP610C8000")[-1].bar_datetime == "2026-05-21"


def test_chain_collector_fetches_twenty_days_when_kline_cache_is_missing() -> None:
    import sqlite3

    connection = sqlite3.connect(":memory:")
    InstrumentRepository(connection).upsert_instruments([_instrument("CZCE.AP610C8000")])

    class FakeApi:
        def __init__(self) -> None:
            self.kline_lengths: list[int] = []

        def get_quote_list(self, symbols: list[str]) -> list[dict[str, object]]:
            return [{"datetime": "2026-05-10T00:00:00+00:00"} for _ in symbols]

        def get_kline_serial(
            self,
            symbol: str,
            *,
            duration_seconds: int,
            data_length: int,
        ) -> list[dict[str, object]]:
            self.kline_lengths.append(data_length)
            return [_single_kline_row(day, symbol) for day in range(1, 21)]

        def wait_update(self, *, deadline: float) -> bool:
            return True

        def query_option_greeks(self, symbol: str) -> None:
            return None

    api = FakeApi()
    collect_persisted_option_chain(
        api,
        connection,
        underlying_symbol="CZCE.AP610",
        option_symbols=("CZCE.AP610C8000",),
        wait_cycles=1,
        iv_calculator=lambda klines, quote: {"impv": [0.2]},
    )

    assert api.kline_lengths == [20, 20]


def test_chain_collector_fetches_underlying_kline_once_per_batch() -> None:
    import sqlite3

    connection = sqlite3.connect(":memory:")
    InstrumentRepository(connection).upsert_instruments(
        [
            _instrument("CZCE.AP610C8000"),
            _instrument("CZCE.AP610P8000", option_class="PUT"),
        ]
    )

    class FakeApi:
        def __init__(self) -> None:
            self.kline_symbols: list[str] = []

        def get_quote_list(self, symbols: list[str]) -> list[dict[str, object]]:
            return [{"datetime": "2026-05-10T00:00:00+00:00"} for _ in symbols]

        def get_kline_serial(
            self,
            symbol: str,
            *,
            duration_seconds: int,
            data_length: int,
        ) -> list[dict[str, object]]:
            self.kline_symbols.append(symbol)
            return [_single_kline_row(day, symbol) for day in range(1, 21)]

        def wait_update(self, *, deadline: float) -> bool:
            return True

        def query_option_greeks(self, symbol: str) -> None:
            return None

    api = FakeApi()
    collect_persisted_option_chain(
        api,
        connection,
        underlying_symbol="CZCE.AP610",
        option_symbols=("CZCE.AP610C8000", "CZCE.AP610P8000"),
        batch_size=2,
        wait_cycles=1,
        iv_calculator=lambda klines, quote: {"impv": [0.2]},
    )

    assert api.kline_symbols == [
        "CZCE.AP610C8000",
        "CZCE.AP610P8000",
        "CZCE.AP610",
    ]


def _instrument(symbol: str, *, option_class: str = "CALL") -> InstrumentRecord:
    return InstrumentRecord(
        symbol=symbol,
        exchange_id="CZCE",
        product_id="AP",
        instrument_id=symbol.split(".", maxsplit=1)[1],
        instrument_name=None,
        ins_class="OPTION",
        underlying_symbol="CZCE.AP610",
        option_class=option_class,
        strike_price=8000,
        expire_datetime=None,
        price_tick=None,
        volume_multiple=None,
        expired=False,
        active=True,
        inactive_reason=None,
        last_seen_at="2026-05-09T00:00:00+00:00",
        raw_payload_json="{}",
    )


def _kline(symbol: str, day: int) -> KlineRecord:
    close = 100.0 + day if symbol.endswith("C8000") else 200.0 + day
    return KlineRecord(
        symbol=symbol,
        bar_datetime=f"2026-05-{day:02d}",
        open_price=close - 1,
        high_price=close + 1,
        low_price=close - 2,
        close_price=close,
        volume=1000.0 + day,
        open_oi=2000.0 + day,
        close_oi=2100.0 + day,
        received_at="2026-05-10T00:00:00+00:00",
        raw_payload_json=(
            '{"close":'
            f"{close}"
            ',"close_oi":'
            f"{2100.0 + day}"
            ',"datetime":"'
            f"2026-05-{day:02d}"
            '","high":'
            f"{close + 1}"
            ',"low":'
            f"{close - 2}"
            ',"open":'
            f"{close - 1}"
            ',"open_oi":'
            f"{2000.0 + day}"
            ',"symbol":"'
            f"{symbol}"
            '","volume":'
            f"{1000.0 + day}"
            "}"
        ),
    )


def _single_kline_row(day: int, symbol: str) -> dict[str, object]:
    close = 200.0 + day if symbol == "CZCE.AP610" else 100.0 + day
    return {
        "datetime": f"2026-05-{day:02d}",
        "symbol": symbol,
        "open": close - 1,
        "high": close + 1,
        "low": close - 2,
        "close": close,
        "volume": 1000.0 + day,
        "open_oi": 2000.0 + day,
        "close_oi": 2100.0 + day,
    }
