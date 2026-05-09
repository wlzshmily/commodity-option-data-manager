import sqlite3

from option_data_manager.instruments import (
    InstrumentRepository,
    normalize_option_chain_discovery,
)
from option_data_manager.quote_streamer import select_quote_symbols, stream_quotes


def test_select_quote_symbols_splits_workers_deterministically() -> None:
    connection = sqlite3.connect(":memory:")
    repository = InstrumentRepository(connection)
    for underlying in ("DCE.a2601", "DCE.b2601"):
        repository.upsert_instruments(
            normalize_option_chain_discovery(
                underlying_symbol=underlying,
                call_symbols=(f"{underlying}C100",),
                put_symbols=(f"{underlying}P100",),
                last_seen_at="2026-05-08T00:00:00+00:00",
            )
        )

    worker_0 = select_quote_symbols(connection, worker_index=0, worker_count=2)
    worker_1 = select_quote_symbols(connection, worker_index=1, worker_count=2)

    assert set(worker_0).isdisjoint(worker_1)
    assert len(worker_0) + len(worker_1) == 6


def test_stream_quotes_writes_initial_snapshot_only_when_unchanged() -> None:
    connection = sqlite3.connect(":memory:")
    InstrumentRepository(connection).upsert_instruments(
        normalize_option_chain_discovery(
            underlying_symbol="DCE.a2601",
            call_symbols=("DCE.a2601C100",),
            put_symbols=("DCE.a2601P100",),
            last_seen_at="2026-05-08T00:00:00+00:00",
        )
    )

    class FakeApi:
        def get_quote_list(self, symbols: list[str]) -> list[dict[str, object]]:
            return [
                {
                    "datetime": "2026-05-09T00:00:00+00:00",
                    "last_price": float(index + 1),
                    "bid_price1": 1.0,
                    "ask_price1": 2.0,
                    "expire_datetime": "2026-06-10",
                    "last_exercise_datetime": "2026-06-09",
                    "exercise_year": 2026,
                    "exercise_month": 6,
                }
                for index, _ in enumerate(symbols)
            ]

        def wait_update(self, *, deadline: float) -> bool:
            return False

        def is_changing(self, quote: object, fields: object) -> bool:
            return False

    result = stream_quotes(
        FakeApi(),
        connection,
        cycles=2,
        wait_deadline_seconds=1,
    )

    assert result.symbol_count == 3
    assert result.quotes_written == 3
    assert result.changed_quotes_written == 0
    assert connection.execute("SELECT COUNT(*) FROM quote_current").fetchone()[0] == 3
    row = connection.execute(
        """
        SELECT expire_datetime, last_exercise_datetime, exercise_year, exercise_month
        FROM instruments
        WHERE symbol = 'DCE.a2601C100'
        """
    ).fetchone()
    assert tuple(row) == ("2026-06-10", "2026-06-09", 2026, 6)


def test_stream_quotes_honors_stop_requested_before_next_cycle() -> None:
    connection = sqlite3.connect(":memory:")
    InstrumentRepository(connection).upsert_instruments(
        normalize_option_chain_discovery(
            underlying_symbol="DCE.a2601",
            call_symbols=("DCE.a2601C100",),
            put_symbols=("DCE.a2601P100",),
            last_seen_at="2026-05-08T00:00:00+00:00",
        )
    )

    class FakeApi:
        def get_quote_list(self, symbols: list[str]) -> list[dict[str, object]]:
            return [{"datetime": "2026-05-09T00:00:00+00:00"} for _ in symbols]

        def wait_update(self, *, deadline: float) -> bool:
            raise AssertionError("wait_update should not run after stop is requested")

    result = stream_quotes(
        FakeApi(),
        connection,
        cycles=5,
        stop_requested=lambda: True,
    )

    assert result.cycles == 0
    assert result.quotes_written == 0
