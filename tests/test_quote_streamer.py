import sqlite3
import time

from option_data_manager.instruments import (
    InstrumentRepository,
    normalize_option_chain_discovery,
)
from option_data_manager.quote_streamer import (
    count_near_expiry_kline_symbols,
    count_near_expiry_quote_symbols,
    select_kline_symbols,
    select_quote_symbols,
    stream_quotes,
)


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


def test_select_kline_symbols_splits_option_workers_deterministically() -> None:
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

    worker_0 = select_kline_symbols(connection, worker_index=0, worker_count=2)
    worker_1 = select_kline_symbols(connection, worker_index=1, worker_count=2)

    assert set(worker_0).isdisjoint(worker_1)
    assert len(worker_0) + len(worker_1) == 6


def test_subscription_order_prioritizes_near_expiry_months() -> None:
    connection = sqlite3.connect(":memory:")
    repository = InstrumentRepository(connection)
    for underlying in ("DCE.c2603", "DCE.a2601", "DCE.b2602"):
        repository.upsert_instruments(
            normalize_option_chain_discovery(
                underlying_symbol=underlying,
                call_symbols=(f"{underlying}C100",),
                put_symbols=(f"{underlying}P100",),
                last_seen_at="2026-05-08T00:00:00+00:00",
            )
        )

    quote_symbols = select_quote_symbols(connection, include_futures=False)
    kline_symbols = select_kline_symbols(connection)

    assert quote_symbols[:2] == ["DCE.a2601C100", "DCE.a2601P100"]
    assert kline_symbols[:6] == [
        "DCE.a2601",
        "DCE.a2601C100",
        "DCE.a2601P100",
        "DCE.b2602",
        "DCE.b2602C100",
        "DCE.b2602P100",
    ]
    assert count_near_expiry_quote_symbols(
        connection,
        include_futures=False,
        near_expiry_months=2,
    ) == 4
    assert count_near_expiry_kline_symbols(
        connection,
        near_expiry_months=2,
    ) == 6


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
        def __init__(self) -> None:
            self.kline_data_lengths: list[int] = []
            self.kline_symbols: list[object] = []

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

        def get_kline_serial(
            self,
            symbol: object,
            *,
            duration_seconds: int,
            data_length: int,
        ) -> list[dict[str, object]]:
            self.kline_symbols.append(symbol)
            self.kline_data_lengths.append(data_length)
            return []

    progress_events: list[dict[str, object]] = []
    api = FakeApi()
    result = stream_quotes(
        api,
        connection,
        cycles=2,
        wait_deadline_seconds=1,
        progress_callback=progress_events.append,
    )

    assert result.symbol_count == 3
    assert result.kline_symbol_count == 3
    assert result.subscribed_quote_count == 3
    assert result.subscribed_kline_count == 3
    assert result.kline_data_length == 20
    assert api.kline_data_lengths == [20, 20, 20]
    assert api.kline_symbols == ["DCE.a2601", "DCE.a2601C100", "DCE.a2601P100"]
    assert result.quotes_written == 3
    assert result.changed_quotes_written == 0
    assert progress_events[-1]["status"] == "running"
    assert progress_events[-1]["subscribed_objects"] == 6
    assert progress_events[-1]["total_objects"] == 6
    assert progress_events[-1]["near_expiry_months"] == 2
    assert progress_events[-1]["near_expiry_subscribed"] == 6
    assert progress_events[-1]["near_expiry_total"] == 6
    assert progress_events[-1]["quote_started_at"] is not None
    assert progress_events[-1]["quote_finished_at"] is not None
    assert progress_events[-1]["kline_started_at"] is not None
    assert progress_events[-1]["cycle_count"] == 1
    assert progress_events[-1]["wait_update_count"] == 0
    assert progress_events[-1]["last_wait_update_at"] is None
    assert progress_events[-1]["last_quote_write_at"] is not None
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
        def __init__(self) -> None:
            self.kline_data_lengths: list[int] = []
            self.kline_symbols: list[object] = []

        def get_quote_list(self, symbols: list[str]) -> list[dict[str, object]]:
            return [{"datetime": "2026-05-09T00:00:00+00:00"} for _ in symbols]

        def wait_update(self, *, deadline: float) -> bool:
            raise AssertionError("wait_update should not run after stop is requested")

        def get_kline_serial(
            self,
            symbol: object,
            *,
            duration_seconds: int,
            data_length: int,
        ) -> list[dict[str, object]]:
            self.kline_symbols.append(symbol)
            self.kline_data_lengths.append(data_length)
            return []

    api = FakeApi()
    result = stream_quotes(
        api,
        connection,
        cycles=5,
        kline_data_length=5,
        stop_requested=lambda: True,
    )

    assert result.cycles == 0
    assert result.quotes_written == 0
    assert result.kline_data_length == 5
    assert api.kline_data_lengths == [5, 5, 5]
    assert api.kline_symbols == ["DCE.a2601", "DCE.a2601C100", "DCE.a2601P100"]


def test_stream_quotes_falls_back_when_kline_batch_subscription_fails() -> None:
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
        def __init__(self) -> None:
            self.kline_symbols: list[object] = []

        def get_quote_list(self, symbols: list[str]) -> list[dict[str, object]]:
            return [{"datetime": "2026-05-09T00:00:00+00:00"} for _ in symbols]

        def wait_update(self, *, deadline: float) -> bool:
            return False

        def is_changing(self, quote: object, fields: object) -> bool:
            return False

        def get_kline_serial(
            self,
            symbol: object,
            *,
            duration_seconds: int,
            data_length: int,
        ) -> list[dict[str, object]]:
            self.kline_symbols.append(symbol)
            if isinstance(symbol, list):
                raise RuntimeError("batch unavailable")
            return []

    api = FakeApi()
    result = stream_quotes(
        api,
        connection,
        cycles=1,
        kline_batch_size=50,
        wait_deadline_seconds=1,
    )

    assert result.subscribed_kline_count == 3
    assert result.error_count == 0
    assert api.kline_symbols == [
        ["DCE.a2601", "DCE.a2601C100", "DCE.a2601P100"],
        "DCE.a2601",
        "DCE.a2601C100",
        "DCE.a2601P100",
    ]


def test_stream_quotes_can_batch_kline_subscriptions_when_requested() -> None:
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
        def __init__(self) -> None:
            self.kline_symbols: list[object] = []

        def get_quote_list(self, symbols: list[str]) -> list[dict[str, object]]:
            return [{"datetime": "2026-05-09T00:00:00+00:00"} for _ in symbols]

        def wait_update(self, *, deadline: float) -> bool:
            return False

        def is_changing(self, quote: object, fields: object) -> bool:
            return False

        def get_kline_serial(
            self,
            symbol: object,
            *,
            duration_seconds: int,
            data_length: int,
        ) -> list[dict[str, object]]:
            self.kline_symbols.append(symbol)
            return []

    api = FakeApi()
    result = stream_quotes(
        api,
        connection,
        cycles=1,
        kline_batch_size=50,
        wait_deadline_seconds=1,
    )

    assert result.subscribed_kline_count == 3
    assert api.kline_symbols == [["DCE.a2601", "DCE.a2601C100", "DCE.a2601P100"]]


def test_stream_quotes_marks_dirty_metrics_without_blocking_quote_writes() -> None:
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
        def __init__(self) -> None:
            self.quotes: dict[str, dict[str, object]] = {}

        def get_quote_list(self, symbols: list[str]) -> list[dict[str, object]]:
            self.quotes = {
                symbol: {
                    "datetime": "2026-05-09T00:00:00+00:00",
                    "last_price": 1.0,
                    "price_tick": 0.5,
                }
                for symbol in symbols
            }
            return [self.quotes[symbol] for symbol in symbols]

        def wait_update(self, *, deadline: float) -> bool:
            for quote in self.quotes.values():
                quote["last_price"] = float(quote["last_price"]) + 1.0
            return True

        def is_changing(self, quote: object, fields: object) -> bool:
            return True

        def get_kline_serial(
            self,
            symbol: object,
            *,
            duration_seconds: int,
            data_length: int,
        ) -> list[dict[str, object]]:
            return []

    result = stream_quotes(
        FakeApi(),
        connection,
        cycles=2,
        wait_deadline_seconds=1,
        underlying_chain_dirty_interval_seconds=30,
    )

    assert result.quotes_written == 6
    rows = connection.execute(
        """
        SELECT symbol, dirty_count, status
        FROM metrics_dirty_queue
        ORDER BY symbol
        """
    ).fetchall()
    assert [(row["symbol"], row["dirty_count"], row["status"]) for row in rows] == [
        ("DCE.a2601C100", 2, "pending"),
        ("DCE.a2601P100", 2, "pending"),
    ]


def test_stream_quotes_records_tqsdk_connection_notifications() -> None:
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
            return True

        def is_changing(self, quote: object, fields: object) -> bool:
            return False

        def get_kline_serial(
            self,
            symbol: object,
            *,
            duration_seconds: int,
            data_length: int,
        ) -> list[dict[str, object]]:
            return []

    class FakeNotify:
        def __init__(self) -> None:
            self.calls = 0

        def get_notifies(self) -> list[dict[str, object]]:
            self.calls += 1
            if self.calls == 1:
                return [
                    {
                        "level": "WARNING",
                        "code": 2019112911,
                        "content": "与行情服务器的网络连接断开",
                    }
                ]
            return [
                {
                    "level": "WARNING",
                    "code": 2019112902,
                    "content": "与行情服务器的网络连接已恢复",
                }
            ]

    progress_events: list[dict[str, object]] = []
    fake_notify = FakeNotify()
    result = stream_quotes(
        FakeApi(),
        connection,
        cycles=2,
        wait_deadline_seconds=1,
        progress_callback=progress_events.append,
        tq_notify_factory=lambda api: fake_notify,
    )

    assert result.tqsdk_notify_count == 2
    assert result.tqsdk_connection_status == "connected"
    assert result.last_tqsdk_notify_code == 2019112902
    assert result.last_tqsdk_disconnect_at is not None
    assert result.last_tqsdk_restore_at is not None
    assert progress_events[-1]["tqsdk_notify_count"] == 2
    assert progress_events[-1]["tqsdk_connection_status"] == "connected"


def test_stream_quotes_reconciles_contract_universe_incrementally() -> None:
    connection = sqlite3.connect(":memory:")
    repository = InstrumentRepository(connection)
    repository.upsert_instruments(
        normalize_option_chain_discovery(
            underlying_symbol="DCE.a2601",
            call_symbols=("DCE.a2601C100",),
            put_symbols=("DCE.a2601P100",),
            last_seen_at="2026-05-11T00:00:00+08:00",
        )
    )

    class FakeApi:
        def __init__(self) -> None:
            self.quote_batches: list[list[str]] = []
            self.kline_symbols: list[object] = []

        def get_quote_list(self, symbols: list[str]) -> list[dict[str, object]]:
            self.quote_batches.append(list(symbols))
            return [
                {
                    "datetime": "2026-05-11T00:00:00+08:00",
                    "last_price": float(index + 1),
                }
                for index, _ in enumerate(symbols)
            ]

        def wait_update(self, *, deadline: float) -> bool:
            time.sleep(0.002)
            return True

        def is_changing(self, quote: object, fields: object) -> bool:
            return False

        def get_kline_serial(
            self,
            symbol: object,
            *,
            duration_seconds: int,
            data_length: int,
        ) -> list[dict[str, object]]:
            self.kline_symbols.append(symbol)
            return []

    refreshed = False

    def refresh_contracts() -> None:
        nonlocal refreshed
        if refreshed:
            return
        refreshed = True
        repository.upsert_instruments(
            normalize_option_chain_discovery(
                underlying_symbol="DCE.a2601",
                call_symbols=("DCE.a2601C200",),
                put_symbols=("DCE.a2601P100",),
                last_seen_at="2026-05-11T08:55:00+08:00",
            )
        )
        repository.mark_missing_inactive(
            underlying_symbol="DCE.a2601",
            seen_symbols={"DCE.a2601", "DCE.a2601C200", "DCE.a2601P100"},
            last_seen_at="2026-05-11T08:55:00+08:00",
        )

    api = FakeApi()
    result = stream_quotes(
        api,
        connection,
        cycles=3,
        wait_deadline_seconds=1,
        contract_refresh_callback=refresh_contracts,
        contract_refresh_interval_seconds=0.001,
    )

    assert api.quote_batches[0] == ["DCE.a2601", "DCE.a2601C100", "DCE.a2601P100"]
    assert api.quote_batches[1] == ["DCE.a2601C200"]
    assert result.subscribed_quote_count == 3
    assert result.subscribed_kline_count == 3
    assert result.contract_refresh_count >= 1
    assert result.contract_reconcile_added_quote_count == 1
    assert result.contract_reconcile_removed_quote_count == 1
    assert result.contract_reconcile_added_kline_count == 1
    assert result.contract_reconcile_removed_kline_count == 1
    assert repository.get_instrument("DCE.a2601C100").active is False
    assert repository.get_instrument("DCE.a2601C200").active is True
