from datetime import UTC, datetime
import sqlite3
from typing import Any

from option_data_manager.acquisition import AcquisitionRepository
from option_data_manager.chain_collector import ChainCollectionResult
from option_data_manager.instruments import (
    InstrumentRepository,
    normalize_option_chain_discovery,
)
from option_data_manager.market_collector import collect_market_batches


def test_collect_market_batches_honors_max_batches(monkeypatch: Any) -> None:
    connection = sqlite3.connect(":memory:")
    records = normalize_option_chain_discovery(
        underlying_symbol="SHFE.cu2606",
        call_symbols=(
            "SHFE.cu2606C70000",
            "SHFE.cu2606C71000",
            "SHFE.cu2606C72000",
        ),
        put_symbols=("SHFE.cu2606P70000",),
        last_seen_at="2026-05-08T00:00:00+00:00",
    )
    InstrumentRepository(connection).upsert_instruments(records)
    collected: list[tuple[str, ...] | None] = []

    def fake_collect(
        api: Any,
        conn: sqlite3.Connection,
        *,
        underlying_symbol: str,
        option_symbols: tuple[str, ...] | None = None,
        batch_size: int = 20,
        wait_cycles: int = 2,
        received_at: str | None = None,
        iv_calculator: Any = None,
    ) -> ChainCollectionResult:
        collected.append(option_symbols)
        run = AcquisitionRepository(conn).start_run(
            trigger="test",
            started_at=datetime.now(UTC).isoformat(),
        )
        run = AcquisitionRepository(conn).finish_run(
            run.run_id,
            status="success",
            symbols_discovered=len(option_symbols or ()),
            quotes_written=len(option_symbols or ()),
            message="ok",
        )
        return ChainCollectionResult(
            run=run,
            underlying_symbol=underlying_symbol,
            option_count=len(option_symbols or ()),
            batch_count=1,
            quotes_written=len(option_symbols or ()),
            kline_rows_written=0,
            metrics_written=0,
            error_count=0,
        )

    monkeypatch.setattr(
        "option_data_manager.market_collector.collect_persisted_option_chain",
        fake_collect,
    )

    result = collect_market_batches(
        object(),
        connection,
        option_batch_size=2,
        max_underlyings=1,
        max_batches=1,
        start_after_underlying=None,
        wait_cycles=0,
        iv_calculator=None,
        scope="test-window",
    )

    assert result.planned_batches == 2
    assert result.selected_batch_count == 1
    assert len(result.batches) == 1
    assert len(collected) == 1


def test_collect_market_batches_keeps_batch_success_for_metric_only_errors(
    monkeypatch: Any,
) -> None:
    connection = sqlite3.connect(":memory:")
    records = normalize_option_chain_discovery(
        underlying_symbol="SHFE.cu2606",
        call_symbols=("SHFE.cu2606C70000", "SHFE.cu2606C71000"),
        put_symbols=(),
        last_seen_at="2026-05-08T00:00:00+00:00",
    )
    InstrumentRepository(connection).upsert_instruments(records)

    def fake_collect(
        api: Any,
        conn: sqlite3.Connection,
        *,
        underlying_symbol: str,
        option_symbols: tuple[str, ...] | None = None,
        batch_size: int = 20,
        wait_cycles: int = 2,
        received_at: str | None = None,
        iv_calculator: Any = None,
    ) -> ChainCollectionResult:
        symbols = option_symbols or ()
        run = AcquisitionRepository(conn).start_run(
            trigger="test",
            started_at=datetime.now(UTC).isoformat(),
        )
        run = AcquisitionRepository(conn).finish_run(
            run.run_id,
            status="partial_failure",
            symbols_discovered=len(symbols),
            quotes_written=len(symbols),
            error_count=1,
            message="metric source unavailable",
        )
        return ChainCollectionResult(
            run=run,
            underlying_symbol=underlying_symbol,
            option_count=len(symbols),
            batch_count=1,
            quotes_written=len(symbols),
            kline_rows_written=0,
            metrics_written=len(symbols) - 1,
            error_count=1,
        )

    monkeypatch.setattr(
        "option_data_manager.market_collector.collect_persisted_option_chain",
        fake_collect,
    )

    result = collect_market_batches(
        object(),
        connection,
        option_batch_size=2,
        max_underlyings=1,
        max_batches=1,
        start_after_underlying=None,
        wait_cycles=0,
        iv_calculator=None,
        scope="test-window",
    )

    assert result.batches[0].collection_batch_status == "success"
