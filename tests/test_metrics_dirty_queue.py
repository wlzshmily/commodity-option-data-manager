import sqlite3

from option_data_manager.instruments import InstrumentRepository, normalize_option_chain_discovery
from option_data_manager.metrics_dirty_queue import MetricsDirtyQueueRepository


def test_dirty_queue_coalesces_option_tasks() -> None:
    connection = sqlite3.connect(":memory:")
    queue = MetricsDirtyQueueRepository(connection)

    queue.mark_option_dirty(
        symbol="DCE.a2601C100",
        underlying_symbol="DCE.a2601",
        reason="option_quote_price_changed",
        dirty_at="2026-05-10T00:00:00+00:00",
    )
    queue.mark_option_dirty(
        symbol="DCE.a2601C100",
        underlying_symbol="DCE.a2601",
        reason="option_quote_price_changed",
        dirty_at="2026-05-10T00:00:01+00:00",
    )

    row = connection.execute("SELECT * FROM metrics_dirty_queue").fetchone()
    assert row["symbol"] == "DCE.a2601C100"
    assert row["dirty_count"] == 2
    assert row["status"] == "pending"


def test_underlying_chain_dirty_is_interval_limited() -> None:
    connection = sqlite3.connect(":memory:")
    InstrumentRepository(connection).upsert_instruments(
        normalize_option_chain_discovery(
            underlying_symbol="DCE.a2601",
            call_symbols=("DCE.a2601C100",),
            put_symbols=("DCE.a2601P100",),
            last_seen_at="2026-05-10T00:00:00+00:00",
        )
    )
    queue = MetricsDirtyQueueRepository(connection)

    first = queue.mark_underlying_chain_dirty(
        underlying_symbol="DCE.a2601",
        reason="underlying_quote_price_changed",
        dirty_at="2026-05-10T00:00:00+00:00",
        min_interval_seconds=30,
    )
    second = queue.mark_underlying_chain_dirty(
        underlying_symbol="DCE.a2601",
        reason="underlying_quote_price_changed",
        dirty_at="2026-05-10T00:00:10+00:00",
        min_interval_seconds=30,
    )

    assert first == 2
    assert second == 0
    assert queue.pending_count() == 2


def test_claim_and_complete_due_tasks() -> None:
    connection = sqlite3.connect(":memory:")
    queue = MetricsDirtyQueueRepository(connection)
    queue.mark_option_dirty(
        symbol="DCE.a2601C100",
        underlying_symbol="DCE.a2601",
        reason="option_quote_price_changed",
        dirty_at="2026-05-10T00:00:00+00:00",
    )

    tasks = queue.claim_due_tasks(now="2026-05-10T00:00:01+00:00", limit=10)

    assert len(tasks) == 1
    assert connection.execute(
        "SELECT status FROM metrics_dirty_queue WHERE symbol = 'DCE.a2601C100'"
    ).fetchone()[0] == "running"
    queue.complete_task(tasks[0])
    assert queue.pending_count() == 0
