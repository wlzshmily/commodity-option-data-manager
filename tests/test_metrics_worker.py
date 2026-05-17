import sqlite3
from types import SimpleNamespace
from typing import Any

from option_data_manager.cli import metrics_worker
from option_data_manager.metrics_dirty_queue import MetricsDirtyQueueRepository


def test_metrics_worker_groups_dirty_tasks_by_underlying(monkeypatch: Any) -> None:
    connection = sqlite3.connect(":memory:")
    queue = MetricsDirtyQueueRepository(connection)
    for symbol, underlying in (
        ("DCE.a2601C100", "DCE.a2601"),
        ("DCE.a2601P100", "DCE.a2601"),
        ("DCE.b2601C100", "DCE.b2601"),
    ):
        queue.mark_option_dirty(
            symbol=symbol,
            underlying_symbol=underlying,
            reason="test",
            dirty_at="2026-05-10T00:00:00+00:00",
        )

    calls: list[tuple[str, tuple[str, ...], int]] = []

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
    ) -> SimpleNamespace:
        calls.append((underlying_symbol, option_symbols or (), batch_size))
        return SimpleNamespace(error_count=0)

    monkeypatch.setattr(metrics_worker, "collect_persisted_option_chain", fake_collect)
    monkeypatch.setattr(metrics_worker, "_create_option_impv_calculator", lambda: None)

    result = metrics_worker.run_metrics_worker(
        object(),
        connection,
        cycles=1,
        poll_interval_seconds=0,
        claim_limit=10,
    )

    assert result.completed_count == 3
    assert queue.pending_count() == 0
    assert calls == [
        ("DCE.a2601", ("DCE.a2601C100", "DCE.a2601P100"), 2),
        ("DCE.b2601", ("DCE.b2601C100",), 1),
    ]


def test_metrics_worker_retries_retryable_sqlite_locks(monkeypatch: Any) -> None:
    connection = sqlite3.connect(":memory:")
    MetricsDirtyQueueRepository(connection)
    original_claim = MetricsDirtyQueueRepository.claim_due_tasks
    calls = {"count": 0}

    def flaky_claim(self, *, now: str, limit: int):
        calls["count"] += 1
        if calls["count"] == 1:
            raise sqlite3.OperationalError("database is locked")
        return original_claim(self, now=now, limit=limit)

    monkeypatch.setattr(MetricsDirtyQueueRepository, "claim_due_tasks", flaky_claim)
    monkeypatch.setattr(metrics_worker, "_create_option_impv_calculator", lambda: None)

    result = metrics_worker.run_metrics_worker(
        object(),
        connection,
        cycles=1,
        poll_interval_seconds=0,
        claim_limit=10,
    )

    assert result.sqlite_retry_count == 1
    assert result.last_sqlite_retry_at is not None
    assert calls["count"] == 2
