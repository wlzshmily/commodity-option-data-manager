import sqlite3

from option_data_manager.collection_plan import build_market_collection_plan
from option_data_manager.collection_state import CollectionStateRepository
from option_data_manager.instruments import (
    InstrumentRepository,
    normalize_option_chain_discovery,
)


def test_materialize_plan_resets_interrupted_running_batches() -> None:
    connection = sqlite3.connect(":memory:")
    records = normalize_option_chain_discovery(
        underlying_symbol="SHFE.cu2606",
        call_symbols=("SHFE.cu2606C70000", "SHFE.cu2606C71000"),
        put_symbols=(),
        last_seen_at="2026-05-09T00:00:00+00:00",
    )
    InstrumentRepository(connection).upsert_instruments(records)
    plan = build_market_collection_plan(connection, option_batch_size=2)
    repository = CollectionStateRepository(connection)
    repository.materialize_plan(plan, scope="test")
    repository.mark_started(scope="test", underlying_symbol="SHFE.cu2606", batch_index=1)

    summary = repository.materialize_plan(plan, scope="test")
    batches = repository.list_batches(scope="test", statuses=("pending",))

    assert summary.reset_count == 1
    assert len(batches) == 1
    assert batches[0].status == "pending"
