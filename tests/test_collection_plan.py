import sqlite3

from option_data_manager.collection_plan import build_market_collection_plan
from option_data_manager.instruments import (
    InstrumentRepository,
    normalize_option_chain_discovery,
)


def test_market_plan_batches_active_options_by_underlying() -> None:
    connection = sqlite3.connect(":memory:")
    records = normalize_option_chain_discovery(
        underlying_symbol="SHFE.cu2606",
        call_symbols=("SHFE.cu2606C70000", "SHFE.cu2606C71000"),
        put_symbols=("SHFE.cu2606P70000",),
        last_seen_at="2026-05-08T00:00:00+00:00",
    )
    InstrumentRepository(connection).upsert_instruments(records)

    plan = build_market_collection_plan(connection, option_batch_size=2)

    assert plan.underlying_count == 1
    assert plan.option_count == 3
    assert plan.batch_count == 2
    assert plan.underlyings[0].underlying_symbol == "SHFE.cu2606"

