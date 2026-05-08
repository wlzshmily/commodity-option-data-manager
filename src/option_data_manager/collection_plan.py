"""Planning helpers for full-market current-slice collection."""

from __future__ import annotations

from dataclasses import dataclass
import sqlite3

from .instruments import InstrumentRepository


@dataclass(frozen=True)
class OptionBatchPlan:
    """One option-symbol batch for a single underlying futures contract."""

    underlying_symbol: str
    batch_index: int
    option_symbols: tuple[str, ...]


@dataclass(frozen=True)
class UnderlyingCollectionPlan:
    """Collection plan for one underlying and all its active options."""

    underlying_symbol: str
    exchange_id: str
    product_id: str | None
    call_count: int
    put_count: int
    option_count: int
    batches: tuple[OptionBatchPlan, ...]


@dataclass(frozen=True)
class MarketCollectionPlan:
    """Deterministic full-market collection plan."""

    underlying_count: int
    option_count: int
    batch_count: int
    underlyings: tuple[UnderlyingCollectionPlan, ...]


def build_market_collection_plan(
    connection: sqlite3.Connection,
    *,
    option_batch_size: int = 20,
    max_underlyings: int | None = None,
    start_after_underlying: str | None = None,
) -> MarketCollectionPlan:
    """Build a deterministic plan over active futures with active options."""

    if option_batch_size < 1:
        raise ValueError("option_batch_size must be positive.")
    if max_underlyings is not None and max_underlyings < 1:
        raise ValueError("max_underlyings must be positive when provided.")

    connection.row_factory = sqlite3.Row
    instrument_repo = InstrumentRepository(connection)
    underlyings = _active_option_underlyings(
        connection,
        start_after_underlying=start_after_underlying,
        max_underlyings=max_underlyings,
    )
    plans: list[UnderlyingCollectionPlan] = []
    for underlying in underlyings:
        options = instrument_repo.list_active_options(str(underlying["symbol"]))
        option_symbols = tuple(record.symbol for record in options)
        batches = tuple(
            OptionBatchPlan(
                underlying_symbol=str(underlying["symbol"]),
                batch_index=index,
                option_symbols=batch,
            )
            for index, batch in enumerate(
                _batch_symbols(option_symbols, option_batch_size),
                start=1,
            )
        )
        plans.append(
            UnderlyingCollectionPlan(
                underlying_symbol=str(underlying["symbol"]),
                exchange_id=str(underlying["exchange_id"]),
                product_id=underlying["product_id"],
                call_count=int(underlying["call_count"]),
                put_count=int(underlying["put_count"]),
                option_count=len(option_symbols),
                batches=batches,
            )
        )

    return MarketCollectionPlan(
        underlying_count=len(plans),
        option_count=sum(plan.option_count for plan in plans),
        batch_count=sum(len(plan.batches) for plan in plans),
        underlyings=tuple(plans),
    )


def _active_option_underlyings(
    connection: sqlite3.Connection,
    *,
    start_after_underlying: str | None,
    max_underlyings: int | None,
) -> list[sqlite3.Row]:
    cursor = connection.execute(
        """
        SELECT
            future.symbol,
            future.exchange_id,
            future.product_id,
            SUM(CASE WHEN option.option_class = 'CALL' THEN 1 ELSE 0 END) AS call_count,
            SUM(CASE WHEN option.option_class = 'PUT' THEN 1 ELSE 0 END) AS put_count
        FROM instruments AS future
        JOIN instruments AS option
          ON option.underlying_symbol = future.symbol
         AND option.active = 1
         AND option.option_class IN ('CALL', 'PUT')
        WHERE future.active = 1
          AND future.ins_class = 'FUTURE'
          AND (? IS NULL OR future.symbol > ?)
        GROUP BY future.symbol, future.exchange_id, future.product_id
        ORDER BY future.symbol
        """,
        (start_after_underlying, start_after_underlying),
    )
    rows = cursor.fetchall()
    if max_underlyings is not None:
        return rows[:max_underlyings]
    return rows


def _batch_symbols(
    symbols: tuple[str, ...],
    batch_size: int,
) -> tuple[tuple[str, ...], ...]:
    return tuple(
        symbols[index : index + batch_size]
        for index in range(0, len(symbols), batch_size)
    )
