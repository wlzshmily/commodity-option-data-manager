"""Stateful market collection runner built on durable collection batches."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
import sqlite3
from typing import Any

from .chain_collector import ChainCollectionResult, collect_persisted_option_chain
from .collection_plan import build_market_collection_plan
from .collection_state import (
    CollectionBatchRecord,
    CollectionPlanMaterializationSummary,
    CollectionStateRepository,
)


@dataclass(frozen=True)
class MarketBatchCollectionResult:
    """Collection result for one persisted option batch."""

    underlying_symbol: str
    batch_index: int
    option_symbols: tuple[str, ...]
    collection_batch_status: str
    run: Any | None
    option_count: int
    batch_count: int
    quotes_written: int
    kline_rows_written: int
    metrics_written: int
    error_count: int
    error: str | None


@dataclass(frozen=True)
class StatefulMarketCollectionResult:
    """Summary of one stateful market collection window."""

    started_at: str
    finished_at: str
    scope: str
    max_underlyings: int
    max_batches: int | None
    start_after_underlying: str | None
    end_before_underlying: str | None
    option_batch_size: int
    planned_underlyings: int
    planned_options: int
    planned_batches: int
    materialization: CollectionPlanMaterializationSummary
    selected_batch_count: int
    underlyings: tuple[str, ...]
    batches: tuple[MarketBatchCollectionResult, ...]


def collect_market_batches(
    api: Any,
    connection: sqlite3.Connection,
    *,
    option_batch_size: int,
    max_underlyings: int,
    max_batches: int | None = None,
    start_after_underlying: str | None,
    wait_cycles: int,
    iv_calculator: Callable[[Any, Any], Any] | None,
    end_before_underlying: str | None = None,
    scope: str = "windowed-market-current-slice",
) -> StatefulMarketCollectionResult:
    """Collect current slices by consuming durable non-stale collection batches."""

    if max_batches is not None and max_batches < 1:
        raise ValueError("max_batches must be positive when provided.")

    started_at = datetime.now(UTC).isoformat()
    plan = build_market_collection_plan(
        connection,
        option_batch_size=option_batch_size,
        max_underlyings=max_underlyings,
        start_after_underlying=start_after_underlying,
        end_before_underlying=end_before_underlying,
    )
    state_repo = CollectionStateRepository(connection)
    materialized = state_repo.materialize_plan(
        plan,
        scope=scope,
        updated_at=started_at,
    )
    pending_batches = state_repo.list_batches(
        scope=scope,
        statuses=("pending", "failed"),
        limit=max_batches,
    )
    results = tuple(
        _collect_batch(
            api,
            connection,
            state_repo=state_repo,
            scope=scope,
            batch=batch,
            wait_cycles=wait_cycles,
            iv_calculator=iv_calculator,
        )
        for batch in pending_batches
    )
    return StatefulMarketCollectionResult(
        started_at=started_at,
        finished_at=datetime.now(UTC).isoformat(),
        scope=scope,
        max_underlyings=max_underlyings,
        max_batches=max_batches,
        start_after_underlying=start_after_underlying,
        end_before_underlying=end_before_underlying,
        option_batch_size=option_batch_size,
        planned_underlyings=plan.underlying_count,
        planned_options=plan.option_count,
        planned_batches=plan.batch_count,
        materialization=materialized,
        selected_batch_count=len(pending_batches),
        underlyings=tuple(item.underlying_symbol for item in plan.underlyings),
        batches=results,
    )


def _collect_batch(
    api: Any,
    connection: sqlite3.Connection,
    *,
    state_repo: CollectionStateRepository,
    scope: str,
    batch: CollectionBatchRecord,
    wait_cycles: int,
    iv_calculator: Callable[[Any, Any], Any] | None,
) -> MarketBatchCollectionResult:
    state_repo.mark_started(
        scope=scope,
        underlying_symbol=batch.underlying_symbol,
        batch_index=batch.batch_index,
    )
    try:
        result = collect_persisted_option_chain(
            api,
            connection,
            underlying_symbol=batch.underlying_symbol,
            option_symbols=batch.option_symbols,
            batch_size=max(len(batch.option_symbols), 1),
            wait_cycles=wait_cycles,
            iv_calculator=iv_calculator,
        )
    except Exception as exc:
        failed = state_repo.mark_failed(
            scope=scope,
            underlying_symbol=batch.underlying_symbol,
            batch_index=batch.batch_index,
            error=f"{type(exc).__name__}: {exc}",
        )
        return _failed_batch_result(batch, failed.status, exc)

    if _batch_has_current_quotes(batch, result):
        batch_state = state_repo.mark_succeeded(
            scope=scope,
            underlying_symbol=batch.underlying_symbol,
            batch_index=batch.batch_index,
        )
    else:
        batch_state = state_repo.mark_failed(
            scope=scope,
            underlying_symbol=batch.underlying_symbol,
            batch_index=batch.batch_index,
            error=result.run.message or result.run.status,
        )
    return _successful_batch_result(batch, result, batch_state.status)


def _batch_has_current_quotes(
    batch: CollectionBatchRecord,
    result: ChainCollectionResult,
) -> bool:
    """Treat source-missing Greeks/IV as a quality gap, not a batch blocker."""

    return result.quotes_written >= len(batch.option_symbols)


def _successful_batch_result(
    batch: CollectionBatchRecord,
    result: ChainCollectionResult,
    batch_status: str,
) -> MarketBatchCollectionResult:
    return MarketBatchCollectionResult(
        underlying_symbol=result.underlying_symbol,
        batch_index=batch.batch_index,
        option_symbols=batch.option_symbols,
        collection_batch_status=batch_status,
        run=result.run,
        option_count=result.option_count,
        batch_count=result.batch_count,
        quotes_written=result.quotes_written,
        kline_rows_written=result.kline_rows_written,
        metrics_written=result.metrics_written,
        error_count=result.error_count,
        error=None,
    )


def _failed_batch_result(
    batch: CollectionBatchRecord,
    batch_status: str,
    exc: Exception,
) -> MarketBatchCollectionResult:
    return MarketBatchCollectionResult(
        underlying_symbol=batch.underlying_symbol,
        batch_index=batch.batch_index,
        option_symbols=batch.option_symbols,
        collection_batch_status=batch_status,
        run=None,
        option_count=len(batch.option_symbols),
        batch_count=1,
        quotes_written=0,
        kline_rows_written=0,
        metrics_written=0,
        error_count=1,
        error=f"{type(exc).__name__}: {exc}",
    )
