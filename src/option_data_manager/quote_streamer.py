"""Long-lived quote-only subscription worker."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import sqlite3
import time
from collections.abc import Callable
from typing import Any

from .instruments import InstrumentRepository
from .quotes import QUOTE_SOURCE_FIELDS, QuoteRepository, normalize_quote


DEFAULT_QUOTE_SHARD_SIZE = 1_000


@dataclass(frozen=True)
class QuoteStreamResult:
    """Summary of one bounded or manually stopped quote stream run."""

    started_at: str
    finished_at: str
    worker_index: int
    worker_count: int
    symbol_count: int
    quote_shard_size: int
    cycles: int
    wait_update_count: int
    quotes_written: int
    changed_quotes_written: int
    error_count: int


def stream_quotes(
    api: Any,
    connection: sqlite3.Connection,
    *,
    worker_index: int = 0,
    worker_count: int = 1,
    quote_shard_size: int = DEFAULT_QUOTE_SHARD_SIZE,
    max_symbols: int | None = None,
    cycles: int | None = None,
    duration_seconds: float | None = None,
    wait_deadline_seconds: float = 1.0,
    include_futures: bool = True,
    include_options: bool = True,
    stop_requested: Callable[[], bool] | None = None,
) -> QuoteStreamResult:
    """Keep quote references alive and write changed current quotes.

    `cycles` or `duration_seconds` can bound the worker for smoke tests and
    acceptance windows. When both are omitted, the loop is intentionally
    long-lived until interrupted by the operator.
    """

    _validate_worker(worker_index, worker_count)
    if quote_shard_size < 1:
        raise ValueError("quote_shard_size must be positive.")
    if max_symbols is not None and max_symbols < 1:
        raise ValueError("max_symbols must be positive when provided.")
    if cycles is not None and cycles < 1:
        raise ValueError("cycles must be positive when provided.")
    if duration_seconds is not None and duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive when provided.")
    if wait_deadline_seconds <= 0:
        raise ValueError("wait_deadline_seconds must be positive.")
    if not include_futures and not include_options:
        raise ValueError("At least one symbol class must be included.")

    started_at = datetime.now(UTC).isoformat()
    symbols = select_quote_symbols(
        connection,
        worker_index=worker_index,
        worker_count=worker_count,
        max_symbols=max_symbols,
        include_futures=include_futures,
        include_options=include_options,
    )
    if not symbols:
        raise ValueError("No active quote symbols are available for this worker shard.")
    repository = QuoteRepository(connection)
    instrument_repository = InstrumentRepository(connection)
    quote_refs = _quote_refs(api, symbols, quote_shard_size=quote_shard_size)
    deadline_at = time.monotonic() + duration_seconds if duration_seconds else None
    cycle_count = 0
    wait_update_count = 0
    quotes_written = 0
    changed_quotes_written = 0
    error_count = 0

    while True:
        if stop_requested is not None and stop_requested():
            break
        if cycles is not None and cycle_count >= cycles:
            break
        if deadline_at is not None and time.monotonic() >= deadline_at:
            break
        cycle_count += 1
        if api.wait_update(deadline=time.time() + wait_deadline_seconds):
            wait_update_count += 1
        received_at = datetime.now(UTC).isoformat()
        for symbol, quote_ref in quote_refs.items():
            should_write = cycle_count == 1 or _is_changing(api, quote_ref)
            if not should_write:
                continue
            try:
                repository.upsert_quote(
                    normalize_quote(symbol, quote_ref, received_at=received_at)
                )
                instrument_repository.update_tqsdk_quote_fields(symbol, quote_ref)
                quotes_written += 1
                if cycle_count > 1:
                    changed_quotes_written += 1
            except Exception:
                error_count += 1

    return QuoteStreamResult(
        started_at=started_at,
        finished_at=datetime.now(UTC).isoformat(),
        worker_index=worker_index,
        worker_count=worker_count,
        symbol_count=len(symbols),
        quote_shard_size=quote_shard_size,
        cycles=cycle_count,
        wait_update_count=wait_update_count,
        quotes_written=quotes_written,
        changed_quotes_written=changed_quotes_written,
        error_count=error_count,
    )


def select_quote_symbols(
    connection: sqlite3.Connection,
    *,
    worker_index: int = 0,
    worker_count: int = 1,
    max_symbols: int | None = None,
    include_futures: bool = True,
    include_options: bool = True,
) -> list[str]:
    """Return a deterministic disjoint symbol shard for one quote worker."""

    _validate_worker(worker_index, worker_count)
    class_predicates: list[str] = []
    if include_futures:
        class_predicates.append("ins_class = 'FUTURE'")
    if include_options:
        class_predicates.append("option_class IN ('CALL', 'PUT')")
    if not class_predicates:
        return []
    rows = connection.execute(
        f"""
        SELECT symbol
        FROM instruments
        WHERE active = 1
          AND ({' OR '.join(class_predicates)})
        ORDER BY exchange_id, product_id, symbol
        """
    ).fetchall()
    symbols = [
        str(row["symbol"] if isinstance(row, sqlite3.Row) else row[0])
        for row in rows
    ]
    shard = symbols[worker_index::worker_count]
    if max_symbols is not None:
        return shard[:max_symbols]
    return shard


def _quote_refs(
    api: Any,
    symbols: list[str],
    *,
    quote_shard_size: int,
) -> dict[str, Any]:
    refs: dict[str, Any] = {}
    for index in range(0, len(symbols), quote_shard_size):
        batch = symbols[index : index + quote_shard_size]
        if not batch:
            continue
        refs.update(zip(batch, _quote_list(api, batch), strict=True))
    return refs


def _quote_list(api: Any, symbols: list[str]) -> list[Any]:
    get_quote_list = getattr(api, "get_quote_list", None)
    if callable(get_quote_list):
        try:
            quote_list = get_quote_list(list(symbols))
            if len(quote_list) == len(symbols):
                return list(quote_list)
        except (AttributeError, TypeError):
            pass
    return [api.get_quote(symbol) for symbol in symbols]


def _is_changing(api: Any, quote_ref: Any) -> bool:
    is_changing = getattr(api, "is_changing", None)
    if not callable(is_changing):
        return True
    try:
        return bool(is_changing(quote_ref, QUOTE_SOURCE_FIELDS))
    except TypeError:
        return bool(is_changing(quote_ref))


def _validate_worker(worker_index: int, worker_count: int) -> None:
    if worker_count < 1:
        raise ValueError("worker_count must be positive.")
    if worker_index < 0 or worker_index >= worker_count:
        raise ValueError("worker_index must be in [0, worker_count).")
