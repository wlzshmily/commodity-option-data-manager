"""Long-lived quote and kline subscription worker."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
import re
import sqlite3
import time
from collections.abc import Callable
from typing import Any

from .instruments import InstrumentRepository
from .metrics_dirty_queue import MetricsDirtyQueueRepository
from .quotes import QUOTE_SOURCE_FIELDS, QuoteRepository, normalize_quote
from .quotes import QuoteRecord


DEFAULT_QUOTE_SHARD_SIZE = 1_000
DEFAULT_KLINE_BATCH_SIZE = 1
SECONDS_PER_DAY = 24 * 60 * 60
DEFAULT_KLINE_DATA_LENGTH = 20
DEFAULT_METRICS_DIRTY_MIN_INTERVAL_SECONDS = 30
DEFAULT_UNDERLYING_CHAIN_DIRTY_INTERVAL_SECONDS = 30
DEFAULT_NEAR_EXPIRY_MONTHS = 2
DEFAULT_CONTRACT_MONTH_LIMIT = 2
DEFAULT_MIN_DAYS_TO_EXPIRY = 1
DEFAULT_CONTRACT_REFRESH_INTERVAL_SECONDS = 30 * 60
_FAR_EXPIRY_KEY = (9999, 99)
_MONTH_PATTERN = re.compile(r"(\d{3,4})")


@dataclass(frozen=True)
class QuoteStreamResult:
    """Summary of one bounded or manually stopped quote stream run."""

    started_at: str
    finished_at: str
    worker_index: int
    worker_count: int
    symbol_count: int
    kline_symbol_count: int
    subscribed_quote_count: int
    subscribed_kline_count: int
    quote_shard_size: int
    kline_data_length: int
    near_expiry_months: int
    contract_month_limit: int | None
    min_days_to_expiry: int
    near_expiry_quote_count: int
    near_expiry_kline_count: int
    cycles: int
    wait_update_count: int
    quotes_written: int
    changed_quotes_written: int
    last_wait_update_at: str | None
    last_quote_write_at: str | None
    last_tqsdk_notify_at: str | None
    last_tqsdk_notify_code: int | None
    last_tqsdk_notify_level: str | None
    last_tqsdk_notify_content: str | None
    tqsdk_connection_status: str
    last_tqsdk_disconnect_at: str | None
    last_tqsdk_restore_at: str | None
    tqsdk_notify_count: int
    contract_refresh_count: int
    last_contract_refresh_at: str | None
    last_contract_reconcile_at: str | None
    contract_reconcile_added_quote_count: int
    contract_reconcile_removed_quote_count: int
    contract_reconcile_added_kline_count: int
    contract_reconcile_removed_kline_count: int
    error_count: int


def stream_quotes(
    api: Any,
    connection: sqlite3.Connection,
    *,
    worker_index: int = 0,
    worker_count: int = 1,
    quote_shard_size: int = DEFAULT_QUOTE_SHARD_SIZE,
    kline_batch_size: int = DEFAULT_KLINE_BATCH_SIZE,
    kline_data_length: int = DEFAULT_KLINE_DATA_LENGTH,
    metrics_dirty_min_interval_seconds: int = DEFAULT_METRICS_DIRTY_MIN_INTERVAL_SECONDS,
    underlying_chain_dirty_interval_seconds: int = DEFAULT_UNDERLYING_CHAIN_DIRTY_INTERVAL_SECONDS,
    max_symbols: int | None = None,
    cycles: int | None = None,
    duration_seconds: float | None = None,
    wait_deadline_seconds: float = 1.0,
    include_futures: bool = True,
    include_options: bool = True,
    include_klines: bool = True,
    prioritize_near_expiry: bool = True,
    near_expiry_months: int = DEFAULT_NEAR_EXPIRY_MONTHS,
    contract_month_limit: int | None = DEFAULT_CONTRACT_MONTH_LIMIT,
    min_days_to_expiry: int = DEFAULT_MIN_DAYS_TO_EXPIRY,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    stop_requested: Callable[[], bool] | None = None,
    tq_notify_factory: Callable[[Any], Any] | None = None,
    contract_refresh_callback: Callable[[], Any] | None = None,
    contract_refresh_interval_seconds: float | None = None,
) -> QuoteStreamResult:
    """Keep quote references alive and write changed current quotes.

    `cycles` or `duration_seconds` can bound the worker for smoke tests and
    acceptance windows. When both are omitted, the loop is intentionally
    long-lived until interrupted by the operator.
    """

    _validate_worker(worker_index, worker_count)
    if quote_shard_size < 1:
        raise ValueError("quote_shard_size must be positive.")
    if kline_batch_size < 1:
        raise ValueError("kline_batch_size must be positive.")
    if kline_data_length < 1:
        raise ValueError("kline_data_length must be positive.")
    if metrics_dirty_min_interval_seconds < 1:
        raise ValueError("metrics_dirty_min_interval_seconds must be positive.")
    if underlying_chain_dirty_interval_seconds < 1:
        raise ValueError("underlying_chain_dirty_interval_seconds must be positive.")
    if max_symbols is not None and max_symbols < 1:
        raise ValueError("max_symbols must be positive when provided.")
    if cycles is not None and cycles < 1:
        raise ValueError("cycles must be positive when provided.")
    if duration_seconds is not None and duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive when provided.")
    if wait_deadline_seconds <= 0:
        raise ValueError("wait_deadline_seconds must be positive.")
    if near_expiry_months < 1:
        raise ValueError("near_expiry_months must be positive.")
    if contract_month_limit is not None and contract_month_limit < 1:
        raise ValueError("contract_month_limit must be positive when provided.")
    if min_days_to_expiry < 0:
        raise ValueError("min_days_to_expiry must be non-negative.")
    if (
        contract_refresh_interval_seconds is not None
        and contract_refresh_interval_seconds <= 0
    ):
        raise ValueError("contract_refresh_interval_seconds must be positive.")
    if not include_futures and not include_options:
        raise ValueError("At least one symbol class must be included.")

    started_at = datetime.now(UTC).isoformat()
    tqsdk_notify_state = _TqsdkNotifyState()
    tq_notify = _create_tq_notify(api, tq_notify_factory)
    _emit_progress(
        progress_callback,
        status="initializing",
        started_at=started_at,
        quote_started_at=None,
        quote_finished_at=None,
        kline_started_at=None,
        worker_index=worker_index,
        worker_count=worker_count,
        quote_subscribed=0,
        quote_total=0,
        kline_subscribed=0,
        kline_total=0,
        near_expiry_months=near_expiry_months,
        near_expiry_quote_subscribed=0,
        near_expiry_quote_total=0,
        near_expiry_kline_subscribed=0,
        near_expiry_kline_total=0,
        contract_month_limit=contract_month_limit,
        tq_notify_state=tqsdk_notify_state,
    )
    symbols = select_quote_symbols(
        connection,
        worker_index=worker_index,
        worker_count=worker_count,
        max_symbols=max_symbols,
        include_futures=include_futures,
        include_options=include_options,
        prioritize_near_expiry=prioritize_near_expiry,
        contract_month_limit=contract_month_limit,
        min_days_to_expiry=min_days_to_expiry,
    )
    if not symbols:
        raise ValueError("No active quote symbols are available for this worker shard.")
    kline_symbols = (
        select_kline_symbols(
            connection,
            worker_index=worker_index,
            worker_count=worker_count,
            max_symbols=max_symbols,
            prioritize_near_expiry=prioritize_near_expiry,
            contract_month_limit=contract_month_limit,
            min_days_to_expiry=min_days_to_expiry,
        )
        if include_options and include_klines
        else []
    )
    near_quote_total = (
        count_near_expiry_quote_symbols(
            connection,
            worker_index=worker_index,
            worker_count=worker_count,
            max_symbols=max_symbols,
            include_futures=include_futures,
            include_options=include_options,
            prioritize_near_expiry=prioritize_near_expiry,
            near_expiry_months=near_expiry_months,
            contract_month_limit=contract_month_limit,
            min_days_to_expiry=min_days_to_expiry,
        )
        if prioritize_near_expiry
        else 0
    )
    near_kline_total = (
        count_near_expiry_kline_symbols(
            connection,
            worker_index=worker_index,
            worker_count=worker_count,
            max_symbols=max_symbols,
            prioritize_near_expiry=prioritize_near_expiry,
            near_expiry_months=near_expiry_months,
            contract_month_limit=contract_month_limit,
            min_days_to_expiry=min_days_to_expiry,
        )
        if include_options and include_klines and prioritize_near_expiry
        else 0
    )
    repository = QuoteRepository(connection)
    instrument_repository = InstrumentRepository(connection)
    metrics_dirty_queue = MetricsDirtyQueueRepository(connection)
    symbol_metadata = _quote_symbol_metadata(connection, symbols)
    error_count = 0
    quote_refs: dict[str, Any] = {}
    kline_refs: dict[str, Any] = {}
    quote_started_at = datetime.now(UTC).isoformat()
    _emit_progress(
        progress_callback,
        status="quote_subscribing",
        started_at=started_at,
        quote_started_at=quote_started_at,
        quote_finished_at=None,
        kline_started_at=None,
        worker_index=worker_index,
        worker_count=worker_count,
        quote_subscribed=0,
        quote_total=len(symbols),
        kline_subscribed=0,
        kline_total=len(kline_symbols),
        near_expiry_months=near_expiry_months,
        near_expiry_quote_subscribed=0,
        near_expiry_quote_total=near_quote_total,
        near_expiry_kline_subscribed=0,
        near_expiry_kline_total=near_kline_total,
        contract_month_limit=contract_month_limit,
        tq_notify_state=tqsdk_notify_state,
    )
    for index in range(0, len(symbols), quote_shard_size):
        batch = symbols[index : index + quote_shard_size]
        if not batch:
            continue
        refs, batch_error_count = _subscribe_quote_ref_batch(api, batch)
        quote_refs.update(refs)
        error_count += batch_error_count
        _emit_progress(
            progress_callback,
            status="quote_subscribing",
            started_at=started_at,
            quote_started_at=quote_started_at,
            quote_finished_at=None,
            kline_started_at=None,
            worker_index=worker_index,
            worker_count=worker_count,
            quote_subscribed=len(quote_refs),
            quote_total=len(symbols),
            kline_subscribed=0,
            kline_total=len(kline_symbols),
            near_expiry_months=near_expiry_months,
            near_expiry_quote_subscribed=min(len(quote_refs), near_quote_total),
            near_expiry_quote_total=near_quote_total,
            near_expiry_kline_subscribed=0,
            near_expiry_kline_total=near_kline_total,
            contract_month_limit=contract_month_limit,
            tq_notify_state=tqsdk_notify_state,
        )
    quote_finished_at = datetime.now(UTC).isoformat()
    kline_started_at = datetime.now(UTC).isoformat()
    kline_progress_step = max(1, min(100, len(kline_symbols) // 100 or 1))
    next_kline_progress_at = kline_progress_step
    attempted_kline_count = 0
    for batch in _batches(kline_symbols, kline_batch_size):
        attempted_kline_count += len(batch)
        refs, batch_error_count = _subscribe_kline_ref_batch(
            api,
            batch,
            data_length=kline_data_length,
        )
        kline_refs.update(refs)
        error_count += batch_error_count
        if (
            attempted_kline_count == len(kline_symbols)
            or attempted_kline_count >= next_kline_progress_at
        ):
            while next_kline_progress_at <= attempted_kline_count:
                next_kline_progress_at += kline_progress_step
            _emit_progress(
                progress_callback,
                status="kline_subscribing",
                started_at=started_at,
                quote_started_at=quote_started_at,
                quote_finished_at=quote_finished_at,
                kline_started_at=kline_started_at,
                worker_index=worker_index,
                worker_count=worker_count,
                quote_subscribed=len(quote_refs),
                quote_total=len(symbols),
                kline_subscribed=len(kline_refs),
                kline_total=len(kline_symbols),
                near_expiry_months=near_expiry_months,
                near_expiry_quote_subscribed=near_quote_total,
                near_expiry_quote_total=near_quote_total,
                near_expiry_kline_subscribed=min(len(kline_refs), near_kline_total),
                near_expiry_kline_total=near_kline_total,
                contract_month_limit=contract_month_limit,
                tq_notify_state=tqsdk_notify_state,
            )
    _emit_progress(
        progress_callback,
        status="running",
        started_at=started_at,
        quote_started_at=quote_started_at,
        quote_finished_at=quote_finished_at,
        kline_started_at=kline_started_at,
        worker_index=worker_index,
        worker_count=worker_count,
        quote_subscribed=len(quote_refs),
        quote_total=len(symbols),
        kline_subscribed=len(kline_refs),
        kline_total=len(kline_symbols),
        near_expiry_months=near_expiry_months,
        near_expiry_quote_subscribed=near_quote_total,
        near_expiry_quote_total=near_quote_total,
        near_expiry_kline_subscribed=min(len(kline_refs), near_kline_total),
        near_expiry_kline_total=near_kline_total,
        contract_month_limit=contract_month_limit,
        tq_notify_state=tqsdk_notify_state,
    )
    deadline_at = time.monotonic() + duration_seconds if duration_seconds else None
    cycle_count = 0
    wait_update_count = 0
    quotes_written = 0
    changed_quotes_written = 0
    last_wait_update_at: str | None = None
    last_quote_write_at: str | None = None
    next_progress_heartbeat_at = time.monotonic()
    next_contract_refresh_at = (
        time.monotonic() + contract_refresh_interval_seconds
        if contract_refresh_interval_seconds is not None
        else None
    )
    contract_refresh_count = 0
    last_contract_refresh_at: str | None = None
    last_contract_reconcile_at: str | None = None
    reconcile_added_quote_count = 0
    reconcile_removed_quote_count = 0
    reconcile_added_kline_count = 0
    reconcile_removed_kline_count = 0

    while True:
        if stop_requested is not None and stop_requested():
            break
        if cycles is not None and cycle_count >= cycles:
            break
        if deadline_at is not None and time.monotonic() >= deadline_at:
            break
        cycle_count += 1
        wait_update_succeeded = api.wait_update(deadline=time.time() + wait_deadline_seconds)
        if wait_update_succeeded:
            wait_update_count += 1
            last_wait_update_at = datetime.now(UTC).isoformat()
        tqsdk_notify_state.consume(tq_notify)
        now_monotonic = time.monotonic()
        if (
            next_contract_refresh_at is not None
            and now_monotonic >= next_contract_refresh_at
        ):
            next_contract_refresh_at = now_monotonic + float(
                contract_refresh_interval_seconds or 0
            )
            if contract_refresh_callback is not None:
                try:
                    contract_refresh_callback()
                    contract_refresh_count += 1
                    last_contract_refresh_at = datetime.now(UTC).isoformat()
                except Exception:
                    error_count += 1
            reconcile = _reconcile_subscriptions(
                api,
                connection,
                quote_refs=quote_refs,
                kline_refs=kline_refs,
                worker_index=worker_index,
                worker_count=worker_count,
                max_symbols=max_symbols,
                include_futures=include_futures,
                include_options=include_options,
                include_klines=include_klines,
                prioritize_near_expiry=prioritize_near_expiry,
                near_expiry_months=near_expiry_months,
                contract_month_limit=contract_month_limit,
                min_days_to_expiry=min_days_to_expiry,
                quote_shard_size=quote_shard_size,
                kline_batch_size=kline_batch_size,
                kline_data_length=kline_data_length,
            )
            symbols = reconcile["symbols"]
            kline_symbols = reconcile["kline_symbols"]
            near_quote_total = reconcile["near_quote_total"]
            near_kline_total = reconcile["near_kline_total"]
            symbol_metadata = _quote_symbol_metadata(connection, symbols)
            reconcile_added_quote_count += reconcile["added_quote_count"]
            reconcile_removed_quote_count += reconcile["removed_quote_count"]
            reconcile_added_kline_count += reconcile["added_kline_count"]
            reconcile_removed_kline_count += reconcile["removed_kline_count"]
            error_count += reconcile["error_count"]
            if (
                reconcile["added_quote_count"]
                or reconcile["removed_quote_count"]
                or reconcile["added_kline_count"]
                or reconcile["removed_kline_count"]
            ):
                last_contract_reconcile_at = datetime.now(UTC).isoformat()
        received_at = datetime.now(UTC).isoformat()
        wrote_quote_this_cycle = False
        for symbol, quote_ref in quote_refs.items():
            should_write = cycle_count == 1 or _is_changing(api, quote_ref)
            if not should_write:
                continue
            try:
                previous_quote = repository.get_quote(symbol)
                quote = normalize_quote(symbol, quote_ref, received_at=received_at)
                repository.upsert_quote(quote)
                instrument_repository.update_tqsdk_quote_fields(symbol, quote_ref)
                if previous_quote is not None and _quote_price_changed(
                    previous_quote,
                    quote,
                ):
                    _mark_metrics_dirty(
                        metrics_dirty_queue,
                        symbol_metadata=symbol_metadata,
                        symbol=symbol,
                        dirty_at=received_at,
                        underlying_chain_interval_seconds=(
                            underlying_chain_dirty_interval_seconds
                        ),
                    )
                quotes_written += 1
                wrote_quote_this_cycle = True
                if cycle_count > 1:
                    changed_quotes_written += 1
            except Exception:
                error_count += 1
        if wrote_quote_this_cycle:
            last_quote_write_at = received_at
        now_monotonic = time.monotonic()
        if (
            progress_callback is not None
            and (
                cycle_count == 1
                or wait_update_succeeded
                or now_monotonic >= next_progress_heartbeat_at
            )
        ):
            next_progress_heartbeat_at = now_monotonic + 5
            _emit_progress(
                progress_callback,
                status="running",
                started_at=started_at,
                quote_started_at=quote_started_at,
                quote_finished_at=quote_finished_at,
                kline_started_at=kline_started_at,
                worker_index=worker_index,
                worker_count=worker_count,
                quote_subscribed=len(quote_refs),
                quote_total=len(symbols),
                kline_subscribed=len(kline_refs),
                kline_total=len(kline_symbols),
                near_expiry_months=near_expiry_months,
                near_expiry_quote_subscribed=near_quote_total,
                near_expiry_quote_total=near_quote_total,
                near_expiry_kline_subscribed=min(
                    len(kline_refs),
                    near_kline_total,
                ),
                near_expiry_kline_total=near_kline_total,
                contract_month_limit=contract_month_limit,
                cycle_count=cycle_count,
                wait_update_count=wait_update_count,
                quotes_written=quotes_written,
                changed_quotes_written=changed_quotes_written,
                last_wait_update_at=last_wait_update_at,
                last_quote_write_at=last_quote_write_at,
                tq_notify_state=tqsdk_notify_state,
                contract_refresh_count=contract_refresh_count,
                last_contract_refresh_at=last_contract_refresh_at,
                last_contract_reconcile_at=last_contract_reconcile_at,
                contract_reconcile_added_quote_count=reconcile_added_quote_count,
                contract_reconcile_removed_quote_count=reconcile_removed_quote_count,
                contract_reconcile_added_kline_count=reconcile_added_kline_count,
                contract_reconcile_removed_kline_count=reconcile_removed_kline_count,
            )

    return QuoteStreamResult(
        started_at=started_at,
        finished_at=datetime.now(UTC).isoformat(),
        worker_index=worker_index,
        worker_count=worker_count,
        symbol_count=len(symbols),
        kline_symbol_count=len(kline_symbols),
        subscribed_quote_count=len(quote_refs),
        subscribed_kline_count=len(kline_refs),
        quote_shard_size=quote_shard_size,
        kline_data_length=kline_data_length,
        near_expiry_months=near_expiry_months,
        contract_month_limit=contract_month_limit,
        min_days_to_expiry=min_days_to_expiry,
        near_expiry_quote_count=near_quote_total,
        near_expiry_kline_count=near_kline_total,
        cycles=cycle_count,
        wait_update_count=wait_update_count,
        quotes_written=quotes_written,
        changed_quotes_written=changed_quotes_written,
        last_wait_update_at=last_wait_update_at,
        last_quote_write_at=last_quote_write_at,
        last_tqsdk_notify_at=tqsdk_notify_state.last_notify_at,
        last_tqsdk_notify_code=tqsdk_notify_state.last_notify_code,
        last_tqsdk_notify_level=tqsdk_notify_state.last_notify_level,
        last_tqsdk_notify_content=tqsdk_notify_state.last_notify_content,
        tqsdk_connection_status=tqsdk_notify_state.connection_status,
        last_tqsdk_disconnect_at=tqsdk_notify_state.last_disconnect_at,
        last_tqsdk_restore_at=tqsdk_notify_state.last_restore_at,
        tqsdk_notify_count=tqsdk_notify_state.notify_count,
        contract_refresh_count=contract_refresh_count,
        last_contract_refresh_at=last_contract_refresh_at,
        last_contract_reconcile_at=last_contract_reconcile_at,
        contract_reconcile_added_quote_count=reconcile_added_quote_count,
        contract_reconcile_removed_quote_count=reconcile_removed_quote_count,
        contract_reconcile_added_kline_count=reconcile_added_kline_count,
        contract_reconcile_removed_kline_count=reconcile_removed_kline_count,
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
    prioritize_near_expiry: bool = True,
    contract_month_limit: int | None = DEFAULT_CONTRACT_MONTH_LIMIT,
    min_days_to_expiry: int = DEFAULT_MIN_DAYS_TO_EXPIRY,
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
    rows = _quote_subscription_rows(
        connection,
        class_predicates=class_predicates,
        prioritize_near_expiry=prioritize_near_expiry,
        contract_month_limit=contract_month_limit,
        min_days_to_expiry=min_days_to_expiry,
    )
    symbols = [
        str(row["symbol"])
        for row in rows
    ]
    shard = symbols[worker_index::worker_count]
    if max_symbols is not None:
        return shard[:max_symbols]
    return shard


def select_kline_symbols(
    connection: sqlite3.Connection,
    *,
    worker_index: int = 0,
    worker_count: int = 1,
    max_symbols: int | None = None,
    prioritize_near_expiry: bool = True,
    contract_month_limit: int | None = DEFAULT_CONTRACT_MONTH_LIMIT,
    min_days_to_expiry: int = DEFAULT_MIN_DAYS_TO_EXPIRY,
) -> list[str]:
    """Return de-duplicated single-symbol realtime kline subscriptions."""

    _validate_worker(worker_index, worker_count)
    rows = _kline_subscription_rows(
        connection,
        prioritize_near_expiry=prioritize_near_expiry,
        contract_month_limit=contract_month_limit,
        min_days_to_expiry=min_days_to_expiry,
    )
    symbols = [str(row["symbol"]) for row in rows]
    shard = symbols[worker_index::worker_count]
    if max_symbols is not None:
        return shard[:max_symbols]
    return shard


def count_near_expiry_quote_symbols(
    connection: sqlite3.Connection,
    *,
    worker_index: int = 0,
    worker_count: int = 1,
    max_symbols: int | None = None,
    include_futures: bool = True,
    include_options: bool = True,
    prioritize_near_expiry: bool = True,
    near_expiry_months: int = DEFAULT_NEAR_EXPIRY_MONTHS,
    contract_month_limit: int | None = DEFAULT_CONTRACT_MONTH_LIMIT,
    min_days_to_expiry: int = DEFAULT_MIN_DAYS_TO_EXPIRY,
) -> int:
    """Count this shard's objects before the first N expiry-month boundary."""

    _validate_worker(worker_index, worker_count)
    class_predicates: list[str] = []
    if include_futures:
        class_predicates.append("ins_class = 'FUTURE'")
    if include_options:
        class_predicates.append("option_class IN ('CALL', 'PUT')")
    if not class_predicates:
        return 0
    rows = _quote_subscription_rows(
        connection,
        class_predicates=class_predicates,
        prioritize_near_expiry=prioritize_near_expiry,
        contract_month_limit=contract_month_limit,
        min_days_to_expiry=min_days_to_expiry,
    )
    return _near_expiry_count_for_shard(
        rows,
        worker_index=worker_index,
        worker_count=worker_count,
        max_symbols=max_symbols,
        near_expiry_months=near_expiry_months,
        min_days_to_expiry=min_days_to_expiry,
    )


def count_near_expiry_kline_symbols(
    connection: sqlite3.Connection,
    *,
    worker_index: int = 0,
    worker_count: int = 1,
    max_symbols: int | None = None,
    prioritize_near_expiry: bool = True,
    near_expiry_months: int = DEFAULT_NEAR_EXPIRY_MONTHS,
    contract_month_limit: int | None = DEFAULT_CONTRACT_MONTH_LIMIT,
    min_days_to_expiry: int = DEFAULT_MIN_DAYS_TO_EXPIRY,
) -> int:
    """Count this shard's Kline objects before the first N expiry-month boundary."""

    _validate_worker(worker_index, worker_count)
    rows = _kline_subscription_rows(
        connection,
        prioritize_near_expiry=prioritize_near_expiry,
        contract_month_limit=contract_month_limit,
        min_days_to_expiry=min_days_to_expiry,
    )
    return _near_expiry_count_for_shard(
        rows,
        worker_index=worker_index,
        worker_count=worker_count,
        max_symbols=max_symbols,
        near_expiry_months=near_expiry_months,
        min_days_to_expiry=min_days_to_expiry,
    )


def selected_underlying_contract_symbols(
    connection: sqlite3.Connection,
    *,
    contract_month_limit: int | None = DEFAULT_CONTRACT_MONTH_LIMIT,
    min_days_to_expiry: int = DEFAULT_MIN_DAYS_TO_EXPIRY,
    prioritize_near_expiry: bool = True,
) -> set[str]:
    """Return underlying contracts covered by the realtime subscription scope."""

    rows = _quote_subscription_rows(
        connection,
        class_predicates=["option_class IN ('CALL', 'PUT')"],
        prioritize_near_expiry=prioritize_near_expiry,
        contract_month_limit=contract_month_limit,
        min_days_to_expiry=min_days_to_expiry,
    )
    return {
        _underlying_contract_symbol(row)
        for row in rows
        if _underlying_contract_symbol(row)
    }


def expected_subscription_counts(
    connection: sqlite3.Connection,
    *,
    worker_count: int = 1,
    max_symbols: int | None = None,
    contract_month_limit: int | None = DEFAULT_CONTRACT_MONTH_LIMIT,
    min_days_to_expiry: int = DEFAULT_MIN_DAYS_TO_EXPIRY,
    prioritize_near_expiry: bool = True,
) -> dict[str, int]:
    """Return expected full-scope subscription object counts from SQLite."""

    quote_total = 0
    kline_total = 0
    for worker_index in range(max(worker_count, 1)):
        quote_total += len(
            select_quote_symbols(
                connection,
                worker_index=worker_index,
                worker_count=max(worker_count, 1),
                max_symbols=max_symbols,
                prioritize_near_expiry=prioritize_near_expiry,
                contract_month_limit=contract_month_limit,
                min_days_to_expiry=min_days_to_expiry,
            )
        )
        kline_total += len(
            select_kline_symbols(
                connection,
                worker_index=worker_index,
                worker_count=max(worker_count, 1),
                max_symbols=max_symbols,
                prioritize_near_expiry=prioritize_near_expiry,
                contract_month_limit=contract_month_limit,
                min_days_to_expiry=min_days_to_expiry,
            )
        )
    return {
        "quote_total": quote_total,
        "kline_total": kline_total,
        "total_objects": quote_total + kline_total,
    }


def _batches(symbols: list[str], batch_size: int) -> list[list[str]]:
    return [
        symbols[index : index + batch_size]
        for index in range(0, len(symbols), batch_size)
    ]


def _subscribe_kline_batch(
    api: Any,
    symbols: list[str],
    *,
    data_length: int,
) -> tuple[list[Any], int, int]:
    """Subscribe one Kline symbol batch, falling back to singles on failure."""

    if not symbols:
        return ([], 0, 0)
    try:
        symbol_arg: str | list[str] = symbols[0] if len(symbols) == 1 else list(symbols)
        return (
            [
                api.get_kline_serial(
                    symbol_arg,
                    duration_seconds=SECONDS_PER_DAY,
                    data_length=data_length,
                )
            ],
            len(symbols),
            0,
        )
    except Exception:
        refs: list[Any] = []
        if len(symbols) == 1:
            return (refs, 0, 1)
        error_count = 0
        for symbol in symbols:
            try:
                refs.append(
                    api.get_kline_serial(
                        symbol,
                        duration_seconds=SECONDS_PER_DAY,
                        data_length=data_length,
                    )
                )
            except Exception:
                error_count += 1
        return (refs, len(refs), error_count)


def _subscribe_kline_ref_batch(
    api: Any,
    symbols: list[str],
    *,
    data_length: int,
) -> tuple[dict[str, Any], int]:
    """Subscribe Kline symbols and retain a symbol-to-reference map."""

    if not symbols:
        return ({}, 0)
    try:
        symbol_arg: str | list[str] = symbols[0] if len(symbols) == 1 else list(symbols)
        ref = api.get_kline_serial(
            symbol_arg,
            duration_seconds=SECONDS_PER_DAY,
            data_length=data_length,
        )
        return ({symbol: ref for symbol in symbols}, 0)
    except Exception:
        refs: dict[str, Any] = {}
        error_count = 0
        for symbol in symbols:
            try:
                refs[symbol] = api.get_kline_serial(
                    symbol,
                    duration_seconds=SECONDS_PER_DAY,
                    data_length=data_length,
                )
            except Exception:
                error_count += 1
        return (refs, error_count)


def _reconcile_subscriptions(
    api: Any,
    connection: sqlite3.Connection,
    *,
    quote_refs: dict[str, Any],
    kline_refs: dict[str, Any],
    worker_index: int,
    worker_count: int,
    max_symbols: int | None,
    include_futures: bool,
    include_options: bool,
    include_klines: bool,
    prioritize_near_expiry: bool,
    near_expiry_months: int,
    contract_month_limit: int | None,
    min_days_to_expiry: int,
    quote_shard_size: int,
    kline_batch_size: int,
    kline_data_length: int,
) -> dict[str, Any]:
    symbols = select_quote_symbols(
        connection,
        worker_index=worker_index,
        worker_count=worker_count,
        max_symbols=max_symbols,
        include_futures=include_futures,
        include_options=include_options,
        prioritize_near_expiry=prioritize_near_expiry,
        contract_month_limit=contract_month_limit,
        min_days_to_expiry=min_days_to_expiry,
    )
    kline_symbols = (
        select_kline_symbols(
            connection,
            worker_index=worker_index,
            worker_count=worker_count,
            max_symbols=max_symbols,
            prioritize_near_expiry=prioritize_near_expiry,
            contract_month_limit=contract_month_limit,
            min_days_to_expiry=min_days_to_expiry,
        )
        if include_options and include_klines
        else []
    )
    target_quotes = set(symbols)
    target_klines = set(kline_symbols)
    removed_quote_count = 0
    for symbol in list(quote_refs):
        if symbol not in target_quotes:
            quote_refs.pop(symbol, None)
            removed_quote_count += 1
    removed_kline_count = 0
    for symbol in list(kline_refs):
        if symbol not in target_klines:
            kline_refs.pop(symbol, None)
            removed_kline_count += 1
    added_quote_symbols = [symbol for symbol in symbols if symbol not in quote_refs]
    added_quote_count = 0
    quote_error_count = 0
    for batch in _batches(added_quote_symbols, quote_shard_size):
        refs, batch_error_count = _subscribe_quote_ref_batch(api, batch)
        quote_refs.update(refs)
        added_quote_count += len(refs)
        quote_error_count += batch_error_count
    added_kline_count = 0
    for batch in _batches(
        [symbol for symbol in kline_symbols if symbol not in kline_refs],
        kline_batch_size,
    ):
        refs, _error_count = _subscribe_kline_ref_batch(
            api,
            batch,
            data_length=kline_data_length,
        )
        kline_refs.update(refs)
        added_kline_count += len(refs)
    near_quote_total = (
        count_near_expiry_quote_symbols(
            connection,
            worker_index=worker_index,
            worker_count=worker_count,
            max_symbols=max_symbols,
            include_futures=include_futures,
            include_options=include_options,
            prioritize_near_expiry=prioritize_near_expiry,
            near_expiry_months=near_expiry_months,
            contract_month_limit=contract_month_limit,
            min_days_to_expiry=min_days_to_expiry,
        )
        if prioritize_near_expiry
        else 0
    )
    near_kline_total = (
        count_near_expiry_kline_symbols(
            connection,
            worker_index=worker_index,
            worker_count=worker_count,
            max_symbols=max_symbols,
            prioritize_near_expiry=prioritize_near_expiry,
            near_expiry_months=near_expiry_months,
            contract_month_limit=contract_month_limit,
            min_days_to_expiry=min_days_to_expiry,
        )
        if include_options and include_klines and prioritize_near_expiry
        else 0
    )
    return {
        "symbols": symbols,
        "kline_symbols": kline_symbols,
        "near_quote_total": near_quote_total,
        "near_kline_total": near_kline_total,
        "added_quote_count": added_quote_count,
        "removed_quote_count": removed_quote_count,
        "added_kline_count": added_kline_count,
        "removed_kline_count": removed_kline_count,
        "error_count": quote_error_count,
    }


def _quote_subscription_rows(
    connection: sqlite3.Connection,
    *,
    class_predicates: list[str],
    prioritize_near_expiry: bool,
    contract_month_limit: int | None,
    min_days_to_expiry: int,
) -> list[dict[str, Any]]:
    predicate_sql = " OR ".join(
        predicate.replace("ins_class", "i.ins_class").replace(
            "option_class",
            "i.option_class",
        )
        for predicate in class_predicates
    )
    quote_current_available = _table_exists(connection, "quote_current")
    quote_join_sql = (
        """
        LEFT JOIN quote_current iq ON iq.symbol = i.symbol
        LEFT JOIN quote_current fq ON fq.symbol = COALESCE(i.underlying_symbol, i.symbol)
        """
        if quote_current_available
        else ""
    )
    quote_expire_rest_sql = (
        "CAST(json_extract(iq.raw_payload_json, '$.expire_rest_days') AS INTEGER)"
        if quote_current_available
        else "NULL"
    )
    quote_expire_datetime_sql = (
        "NULLIF(json_extract(iq.raw_payload_json, '$.expire_datetime'), '')"
        if quote_current_available
        else "NULL"
    )
    future_quote_expire_rest_sql = (
        "CAST(json_extract(fq.raw_payload_json, '$.expire_rest_days') AS INTEGER)"
        if quote_current_available
        else "NULL"
    )
    future_quote_expire_datetime_sql = (
        "NULLIF(json_extract(fq.raw_payload_json, '$.expire_datetime'), '')"
        if quote_current_available
        else "NULL"
    )
    cursor = _execute_read_with_retry(
        connection,
        f"""
        SELECT
            i.symbol,
            i.exchange_id,
            i.product_id,
            i.instrument_id,
            i.ins_class,
            i.option_class,
            i.underlying_symbol,
            i.delivery_year,
            i.delivery_month,
            i.exercise_year,
            i.exercise_month,
            COALESCE(
                CAST(json_extract(i.raw_payload_json, '$.expire_rest_days') AS INTEGER),
                {quote_expire_rest_sql}
            ) AS expire_rest_days,
            COALESCE(NULLIF(i.expire_datetime, ''), {quote_expire_datetime_sql}) AS expire_datetime,
            COALESCE(NULLIF(future.expire_datetime, ''), {future_quote_expire_datetime_sql}) AS underlying_expire_datetime,
            COALESCE(
                CAST(json_extract(future.raw_payload_json, '$.expire_rest_days') AS INTEGER),
                {future_quote_expire_rest_sql}
            ) AS underlying_expire_rest_days
        FROM instruments i
        LEFT JOIN instruments future ON future.symbol = i.underlying_symbol
        {quote_join_sql}
        WHERE i.active = 1
          AND ({predicate_sql})
        """
    )
    rows = _dict_rows(cursor)
    rows = _sort_subscription_rows(rows, prioritize_near_expiry=prioritize_near_expiry)
    return _limit_rows_by_expiry_months(
        rows,
        contract_month_limit,
        min_days_to_expiry=min_days_to_expiry,
    )


def _kline_subscription_rows(
    connection: sqlite3.Connection,
    *,
    prioritize_near_expiry: bool,
    contract_month_limit: int | None,
    min_days_to_expiry: int,
) -> list[dict[str, Any]]:
    quote_current_available = _table_exists(connection, "quote_current")
    quote_join_sql = (
        """
        LEFT JOIN quote_current iq ON iq.symbol = i.symbol
        LEFT JOIN quote_current fq ON fq.symbol = i.underlying_symbol
        """
        if quote_current_available
        else ""
    )
    quote_expire_rest_sql = (
        "CAST(json_extract(iq.raw_payload_json, '$.expire_rest_days') AS INTEGER)"
        if quote_current_available
        else "NULL"
    )
    quote_expire_datetime_sql = (
        "NULLIF(json_extract(iq.raw_payload_json, '$.expire_datetime'), '')"
        if quote_current_available
        else "NULL"
    )
    future_quote_expire_rest_sql = (
        "CAST(json_extract(fq.raw_payload_json, '$.expire_rest_days') AS INTEGER)"
        if quote_current_available
        else "NULL"
    )
    future_quote_expire_datetime_sql = (
        "NULLIF(json_extract(fq.raw_payload_json, '$.expire_datetime'), '')"
        if quote_current_available
        else "NULL"
    )
    cursor = _execute_read_with_retry(
        connection,
        f"""
        SELECT
            i.symbol,
            i.exchange_id,
            i.product_id,
            i.instrument_id,
            i.ins_class,
            i.option_class,
            i.underlying_symbol,
            i.delivery_year,
            i.delivery_month,
            i.exercise_year,
            i.exercise_month,
            COALESCE(
                CAST(json_extract(i.raw_payload_json, '$.expire_rest_days') AS INTEGER),
                {quote_expire_rest_sql}
            ) AS expire_rest_days,
            COALESCE(NULLIF(i.expire_datetime, ''), {quote_expire_datetime_sql}) AS expire_datetime,
            COALESCE(NULLIF(future.expire_datetime, ''), {future_quote_expire_datetime_sql}) AS underlying_expire_datetime,
            COALESCE(
                CAST(json_extract(future.raw_payload_json, '$.expire_rest_days') AS INTEGER),
                {future_quote_expire_rest_sql}
            ) AS underlying_expire_rest_days
        FROM instruments i
        LEFT JOIN instruments future ON future.symbol = i.underlying_symbol
        {quote_join_sql}
        WHERE i.active = 1
          AND i.option_class IN ('CALL', 'PUT')
          AND i.underlying_symbol IS NOT NULL
          AND TRIM(i.underlying_symbol) <> ''
        """
    )
    option_rows = _dict_rows(cursor)
    rows: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()
    for row in option_rows:
        underlying_symbol = str(row["underlying_symbol"] or "").strip()
        if underlying_symbol and underlying_symbol not in seen_symbols:
            underlying_row = dict(row)
            underlying_row["symbol"] = underlying_symbol
            underlying_row["instrument_id"] = underlying_symbol.split(".", maxsplit=1)[-1]
            underlying_row["ins_class"] = "FUTURE"
            underlying_row["option_class"] = None
            underlying_row["underlying_symbol"] = underlying_symbol
            rows.append(underlying_row)
            seen_symbols.add(underlying_symbol)
        option_symbol = str(row["symbol"] or "").strip()
        if option_symbol and option_symbol not in seen_symbols:
            rows.append(row)
            seen_symbols.add(option_symbol)
    rows = _sort_subscription_rows(rows, prioritize_near_expiry=prioritize_near_expiry)
    return _limit_rows_by_expiry_months(
        rows,
        contract_month_limit,
        min_days_to_expiry=min_days_to_expiry,
    )


def _dict_rows(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    columns = [column[0] for column in cursor.description or ()]
    rows = cursor.fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, sqlite3.Row):
            result.append(dict(row))
        else:
            result.append({column: row[index] for index, column in enumerate(columns)})
    return result


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = _execute_read_with_retry(
        connection,
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def _execute_read_with_retry(
    connection: sqlite3.Connection,
    sql: str,
    parameters: tuple[Any, ...] = (),
    *,
    attempts: int = 6,
    delay_seconds: float = 2.0,
) -> sqlite3.Cursor:
    for attempt in range(attempts):
        try:
            return connection.execute(sql, parameters)
        except sqlite3.OperationalError as exc:
            if "database is locked" not in str(exc).lower() or attempt == attempts - 1:
                raise
            time.sleep(delay_seconds)
    raise RuntimeError("unreachable sqlite retry state")


def _sort_subscription_rows(
    rows: list[dict[str, Any]],
    *,
    prioritize_near_expiry: bool,
) -> list[dict[str, Any]]:
    if prioritize_near_expiry:
        return sorted(rows, key=_subscription_sort_key)
    return sorted(
        rows,
        key=lambda row: (
            str(row["exchange_id"] or ""),
            str(row["product_id"] or ""),
            str(row["symbol"] or ""),
        ),
    )


def _limit_rows_by_expiry_months(
    rows: list[dict[str, Any]],
    contract_month_limit: int | None,
    *,
    min_days_to_expiry: int,
) -> list[dict[str, Any]]:
    if contract_month_limit is None:
        if min_days_to_expiry <= 0:
            return rows
        valid_contracts = _valid_underlying_contracts(
            rows,
            min_days_to_expiry=min_days_to_expiry,
        )
        if not valid_contracts:
            return []
        return [
            row
            for row in rows
            if _underlying_contract_scope_key(row) in valid_contracts
        ]
    if contract_month_limit < 1:
        return []
    allowed_contracts = _allowed_underlying_contracts(
        rows,
        contract_month_limit=contract_month_limit,
        min_days_to_expiry=min_days_to_expiry,
    )
    if not allowed_contracts:
        return []
    return [
        row
        for row in rows
        if _underlying_contract_scope_key(row) in allowed_contracts
    ]


def _allowed_underlying_contracts(
    rows: list[dict[str, Any]],
    *,
    contract_month_limit: int,
    min_days_to_expiry: int = DEFAULT_MIN_DAYS_TO_EXPIRY,
) -> set[tuple[str, str, str]]:
    candidates_by_group: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
    for scope_key, row in _valid_underlying_contract_rows(
        rows,
        min_days_to_expiry=min_days_to_expiry,
    ).items():
        exchange_id, product_id, contract_symbol = scope_key
        candidates_by_group.setdefault((exchange_id, product_id), {}).setdefault(
            contract_symbol,
            row,
        )
    allowed: set[tuple[str, str, str]] = set()
    for (exchange_id, product_id), contract_rows in candidates_by_group.items():
        selected = sorted(
            contract_rows.items(),
            key=lambda item: _underlying_contract_sort_key(item[1]),
        )[:contract_month_limit]
        for contract_symbol, _row in selected:
            allowed.add((exchange_id, product_id, contract_symbol))
    return allowed


def _valid_underlying_contracts(
    rows: list[dict[str, Any]],
    *,
    min_days_to_expiry: int,
) -> set[tuple[str, str, str]]:
    return set(
        _valid_underlying_contract_rows(
            rows,
            min_days_to_expiry=min_days_to_expiry,
        )
    )


def _valid_underlying_contract_rows(
    rows: list[dict[str, Any]],
    *,
    min_days_to_expiry: int,
) -> dict[tuple[str, str, str], dict[str, Any]]:
    option_rows = [row for row in rows if row.get("option_class") in {"CALL", "PUT"}]
    option_groups = {
        (str(row.get("exchange_id") or ""), str(row.get("product_id") or ""))
        for row in option_rows
    }
    candidate_rows = option_rows + [
        row
        for row in rows
        if row.get("option_class") not in {"CALL", "PUT"}
        and (
            str(row.get("exchange_id") or ""),
            str(row.get("product_id") or ""),
        )
        not in option_groups
    ]
    rows_by_contract: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in candidate_rows:
        contract_symbol = _underlying_contract_symbol(row)
        if not contract_symbol:
            continue
        group_key = (
            str(row.get("exchange_id") or ""),
            str(row.get("product_id") or ""),
        )
        rows_by_contract.setdefault((*group_key, contract_symbol), []).append(row)
    candidates: dict[tuple[str, str, str], dict[str, Any]] = {}
    for scope_key, contract_rows in rows_by_contract.items():
        remaining_values = [
            remaining_days
            for remaining_days in (
                _contract_remaining_days(row) for row in contract_rows
            )
            if remaining_days is not None
        ]
        if remaining_values and min(remaining_values) < min_days_to_expiry:
            continue
        candidates[scope_key] = sorted(
            contract_rows,
            key=_subscription_sort_key,
        )[0]
    return candidates


def _contract_months_progress_value(contract_month_limit: int | None) -> str:
    return "all" if contract_month_limit is None else str(contract_month_limit)


def _subscription_sort_key(row: dict[str, Any]) -> tuple[object, ...]:
    return (
        _underlying_contract_sort_key(row),
        0 if row.get("option_class") not in {"CALL", "PUT"} else 1,
        _expiry_key(row),
        str(row["exchange_id"] or ""),
        str(row["product_id"] or ""),
        str(row["underlying_symbol"] or ""),
        str(row["symbol"] or ""),
    )


def _near_expiry_count_for_shard(
    rows: list[dict[str, Any]],
    *,
    worker_index: int,
    worker_count: int,
    max_symbols: int | None,
    near_expiry_months: int,
    min_days_to_expiry: int,
) -> int:
    if near_expiry_months < 1:
        return 0
    allowed_contracts = _allowed_underlying_contracts(
        rows,
        contract_month_limit=near_expiry_months,
        min_days_to_expiry=min_days_to_expiry,
    )
    if not allowed_contracts:
        return 0
    shard = rows[worker_index::worker_count]
    if max_symbols is not None:
        shard = shard[:max_symbols]
    return sum(
        1 for row in shard if _underlying_contract_scope_key(row) in allowed_contracts
    )


def _underlying_contract_scope_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("exchange_id") or ""),
        str(row.get("product_id") or ""),
        _underlying_contract_symbol(row),
    )


def _underlying_contract_symbol(row: dict[str, Any]) -> str:
    underlying_symbol = str(row.get("underlying_symbol") or "").strip()
    if underlying_symbol:
        return underlying_symbol
    return str(row.get("symbol") or "").strip()


def _underlying_contract_sort_key(row: dict[str, Any]) -> tuple[object, ...]:
    contract_symbol = _underlying_contract_symbol(row)
    return (
        _contract_month_key(contract_symbol),
        str(row.get("exchange_id") or ""),
        str(row.get("product_id") or ""),
        contract_symbol,
    )


def _contract_remaining_days(row: dict[str, Any]) -> int | None:
    values: list[int] = []
    for key in ("underlying_expire_rest_days", "expire_rest_days"):
        value = _optional_int_value(row.get(key))
        if value is not None:
            values.append(value)
    for key in ("underlying_expire_datetime", "expire_datetime"):
        parsed = _parse_expiry_date(row.get(key))
        if parsed is not None:
            values.append((parsed - date.today()).days)
    return min(values) if values else None


def _parse_expiry_date(value: object) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        try:
            return datetime.fromtimestamp(float(text), UTC).date()
        except (OverflowError, OSError, ValueError):
            return None
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).date()
    except ValueError:
        pass
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _contract_month_key(symbol: str) -> tuple[int, int]:
    match = _MONTH_PATTERN.search(symbol)
    if match:
        return _month_key_from_digits(match.group(1))
    return _FAR_EXPIRY_KEY


def _expiry_key(row: dict[str, Any]) -> tuple[int, int]:
    for year_key, month_key in (
        ("exercise_year", "exercise_month"),
        ("delivery_year", "delivery_month"),
    ):
        year = _optional_int_value(row[year_key])
        month = _optional_int_value(row[month_key])
        if year is not None and month is not None:
            return (year, month)
    expire_datetime = str(row["expire_datetime"] or "")
    if len(expire_datetime) >= 7:
        try:
            return (int(expire_datetime[:4]), int(expire_datetime[5:7]))
        except ValueError:
            pass
    match = _MONTH_PATTERN.search(str(row["instrument_id"] or row["symbol"] or ""))
    if match:
        return _month_key_from_digits(match.group(1))
    return _FAR_EXPIRY_KEY


def _month_key_from_digits(digits: str) -> tuple[int, int]:
    if len(digits) == 3:
        year = 2020 + int(digits[:1])
        month = int(digits[1:3])
    else:
        year = 2000 + int(digits[:2])
        month = int(digits[2:4])
    if 1 <= month <= 12:
        return (year, month)
    return _FAR_EXPIRY_KEY


def _optional_int_value(value: object) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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


def _subscribe_quote_ref_batch(api: Any, symbols: list[str]) -> tuple[dict[str, Any], int]:
    """Subscribe one Quote batch, falling back to single symbols on source timeouts."""

    if not symbols:
        return ({}, 0)
    try:
        return (dict(zip(symbols, _quote_list(api, symbols), strict=True)), 0)
    except Exception:
        refs: dict[str, Any] = {}
        error_count = 0
        for symbol in symbols:
            try:
                refs[symbol] = api.get_quote(symbol)
            except Exception:
                error_count += 1
        return (refs, error_count)


def _is_changing(api: Any, quote_ref: Any) -> bool:
    is_changing = getattr(api, "is_changing", None)
    if not callable(is_changing):
        return True
    try:
        return bool(is_changing(quote_ref, QUOTE_SOURCE_FIELDS))
    except TypeError:
        return bool(is_changing(quote_ref))


def _quote_symbol_metadata(
    connection: sqlite3.Connection,
    symbols: list[str],
) -> dict[str, dict[str, str | None]]:
    if not symbols:
        return {}
    placeholders = ",".join("?" for _ in symbols)
    rows = connection.execute(
        f"""
        SELECT symbol, ins_class, option_class, underlying_symbol
        FROM instruments
        WHERE symbol IN ({placeholders})
        """,
        symbols,
    ).fetchall()
    return {
        str(row["symbol"]): {
            "ins_class": row["ins_class"],
            "option_class": row["option_class"],
            "underlying_symbol": row["underlying_symbol"],
        }
        for row in rows
    }


def _quote_price_changed(previous: QuoteRecord, current: QuoteRecord) -> bool:
    if _changed_price(previous.last_price, current.last_price):
        return True
    previous_mid = _best_mid_price(previous)
    current_mid = _best_mid_price(current)
    if previous_mid is None and current_mid is not None:
        return True
    if previous_mid is None or current_mid is None:
        return False
    threshold = current.price_tick or previous.price_tick or 0.0
    if threshold <= 0:
        return current_mid != previous_mid
    return abs(current_mid - previous_mid) >= threshold


def _changed_price(previous: float | None, current: float | None) -> bool:
    if previous is None and current is not None:
        return True
    if previous is None or current is None:
        return False
    return previous != current


def _best_mid_price(quote: QuoteRecord) -> float | None:
    if quote.bid_price1 is None or quote.ask_price1 is None:
        return None
    return (quote.bid_price1 + quote.ask_price1) / 2


def _mark_metrics_dirty(
    queue: MetricsDirtyQueueRepository,
    *,
    symbol_metadata: dict[str, dict[str, str | None]],
    symbol: str,
    dirty_at: str,
    underlying_chain_interval_seconds: int,
) -> None:
    metadata = symbol_metadata.get(symbol)
    if metadata is None:
        return
    option_class = metadata.get("option_class")
    underlying_symbol = metadata.get("underlying_symbol")
    if option_class in {"CALL", "PUT"} and underlying_symbol:
        queue.mark_option_dirty(
            symbol=symbol,
            underlying_symbol=underlying_symbol,
            reason="option_quote_price_changed",
            dirty_at=dirty_at,
        )
        return
    if metadata.get("ins_class") == "FUTURE":
        queue.mark_underlying_chain_dirty(
            underlying_symbol=symbol,
            reason="underlying_quote_price_changed",
            dirty_at=dirty_at,
            min_interval_seconds=underlying_chain_interval_seconds,
        )


class _TqsdkNotifyState:
    """Track TQSDK server notifications without controlling reconnect behavior."""

    CONNECTION_CODES = {
        2019112901: "connected",
        2019112902: "connected",
        2019112910: "reconnecting",
        2019112911: "disconnected",
    }

    def __init__(self) -> None:
        self.connection_status = "unknown"
        self.notify_count = 0
        self.last_notify_at: str | None = None
        self.last_notify_code: int | None = None
        self.last_notify_level: str | None = None
        self.last_notify_content: str | None = None
        self.last_disconnect_at: str | None = None
        self.last_restore_at: str | None = None

    def consume(self, tq_notify: Any | None) -> None:
        if tq_notify is None:
            return
        try:
            notifies = tq_notify.get_notifies()
        except Exception:
            return
        for notify in notifies:
            self._record(notify)

    def as_progress_fields(self) -> dict[str, Any]:
        return {
            "last_tqsdk_notify_at": self.last_notify_at,
            "last_tqsdk_notify_code": self.last_notify_code,
            "last_tqsdk_notify_level": self.last_notify_level,
            "last_tqsdk_notify_content": self.last_notify_content,
            "tqsdk_connection_status": self.connection_status,
            "last_tqsdk_disconnect_at": self.last_disconnect_at,
            "last_tqsdk_restore_at": self.last_restore_at,
            "tqsdk_notify_count": self.notify_count,
        }

    def _record(self, notify: Any) -> None:
        if not isinstance(notify, dict):
            return
        recorded_at = datetime.now(UTC).isoformat()
        code = _int_or_none(notify.get("code"))
        self.notify_count += 1
        self.last_notify_at = recorded_at
        self.last_notify_code = code
        self.last_notify_level = str(notify.get("level") or "") or None
        self.last_notify_content = str(notify.get("content") or "") or None
        if code in self.CONNECTION_CODES:
            self.connection_status = self.CONNECTION_CODES[code]
        if code == 2019112911:
            self.last_disconnect_at = recorded_at
        elif code in {2019112901, 2019112902}:
            self.last_restore_at = recorded_at


def _create_tq_notify(
    api: Any,
    tq_notify_factory: Callable[[Any], Any] | None,
) -> Any | None:
    if tq_notify_factory is not None:
        try:
            return tq_notify_factory(api)
        except Exception:
            return None
    if not all(
        hasattr(api, attr)
        for attr in ("_data", "create_task", "register_update_notify")
    ):
        return None
    try:
        from tqsdk import TqNotify

        return TqNotify(api)
    except Exception:
        return None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _emit_progress(
    progress_callback: Callable[[dict[str, Any]], None] | None,
    *,
    status: str,
    started_at: str,
    quote_started_at: str | None,
    quote_finished_at: str | None,
    kline_started_at: str | None,
    worker_index: int,
    worker_count: int,
    quote_subscribed: int,
    quote_total: int,
    kline_subscribed: int,
    kline_total: int,
    near_expiry_months: int,
    near_expiry_quote_subscribed: int,
    near_expiry_quote_total: int,
    near_expiry_kline_subscribed: int,
    near_expiry_kline_total: int,
    contract_month_limit: int | None,
    cycle_count: int = 0,
    wait_update_count: int = 0,
    quotes_written: int = 0,
    changed_quotes_written: int = 0,
    last_wait_update_at: str | None = None,
    last_quote_write_at: str | None = None,
    tq_notify_state: _TqsdkNotifyState | None = None,
    contract_refresh_count: int = 0,
    last_contract_refresh_at: str | None = None,
    last_contract_reconcile_at: str | None = None,
    contract_reconcile_added_quote_count: int = 0,
    contract_reconcile_removed_quote_count: int = 0,
    contract_reconcile_added_kline_count: int = 0,
    contract_reconcile_removed_kline_count: int = 0,
) -> None:
    if progress_callback is None:
        return
    subscribed_objects = quote_subscribed + kline_subscribed
    total_objects = quote_total + kline_total
    near_expiry_subscribed = (
        near_expiry_quote_subscribed + near_expiry_kline_subscribed
    )
    near_expiry_total = near_expiry_quote_total + near_expiry_kline_total
    payload = {
            "status": status,
            "started_at": started_at,
            "updated_at": datetime.now(UTC).isoformat(),
            "quote_started_at": quote_started_at,
            "quote_finished_at": quote_finished_at,
            "kline_started_at": kline_started_at,
            "worker_index": worker_index,
            "worker_count": worker_count,
            "quote_subscribed": quote_subscribed,
            "quote_total": quote_total,
            "kline_subscribed": kline_subscribed,
            "kline_total": kline_total,
            "near_expiry_months": near_expiry_months,
            "near_expiry_quote_subscribed": near_expiry_quote_subscribed,
            "near_expiry_quote_total": near_expiry_quote_total,
            "near_expiry_kline_subscribed": near_expiry_kline_subscribed,
            "near_expiry_kline_total": near_expiry_kline_total,
            "near_expiry_subscribed": near_expiry_subscribed,
            "near_expiry_total": near_expiry_total,
            "contract_months": _contract_months_progress_value(
                contract_month_limit
            ),
            "subscribed_objects": subscribed_objects,
            "total_objects": total_objects,
            "cycle_count": cycle_count,
            "wait_update_count": wait_update_count,
            "quotes_written": quotes_written,
            "changed_quotes_written": changed_quotes_written,
            "last_wait_update_at": last_wait_update_at,
            "last_quote_write_at": last_quote_write_at,
            "contract_refresh_count": contract_refresh_count,
            "last_contract_refresh_at": last_contract_refresh_at,
            "last_contract_reconcile_at": last_contract_reconcile_at,
            "contract_reconcile_added_quote_count": (
                contract_reconcile_added_quote_count
            ),
            "contract_reconcile_removed_quote_count": (
                contract_reconcile_removed_quote_count
            ),
            "contract_reconcile_added_kline_count": (
                contract_reconcile_added_kline_count
            ),
            "contract_reconcile_removed_kline_count": (
                contract_reconcile_removed_kline_count
            ),
            "completion_ratio": subscribed_objects / total_objects
            if total_objects
            else 0.0,
    }
    if tq_notify_state is not None:
        payload.update(tq_notify_state.as_progress_fields())
    progress_callback(payload)


def _validate_worker(worker_index: int, worker_count: int) -> None:
    if worker_count < 1:
        raise ValueError("worker_count must be positive.")
    if worker_index < 0 or worker_index >= worker_count:
        raise ValueError("worker_index must be in [0, worker_count).")
