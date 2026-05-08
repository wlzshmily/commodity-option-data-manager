"""Subscription-driven continuous collection for staged acquisition rollout."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import sqlite3
import time
from typing import Any

from .acquisition import AcquisitionRepository, AcquisitionRunRecord
from .collection_plan import (
    MarketCollectionPlan,
    OptionBatchPlan,
    UnderlyingCollectionPlan,
)
from .collection_state import CollectionStateRepository
from .instruments import InstrumentRepository, InstrumentRecord
from .klines import KlineRepository, KlineRecord, normalize_multi_symbol_kline_rows
from .option_metrics import (
    GREEK_FIELDS,
    OptionMetricsRecord,
    OptionMetricsRepository,
    normalize_option_metrics,
)
from .quotes import QUOTE_SOURCE_FIELDS, QuoteRecord, QuoteRepository, normalize_quote
from .source_quality import (
    COMPLETE,
    PARTIAL,
    SOURCE_EMPTY,
    FieldGroupObservation,
    SourceQualityRepository,
)


DEV031A_UNDERLYING_SYMBOL = "SHFE.cu2606"
DEV031B_EXCHANGE_ID = "SHFE"
DEV031B_PRODUCT_ID = "cu"
DEV031B_COLLECTION_SCOPE = "dev-031b-shfe-cu-continuous-shards"
SECONDS_PER_DAY = 24 * 60 * 60


@dataclass(frozen=True)
class ContinuousCollectionResult:
    """Summary of one bounded continuous collection run."""

    run: AcquisitionRunRecord
    stage_id: str
    underlying_symbol: str
    option_count: int
    cycles: int
    quotes_written: int
    kline_rows_written: int
    metrics_written: int
    quality_rows_written: int
    source_unavailable_count: int
    error_count: int


@dataclass(frozen=True)
class ContinuousProductShardResult:
    """Summary of one underlying shard inside a product-level run."""

    underlying_symbol: str
    batch_index: int
    option_count: int
    quotes_written: int
    kline_rows_written: int
    metrics_written: int
    quality_rows_written: int
    source_unavailable_count: int
    error_count: int
    collection_batch_status: str
    error: str | None = None


@dataclass(frozen=True)
class ContinuousProductCollectionResult:
    """Summary of one bounded concurrent product-level continuous collection run."""

    run: AcquisitionRunRecord
    stage_id: str
    exchange_id: str
    product_id: str
    shard_count: int
    underlying_count: int
    option_count: int
    cycles: int
    planned_shard_count: int
    completed_shard_count: int
    failed_shard_count: int
    quotes_written: int
    kline_rows_written: int
    metrics_written: int
    quality_rows_written: int
    source_unavailable_count: int
    error_count: int
    shards: tuple[ContinuousProductShardResult, ...]
    no_option_underlyings: tuple[str, ...] = ()


@dataclass
class _MutableShardResult:
    underlying_symbol: str
    option_count: int
    batch_index: int = 1
    quotes_written: int = 0
    kline_rows_written: int = 0
    metrics_written: int = 0
    quality_rows_written: int = 0
    source_unavailable_count: int = 0
    error_count: int = 0
    collection_batch_status: str = "success"
    error: str | None = None

    def freeze(self) -> ContinuousProductShardResult:
        return ContinuousProductShardResult(
            underlying_symbol=self.underlying_symbol,
            batch_index=self.batch_index,
            option_count=self.option_count,
            quotes_written=self.quotes_written,
            kline_rows_written=self.kline_rows_written,
            metrics_written=self.metrics_written,
            quality_rows_written=self.quality_rows_written,
            source_unavailable_count=self.source_unavailable_count,
            error_count=self.error_count,
            collection_batch_status=self.collection_batch_status,
            error=self.error,
        )


def collect_dev031a_cu2606(
    api: Any,
    connection: sqlite3.Connection,
    *,
    option_symbols: tuple[str, ...] | None = None,
    cycles: int = 2,
    wait_deadline: float = 1.0,
    received_at: str | None = None,
    iv_calculator: Callable[[Any, Any], Any] | None = None,
    collect_klines: bool = True,
    collect_metrics: bool = True,
    quote_interval_cycles: int = 1,
    kline_interval_cycles: int = 1,
    metrics_interval_cycles: int = 1,
) -> ContinuousCollectionResult:
    """Collect the approved DEV-031A single-underlying continuous slice."""

    return collect_continuous_underlying(
        api,
        connection,
        stage_id="DEV-031A",
        underlying_symbol=DEV031A_UNDERLYING_SYMBOL,
        option_symbols=option_symbols,
        cycles=cycles,
        wait_deadline=wait_deadline,
        received_at=received_at,
        iv_calculator=iv_calculator,
        collect_klines=collect_klines,
        collect_metrics=collect_metrics,
        quote_interval_cycles=quote_interval_cycles,
        kline_interval_cycles=kline_interval_cycles,
        metrics_interval_cycles=metrics_interval_cycles,
    )


def collect_dev031b_shfe_cu_product(
    api: Any,
    connection: sqlite3.Connection,
    *,
    api_factory: Callable[[], Any] | None = None,
    cycles: int = 2,
    wait_deadline: float = 1.0,
    received_at: str | None = None,
    iv_calculator: Callable[[Any, Any], Any] | None = None,
    collect_klines: bool = True,
    collect_metrics: bool = True,
    quote_interval_cycles: int = 1,
    kline_interval_cycles: int = 1,
    metrics_interval_cycles: int = 1,
    resume_successful_shards: bool = False,
) -> ContinuousProductCollectionResult:
    """Collect the approved DEV-031B SHFE copper product continuous slice."""

    return collect_continuous_product(
        api,
        connection,
        stage_id="DEV-031B",
        exchange_id=DEV031B_EXCHANGE_ID,
        product_id=DEV031B_PRODUCT_ID,
        api_factory=api_factory,
        cycles=cycles,
        wait_deadline=wait_deadline,
        received_at=received_at,
        iv_calculator=iv_calculator,
        collect_klines=collect_klines,
        collect_metrics=collect_metrics,
        quote_interval_cycles=quote_interval_cycles,
        kline_interval_cycles=kline_interval_cycles,
        metrics_interval_cycles=metrics_interval_cycles,
        resume_successful_shards=resume_successful_shards,
    )


def collect_continuous_product(
    api: Any,
    connection: sqlite3.Connection,
    *,
    stage_id: str,
    exchange_id: str,
    product_id: str,
    api_factory: Callable[[], Any] | None = None,
    cycles: int = 2,
    wait_deadline: float = 1.0,
    received_at: str | None = None,
    iv_calculator: Callable[[Any, Any], Any] | None = None,
    collect_klines: bool = True,
    collect_metrics: bool = True,
    quote_interval_cycles: int = 1,
    kline_interval_cycles: int = 1,
    metrics_interval_cycles: int = 1,
    resume_successful_shards: bool = False,
) -> ContinuousProductCollectionResult:
    """Run bounded concurrent continuous collection for one approved product stage."""

    cleaned_stage = _required_text(stage_id, "stage_id")
    cleaned_exchange = _required_text(exchange_id, "exchange_id").upper()
    cleaned_product = _required_text(product_id, "product_id").lower()
    _validate_product_stage_scope(cleaned_stage, cleaned_exchange, cleaned_product)
    if cycles < 1:
        raise ValueError("Continuous collector cycles must be positive.")
    if wait_deadline <= 0:
        raise ValueError("Continuous collector wait_deadline must be positive.")
    if quote_interval_cycles < 1:
        raise ValueError("quote_interval_cycles must be positive.")
    if kline_interval_cycles < 1:
        raise ValueError("kline_interval_cycles must be positive.")
    if metrics_interval_cycles < 1:
        raise ValueError("metrics_interval_cycles must be positive.")

    acquisition_repo = AcquisitionRepository(connection)
    instrument_repo = InstrumentRepository(connection)
    state_repo = CollectionStateRepository(connection)

    underlyings = tuple(
        instrument_repo.list_active_underlyings_for_product(
            exchange_id=cleaned_exchange,
            product_id=cleaned_product,
        )
    )
    if not underlyings:
        raise ValueError(f"No active underlyings are persisted for {cleaned_exchange}/{cleaned_product}.")

    shard_options = {
        underlying.symbol: _select_options(
            instrument_repo.list_active_options(underlying.symbol),
            option_symbols=None,
        )
        for underlying in underlyings
    }
    no_option_underlyings = tuple(
        sorted(symbol for symbol, options in shard_options.items() if not options)
    )
    shard_options = {
        symbol: options
        for symbol, options in shard_options.items()
        if options
    }
    underlyings = tuple(
        underlying
        for underlying in underlyings
        if underlying.symbol in shard_options
    )
    if not shard_options:
        raise ValueError(
            f"No active option chains are persisted for {cleaned_exchange}/{cleaned_product}."
        )

    option_count = sum(len(options) for options in shard_options.values())
    plan = _build_product_shard_plan(
        underlyings=underlyings,
        shard_options=shard_options,
    )
    materialized_at = _now()
    state_repo.materialize_plan(
        plan,
        scope=DEV031B_COLLECTION_SCOPE,
        updated_at=materialized_at,
    )
    shard_statuses = (
        ("pending", "failed")
        if resume_successful_shards
        else ("pending", "failed", "success")
    )
    product_batches = tuple(
        state_repo.list_batches(
            scope=DEV031B_COLLECTION_SCOPE,
            statuses=shard_statuses,
        )
    )
    run = acquisition_repo.start_run(
        trigger=f"{cleaned_stage.lower()}-continuous-product-collector",
        started_at=_now(),
        message=f"Collect continuous current slice for {cleaned_exchange}/{cleaned_product}",
    )

    mutable_shards: list[_MutableShardResult] = []
    for batch in product_batches:
        state_repo.mark_started(
            scope=DEV031B_COLLECTION_SCOPE,
            underlying_symbol=batch.underlying_symbol,
            batch_index=batch.batch_index,
        )
        options = _select_options(
            shard_options[batch.underlying_symbol],
            option_symbols=batch.option_symbols,
        )
        shard_api = None
        try:
            shard_api = api_factory() if api_factory is not None else api
            shard_result = collect_continuous_underlying(
                shard_api,
                connection,
                stage_id=cleaned_stage,
                underlying_symbol=batch.underlying_symbol,
                option_symbols=tuple(option.symbol for option in options),
                cycles=cycles,
                wait_deadline=wait_deadline,
                received_at=received_at,
                iv_calculator=iv_calculator,
                collect_klines=collect_klines,
                collect_metrics=collect_metrics,
                quote_interval_cycles=quote_interval_cycles,
                kline_interval_cycles=kline_interval_cycles,
                metrics_interval_cycles=metrics_interval_cycles,
            )
            if shard_result.error_count == 0:
                batch_state = state_repo.mark_succeeded(
                    scope=DEV031B_COLLECTION_SCOPE,
                    underlying_symbol=batch.underlying_symbol,
                    batch_index=batch.batch_index,
                )
            else:
                batch_state = state_repo.mark_failed(
                    scope=DEV031B_COLLECTION_SCOPE,
                    underlying_symbol=batch.underlying_symbol,
                    batch_index=batch.batch_index,
                    error=shard_result.run.message or shard_result.run.status,
                )
        except Exception as exc:
            error_text = f"{type(exc).__name__}: {exc}"
            batch_state = state_repo.mark_failed(
                scope=DEV031B_COLLECTION_SCOPE,
                underlying_symbol=batch.underlying_symbol,
                batch_index=batch.batch_index,
                error=error_text,
            )
            _record_error(
                acquisition_repo,
                run_id=run.run_id,
                symbol=batch.underlying_symbol,
                stage="product_shard",
                exc=exc,
                context={
                    "stage_id": cleaned_stage,
                    "batch_index": batch.batch_index,
                    "option_count": len(batch.option_symbols),
                },
            )
            mutable_shards.append(
                _MutableShardResult(
                    underlying_symbol=batch.underlying_symbol,
                    batch_index=batch.batch_index,
                    option_count=len(batch.option_symbols),
                    error_count=1,
                    collection_batch_status=batch_state.status,
                    error=error_text,
                )
            )
            continue
        finally:
            if api_factory is not None and shard_api is not None and hasattr(shard_api, "close"):
                shard_api.close()
        mutable_shards.append(
            _MutableShardResult(
                underlying_symbol=shard_result.underlying_symbol,
                batch_index=batch.batch_index,
                option_count=shard_result.option_count,
                quotes_written=shard_result.quotes_written,
                kline_rows_written=shard_result.kline_rows_written,
                metrics_written=shard_result.metrics_written,
                quality_rows_written=shard_result.quality_rows_written,
                source_unavailable_count=shard_result.source_unavailable_count,
                error_count=shard_result.error_count,
                collection_batch_status=batch_state.status,
                error=None if shard_result.error_count == 0 else shard_result.run.message,
            )
        )

    immutable_shards = tuple(
        result.freeze()
        for result in sorted(mutable_shards, key=lambda item: item.underlying_symbol)
    )
    quotes_written = sum(result.quotes_written for result in immutable_shards)
    kline_rows_written = sum(result.kline_rows_written for result in immutable_shards)
    metrics_written = sum(result.metrics_written for result in immutable_shards)
    quality_rows_written = sum(result.quality_rows_written for result in immutable_shards)
    source_unavailable_count = sum(result.source_unavailable_count for result in immutable_shards)
    error_count = sum(result.error_count for result in immutable_shards)
    completed_shard_count = sum(
        1 for result in immutable_shards if result.collection_batch_status == "success"
    )
    failed_shard_count = sum(
        1 for result in immutable_shards if result.collection_batch_status == "failed"
    )
    status = _finish_status(error_count, source_unavailable_count)
    finished = acquisition_repo.finish_run(
        run.run_id,
        status=status,
        symbols_discovered=option_count + len(underlyings),
        quotes_written=quotes_written,
        kline_rows_written=kline_rows_written,
        metrics_written=metrics_written,
        error_count=error_count,
        message=f"{cleaned_stage} continuous product collection completed",
    )
    return ContinuousProductCollectionResult(
        run=finished,
        stage_id=cleaned_stage,
        exchange_id=cleaned_exchange,
        product_id=cleaned_product,
        shard_count=len(immutable_shards),
        underlying_count=len(underlyings),
        option_count=option_count,
        cycles=cycles,
        planned_shard_count=plan.batch_count,
        completed_shard_count=completed_shard_count,
        failed_shard_count=failed_shard_count,
        quotes_written=quotes_written,
        kline_rows_written=kline_rows_written,
        metrics_written=metrics_written,
        quality_rows_written=quality_rows_written,
        source_unavailable_count=source_unavailable_count,
        error_count=error_count,
        shards=immutable_shards,
        no_option_underlyings=no_option_underlyings,
    )


def collect_continuous_underlying(
    api: Any,
    connection: sqlite3.Connection,
    *,
    stage_id: str,
    underlying_symbol: str,
    option_symbols: tuple[str, ...] | None = None,
    cycles: int = 2,
    wait_deadline: float = 1.0,
    received_at: str | None = None,
    iv_calculator: Callable[[Any, Any], Any] | None = None,
    collect_klines: bool = True,
    collect_metrics: bool = True,
    quote_interval_cycles: int = 1,
    kline_interval_cycles: int = 1,
    metrics_interval_cycles: int = 1,
) -> ContinuousCollectionResult:
    """Run a bounded subscription-driven collector for one approved stage shard."""

    cleaned_stage = _required_text(stage_id, "stage_id")
    cleaned_underlying = _required_text(underlying_symbol, "underlying_symbol")
    _validate_stage_scope(cleaned_stage, cleaned_underlying)
    if cycles < 1:
        raise ValueError("Continuous collector cycles must be positive.")
    if wait_deadline <= 0:
        raise ValueError("Continuous collector wait_deadline must be positive.")
    if quote_interval_cycles < 1:
        raise ValueError("quote_interval_cycles must be positive.")
    if kline_interval_cycles < 1:
        raise ValueError("kline_interval_cycles must be positive.")
    if metrics_interval_cycles < 1:
        raise ValueError("metrics_interval_cycles must be positive.")

    acquisition_repo = AcquisitionRepository(connection)
    instrument_repo = InstrumentRepository(connection)
    quote_repo = QuoteRepository(connection)
    kline_repo = KlineRepository(connection)
    metrics_repo = OptionMetricsRepository(connection)
    quality_repo = SourceQualityRepository(connection)

    options = _select_options(
        instrument_repo.list_active_options(cleaned_underlying),
        option_symbols=option_symbols,
    )
    if not options:
        raise ValueError(f"No active options are persisted for {cleaned_underlying}.")

    run = acquisition_repo.start_run(
        trigger=f"{cleaned_stage.lower()}-continuous-collector",
        started_at=_now(),
        message=f"Collect continuous current slice for {cleaned_underlying}",
    )
    quotes_written = 0
    kline_rows_written = 0
    metrics_written = 0
    quality_rows_written = 0
    source_unavailable_count = 0
    error_count = 0

    try:
        quote_refs = _quote_refs(api, cleaned_underlying, options)
        kline_refs = (
            _kline_refs(api, cleaned_underlying, options)
            if collect_klines or collect_metrics
            else {}
        )
    except Exception as exc:
        error_count += 1
        _record_error(
            acquisition_repo,
            run_id=run.run_id,
            symbol=cleaned_underlying,
            stage="reference_setup",
            exc=exc,
            context={"stage_id": cleaned_stage},
        )
        finished = acquisition_repo.finish_run(
            run.run_id,
            status="failed",
            symbols_discovered=len(options) + 1,
            quotes_written=0,
            kline_rows_written=0,
            metrics_written=0,
            error_count=error_count,
            message=f"{cleaned_stage} continuous collection failed during reference setup",
        )
        return ContinuousCollectionResult(
            run=finished,
            stage_id=cleaned_stage,
            underlying_symbol=cleaned_underlying,
            option_count=len(options),
            cycles=cycles,
            quotes_written=0,
            kline_rows_written=0,
            metrics_written=0,
            quality_rows_written=0,
            source_unavailable_count=0,
            error_count=error_count,
        )

    for cycle_index in range(1, cycles + 1):
        cycle_received_at = received_at or _now()
        api.wait_update(deadline=time.time() + wait_deadline)

        for symbol, quote_ref in quote_refs.items():
            quote_due = cycle_index == 1 or (cycle_index - 1) % quote_interval_cycles == 0
            if (
                cycle_index > 1
                and not quote_due
                and not _is_changing(api, quote_ref, QUOTE_SOURCE_FIELDS)
            ):
                continue
            try:
                quote = normalize_quote(symbol, quote_ref, received_at=cycle_received_at)
                quote_repo.upsert_quote(quote)
                quotes_written += 1
                trade_quality = _quote_trade_quality(quote)
                depth_quality = _quote_depth_quality(quote)
                source_unavailable_count += _source_unavailable_count(
                    trade_quality,
                    depth_quality,
                )
                quality_rows_written += _upsert_quote_quality(
                    quality_repo,
                    quote,
                    trade_quality=trade_quality,
                    depth_quality=depth_quality,
                )
            except Exception as exc:
                error_count += 1
                _record_error(
                    acquisition_repo,
                    run_id=run.run_id,
                    symbol=symbol,
                    stage="quote",
                    exc=exc,
                    context={"stage_id": cleaned_stage, "cycle_index": cycle_index},
                )

        for option in options:
            kline_ref = kline_refs.get(option.symbol)
            kline_due = cycle_index == 1 or (cycle_index - 1) % kline_interval_cycles == 0
            if (
                collect_klines
                and kline_ref is not None
                and kline_due
                and (cycle_index == 1 or _is_changing(api, kline_ref, None))
            ):
                try:
                    normalized = normalize_multi_symbol_kline_rows(
                        [option.symbol, cleaned_underlying],
                        kline_ref,
                        received_at=cycle_received_at,
                    )
                    for symbol, records in normalized.items():
                        kline_repo.replace_symbol_klines(symbol, records)
                        kline_rows_written += len(records)
                        quality = _daily_kline_quality(records)
                        source_unavailable_count += _source_unavailable_count(quality)
                        quality_rows_written += 1
                        quality_repo.upsert_observation(
                            FieldGroupObservation(
                                symbol=symbol,
                                field_group="daily_kline",
                                quality=quality,
                                source_datetime=_latest_kline_datetime(records),
                                received_at=cycle_received_at,
                                reason=_reason_for_quality(quality, "daily_kline"),
                                context={"stage_id": cleaned_stage, "cycle_index": cycle_index},
                            ),
                            payload=_latest_kline_payload(records) if quality != SOURCE_EMPTY else None,
                        )
                except Exception as exc:
                    error_count += 1
                    _record_error(
                        acquisition_repo,
                        run_id=run.run_id,
                        symbol=option.symbol,
                        stage="kline",
                        exc=exc,
                        context={"stage_id": cleaned_stage, "cycle_index": cycle_index},
                    )

            if not collect_metrics or kline_ref is None:
                continue
            if (cycle_index - 1) % metrics_interval_cycles != 0:
                continue
            try:
                option_quote = quote_refs.get(option.symbol)
                greeks = _query_greeks(api, option.symbol)
                iv_payload = None
                if iv_calculator is not None:
                    iv_payload = iv_calculator(kline_ref, option_quote)
                metrics = normalize_option_metrics(
                    option.symbol,
                    received_at=cycle_received_at,
                    greeks_payload=greeks,
                    iv_payload=iv_payload,
                )
                metrics_repo.upsert_metrics(metrics)
                metrics_written += 1
                greeks_quality = _greeks_quality(metrics)
                iv_quality = _iv_quality(metrics)
                source_unavailable_count += _source_unavailable_count(
                    greeks_quality,
                    iv_quality,
                )
                source_datetime = _quote_source_datetime(option_quote, cycle_received_at)
                quality_rows_written += _upsert_metrics_quality(
                    quality_repo,
                    metrics,
                    source_datetime=source_datetime,
                    greeks_quality=greeks_quality,
                    iv_quality=iv_quality,
                    stage_id=cleaned_stage,
                    cycle_index=cycle_index,
                )
            except Exception as exc:
                error_count += 1
                _record_error(
                    acquisition_repo,
                    run_id=run.run_id,
                    symbol=option.symbol,
                    stage="metrics",
                    exc=exc,
                    context={"stage_id": cleaned_stage, "cycle_index": cycle_index},
                )

    status = _finish_status(error_count, source_unavailable_count)
    finished = acquisition_repo.finish_run(
        run.run_id,
        status=status,
        symbols_discovered=len(options) + 1,
        quotes_written=quotes_written,
        kline_rows_written=kline_rows_written,
        metrics_written=metrics_written,
        error_count=error_count,
        message=f"{cleaned_stage} continuous collection completed",
    )
    return ContinuousCollectionResult(
        run=finished,
        stage_id=cleaned_stage,
        underlying_symbol=cleaned_underlying,
        option_count=len(options),
        cycles=cycles,
        quotes_written=quotes_written,
        kline_rows_written=kline_rows_written,
        metrics_written=metrics_written,
        quality_rows_written=quality_rows_written,
        source_unavailable_count=source_unavailable_count,
        error_count=error_count,
    )


def _validate_stage_scope(stage_id: str, underlying_symbol: str) -> None:
    if stage_id == "DEV-031A" and underlying_symbol != DEV031A_UNDERLYING_SYMBOL:
        raise ValueError("DEV-031A scope is restricted to SHFE.cu2606.")


def _validate_product_stage_scope(stage_id: str, exchange_id: str, product_id: str) -> None:
    if stage_id == "DEV-031B" and (
        exchange_id != DEV031B_EXCHANGE_ID or product_id != DEV031B_PRODUCT_ID
    ):
        raise ValueError("DEV-031B scope is restricted to SHFE/cu.")


def _build_product_shard_plan(
    *,
    underlyings: tuple[InstrumentRecord, ...],
    shard_options: dict[str, tuple[InstrumentRecord, ...]],
) -> MarketCollectionPlan:
    plans: list[UnderlyingCollectionPlan] = []
    for underlying in sorted(underlyings, key=lambda item: item.symbol):
        options = shard_options[underlying.symbol]
        option_symbols = tuple(option.symbol for option in options)
        plans.append(
            UnderlyingCollectionPlan(
                underlying_symbol=underlying.symbol,
                exchange_id=underlying.exchange_id,
                product_id=underlying.product_id,
                call_count=sum(1 for option in options if option.option_class == "CALL"),
                put_count=sum(1 for option in options if option.option_class == "PUT"),
                option_count=len(options),
                batches=(
                    OptionBatchPlan(
                        underlying_symbol=underlying.symbol,
                        batch_index=1,
                        option_symbols=option_symbols,
                    ),
                ),
            )
        )
    return MarketCollectionPlan(
        underlying_count=len(plans),
        option_count=sum(plan.option_count for plan in plans),
        batch_count=sum(len(plan.batches) for plan in plans),
        underlyings=tuple(plans),
    )


def _shard_for_symbol(
    symbol: str,
    shard_options: dict[str, tuple[InstrumentRecord, ...]],
) -> str:
    if symbol in shard_options:
        return symbol
    for underlying_symbol, options in shard_options.items():
        if any(option.symbol == symbol for option in options):
            return underlying_symbol
    raise ValueError(f"Symbol is outside the active product shards: {symbol}")


def _select_options(
    options: Iterable[InstrumentRecord],
    *,
    option_symbols: tuple[str, ...] | None,
) -> tuple[InstrumentRecord, ...]:
    ordered = tuple(sorted(options, key=lambda item: item.symbol))
    if option_symbols is None:
        return ordered

    requested = {_required_text(symbol, "option_symbol") for symbol in option_symbols}
    selected = tuple(option for option in ordered if option.symbol in requested)
    missing = requested.difference(option.symbol for option in selected)
    if missing:
        raise ValueError(f"Requested option symbols are not active: {sorted(missing)}")
    return selected


def _quote_refs(
    api: Any,
    underlying_symbol: str,
    options: tuple[InstrumentRecord, ...],
) -> dict[str, Any]:
    symbols = (underlying_symbol, *(option.symbol for option in options))
    return {symbol: api.get_quote(symbol) for symbol in symbols}


def _kline_refs(
    api: Any,
    underlying_symbol: str,
    options: tuple[InstrumentRecord, ...],
) -> dict[str, Any]:
    return {
        option.symbol: api.get_kline_serial(
            [option.symbol, underlying_symbol],
            duration_seconds=SECONDS_PER_DAY,
            data_length=20,
        )
        for option in options
    }


def _is_changing(api: Any, ref: Any, fields: object) -> bool:
    if not hasattr(api, "is_changing"):
        return True
    try:
        if fields is None:
            return bool(api.is_changing(ref))
        return bool(api.is_changing(ref, fields))
    except TypeError:
        return bool(api.is_changing(ref))


def _query_greeks(api: Any, symbol: str) -> Any:
    if not hasattr(api, "query_option_greeks"):
        return None
    return api.query_option_greeks(symbol)


def _quote_trade_quality(record: QuoteRecord) -> str:
    if record.last_price is not None:
        return COMPLETE
    if record.volume is not None or record.open_interest is not None:
        return PARTIAL
    return SOURCE_EMPTY


def _quote_depth_quality(record: QuoteRecord) -> str:
    if record.bid_price1 is not None or record.ask_price1 is not None:
        return COMPLETE
    return SOURCE_EMPTY


def _daily_kline_quality(records: list[KlineRecord]) -> str:
    latest = _latest_kline(records)
    if latest is None:
        return SOURCE_EMPTY
    if latest.close_price is not None:
        return COMPLETE
    return PARTIAL


def _greeks_quality(record: OptionMetricsRecord) -> str:
    values = [getattr(record, field) for field in GREEK_FIELDS]
    return COMPLETE if any(value is not None for value in values) else SOURCE_EMPTY


def _iv_quality(record: OptionMetricsRecord) -> str:
    return COMPLETE if record.iv is not None else SOURCE_EMPTY


def _source_unavailable_count(*qualities: str) -> int:
    return sum(1 for quality in qualities if quality == SOURCE_EMPTY)


def _upsert_quote_quality(
    repository: SourceQualityRepository,
    record: QuoteRecord,
    *,
    trade_quality: str,
    depth_quality: str,
) -> int:
    repository.upsert_observation(
        FieldGroupObservation(
            symbol=record.symbol,
            field_group="quote_trade",
            quality=trade_quality,
            source_datetime=record.source_datetime,
            received_at=record.received_at,
            reason=_reason_for_quality(trade_quality, "quote_trade"),
        ),
        payload=_quote_trade_payload(record) if trade_quality != SOURCE_EMPTY else None,
    )
    repository.upsert_observation(
        FieldGroupObservation(
            symbol=record.symbol,
            field_group="quote_depth",
            quality=depth_quality,
            source_datetime=record.source_datetime,
            received_at=record.received_at,
            reason=_reason_for_quality(depth_quality, "quote_depth"),
        ),
        payload=_quote_depth_payload(record) if depth_quality != SOURCE_EMPTY else None,
    )
    return 2


def _upsert_metrics_quality(
    repository: SourceQualityRepository,
    record: OptionMetricsRecord,
    *,
    source_datetime: str | None,
    greeks_quality: str,
    iv_quality: str,
    stage_id: str,
    cycle_index: int,
) -> int:
    repository.upsert_observation(
        FieldGroupObservation(
            symbol=record.symbol,
            field_group="greeks",
            quality=greeks_quality,
            source_datetime=source_datetime,
            received_at=record.received_at,
            reason=_reason_for_quality(greeks_quality, "greeks"),
            context={"stage_id": stage_id, "cycle_index": cycle_index},
        ),
        payload=_greeks_payload(record) if greeks_quality != SOURCE_EMPTY else None,
    )
    repository.upsert_observation(
        FieldGroupObservation(
            symbol=record.symbol,
            field_group="iv",
            quality=iv_quality,
            source_datetime=source_datetime,
            received_at=record.received_at,
            reason=_reason_for_quality(iv_quality, "iv"),
            context={"stage_id": stage_id, "cycle_index": cycle_index},
        ),
        payload=_iv_payload(record) if iv_quality != SOURCE_EMPTY else None,
    )
    return 2


def _quote_trade_payload(record: QuoteRecord) -> dict[str, Any]:
    return {
        "last_price": record.last_price,
        "volume": record.volume,
        "open_interest": record.open_interest,
        "open_price": record.open_price,
        "high_price": record.high_price,
        "low_price": record.low_price,
        "close_price": record.close_price,
    }


def _quote_depth_payload(record: QuoteRecord) -> dict[str, Any]:
    return {
        "bid_price1": record.bid_price1,
        "bid_volume1": record.bid_volume1,
        "ask_price1": record.ask_price1,
        "ask_volume1": record.ask_volume1,
    }


def _greeks_payload(record: OptionMetricsRecord) -> dict[str, Any]:
    return {field: getattr(record, field) for field in GREEK_FIELDS}


def _iv_payload(record: OptionMetricsRecord) -> dict[str, Any]:
    return {"iv": record.iv, "source_method": record.source_method}


def _latest_kline_payload(records: list[KlineRecord]) -> dict[str, Any] | None:
    latest = _latest_kline(records)
    return asdict(latest) if latest is not None else None


def _latest_kline_datetime(records: list[KlineRecord]) -> str | None:
    latest = _latest_kline(records)
    return latest.bar_datetime if latest is not None else None


def _latest_kline(records: list[KlineRecord]) -> KlineRecord | None:
    if not records:
        return None
    return max(records, key=lambda item: item.bar_datetime)


def _quote_source_datetime(quote_ref: Any, received_at: str) -> str | None:
    try:
        return normalize_quote("__metrics_source__", quote_ref, received_at=received_at).source_datetime
    except Exception:
        return None


def _reason_for_quality(quality: str, field_group: str) -> str | None:
    if quality == SOURCE_EMPTY:
        return f"{field_group}_source_empty"
    if quality == PARTIAL:
        return f"{field_group}_partial"
    return None


def _finish_status(error_count: int, source_unavailable_count: int) -> str:
    if error_count:
        return "partial_failure"
    if source_unavailable_count:
        return "partial_source_unavailable"
    return "success"


def _record_error(
    repository: AcquisitionRepository,
    *,
    run_id: int,
    symbol: str,
    stage: str,
    exc: Exception,
    context: dict[str, Any],
) -> None:
    repository.record_error(
        run_id=run_id,
        symbol=symbol,
        stage=stage,
        error_type=type(exc).__name__,
        message=str(exc),
        context=context,
    )


def _required_text(value: str, field_name: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{field_name} must not be empty.")
    return cleaned


def _now() -> str:
    return datetime.now(UTC).isoformat()
