"""Batch collection for one persisted option chain."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
import sqlite3
import time
from typing import Any

from .acquisition import AcquisitionRepository, AcquisitionRunRecord
from .instruments import InstrumentRepository, InstrumentRecord
from .klines import KlineRepository, normalize_multi_symbol_kline_rows
from .option_metrics import OptionMetricsRepository, normalize_option_metrics
from .quotes import QuoteRepository, normalize_quote


SECONDS_PER_DAY = 24 * 60 * 60


@dataclass(frozen=True)
class ChainCollectionResult:
    """Summary of one single-underlying chain collection run."""

    run: AcquisitionRunRecord
    underlying_symbol: str
    option_count: int
    batch_count: int
    quotes_written: int
    kline_rows_written: int
    metrics_written: int
    error_count: int


def collect_persisted_option_chain(
    api: Any,
    connection: sqlite3.Connection,
    *,
    underlying_symbol: str,
    option_symbols: tuple[str, ...] | None = None,
    batch_size: int = 20,
    wait_cycles: int = 2,
    received_at: str | None = None,
    iv_calculator: Callable[[Any, Any], Any] | None = None,
) -> ChainCollectionResult:
    """Collect current slices for one underlying's active persisted option chain."""

    cleaned_underlying = _required_symbol(underlying_symbol)
    if batch_size < 1:
        raise ValueError("Chain collection batch_size must be positive.")

    acquisition_repo = AcquisitionRepository(connection)
    instrument_repo = InstrumentRepository(connection)
    quote_repo = QuoteRepository(connection)
    kline_repo = KlineRepository(connection)
    metrics_repo = OptionMetricsRepository(connection)

    options = _select_options(
        instrument_repo.list_active_options(cleaned_underlying),
        option_symbols=option_symbols,
    )
    if not options:
        raise ValueError(f"No active options are persisted for {cleaned_underlying}.")

    started_at = datetime.now(UTC).isoformat()
    run = acquisition_repo.start_run(
        trigger="chain-collector",
        started_at=started_at,
        message=f"Collect current slices for {cleaned_underlying}",
    )
    actual_received_at = received_at or datetime.now(UTC).isoformat()
    error_count = 0
    quotes_written = 0
    kline_rows_written = 0
    metrics_written = 0
    batches = list(_batches(options, batch_size))

    try:
        quote_refs = _quote_refs(api, cleaned_underlying, options)
        _wait_updates(api, wait_cycles=wait_cycles)
        for symbol, quote_ref in quote_refs.items():
            try:
                quote_repo.upsert_quote(
                    normalize_quote(symbol, quote_ref, received_at=actual_received_at)
                )
                quotes_written += 1
            except Exception as exc:
                error_count += 1
                _record_error(
                    acquisition_repo,
                    run_id=run.run_id,
                    symbol=symbol,
                    stage="quote",
                    exc=exc,
                    context={"underlying_symbol": cleaned_underlying},
                )

        underlying_kline_written = False
        for batch_index, batch in enumerate(batches, start=1):
            kline_refs: dict[str, Any] = {}
            for option in batch:
                try:
                    kline_refs[option.symbol] = api.get_kline_serial(
                        [option.symbol, cleaned_underlying],
                        duration_seconds=SECONDS_PER_DAY,
                        data_length=20,
                    )
                except Exception as exc:
                    error_count += 1
                    _record_error(
                        acquisition_repo,
                        run_id=run.run_id,
                        symbol=option.symbol,
                        stage="kline",
                        exc=exc,
                        context={
                            "batch_index": batch_index,
                            "symbols": [option.symbol, cleaned_underlying],
                        },
                    )
            _wait_updates(api, wait_cycles=wait_cycles)

            for option in batch:
                klines = kline_refs.get(option.symbol)
                if klines is not None:
                    try:
                        normalized = normalize_multi_symbol_kline_rows(
                            [option.symbol, cleaned_underlying],
                            klines,
                            received_at=actual_received_at,
                        )
                        for symbol, records in normalized.items():
                            if symbol == cleaned_underlying and underlying_kline_written:
                                continue
                            kline_repo.replace_symbol_klines(symbol, records)
                            kline_rows_written += len(records)
                            if symbol == cleaned_underlying:
                                underlying_kline_written = True
                    except Exception as exc:
                        error_count += 1
                        _record_error(
                            acquisition_repo,
                            run_id=run.run_id,
                            symbol=option.symbol,
                            stage="kline",
                            exc=exc,
                            context={
                                "batch_index": batch_index,
                                "symbols": [option.symbol, cleaned_underlying],
                            },
                        )
                        klines = None

                option_quote = quote_refs.get(option.symbol)
                greeks = _query_greeks(api, option.symbol)
                iv_payload = None
                if iv_calculator is not None and klines is not None and option_quote is not None:
                    try:
                        iv_payload = iv_calculator(klines, option_quote)
                    except Exception as exc:
                        error_count += 1
                        _record_error(
                            acquisition_repo,
                            run_id=run.run_id,
                            symbol=option.symbol,
                            stage="iv",
                            exc=exc,
                            context={"source": "iv_calculator"},
                        )

                try:
                    metrics = normalize_option_metrics(
                        option.symbol,
                        received_at=actual_received_at,
                        greeks_payload=greeks,
                        iv_payload=iv_payload,
                    )
                    if _metrics_are_empty(metrics):
                        error_count += 1
                        _record_error(
                            acquisition_repo,
                            run_id=run.run_id,
                            symbol=option.symbol,
                            stage="metrics",
                            exc=EmptyMetricsError("No finite Greeks or IV values."),
                            context={"source": "query_option_greeks+iv_calculator"},
                        )
                    metrics_repo.upsert_metrics(metrics)
                    metrics_written += 1
                except Exception as exc:
                    error_count += 1
                    _record_error(
                        acquisition_repo,
                        run_id=run.run_id,
                        symbol=option.symbol,
                        stage="metrics",
                        exc=exc,
                        context={"underlying_symbol": cleaned_underlying},
                    )
    except Exception as exc:
        error_count += 1
        _record_error(
            acquisition_repo,
            run_id=run.run_id,
            symbol=cleaned_underlying,
            stage="collector",
            exc=exc,
            context={"underlying_symbol": cleaned_underlying},
        )
        finished = acquisition_repo.finish_run(
            run.run_id,
            status="failed",
            symbols_discovered=len(options) + 1,
            quotes_written=quotes_written,
            kline_rows_written=kline_rows_written,
            metrics_written=metrics_written,
            error_count=error_count,
            message=f"Chain collection failed: {type(exc).__name__}",
        )
        return ChainCollectionResult(
            run=finished,
            underlying_symbol=cleaned_underlying,
            option_count=len(options),
            batch_count=len(batches),
            quotes_written=quotes_written,
            kline_rows_written=kline_rows_written,
            metrics_written=metrics_written,
            error_count=error_count,
        )

    status = "partial_failure" if error_count else "success"
    finished = acquisition_repo.finish_run(
        run.run_id,
        status=status,
        symbols_discovered=len(options) + 1,
        quotes_written=quotes_written,
        kline_rows_written=kline_rows_written,
        metrics_written=metrics_written,
        error_count=error_count,
        message="Chain collection completed"
        if not error_count
        else "Chain collection completed with errors",
    )
    return ChainCollectionResult(
        run=finished,
        underlying_symbol=cleaned_underlying,
        option_count=len(options),
        batch_count=len(batches),
        quotes_written=quotes_written,
        kline_rows_written=kline_rows_written,
        metrics_written=metrics_written,
        error_count=error_count,
    )


class EmptyMetricsError(ValueError):
    """Raised internally when metrics normalize but contain no finite values."""


def _required_symbol(symbol: str) -> str:
    cleaned = symbol.strip()
    if not cleaned:
        raise ValueError("Underlying symbol must not be empty.")
    return cleaned


def _batches(
    records: Iterable[InstrumentRecord],
    batch_size: int,
) -> Iterable[list[InstrumentRecord]]:
    batch: list[InstrumentRecord] = []
    for record in records:
        batch.append(record)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _select_options(
    active_options: list[InstrumentRecord],
    *,
    option_symbols: tuple[str, ...] | None,
) -> list[InstrumentRecord]:
    if option_symbols is None:
        return active_options
    cleaned_symbols = tuple(symbol.strip() for symbol in option_symbols if symbol.strip())
    if not cleaned_symbols:
        raise ValueError("Option symbols filter must not be empty.")
    options_by_symbol = {record.symbol: record for record in active_options}
    missing = [symbol for symbol in cleaned_symbols if symbol not in options_by_symbol]
    if missing:
        raise ValueError(f"Option symbols are not active for underlying: {', '.join(missing)}")
    return [options_by_symbol[symbol] for symbol in cleaned_symbols]


def _quote_refs(
    api: Any,
    underlying_symbol: str,
    options: list[InstrumentRecord],
) -> dict[str, Any]:
    symbols = [underlying_symbol] + [record.symbol for record in options]
    quote_list = _get_quote_list(api, symbols)
    if quote_list is not None:
        return dict(zip(symbols, quote_list, strict=True))
    return {symbol: api.get_quote(symbol) for symbol in symbols}


def _get_quote_list(api: Any, symbols: Sequence[str]) -> Any | None:
    get_quote_list = getattr(api, "get_quote_list", None)
    if not callable(get_quote_list):
        return None
    try:
        quote_list = get_quote_list(list(symbols))
    except (AttributeError, TypeError):
        return None
    if len(quote_list) != len(symbols):
        return None
    return quote_list


def _wait_updates(api: Any, *, wait_cycles: int) -> None:
    for _ in range(max(wait_cycles, 0)):
        api.wait_update(deadline=time.time() + 1)


def _query_greeks(api: Any, symbol: str) -> Any:
    try:
        return api.query_option_greeks(symbol)
    except Exception:
        return None


def _metrics_are_empty(metrics: Any) -> bool:
    return all(
        value is None
        for value in (
            metrics.delta,
            metrics.gamma,
            metrics.theta,
            metrics.vega,
            metrics.rho,
            metrics.iv,
        )
    )


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
        retryable=True,
        context=context,
    )
