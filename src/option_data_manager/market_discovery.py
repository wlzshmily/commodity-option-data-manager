"""Full-market option symbol discovery helpers."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import re
import sqlite3

from .instruments import (
    InstrumentNormalizationError,
    InstrumentRecord,
    InstrumentRepository,
    normalize_option_chain_discovery,
)


OPTION_SYMBOL_PATTERN = re.compile(
    r"^(?P<exchange>[A-Za-z]+)\.(?P<underlying>.+?)[-_](?P<class>[CP])[-_]?(?P<strike>[0-9]+(?:\.[0-9]+)?)$",
    re.IGNORECASE,
)
SERIES_OPTION_SYMBOL_PATTERN = re.compile(
    r"^(?P<exchange>[A-Za-z]+)\.(?P<underlying>.+?)[-_]?MS[-_]?(?P<class>[CP])[-_]?(?P<strike>[0-9]+(?:\.[0-9]+)?)$",
    re.IGNORECASE,
)
COMPACT_OPTION_SYMBOL_PATTERN = re.compile(
    r"^(?P<exchange>[A-Za-z]+)\.(?P<underlying>.+)(?P<class>[CP])(?P<strike>[0-9]+(?:\.[0-9]+)?)$",
    re.IGNORECASE,
)
COMMODITY_OPTION_EXCHANGES = frozenset({"DCE", "CZCE", "SHFE", "INE", "GFEX"})


@dataclass(frozen=True)
class MarketDiscoverySummary:
    """Summary of a full-market option symbol discovery run."""

    option_symbol_count: int
    underlying_count: int
    record_count: int
    inactive_marked: int
    exchange_counts: dict[str, int]
    product_counts: dict[str, int]


def derive_underlying_symbol(option_symbol: str) -> str:
    """Derive the underlying TQSDK symbol from a parseable option symbol."""

    match = _match_option_symbol(option_symbol)
    if not match:
        raise InstrumentNormalizationError(f"Invalid option symbol: {option_symbol}")
    return f"{match.group('exchange').upper()}.{match.group('underlying')}"


def group_option_symbols_by_underlying(
    option_symbols: list[str],
    *,
    allowed_underlyings: set[str] | None = None,
) -> dict[str, dict[str, list[str]]]:
    """Group active option symbols by derived underlying and CALL/PUT side."""

    allowed = {symbol.upper(): symbol for symbol in allowed_underlyings or set()}
    grouped: dict[str, dict[str, list[str]]] = defaultdict(lambda: {"CALL": [], "PUT": []})
    for symbol in option_symbols:
        cleaned = symbol.strip()
        if not cleaned:
            continue
        if _exchange_id(cleaned) not in COMMODITY_OPTION_EXCHANGES:
            continue
        match = _match_option_symbol(cleaned)
        if not match:
            raise InstrumentNormalizationError(f"Invalid option symbol: {symbol}")
        underlying = f"{match.group('exchange').upper()}.{match.group('underlying')}"
        if allowed and underlying.upper() not in allowed:
            continue
        option_class = "CALL" if match.group("class").upper() == "C" else "PUT"
        grouped[underlying][option_class].append(cleaned)

    return {
        underlying: {
            "CALL": sorted(classes["CALL"]),
            "PUT": sorted(classes["PUT"]),
        }
        for underlying, classes in sorted(grouped.items())
    }


def normalize_market_option_symbols(
    option_symbols: list[str],
    *,
    last_seen_at: str,
    allowed_underlyings: set[str] | None = None,
) -> list[InstrumentRecord]:
    """Normalize all discovered option symbols into instrument records."""

    records: list[InstrumentRecord] = []
    for underlying, classes in group_option_symbols_by_underlying(
        option_symbols,
        allowed_underlyings=allowed_underlyings,
    ).items():
        records.extend(
            normalize_option_chain_discovery(
                underlying_symbol=underlying,
                call_symbols=classes["CALL"],
                put_symbols=classes["PUT"],
                last_seen_at=last_seen_at,
            )
        )
    return sorted({record.symbol: record for record in records}.values(), key=lambda item: item.symbol)


def persist_market_option_symbols(
    connection: sqlite3.Connection,
    option_symbols: list[str],
    *,
    last_seen_at: str,
    allowed_underlyings: set[str] | None = None,
) -> MarketDiscoverySummary:
    """Persist full-market option symbols and mark missing prior options inactive."""

    grouped = group_option_symbols_by_underlying(
        option_symbols,
        allowed_underlyings=allowed_underlyings,
    )
    records = normalize_market_option_symbols(
        option_symbols,
        last_seen_at=last_seen_at,
        allowed_underlyings=allowed_underlyings,
    )
    repository = InstrumentRepository(connection)
    repository.upsert_instruments(records)
    inactive_marked = 0
    for underlying, classes in grouped.items():
        seen = {underlying, *classes["CALL"], *classes["PUT"]}
        inactive_marked += repository.mark_missing_inactive(
            underlying_symbol=underlying,
            seen_symbols=seen,
            last_seen_at=last_seen_at,
        )

    return MarketDiscoverySummary(
        option_symbol_count=sum(
            len(classes["CALL"]) + len(classes["PUT"])
            for classes in grouped.values()
        ),
        underlying_count=len(grouped),
        record_count=len(records),
        inactive_marked=inactive_marked,
        exchange_counts=_exchange_counts(grouped.keys()),
        product_counts=_product_counts(grouped.keys()),
    )


def _exchange_counts(underlyings: object) -> dict[str, int]:
    counts: dict[str, int] = {}
    for symbol in underlyings:
        exchange = str(symbol).split(".", 1)[0]
        counts[exchange] = counts.get(exchange, 0) + 1
    return dict(sorted(counts.items()))


def _product_counts(underlyings: object) -> dict[str, int]:
    counts: dict[str, int] = {}
    for symbol in underlyings:
        instrument_id = str(symbol).split(".", 1)[1]
        match = re.match(r"([A-Za-z]+)", instrument_id)
        product = match.group(1).lower() if match else instrument_id
        counts[product] = counts.get(product, 0) + 1
    return dict(sorted(counts.items()))


def _match_option_symbol(symbol: str):
    cleaned = symbol.strip()
    return (
        SERIES_OPTION_SYMBOL_PATTERN.match(cleaned)
        or OPTION_SYMBOL_PATTERN.match(cleaned)
        or COMPACT_OPTION_SYMBOL_PATTERN.match(cleaned)
    )


def _exchange_id(symbol: str) -> str:
    return symbol.split(".", 1)[0].upper() if "." in symbol else ""
