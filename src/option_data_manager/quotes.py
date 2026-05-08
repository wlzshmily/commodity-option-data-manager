"""Quote current-slice normalization and SQLite persistence."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
import json
import sqlite3
from typing import Any

from .source_values import (
    convert_tqsdk_time,
    finite_float,
    json_safe,
    normalize_datetime,
)
from .storage import Migration, apply_migrations


QUOTE_CURRENT_MIGRATION = Migration(
    200,
    "create quote_current",
    (
        """
        CREATE TABLE IF NOT EXISTS quote_current (
            symbol TEXT PRIMARY KEY,
            source_datetime TEXT,
            received_at TEXT NOT NULL,
            last_price REAL,
            ask_price1 REAL,
            bid_price1 REAL,
            ask_volume1 REAL,
            bid_volume1 REAL,
            volume REAL,
            open_interest REAL,
            open_price REAL,
            high_price REAL,
            low_price REAL,
            close_price REAL,
            average_price REAL,
            price_tick REAL,
            volume_multiple REAL,
            raw_payload_json TEXT NOT NULL
        )
        """,
    ),
)


QUOTE_SOURCE_FIELDS = (
    "datetime",
    "last_price",
    "ask_price1",
    "bid_price1",
    "ask_volume1",
    "bid_volume1",
    "volume",
    "open_interest",
    "open",
    "highest",
    "lowest",
    "close",
    "average",
    "price_tick",
    "volume_multiple",
)


@dataclass(frozen=True)
class QuoteRecord:
    """Normalized current Quote data ready for SQLite storage."""

    symbol: str
    source_datetime: str | None
    received_at: str
    last_price: float | None
    ask_price1: float | None
    bid_price1: float | None
    ask_volume1: float | None
    bid_volume1: float | None
    volume: float | None
    open_interest: float | None
    open_price: float | None
    high_price: float | None
    low_price: float | None
    close_price: float | None
    average_price: float | None
    price_tick: float | None
    volume_multiple: float | None
    raw_payload_json: str


class QuoteRepository:
    """SQLite repository for the latest Quote slice per symbol."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        connection.row_factory = sqlite3.Row
        self._connection = connection
        apply_migrations(connection, (QUOTE_CURRENT_MIGRATION,))

    def upsert_quote(self, record: QuoteRecord) -> None:
        """Insert or replace the current Quote slice for one symbol."""

        self._connection.execute(
            """
            INSERT INTO quote_current (
                symbol,
                source_datetime,
                received_at,
                last_price,
                ask_price1,
                bid_price1,
                ask_volume1,
                bid_volume1,
                volume,
                open_interest,
                open_price,
                high_price,
                low_price,
                close_price,
                average_price,
                price_tick,
                volume_multiple,
                raw_payload_json
            )
            VALUES (
                :symbol,
                :source_datetime,
                :received_at,
                :last_price,
                :ask_price1,
                :bid_price1,
                :ask_volume1,
                :bid_volume1,
                :volume,
                :open_interest,
                :open_price,
                :high_price,
                :low_price,
                :close_price,
                :average_price,
                :price_tick,
                :volume_multiple,
                :raw_payload_json
            )
            ON CONFLICT(symbol) DO UPDATE SET
                source_datetime = excluded.source_datetime,
                received_at = excluded.received_at,
                last_price = excluded.last_price,
                ask_price1 = excluded.ask_price1,
                bid_price1 = excluded.bid_price1,
                ask_volume1 = excluded.ask_volume1,
                bid_volume1 = excluded.bid_volume1,
                volume = excluded.volume,
                open_interest = excluded.open_interest,
                open_price = excluded.open_price,
                high_price = excluded.high_price,
                low_price = excluded.low_price,
                close_price = excluded.close_price,
                average_price = excluded.average_price,
                price_tick = excluded.price_tick,
                volume_multiple = excluded.volume_multiple,
                raw_payload_json = excluded.raw_payload_json
            """,
            asdict(record),
        )
        self._connection.commit()

    def get_quote(self, symbol: str) -> QuoteRecord | None:
        """Return the current Quote slice for one symbol."""

        row = self._connection.execute(
            """
            SELECT
                symbol,
                source_datetime,
                received_at,
                last_price,
                ask_price1,
                bid_price1,
                ask_volume1,
                bid_volume1,
                volume,
                open_interest,
                open_price,
                high_price,
                low_price,
                close_price,
                average_price,
                price_tick,
                volume_multiple,
                raw_payload_json
            FROM quote_current
            WHERE symbol = ?
            """,
            (symbol,),
        ).fetchone()
        if row is None:
            return None
        return QuoteRecord(**dict(row))


def normalize_quote(
    symbol: str,
    raw_quote: Mapping[str, Any] | object,
    *,
    received_at: str,
    time_converter: Callable[[Any], Any] | None = None,
) -> QuoteRecord:
    """Normalize a TQSDK Quote mapping or object into a current-slice record."""

    cleaned_symbol = symbol.strip()
    if not cleaned_symbol:
        raise ValueError("Quote symbol must not be empty.")
    if not received_at.strip():
        raise ValueError("Quote received_at must not be empty.")

    payload = _payload_from_raw_quote(raw_quote)
    converter = time_converter or convert_tqsdk_time
    source_datetime = normalize_datetime(payload.get("datetime"), converter)
    raw_payload_json = json.dumps(
        json_safe(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )

    return QuoteRecord(
        symbol=cleaned_symbol,
        source_datetime=source_datetime,
        received_at=received_at,
        last_price=finite_float(payload.get("last_price")),
        ask_price1=finite_float(payload.get("ask_price1")),
        bid_price1=finite_float(payload.get("bid_price1")),
        ask_volume1=finite_float(payload.get("ask_volume1")),
        bid_volume1=finite_float(payload.get("bid_volume1")),
        volume=finite_float(payload.get("volume")),
        open_interest=finite_float(payload.get("open_interest")),
        open_price=finite_float(payload.get("open")),
        high_price=finite_float(payload.get("highest")),
        low_price=finite_float(payload.get("lowest")),
        close_price=finite_float(payload.get("close")),
        average_price=finite_float(payload.get("average")),
        price_tick=finite_float(payload.get("price_tick")),
        volume_multiple=finite_float(payload.get("volume_multiple")),
        raw_payload_json=raw_payload_json,
    )


def _payload_from_raw_quote(raw_quote: Mapping[str, Any] | object) -> dict[str, Any]:
    if isinstance(raw_quote, Mapping):
        return dict(raw_quote)

    payload: dict[str, Any] = {}
    for field in QUOTE_SOURCE_FIELDS:
        if hasattr(raw_quote, field):
            payload[field] = getattr(raw_quote, field)
    if not payload:
        raise ValueError("Quote payload does not expose expected source fields.")
    return payload
