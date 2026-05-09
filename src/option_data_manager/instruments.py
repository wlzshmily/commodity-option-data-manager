"""Instrument discovery normalization."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
import sqlite3
from typing import Any, Mapping

from .storage import Migration, apply_migrations


INSTRUMENTS_MIGRATION = Migration(
    100,
    "create instruments",
    (
        """
        CREATE TABLE IF NOT EXISTS instruments (
            symbol TEXT PRIMARY KEY,
            exchange_id TEXT NOT NULL,
            product_id TEXT,
            instrument_id TEXT NOT NULL,
            instrument_name TEXT,
            ins_class TEXT,
            underlying_symbol TEXT,
            option_class TEXT,
            strike_price REAL,
            expire_datetime TEXT,
            price_tick REAL,
            volume_multiple INTEGER,
            expired INTEGER NOT NULL CHECK (expired IN (0, 1)),
            active INTEGER NOT NULL CHECK (active IN (0, 1)),
            inactive_reason TEXT,
            last_seen_at TEXT NOT NULL,
            raw_payload_json TEXT NOT NULL
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_instruments_underlying_option_class
        ON instruments(underlying_symbol, option_class, active)
        """,
    ),
)

INSTRUMENT_TQSDK_FIELDS_MIGRATION = Migration(
    101,
    "add tqsdk instrument date fields",
    (
        "ALTER TABLE instruments ADD COLUMN delivery_year INTEGER",
        "ALTER TABLE instruments ADD COLUMN delivery_month INTEGER",
        "ALTER TABLE instruments ADD COLUMN last_exercise_datetime TEXT",
        "ALTER TABLE instruments ADD COLUMN exercise_year INTEGER",
        "ALTER TABLE instruments ADD COLUMN exercise_month INTEGER",
    ),
)


class InstrumentNormalizationError(ValueError):
    """Raised when a discovered instrument cannot be normalized safely."""


@dataclass(frozen=True)
class InstrumentRecord:
    """Normalized instrument metadata aligned with the draft instruments table."""

    symbol: str
    exchange_id: str
    product_id: str | None
    instrument_id: str
    instrument_name: str | None
    ins_class: str | None
    underlying_symbol: str | None
    option_class: str | None
    strike_price: float | None
    expire_datetime: str | None
    price_tick: float | None
    volume_multiple: int | None
    expired: bool
    active: bool
    inactive_reason: str | None
    last_seen_at: str
    raw_payload_json: str
    delivery_year: int | None = None
    delivery_month: int | None = None
    last_exercise_datetime: str | None = None
    exercise_year: int | None = None
    exercise_month: int | None = None


class InstrumentRepository:
    """SQLite repository for discovered instruments."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        connection.row_factory = sqlite3.Row
        self._connection = connection
        apply_migrations(
            connection,
            (INSTRUMENTS_MIGRATION, INSTRUMENT_TQSDK_FIELDS_MIGRATION),
        )

    def upsert_instruments(self, records: list[InstrumentRecord]) -> None:
        """Insert or update discovered instruments."""

        self._connection.executemany(
            """
            INSERT INTO instruments (
                symbol,
                exchange_id,
                product_id,
                instrument_id,
                instrument_name,
                ins_class,
                underlying_symbol,
                option_class,
                strike_price,
                expire_datetime,
                delivery_year,
                delivery_month,
                last_exercise_datetime,
                exercise_year,
                exercise_month,
                price_tick,
                volume_multiple,
                expired,
                active,
                inactive_reason,
                last_seen_at,
                raw_payload_json
            )
            VALUES (
                :symbol,
                :exchange_id,
                :product_id,
                :instrument_id,
                :instrument_name,
                :ins_class,
                :underlying_symbol,
                :option_class,
                :strike_price,
                :expire_datetime,
                :delivery_year,
                :delivery_month,
                :last_exercise_datetime,
                :exercise_year,
                :exercise_month,
                :price_tick,
                :volume_multiple,
                :expired,
                :active,
                :inactive_reason,
                :last_seen_at,
                :raw_payload_json
            )
            ON CONFLICT(symbol) DO UPDATE SET
                exchange_id = excluded.exchange_id,
                product_id = excluded.product_id,
                instrument_id = excluded.instrument_id,
                instrument_name = excluded.instrument_name,
                ins_class = excluded.ins_class,
                underlying_symbol = excluded.underlying_symbol,
                option_class = excluded.option_class,
                strike_price = excluded.strike_price,
                expire_datetime = excluded.expire_datetime,
                delivery_year = excluded.delivery_year,
                delivery_month = excluded.delivery_month,
                last_exercise_datetime = excluded.last_exercise_datetime,
                exercise_year = excluded.exercise_year,
                exercise_month = excluded.exercise_month,
                price_tick = excluded.price_tick,
                volume_multiple = excluded.volume_multiple,
                expired = excluded.expired,
                active = excluded.active,
                inactive_reason = excluded.inactive_reason,
                last_seen_at = excluded.last_seen_at,
                raw_payload_json = excluded.raw_payload_json
            """,
            [_record_to_row(record) for record in records],
        )
        self._connection.commit()

    def update_tqsdk_quote_fields(
        self,
        symbol: str,
        raw_quote: Mapping[str, Any] | object,
    ) -> bool:
        """Update persisted instrument metadata with TQSDK same-name quote fields."""

        payload = _payload_from_object(raw_quote)
        values = {
            "expire_datetime": _optional_text(payload, "expire_datetime"),
            "delivery_year": _optional_int(payload, "delivery_year"),
            "delivery_month": _optional_int(payload, "delivery_month"),
            "last_exercise_datetime": _optional_text(payload, "last_exercise_datetime"),
            "exercise_year": _optional_int(payload, "exercise_year"),
            "exercise_month": _optional_int(payload, "exercise_month"),
        }
        updates = {key: value for key, value in values.items() if value is not None}
        if not updates:
            return False
        assignments = ", ".join(f"{key} = ?" for key in updates)
        cursor = self._connection.execute(
            f"UPDATE instruments SET {assignments} WHERE symbol = ?",
            (*updates.values(), symbol),
        )
        self._connection.commit()
        return cursor.rowcount > 0

    def mark_missing_inactive(
        self,
        *,
        underlying_symbol: str,
        seen_symbols: set[str],
        last_seen_at: str,
    ) -> int:
        """Mark previously active option rows for an underlying inactive if missing."""

        placeholders = ",".join("?" for _ in seen_symbols)
        params: list[object] = [
            "missing_from_source",
            last_seen_at,
            underlying_symbol,
        ]
        predicate = ""
        if seen_symbols:
            predicate = f"AND symbol NOT IN ({placeholders})"
            params.extend(sorted(seen_symbols))

        cursor = self._connection.execute(
            f"""
            UPDATE instruments
            SET
                active = 0,
                inactive_reason = ?,
                last_seen_at = ?
            WHERE underlying_symbol = ?
              AND option_class IN ('CALL', 'PUT')
              AND active = 1
              {predicate}
            """,
            params,
        )
        self._connection.commit()
        return int(cursor.rowcount)

    def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Return one instrument by symbol."""

        row = self._connection.execute(
            """
            SELECT *
            FROM instruments
            WHERE symbol = ?
            """,
            (symbol,),
        ).fetchone()
        if row is None:
            return None
        return _record_from_row(row)

    def list_active_options(self, underlying_symbol: str) -> list[InstrumentRecord]:
        """Return active CALL/PUT options for one underlying."""

        rows = self._connection.execute(
            """
            SELECT *
            FROM instruments
            WHERE underlying_symbol = ?
              AND option_class IN ('CALL', 'PUT')
              AND active = 1
            ORDER BY option_class, strike_price, symbol
            """,
            (underlying_symbol,),
        ).fetchall()
        return [_record_from_row(row) for row in rows]

    def list_active_underlyings_for_product(
        self,
        *,
        exchange_id: str,
        product_id: str,
    ) -> list[InstrumentRecord]:
        """Return active futures underlyings for one exchange/product."""

        rows = self._connection.execute(
            """
            SELECT *
            FROM instruments
            WHERE exchange_id = ?
              AND product_id = ?
              AND ins_class = 'FUTURE'
              AND active = 1
            ORDER BY symbol
            """,
            (exchange_id, product_id),
        ).fetchall()
        return [_record_from_row(row) for row in rows]

    def count_by_underlying(self, underlying_symbol: str) -> dict[str, int]:
        """Return active CALL/PUT counts for one underlying."""

        rows = self._connection.execute(
            """
            SELECT option_class, COUNT(*) AS count
            FROM instruments
            WHERE underlying_symbol = ?
              AND active = 1
              AND option_class IN ('CALL', 'PUT')
            GROUP BY option_class
            """,
            (underlying_symbol,),
        ).fetchall()
        counts = {"CALL": 0, "PUT": 0}
        for row in rows:
            counts[str(row["option_class"])] = int(row["count"])
        return counts


def normalize_discovery_result(
    records: list[Mapping[str, Any]],
    *,
    last_seen_at: str,
) -> list[InstrumentRecord]:
    """Normalize a batch of raw discovery records and reject duplicates."""

    normalized = [
        normalize_instrument(record, last_seen_at=last_seen_at)
        for record in records
    ]
    seen: set[str] = set()
    duplicates: set[str] = set()
    for instrument in normalized:
        if instrument.symbol in seen:
            duplicates.add(instrument.symbol)
        seen.add(instrument.symbol)
    if duplicates:
        duplicate_list = ", ".join(sorted(duplicates))
        raise InstrumentNormalizationError(f"Duplicate symbols: {duplicate_list}")
    return sorted(normalized, key=lambda item: item.symbol)


def normalize_option_chain_discovery(
    *,
    underlying_symbol: str,
    call_symbols: list[str],
    put_symbols: list[str],
    last_seen_at: str,
) -> list[InstrumentRecord]:
    """Normalize one underlying futures symbol and its CALL/PUT option symbols."""

    underlying_exchange, underlying_instrument = split_tqsdk_symbol(underlying_symbol)
    records: list[dict[str, Any]] = [
        {
            "symbol": underlying_symbol,
            "exchange_id": underlying_exchange,
            "instrument_id": underlying_instrument,
            "ins_class": "FUTURE",
        }
    ]
    records.extend(
        _option_record(symbol, underlying_symbol=underlying_symbol, option_class="CALL")
        for symbol in call_symbols
    )
    records.extend(
        _option_record(symbol, underlying_symbol=underlying_symbol, option_class="PUT")
        for symbol in put_symbols
    )
    return normalize_discovery_result(records, last_seen_at=last_seen_at)


def split_tqsdk_symbol(symbol: str) -> tuple[str, str]:
    """Split a TQSDK symbol into exchange and instrument id."""

    cleaned = symbol.strip()
    if "." not in cleaned:
        raise InstrumentNormalizationError(f"Invalid TQSDK symbol: {symbol}")
    exchange, instrument_id = cleaned.split(".", 1)
    if not exchange or not instrument_id:
        raise InstrumentNormalizationError(f"Invalid TQSDK symbol: {symbol}")
    return exchange.upper(), instrument_id


def normalize_instrument(
    record: Mapping[str, Any],
    *,
    last_seen_at: str,
) -> InstrumentRecord:
    """Normalize one raw TQSDK/reference discovery record."""

    exchange_id = _required_text(record, "exchange_id").upper()
    instrument_id = _required_text(record, "instrument_id")
    symbol = _normalize_symbol(record.get("symbol"), exchange_id, instrument_id)
    expired = _to_bool(record.get("expired", False))
    active = _to_bool(record.get("active", not expired)) and not expired
    inactive_reason = _inactive_reason(record, expired=expired, active=active)

    return InstrumentRecord(
        symbol=symbol,
        exchange_id=exchange_id,
        product_id=_optional_text(record, "product_id")
        or _derive_product_id(instrument_id),
        instrument_id=instrument_id,
        instrument_name=_optional_text(record, "instrument_name"),
        ins_class=_optional_text(record, "ins_class"),
        underlying_symbol=_optional_text(record, "underlying_symbol"),
        option_class=_normalize_option_class(record),
        strike_price=_optional_float(record, "strike_price", "strike"),
        expire_datetime=_optional_text(record, "expire_datetime"),
        price_tick=_optional_float(record, "price_tick"),
        volume_multiple=_optional_int(record, "volume_multiple"),
        expired=expired,
        active=active,
        inactive_reason=inactive_reason,
        last_seen_at=last_seen_at,
        raw_payload_json=json.dumps(record, ensure_ascii=False, sort_keys=True),
        delivery_year=_optional_int(record, "delivery_year"),
        delivery_month=_optional_int(record, "delivery_month"),
        last_exercise_datetime=_optional_text(record, "last_exercise_datetime"),
        exercise_year=_optional_int(record, "exercise_year"),
        exercise_month=_optional_int(record, "exercise_month"),
    )


def _normalize_symbol(
    value: object,
    exchange_id: str,
    instrument_id: str,
) -> str:
    if value is None or not str(value).strip():
        return f"{exchange_id}.{instrument_id}"
    symbol = str(value).strip()
    if "." in symbol:
        return symbol
    return f"{exchange_id}.{symbol}"


def _normalize_option_class(record: Mapping[str, Any]) -> str | None:
    value = (
        record.get("option_class")
        or record.get("option_type")
        or record.get("option_side")
    )
    if value is None or not str(value).strip():
        return None
    normalized = str(value).strip().upper()
    if normalized in {"C", "CALL", "看涨"}:
        return "CALL"
    if normalized in {"P", "PUT", "看跌"}:
        return "PUT"
    raise InstrumentNormalizationError(f"Unsupported option class: {value}")


def _inactive_reason(
    record: Mapping[str, Any],
    *,
    expired: bool,
    active: bool,
) -> str | None:
    reason = _optional_text(record, "inactive_reason")
    if active:
        return None
    if reason:
        return reason
    if expired:
        return "expired"
    return "missing_from_source"


def _derive_product_id(instrument_id: str) -> str | None:
    match = re.match(r"([A-Za-z]+)", instrument_id)
    return match.group(1).lower() if match else None


def _required_text(record: Mapping[str, Any], key: str) -> str:
    value = record.get(key)
    if value is None or not str(value).strip():
        raise InstrumentNormalizationError(f"Missing required field: {key}")
    return str(value).strip()


def _optional_text(record: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = record.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _optional_float(record: Mapping[str, Any], *keys: str) -> float | None:
    value = _first_value(record, keys)
    if value is None or value == "":
        return None
    return float(value)


def _optional_int(record: Mapping[str, Any], key: str) -> int | None:
    value = record.get(key)
    if value is None or value == "":
        return None
    return int(value)


def _payload_from_object(raw: Mapping[str, Any] | object) -> dict[str, Any]:
    if isinstance(raw, Mapping):
        return dict(raw)
    payload: dict[str, Any] = {}
    for field in (
        "expire_datetime",
        "delivery_year",
        "delivery_month",
        "last_exercise_datetime",
        "exercise_year",
        "exercise_month",
    ):
        if hasattr(raw, field):
            payload[field] = getattr(raw, field)
    return payload


def _first_value(
    record: Mapping[str, Any],
    keys: tuple[str, ...],
) -> object | None:
    for key in keys:
        if key in record:
            return record[key]
    return None


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if value is None:
        return False
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n", ""}:
        return False
    raise InstrumentNormalizationError(f"Cannot normalize boolean value: {value}")


def _option_record(
    symbol: str,
    *,
    underlying_symbol: str,
    option_class: str,
) -> dict[str, Any]:
    exchange_id, instrument_id = split_tqsdk_symbol(symbol)
    strike_price = _derive_strike_price(instrument_id)
    return {
        "symbol": symbol,
        "exchange_id": exchange_id,
        "instrument_id": instrument_id,
        "ins_class": "OPTION",
        "underlying_symbol": underlying_symbol,
        "option_class": option_class,
        "strike_price": strike_price,
    }


def _derive_strike_price(instrument_id: str) -> float | None:
    match = re.search(r"[-_]([CP])[-_]?([0-9]+(?:\.[0-9]+)?)$", instrument_id, re.IGNORECASE)
    if match:
        return float(match.group(2))
    compact = re.search(r"([CP])([0-9]+(?:\.[0-9]+)?)$", instrument_id, re.IGNORECASE)
    if compact:
        return float(compact.group(2))
    return None


def _record_to_row(record: InstrumentRecord) -> dict[str, Any]:
    row = record.__dict__.copy()
    row["expired"] = 1 if record.expired else 0
    row["active"] = 1 if record.active else 0
    return row


def _record_from_row(row: sqlite3.Row) -> InstrumentRecord:
    data = dict(row)
    data["expired"] = bool(data["expired"])
    data["active"] = bool(data["active"])
    return InstrumentRecord(**data)
