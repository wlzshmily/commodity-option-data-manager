"""20-day K-line current-slice normalization and SQLite persistence."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
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


KLINE_20D_CURRENT_MIGRATION = Migration(
    300,
    "create kline_20d_current",
    (
        """
        CREATE TABLE IF NOT EXISTS kline_20d_current (
            symbol TEXT NOT NULL,
            bar_datetime TEXT NOT NULL,
            open_price REAL,
            high_price REAL,
            low_price REAL,
            close_price REAL,
            volume REAL,
            open_oi REAL,
            close_oi REAL,
            received_at TEXT NOT NULL,
            raw_payload_json TEXT NOT NULL,
            PRIMARY KEY (symbol, bar_datetime)
        )
        """,
    ),
)


KLINE_BASE_FIELDS = (
    "datetime",
    "id",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "open_oi",
    "close_oi",
    "symbol",
    "duration",
)

KLINE_NUMERIC_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "open_oi",
    "close_oi",
)


@dataclass(frozen=True)
class KlineRecord:
    """Normalized current 20-day K-line row ready for SQLite storage."""

    symbol: str
    bar_datetime: str
    open_price: float | None
    high_price: float | None
    low_price: float | None
    close_price: float | None
    volume: float | None
    open_oi: float | None
    close_oi: float | None
    received_at: str
    raw_payload_json: str


class KlineRepository:
    """SQLite repository for current 20-day K-line rows."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        connection.row_factory = sqlite3.Row
        self._connection = connection
        apply_migrations(connection, (KLINE_20D_CURRENT_MIGRATION,))

    def replace_symbol_klines(self, symbol: str, records: Iterable[KlineRecord]) -> None:
        """Replace the current K-line slice for one symbol."""

        cleaned_symbol = symbol.strip()
        if not cleaned_symbol:
            raise ValueError("K-line symbol must not be empty.")

        rows_by_datetime: dict[str, KlineRecord] = {}
        for record in records:
            if record.symbol != cleaned_symbol:
                raise ValueError("All K-line records must match the replaced symbol.")
            rows_by_datetime[record.bar_datetime] = record
        rows = [
            rows_by_datetime[bar_datetime]
            for bar_datetime in sorted(rows_by_datetime)
        ]

        self._connection.execute("BEGIN")
        try:
            self._connection.execute(
                "DELETE FROM kline_20d_current WHERE symbol = ?",
                (cleaned_symbol,),
            )
            self._connection.executemany(
                """
                INSERT INTO kline_20d_current (
                    symbol,
                    bar_datetime,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    open_oi,
                    close_oi,
                    received_at,
                    raw_payload_json
                )
                VALUES (
                    :symbol,
                    :bar_datetime,
                    :open_price,
                    :high_price,
                    :low_price,
                    :close_price,
                    :volume,
                    :open_oi,
                    :close_oi,
                    :received_at,
                    :raw_payload_json
                )
                """,
                [asdict(record) for record in rows],
            )
        except Exception:
            self._connection.rollback()
            raise
        else:
            self._connection.commit()

    def get_klines(self, symbol: str) -> list[KlineRecord]:
        """Return current K-line rows for one symbol ordered by bar time."""

        rows = self._connection.execute(
            """
            SELECT
                symbol,
                bar_datetime,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                open_oi,
                close_oi,
                received_at,
                raw_payload_json
            FROM kline_20d_current
            WHERE symbol = ?
            ORDER BY bar_datetime
            """,
            (symbol,),
        ).fetchall()
        return [KlineRecord(**dict(row)) for row in rows]


def merge_kline_records(
    existing: Iterable[KlineRecord],
    fresh: Iterable[KlineRecord],
    *,
    limit: int = 20,
) -> list[KlineRecord]:
    """Merge cached and fresh rows by bar time, keeping the latest rows.

    Fresh rows win when the same bar exists in both inputs. The returned rows
    are sorted by bar time and clipped to the latest `limit` records.
    """

    if limit < 1:
        raise ValueError("K-line merge limit must be positive.")
    rows_by_datetime: dict[str, KlineRecord] = {}
    for record in existing:
        rows_by_datetime[record.bar_datetime] = record
    for record in fresh:
        rows_by_datetime[record.bar_datetime] = record
    return [
        rows_by_datetime[bar_datetime]
        for bar_datetime in sorted(rows_by_datetime)[-limit:]
    ]


def records_to_multi_symbol_kline_frame(
    symbols: Sequence[str],
    records_by_symbol: Mapping[str, Sequence[KlineRecord]],
) -> object:
    """Build a TQSDK-like multi-symbol K-line DataFrame from normalized rows."""

    cleaned_symbols = [symbol.strip() for symbol in symbols]
    if not cleaned_symbols or any(not symbol for symbol in cleaned_symbols):
        raise ValueError("K-line symbols must not be empty.")
    records_by_datetime = {
        symbol: {record.bar_datetime: record for record in records_by_symbol.get(symbol, ())}
        for symbol in cleaned_symbols
    }
    common_datetimes: set[str] | None = None
    for symbol in cleaned_symbols:
        datetimes = set(records_by_datetime[symbol])
        common_datetimes = datetimes if common_datetimes is None else common_datetimes & datetimes
    ordered_datetimes = sorted(common_datetimes or set())
    rows: list[dict[str, Any]] = []
    for bar_datetime in ordered_datetimes:
        row: dict[str, Any] = {"datetime": bar_datetime}
        for index, symbol in enumerate(cleaned_symbols):
            suffix = "" if index == 0 else str(index)
            record = records_by_datetime[symbol][bar_datetime]
            payload = _record_payload(record)
            row["duration"] = payload.get("duration", 24 * 60 * 60)
            if suffix:
                row[_field_name("datetime", suffix)] = payload.get(
                    "datetime",
                    bar_datetime,
                )
            else:
                row["datetime"] = payload.get("datetime", bar_datetime)
            row[_field_name("symbol", suffix)] = symbol
            row[_field_name("id", suffix)] = payload.get("id", float(len(rows)))
            for field in KLINE_NUMERIC_FIELDS:
                row[_field_name(field, suffix)] = payload.get(
                    field,
                    _record_numeric_value(record, field),
                )
        rows.append(row)

    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - TQSDK runtime provides pandas.
        raise RuntimeError("pandas is required to build spliced K-line frames.") from exc
    return pd.DataFrame(rows)


def normalize_kline_rows(
    symbol: str,
    raw_rows: Iterable[Mapping[str, Any]] | object,
    *,
    received_at: str,
    column_suffix: str = "",
    time_converter: Callable[[Any], Any] | None = None,
) -> list[KlineRecord]:
    """Normalize single-symbol K-line rows from records or a DataFrame-like object."""

    cleaned_symbol = symbol.strip()
    if not cleaned_symbol:
        raise ValueError("K-line symbol must not be empty.")
    if not received_at.strip():
        raise ValueError("K-line received_at must not be empty.")

    converter = time_converter or convert_tqsdk_time
    rows = _rows_from_raw(raw_rows)
    records: list[KlineRecord] = []
    for row in rows:
        record = _normalize_one_row(
            cleaned_symbol,
            row,
            received_at=received_at,
            column_suffix=column_suffix,
            time_converter=converter,
        )
        if record is not None:
            records.append(record)
    return records


def normalize_multi_symbol_kline_rows(
    symbols: Iterable[str],
    raw_rows: Iterable[Mapping[str, Any]] | object,
    *,
    received_at: str,
    time_converter: Callable[[Any], Any] | None = None,
) -> dict[str, list[KlineRecord]]:
    """Normalize TQSDK multi-symbol K-line rows using observed suffix rules."""

    cleaned_symbols = [symbol.strip() for symbol in symbols]
    if not cleaned_symbols or any(not symbol for symbol in cleaned_symbols):
        raise ValueError("K-line symbols must not be empty.")

    rows = _rows_from_raw(raw_rows)
    normalized: dict[str, list[KlineRecord]] = {}
    for index, symbol in enumerate(cleaned_symbols):
        suffix = "" if index == 0 else str(index)
        normalized[symbol] = normalize_kline_rows(
            symbol,
            rows,
            received_at=received_at,
            column_suffix=suffix,
            time_converter=time_converter,
        )
    return normalized


def _normalize_one_row(
    symbol: str,
    row: Mapping[str, Any],
    *,
    received_at: str,
    column_suffix: str,
    time_converter: Callable[[Any], Any],
) -> KlineRecord | None:
    source_symbol = row.get(_field_name("symbol", column_suffix))
    if source_symbol is not None and str(source_symbol).strip() != symbol:
        return None

    bar_datetime = normalize_datetime(
        _value_with_shared_datetime(row, "datetime", column_suffix),
        time_converter,
    )
    if bar_datetime is None:
        return None

    raw_payload = _raw_payload_for_suffix(row, column_suffix)
    raw_payload_json = json.dumps(
        json_safe(raw_payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )

    return KlineRecord(
        symbol=symbol,
        bar_datetime=bar_datetime,
        open_price=finite_float(row.get(_field_name("open", column_suffix))),
        high_price=finite_float(row.get(_field_name("high", column_suffix))),
        low_price=finite_float(row.get(_field_name("low", column_suffix))),
        close_price=finite_float(row.get(_field_name("close", column_suffix))),
        volume=finite_float(row.get(_field_name("volume", column_suffix))),
        open_oi=finite_float(row.get(_field_name("open_oi", column_suffix))),
        close_oi=finite_float(row.get(_field_name("close_oi", column_suffix))),
        received_at=received_at,
        raw_payload_json=raw_payload_json,
    )


def _rows_from_raw(raw_rows: Iterable[Mapping[str, Any]] | object) -> list[dict[str, Any]]:
    if hasattr(raw_rows, "to_dict"):
        frame_records = raw_rows.to_dict("records")
        return [dict(row) for row in frame_records]
    return [dict(row) for row in raw_rows]  # type: ignore[arg-type]


def _record_payload(record: KlineRecord) -> dict[str, Any]:
    try:
        payload = json.loads(record.raw_payload_json)
    except json.JSONDecodeError:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _record_numeric_value(record: KlineRecord, field: str) -> float | None:
    return {
        "open": record.open_price,
        "high": record.high_price,
        "low": record.low_price,
        "close": record.close_price,
        "volume": record.volume,
        "open_oi": record.open_oi,
        "close_oi": record.close_oi,
    }[field]


def _field_name(base_name: str, suffix: str) -> str:
    return f"{base_name}{suffix}" if suffix else base_name


def _raw_payload_for_suffix(row: Mapping[str, Any], suffix: str) -> dict[str, Any]:
    return {
        field: _value_with_shared_datetime(row, field, suffix)
        for field in KLINE_BASE_FIELDS
        if (
            _field_name(field, suffix) in row
            or (suffix and field in {"datetime", "duration"} and field in row)
        )
    }


def _value_with_shared_datetime(
    row: Mapping[str, Any],
    base_name: str,
    suffix: str,
) -> Any:
    field_name = _field_name(base_name, suffix)
    if field_name in row:
        return row.get(field_name)
    if suffix and base_name in {"datetime", "duration"}:
        return row.get(base_name)
    return None
