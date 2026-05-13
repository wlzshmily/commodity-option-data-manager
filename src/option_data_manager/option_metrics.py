"""Option Greeks and IV current-slice normalization and persistence."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
import json
import sqlite3
from typing import Any

from .source_values import finite_float, json_safe
from .storage import Migration, apply_migrations


OPTION_SOURCE_METRICS_CURRENT_MIGRATION = Migration(
    400,
    "create option_source_metrics_current",
    (
        """
        CREATE TABLE IF NOT EXISTS option_source_metrics_current (
            symbol TEXT PRIMARY KEY,
            received_at TEXT NOT NULL,
            delta REAL,
            gamma REAL,
            theta REAL,
            vega REAL,
            rho REAL,
            iv REAL,
            source_method TEXT NOT NULL,
            raw_payload_json TEXT NOT NULL
        )
        """,
    ),
)

OPTION_SOURCE_METRICS_READ_MODEL_INDEX_MIGRATION = Migration(
    401,
    "add option_source_metrics read model indexes",
    (
        """
        CREATE INDEX IF NOT EXISTS idx_option_source_metrics_received_at
        ON option_source_metrics_current(received_at)
        """,
    ),
)


GREEK_FIELDS = ("delta", "gamma", "theta", "vega", "rho")
DEFAULT_SOURCE_METHOD = "query_option_greeks+OPTION_IMPV"


@dataclass(frozen=True)
class OptionMetricsRecord:
    """Normalized option source metrics ready for SQLite storage."""

    symbol: str
    received_at: str
    delta: float | None
    gamma: float | None
    theta: float | None
    vega: float | None
    rho: float | None
    iv: float | None
    source_method: str
    raw_payload_json: str


class OptionMetricsRepository:
    """SQLite repository for latest option Greeks and IV metrics."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        connection.row_factory = sqlite3.Row
        self._connection = connection
        apply_migrations(
            connection,
            (
                OPTION_SOURCE_METRICS_CURRENT_MIGRATION,
                OPTION_SOURCE_METRICS_READ_MODEL_INDEX_MIGRATION,
            ),
        )

    def upsert_metrics(self, record: OptionMetricsRecord) -> None:
        """Insert or replace the current metrics slice for one option symbol."""

        self._connection.execute(
            """
            INSERT INTO option_source_metrics_current (
                symbol,
                received_at,
                delta,
                gamma,
                theta,
                vega,
                rho,
                iv,
                source_method,
                raw_payload_json
            )
            VALUES (
                :symbol,
                :received_at,
                :delta,
                :gamma,
                :theta,
                :vega,
                :rho,
                :iv,
                :source_method,
                :raw_payload_json
            )
            ON CONFLICT(symbol) DO UPDATE SET
                received_at = CASE
                    WHEN
                        excluded.delta IS NOT NULL
                        OR excluded.gamma IS NOT NULL
                        OR excluded.theta IS NOT NULL
                        OR excluded.vega IS NOT NULL
                        OR excluded.rho IS NOT NULL
                        OR excluded.iv IS NOT NULL
                    THEN excluded.received_at
                    ELSE option_source_metrics_current.received_at
                END,
                delta = COALESCE(excluded.delta, option_source_metrics_current.delta),
                gamma = COALESCE(excluded.gamma, option_source_metrics_current.gamma),
                theta = COALESCE(excluded.theta, option_source_metrics_current.theta),
                vega = COALESCE(excluded.vega, option_source_metrics_current.vega),
                rho = COALESCE(excluded.rho, option_source_metrics_current.rho),
                iv = COALESCE(excluded.iv, option_source_metrics_current.iv),
                source_method = excluded.source_method,
                raw_payload_json = excluded.raw_payload_json
            """,
            asdict(record),
        )
        self._connection.commit()

    def get_metrics(self, symbol: str) -> OptionMetricsRecord | None:
        """Return the current metrics row for one option symbol."""

        row = self._connection.execute(
            """
            SELECT
                symbol,
                received_at,
                delta,
                gamma,
                theta,
                vega,
                rho,
                iv,
                source_method,
                raw_payload_json
            FROM option_source_metrics_current
            WHERE symbol = ?
            """,
            (symbol,),
        ).fetchone()
        if row is None:
            return None
        return OptionMetricsRecord(**dict(row))


def normalize_option_metrics(
    symbol: str,
    *,
    received_at: str,
    greeks_payload: Mapping[str, Any] | object | None = None,
    iv_payload: Mapping[str, Any] | object | None = None,
    source_method: str = DEFAULT_SOURCE_METHOD,
) -> OptionMetricsRecord:
    """Normalize TQSDK Greeks and OPTION_IMPV outputs into one metrics row."""

    cleaned_symbol = symbol.strip()
    if not cleaned_symbol:
        raise ValueError("Option metrics symbol must not be empty.")
    if not received_at.strip():
        raise ValueError("Option metrics received_at must not be empty.")
    if not source_method.strip():
        raise ValueError("Option metrics source_method must not be empty.")

    greeks = _tabular_payload_to_dict(greeks_payload)
    iv_data = _tabular_payload_to_dict(iv_payload)
    raw_payload = {
        "greeks": greeks,
        "iv": iv_data,
        "source_method": source_method,
    }
    raw_payload_json = json.dumps(
        json_safe(raw_payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )

    return OptionMetricsRecord(
        symbol=cleaned_symbol,
        received_at=received_at,
        delta=finite_float(_first_series_value(greeks.get("delta"))),
        gamma=finite_float(_first_series_value(greeks.get("gamma"))),
        theta=finite_float(_first_series_value(greeks.get("theta"))),
        vega=finite_float(_first_series_value(greeks.get("vega"))),
        rho=finite_float(_first_series_value(greeks.get("rho"))),
        iv=finite_float(_last_series_value(iv_data.get("impv"))),
        source_method=source_method,
        raw_payload_json=raw_payload_json,
    )


def _tabular_payload_to_dict(payload: Mapping[str, Any] | object | None) -> dict[str, Any]:
    if payload is None:
        return {}
    if isinstance(payload, Mapping):
        return dict(payload)
    if hasattr(payload, "to_dict"):
        return dict(payload.to_dict())
    raise ValueError("Option metrics payload must be mapping-like or DataFrame-like.")


def _first_series_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        values = list(value.values())
        return values[0] if values else None
    if isinstance(value, list | tuple):
        return value[0] if value else None
    return value


def _last_series_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        values = list(value.values())
        return values[-1] if values else None
    if isinstance(value, list | tuple):
        return value[-1] if value else None
    return value
