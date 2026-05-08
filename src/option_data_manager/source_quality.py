"""Field-group source quality and effective current-value persistence."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import sqlite3
from typing import Any

from .source_values import json_safe
from .storage import Migration, apply_migrations


SOURCE_QUALITY_MIGRATION = Migration(
    600,
    "create source field group quality current",
    (
        """
        CREATE TABLE IF NOT EXISTS source_field_group_quality_current (
            symbol TEXT NOT NULL,
            field_group TEXT NOT NULL,
            quality TEXT NOT NULL,
            source_datetime TEXT,
            received_at TEXT NOT NULL,
            effective_state TEXT NOT NULL,
            effective_source_datetime TEXT,
            effective_received_at TEXT,
            retained_from_source_datetime TEXT,
            retained_from_received_at TEXT,
            current_observed_at TEXT NOT NULL,
            reason TEXT,
            raw_context_json TEXT NOT NULL,
            PRIMARY KEY (symbol, field_group)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS source_field_group_effective_current (
            symbol TEXT NOT NULL,
            field_group TEXT NOT NULL,
            payload_json TEXT,
            source_datetime TEXT,
            received_at TEXT,
            effective_state TEXT NOT NULL,
            retained_from_source_datetime TEXT,
            retained_from_received_at TEXT,
            current_observed_at TEXT NOT NULL,
            reason TEXT,
            updated_at TEXT NOT NULL,
            raw_context_json TEXT NOT NULL,
            PRIMARY KEY (symbol, field_group)
        )
        """,
    ),
)


COMPLETE = "complete"
PARTIAL = "partial"
SOURCE_EMPTY = "source_empty"
SESSION_CLOSED_OR_STATIC = "session_closed_or_static"
STALE_SOURCE = "stale_source"
COLLECTOR_ERROR = "collector_error"

OBSERVED = "observed"
RETAINED = "retained"
MISSING = "missing"
ERROR = "error"

FIELD_GROUPS = frozenset(
    {
        "quote_trade",
        "quote_depth",
        "greeks",
        "iv",
        "daily_kline",
    }
)
QUALITIES = frozenset(
    {
        COMPLETE,
        PARTIAL,
        SOURCE_EMPTY,
        SESSION_CLOSED_OR_STATIC,
        STALE_SOURCE,
        COLLECTOR_ERROR,
    }
)
EFFECTIVE_STATES = frozenset({OBSERVED, RETAINED, MISSING, ERROR})
EFFECTIVE_UPDATE_QUALITIES = frozenset({COMPLETE, PARTIAL})


@dataclass(frozen=True)
class FieldGroupObservation:
    """One source-quality observation for a symbol field group."""

    symbol: str
    field_group: str
    quality: str
    source_datetime: str | None
    received_at: str
    reason: str | None = None
    context: dict[str, Any] | None = None


@dataclass(frozen=True)
class FieldGroupQualityRecord:
    """Current source-quality state for a symbol field group."""

    symbol: str
    field_group: str
    quality: str
    source_datetime: str | None
    received_at: str
    effective_state: str
    effective_source_datetime: str | None
    effective_received_at: str | None
    retained_from_source_datetime: str | None
    retained_from_received_at: str | None
    current_observed_at: str
    reason: str | None
    raw_context_json: str


@dataclass(frozen=True)
class FieldGroupEffectiveRecord:
    """Current effective downstream value for a symbol field group."""

    symbol: str
    field_group: str
    payload_json: str | None
    source_datetime: str | None
    received_at: str | None
    effective_state: str
    retained_from_source_datetime: str | None
    retained_from_received_at: str | None
    current_observed_at: str
    reason: str | None
    updated_at: str
    raw_context_json: str

    def payload(self) -> dict[str, Any] | None:
        """Return decoded payload data when an effective value exists."""

        if self.payload_json is None:
            return None
        return json.loads(self.payload_json)


class SourceQualityRepository:
    """SQLite repository for field-group quality and effective values."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        connection.row_factory = sqlite3.Row
        self._connection = connection
        apply_migrations(connection, (SOURCE_QUALITY_MIGRATION,))

    def upsert_observation(
        self,
        observation: FieldGroupObservation,
        *,
        payload: dict[str, Any] | None = None,
    ) -> FieldGroupQualityRecord:
        """Persist one observed field-group state and merge its effective value."""

        cleaned = _validate_observation(observation)
        context_json = _json(cleaned.context or {})
        prior = self.get_effective(cleaned.symbol, cleaned.field_group)
        effective = _merge_effective(cleaned, payload=payload, prior=prior, context_json=context_json)
        quality = FieldGroupQualityRecord(
            symbol=cleaned.symbol,
            field_group=cleaned.field_group,
            quality=cleaned.quality,
            source_datetime=cleaned.source_datetime,
            received_at=cleaned.received_at,
            effective_state=effective.effective_state,
            effective_source_datetime=effective.source_datetime,
            effective_received_at=effective.received_at,
            retained_from_source_datetime=effective.retained_from_source_datetime,
            retained_from_received_at=effective.retained_from_received_at,
            current_observed_at=cleaned.received_at,
            reason=cleaned.reason,
            raw_context_json=context_json,
        )
        self._connection.execute("BEGIN")
        try:
            self._connection.execute(
                """
                INSERT INTO source_field_group_effective_current (
                    symbol,
                    field_group,
                    payload_json,
                    source_datetime,
                    received_at,
                    effective_state,
                    retained_from_source_datetime,
                    retained_from_received_at,
                    current_observed_at,
                    reason,
                    updated_at,
                    raw_context_json
                )
                VALUES (
                    :symbol,
                    :field_group,
                    :payload_json,
                    :source_datetime,
                    :received_at,
                    :effective_state,
                    :retained_from_source_datetime,
                    :retained_from_received_at,
                    :current_observed_at,
                    :reason,
                    :updated_at,
                    :raw_context_json
                )
                ON CONFLICT(symbol, field_group) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    source_datetime = excluded.source_datetime,
                    received_at = excluded.received_at,
                    effective_state = excluded.effective_state,
                    retained_from_source_datetime = excluded.retained_from_source_datetime,
                    retained_from_received_at = excluded.retained_from_received_at,
                    current_observed_at = excluded.current_observed_at,
                    reason = excluded.reason,
                    updated_at = excluded.updated_at,
                    raw_context_json = excluded.raw_context_json
                """,
                asdict(effective),
            )
            self._connection.execute(
                """
                INSERT INTO source_field_group_quality_current (
                    symbol,
                    field_group,
                    quality,
                    source_datetime,
                    received_at,
                    effective_state,
                    effective_source_datetime,
                    effective_received_at,
                    retained_from_source_datetime,
                    retained_from_received_at,
                    current_observed_at,
                    reason,
                    raw_context_json
                )
                VALUES (
                    :symbol,
                    :field_group,
                    :quality,
                    :source_datetime,
                    :received_at,
                    :effective_state,
                    :effective_source_datetime,
                    :effective_received_at,
                    :retained_from_source_datetime,
                    :retained_from_received_at,
                    :current_observed_at,
                    :reason,
                    :raw_context_json
                )
                ON CONFLICT(symbol, field_group) DO UPDATE SET
                    quality = excluded.quality,
                    source_datetime = excluded.source_datetime,
                    received_at = excluded.received_at,
                    effective_state = excluded.effective_state,
                    effective_source_datetime = excluded.effective_source_datetime,
                    effective_received_at = excluded.effective_received_at,
                    retained_from_source_datetime = excluded.retained_from_source_datetime,
                    retained_from_received_at = excluded.retained_from_received_at,
                    current_observed_at = excluded.current_observed_at,
                    reason = excluded.reason,
                    raw_context_json = excluded.raw_context_json
                """,
                asdict(quality),
            )
        except Exception:
            self._connection.rollback()
            raise
        else:
            self._connection.commit()
        return quality

    def get_quality(self, symbol: str, field_group: str) -> FieldGroupQualityRecord | None:
        """Return current quality state for one symbol field group."""

        row = self._connection.execute(
            """
            SELECT
                symbol,
                field_group,
                quality,
                source_datetime,
                received_at,
                effective_state,
                effective_source_datetime,
                effective_received_at,
                retained_from_source_datetime,
                retained_from_received_at,
                current_observed_at,
                reason,
                raw_context_json
            FROM source_field_group_quality_current
            WHERE symbol = ? AND field_group = ?
            """,
            (_required_symbol(symbol), _required_field_group(field_group)),
        ).fetchone()
        if row is None:
            return None
        return FieldGroupQualityRecord(**dict(row))

    def get_effective(self, symbol: str, field_group: str) -> FieldGroupEffectiveRecord | None:
        """Return current effective value for one symbol field group."""

        row = self._connection.execute(
            """
            SELECT
                symbol,
                field_group,
                payload_json,
                source_datetime,
                received_at,
                effective_state,
                retained_from_source_datetime,
                retained_from_received_at,
                current_observed_at,
                reason,
                updated_at,
                raw_context_json
            FROM source_field_group_effective_current
            WHERE symbol = ? AND field_group = ?
            """,
            (_required_symbol(symbol), _required_field_group(field_group)),
        ).fetchone()
        if row is None:
            return None
        return FieldGroupEffectiveRecord(**dict(row))


def _merge_effective(
    observation: FieldGroupObservation,
    *,
    payload: dict[str, Any] | None,
    prior: FieldGroupEffectiveRecord | None,
    context_json: str,
) -> FieldGroupEffectiveRecord:
    if observation.quality in EFFECTIVE_UPDATE_QUALITIES:
        if payload is None:
            raise ValueError("Complete or partial field-group observations require payload.")
        return FieldGroupEffectiveRecord(
            symbol=observation.symbol,
            field_group=observation.field_group,
            payload_json=_json(payload),
            source_datetime=observation.source_datetime,
            received_at=observation.received_at,
            effective_state=OBSERVED,
            retained_from_source_datetime=None,
            retained_from_received_at=None,
            current_observed_at=observation.received_at,
            reason=observation.reason,
            updated_at=observation.received_at,
            raw_context_json=context_json,
        )

    if observation.quality == COLLECTOR_ERROR:
        if prior is not None and prior.payload_json is not None:
            return FieldGroupEffectiveRecord(
                symbol=observation.symbol,
                field_group=observation.field_group,
                payload_json=prior.payload_json,
                source_datetime=prior.source_datetime,
                received_at=prior.received_at,
                effective_state=RETAINED,
                retained_from_source_datetime=prior.source_datetime,
                retained_from_received_at=prior.received_at,
                current_observed_at=observation.received_at,
                reason=observation.reason or "collector_error_retained_prior_effective",
                updated_at=observation.received_at,
                raw_context_json=context_json,
            )
        return _missing_effective(observation, state=ERROR, context_json=context_json)

    if prior is not None and prior.payload_json is not None:
        return FieldGroupEffectiveRecord(
            symbol=observation.symbol,
            field_group=observation.field_group,
            payload_json=prior.payload_json,
            source_datetime=prior.source_datetime,
            received_at=prior.received_at,
            effective_state=RETAINED,
            retained_from_source_datetime=prior.source_datetime,
            retained_from_received_at=prior.received_at,
            current_observed_at=observation.received_at,
            reason=observation.reason or "source_empty_retained_prior_effective",
            updated_at=observation.received_at,
            raw_context_json=context_json,
        )

    return _missing_effective(observation, state=MISSING, context_json=context_json)


def _missing_effective(
    observation: FieldGroupObservation,
    *,
    state: str,
    context_json: str,
) -> FieldGroupEffectiveRecord:
    return FieldGroupEffectiveRecord(
        symbol=observation.symbol,
        field_group=observation.field_group,
        payload_json=None,
        source_datetime=None,
        received_at=None,
        effective_state=state,
        retained_from_source_datetime=None,
        retained_from_received_at=None,
        current_observed_at=observation.received_at,
        reason=observation.reason,
        updated_at=observation.received_at,
        raw_context_json=context_json,
    )


def _validate_observation(observation: FieldGroupObservation) -> FieldGroupObservation:
    return FieldGroupObservation(
        symbol=_required_symbol(observation.symbol),
        field_group=_required_field_group(observation.field_group),
        quality=_required_quality(observation.quality),
        source_datetime=observation.source_datetime,
        received_at=_required_text(observation.received_at, "received_at"),
        reason=observation.reason,
        context=observation.context,
    )


def _required_symbol(symbol: str) -> str:
    return _required_text(symbol, "symbol")


def _required_field_group(field_group: str) -> str:
    cleaned = _required_text(field_group, "field_group")
    if cleaned not in FIELD_GROUPS:
        raise ValueError("Unsupported field group.")
    return cleaned


def _required_quality(quality: str) -> str:
    cleaned = _required_text(quality, "quality")
    if cleaned not in QUALITIES:
        raise ValueError("Unsupported field-group quality.")
    return cleaned


def _required_text(value: str, field_name: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{field_name} must not be empty.")
    return cleaned


def _json(payload: dict[str, Any]) -> str:
    return json.dumps(
        json_safe(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
