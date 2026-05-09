"""Persistent collection batch state for resumable market acquisition."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
import sqlite3

from .collection_plan import MarketCollectionPlan
from .storage import Migration, apply_migrations


COLLECTION_STATE_MIGRATION = Migration(
    550,
    "create collection batch state",
    (
        """
        CREATE TABLE IF NOT EXISTS collection_plan_batches (
            plan_scope TEXT NOT NULL,
            underlying_symbol TEXT NOT NULL,
            batch_index INTEGER NOT NULL,
            exchange_id TEXT NOT NULL,
            product_id TEXT,
            option_symbols_json TEXT NOT NULL,
            option_count INTEGER NOT NULL,
            status TEXT NOT NULL,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT,
            stale INTEGER NOT NULL CHECK (stale IN (0, 1)),
            PRIMARY KEY (plan_scope, underlying_symbol, batch_index)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_collection_plan_batches_next
        ON collection_plan_batches(plan_scope, stale, status, underlying_symbol, batch_index)
        """,
    ),
)


COLLECTION_BATCH_STATUSES = frozenset(
    {"pending", "running", "success", "failed", "stale"}
)


@dataclass(frozen=True)
class CollectionBatchRecord:
    """One persisted option batch from the latest materialized collection plan."""

    plan_scope: str
    underlying_symbol: str
    batch_index: int
    exchange_id: str
    product_id: str | None
    option_symbols: tuple[str, ...]
    option_count: int
    status: str
    attempt_count: int
    last_error: str | None
    created_at: str
    updated_at: str
    completed_at: str | None
    stale: bool


@dataclass(frozen=True)
class CollectionPlanMaterializationSummary:
    """Change summary after syncing the current plan into SQLite state."""

    scope: str
    active_batch_count: int
    inserted_count: int
    reset_count: int
    preserved_count: int
    stale_count: int


class CollectionStateRepository:
    """SQLite repository for resumable current-slice collection batches."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        connection.row_factory = sqlite3.Row
        self._connection = connection
        apply_migrations(connection, (COLLECTION_STATE_MIGRATION,))

    def materialize_plan(
        self,
        plan: MarketCollectionPlan,
        *,
        scope: str = "full-market-current-slice",
        updated_at: str | None = None,
    ) -> CollectionPlanMaterializationSummary:
        """Sync the latest active plan into durable batch state.

        Existing completed batches are preserved only when their option-symbol
        payload is unchanged. New, stale, changed, or previously running
        batches become pending so contract changes and interrupted workers
        cannot be silently skipped.
        """

        cleaned_scope = _required_text(scope, "Collection plan scope")
        actual_updated_at = updated_at or datetime.now(UTC).isoformat()
        existing = self._existing_batches(cleaned_scope)
        seen_keys: set[tuple[str, int]] = set()
        inserted_count = 0
        reset_count = 0
        preserved_count = 0

        for underlying in plan.underlyings:
            for batch in underlying.batches:
                key = (underlying.underlying_symbol, batch.batch_index)
                seen_keys.add(key)
                option_symbols_json = _option_symbols_json(batch.option_symbols)
                current = existing.get(key)
                if current is None:
                    self._insert_batch(
                        scope=cleaned_scope,
                        underlying_symbol=underlying.underlying_symbol,
                        batch_index=batch.batch_index,
                        exchange_id=underlying.exchange_id,
                        product_id=underlying.product_id,
                        option_symbols_json=option_symbols_json,
                        option_count=len(batch.option_symbols),
                        now=actual_updated_at,
                    )
                    inserted_count += 1
                    continue

                if (
                    current["option_symbols_json"] != option_symbols_json
                    or bool(current["stale"])
                    or current["status"] == "stale"
                    or current["status"] == "running"
                ):
                    self._reset_batch(
                        scope=cleaned_scope,
                        underlying_symbol=underlying.underlying_symbol,
                        batch_index=batch.batch_index,
                        exchange_id=underlying.exchange_id,
                        product_id=underlying.product_id,
                        option_symbols_json=option_symbols_json,
                        option_count=len(batch.option_symbols),
                        now=actual_updated_at,
                    )
                    reset_count += 1
                else:
                    self._preserve_batch(
                        scope=cleaned_scope,
                        underlying_symbol=underlying.underlying_symbol,
                        batch_index=batch.batch_index,
                        exchange_id=underlying.exchange_id,
                        product_id=underlying.product_id,
                        option_count=len(batch.option_symbols),
                        now=actual_updated_at,
                    )
                    preserved_count += 1

        stale_count = self._mark_unseen_stale(
            scope=cleaned_scope,
            seen_keys=seen_keys,
            now=actual_updated_at,
        )
        self._connection.commit()
        return CollectionPlanMaterializationSummary(
            scope=cleaned_scope,
            active_batch_count=plan.batch_count,
            inserted_count=inserted_count,
            reset_count=reset_count,
            preserved_count=preserved_count,
            stale_count=stale_count,
        )

    def list_batches(
        self,
        *,
        scope: str = "full-market-current-slice",
        statuses: tuple[str, ...] = ("pending",),
        include_stale: bool = False,
        limit: int | None = None,
    ) -> list[CollectionBatchRecord]:
        """List batches in deterministic execution order."""

        cleaned_scope = _required_text(scope, "Collection plan scope")
        cleaned_statuses = _validate_statuses(statuses)
        params: list[object] = [cleaned_scope, *cleaned_statuses]
        stale_predicate = "" if include_stale else "AND stale = 0"
        limit_clause = ""
        if limit is not None:
            if limit < 1:
                raise ValueError("Collection batch list limit must be positive.")
            limit_clause = "LIMIT ?"
            params.append(limit)
        placeholders = ",".join("?" for _ in cleaned_statuses)
        rows = self._connection.execute(
            f"""
            SELECT *
            FROM collection_plan_batches
            WHERE plan_scope = ?
              AND status IN ({placeholders})
              {stale_predicate}
            ORDER BY underlying_symbol, batch_index
            {limit_clause}
            """,
            params,
        ).fetchall()
        return [_record_from_row(row) for row in rows]

    def mark_started(
        self,
        *,
        scope: str,
        underlying_symbol: str,
        batch_index: int,
        updated_at: str | None = None,
    ) -> CollectionBatchRecord:
        """Mark one current batch as running."""

        return self._mark_status(
            scope=scope,
            underlying_symbol=underlying_symbol,
            batch_index=batch_index,
            status="running",
            updated_at=updated_at,
        )

    def mark_succeeded(
        self,
        *,
        scope: str,
        underlying_symbol: str,
        batch_index: int,
        updated_at: str | None = None,
    ) -> CollectionBatchRecord:
        """Mark one current batch as successfully collected."""

        actual_updated_at = updated_at or datetime.now(UTC).isoformat()
        return self._mark_status(
            scope=scope,
            underlying_symbol=underlying_symbol,
            batch_index=batch_index,
            status="success",
            updated_at=actual_updated_at,
            completed_at=actual_updated_at,
        )

    def mark_failed(
        self,
        *,
        scope: str,
        underlying_symbol: str,
        batch_index: int,
        error: str,
        updated_at: str | None = None,
    ) -> CollectionBatchRecord:
        """Mark one current batch as failed and increment its attempt count."""

        return self._mark_status(
            scope=scope,
            underlying_symbol=underlying_symbol,
            batch_index=batch_index,
            status="failed",
            updated_at=updated_at,
            last_error=_required_text(error, "Collection batch error"),
            increment_attempt=True,
        )

    def _existing_batches(self, scope: str) -> dict[tuple[str, int], sqlite3.Row]:
        rows = self._connection.execute(
            """
            SELECT *
            FROM collection_plan_batches
            WHERE plan_scope = ?
            """,
            (scope,),
        ).fetchall()
        return {
            (str(row["underlying_symbol"]), int(row["batch_index"])): row
            for row in rows
        }

    def _insert_batch(
        self,
        *,
        scope: str,
        underlying_symbol: str,
        batch_index: int,
        exchange_id: str,
        product_id: str | None,
        option_symbols_json: str,
        option_count: int,
        now: str,
    ) -> None:
        self._connection.execute(
            """
            INSERT INTO collection_plan_batches (
                plan_scope,
                underlying_symbol,
                batch_index,
                exchange_id,
                product_id,
                option_symbols_json,
                option_count,
                status,
                attempt_count,
                last_error,
                created_at,
                updated_at,
                completed_at,
                stale
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, NULL, ?, ?, NULL, 0)
            """,
            (
                scope,
                underlying_symbol,
                batch_index,
                exchange_id,
                product_id,
                option_symbols_json,
                option_count,
                now,
                now,
            ),
        )

    def _reset_batch(
        self,
        *,
        scope: str,
        underlying_symbol: str,
        batch_index: int,
        exchange_id: str,
        product_id: str | None,
        option_symbols_json: str,
        option_count: int,
        now: str,
    ) -> None:
        self._connection.execute(
            """
            UPDATE collection_plan_batches
            SET
                exchange_id = ?,
                product_id = ?,
                option_symbols_json = ?,
                option_count = ?,
                status = 'pending',
                attempt_count = 0,
                last_error = NULL,
                updated_at = ?,
                completed_at = NULL,
                stale = 0
            WHERE plan_scope = ?
              AND underlying_symbol = ?
              AND batch_index = ?
            """,
            (
                exchange_id,
                product_id,
                option_symbols_json,
                option_count,
                now,
                scope,
                underlying_symbol,
                batch_index,
            ),
        )

    def _preserve_batch(
        self,
        *,
        scope: str,
        underlying_symbol: str,
        batch_index: int,
        exchange_id: str,
        product_id: str | None,
        option_count: int,
        now: str,
    ) -> None:
        self._connection.execute(
            """
            UPDATE collection_plan_batches
            SET
                exchange_id = ?,
                product_id = ?,
                option_count = ?,
                updated_at = ?,
                stale = 0
            WHERE plan_scope = ?
              AND underlying_symbol = ?
              AND batch_index = ?
            """,
            (
                exchange_id,
                product_id,
                option_count,
                now,
                scope,
                underlying_symbol,
                batch_index,
            ),
        )

    def _mark_unseen_stale(
        self,
        *,
        scope: str,
        seen_keys: set[tuple[str, int]],
        now: str,
    ) -> int:
        rows = self._connection.execute(
            """
            SELECT underlying_symbol, batch_index
            FROM collection_plan_batches
            WHERE plan_scope = ?
              AND stale = 0
            """,
            (scope,),
        ).fetchall()
        stale_keys = [
            (str(row["underlying_symbol"]), int(row["batch_index"]))
            for row in rows
            if (str(row["underlying_symbol"]), int(row["batch_index"])) not in seen_keys
        ]
        for underlying_symbol, batch_index in stale_keys:
            self._connection.execute(
                """
                UPDATE collection_plan_batches
                SET
                    status = 'stale',
                    stale = 1,
                    updated_at = ?
                WHERE plan_scope = ?
                  AND underlying_symbol = ?
                  AND batch_index = ?
                """,
                (now, scope, underlying_symbol, batch_index),
            )
        return len(stale_keys)

    def _mark_status(
        self,
        *,
        scope: str,
        underlying_symbol: str,
        batch_index: int,
        status: str,
        updated_at: str | None = None,
        completed_at: str | None = None,
        last_error: str | None = None,
        increment_attempt: bool = False,
    ) -> CollectionBatchRecord:
        cleaned_scope = _required_text(scope, "Collection plan scope")
        cleaned_underlying = _required_text(underlying_symbol, "Underlying symbol")
        _validate_status(status)
        if batch_index < 1:
            raise ValueError("Batch index must be positive.")
        actual_updated_at = updated_at or datetime.now(UTC).isoformat()
        cursor = self._connection.execute(
            """
            UPDATE collection_plan_batches
            SET
                status = ?,
                updated_at = ?,
                completed_at = ?,
                last_error = ?,
                attempt_count = attempt_count + ?
            WHERE plan_scope = ?
              AND underlying_symbol = ?
              AND batch_index = ?
              AND stale = 0
            """,
            (
                status,
                actual_updated_at,
                completed_at,
                last_error,
                1 if increment_attempt else 0,
                cleaned_scope,
                cleaned_underlying,
                batch_index,
            ),
        )
        self._connection.commit()
        if cursor.rowcount != 1:
            raise ValueError("Current collection batch does not exist.")
        row = self._connection.execute(
            """
            SELECT *
            FROM collection_plan_batches
            WHERE plan_scope = ?
              AND underlying_symbol = ?
              AND batch_index = ?
            """,
            (cleaned_scope, cleaned_underlying, batch_index),
        ).fetchone()
        return _record_from_row(row)


def _record_from_row(row: sqlite3.Row) -> CollectionBatchRecord:
    data = dict(row)
    data["option_symbols"] = tuple(json.loads(str(data.pop("option_symbols_json"))))
    data["stale"] = bool(data["stale"])
    return CollectionBatchRecord(**data)


def _option_symbols_json(symbols: tuple[str, ...]) -> str:
    return json.dumps(tuple(symbols), ensure_ascii=False, separators=(",", ":"))


def _required_text(value: str, field_name: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{field_name} must not be empty.")
    return cleaned


def _validate_status(status: str) -> None:
    if status not in COLLECTION_BATCH_STATUSES:
        raise ValueError("Unsupported collection batch status.")


def _validate_statuses(statuses: tuple[str, ...]) -> tuple[str, ...]:
    if not statuses:
        raise ValueError("At least one collection batch status is required.")
    for status in statuses:
        _validate_status(status)
    return statuses
