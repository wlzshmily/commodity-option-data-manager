"""Local API key repository."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import secrets
import sqlite3

from .storage import Migration, apply_migrations


API_KEYS_MIGRATION = Migration(
    900,
    "create api keys",
    (
        """
        CREATE TABLE IF NOT EXISTS api_keys (
            key_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            fingerprint TEXT NOT NULL UNIQUE,
            key_hash TEXT NOT NULL UNIQUE,
            scope TEXT NOT NULL,
            enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
            created_at TEXT NOT NULL,
            last_used_at TEXT,
            revoked_at TEXT
        )
        """,
    ),
)


@dataclass(frozen=True)
class ApiKeyRecord:
    """Safe API key metadata."""

    key_id: int
    name: str
    fingerprint: str
    scope: str
    enabled: bool
    created_at: str
    last_used_at: str | None
    revoked_at: str | None


@dataclass(frozen=True)
class CreatedApiKey:
    """Newly-created API key plus its one-time secret."""

    record: ApiKeyRecord
    secret: str


class ApiKeyRepository:
    """SQLite-backed API key metadata and verification."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        connection.row_factory = sqlite3.Row
        self._connection = connection
        apply_migrations(connection, (API_KEYS_MIGRATION,))

    def create_key(self, *, name: str, scope: str = "read") -> CreatedApiKey:
        cleaned_name = name.strip()
        cleaned_scope = scope.strip() or "read"
        if not cleaned_name:
            raise ValueError("API key name must not be empty.")
        secret = f"odm_{secrets.token_urlsafe(32)}"
        key_hash = _hash_secret(secret)
        fingerprint = _fingerprint(secret)
        now = datetime.now(UTC).isoformat()
        cursor = self._connection.execute(
            """
            INSERT INTO api_keys (
                name, fingerprint, key_hash, scope, enabled, created_at
            )
            VALUES (?, ?, ?, ?, 1, ?)
            """,
            (cleaned_name, fingerprint, key_hash, cleaned_scope, now),
        )
        self._connection.commit()
        record = self.get_key(int(cursor.lastrowid))
        if record is None:
            raise RuntimeError("API key was not persisted.")
        return CreatedApiKey(record=record, secret=secret)

    def list_keys(self) -> list[ApiKeyRecord]:
        rows = self._connection.execute(
            """
            SELECT
                key_id,
                name,
                CASE
                    WHEN instr(fingerprint, '...') > 0 THEN key_hash
                    ELSE fingerprint
                END AS fingerprint,
                scope,
                enabled,
                created_at,
                last_used_at,
                revoked_at
            FROM api_keys
            ORDER BY key_id DESC
            """
        ).fetchall()
        return [_record_from_row(row) for row in rows]

    def get_key(self, key_id: int) -> ApiKeyRecord | None:
        row = self._connection.execute(
            """
            SELECT
                key_id,
                name,
                CASE
                    WHEN instr(fingerprint, '...') > 0 THEN key_hash
                    ELSE fingerprint
                END AS fingerprint,
                scope,
                enabled,
                created_at,
                last_used_at,
                revoked_at
            FROM api_keys
            WHERE key_id = ?
            """,
            (key_id,),
        ).fetchone()
        return _record_from_row(row) if row is not None else None

    def set_enabled(self, key_id: int, enabled: bool) -> ApiKeyRecord:
        self._connection.execute(
            """
            UPDATE api_keys
            SET enabled = ?
            WHERE key_id = ? AND revoked_at IS NULL
            """,
            (1 if enabled else 0, key_id),
        )
        self._connection.commit()
        record = self.get_key(key_id)
        if record is None:
            raise KeyError(f"Unknown API key id: {key_id}")
        return record

    def revoke(self, key_id: int) -> ApiKeyRecord:
        self._connection.execute(
            """
            UPDATE api_keys
            SET enabled = 0, revoked_at = COALESCE(revoked_at, ?)
            WHERE key_id = ?
            """,
            (datetime.now(UTC).isoformat(), key_id),
        )
        self._connection.commit()
        record = self.get_key(key_id)
        if record is None:
            raise KeyError(f"Unknown API key id: {key_id}")
        return record

    def delete_key(self, key_id: int) -> None:
        cursor = self._connection.execute(
            "DELETE FROM api_keys WHERE key_id = ?",
            (key_id,),
        )
        self._connection.commit()
        if cursor.rowcount == 0:
            raise KeyError(f"Unknown API key id: {key_id}")

    def verify(self, secret: str) -> ApiKeyRecord | None:
        row = self._connection.execute(
            """
            SELECT
                key_id,
                name,
                CASE
                    WHEN instr(fingerprint, '...') > 0 THEN key_hash
                    ELSE fingerprint
                END AS fingerprint,
                scope,
                enabled,
                created_at,
                last_used_at,
                revoked_at
            FROM api_keys
            WHERE key_hash = ?
              AND enabled = 1
              AND revoked_at IS NULL
            """,
            (_hash_secret(secret),),
        ).fetchone()
        if row is None:
            return None
        self._connection.execute(
            "UPDATE api_keys SET last_used_at = ? WHERE key_id = ?",
            (datetime.now(UTC).isoformat(), row["key_id"]),
        )
        self._connection.commit()
        return _record_from_row(row)


def _hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def _fingerprint(secret: str) -> str:
    return _hash_secret(secret)


def _record_from_row(row: sqlite3.Row) -> ApiKeyRecord:
    return ApiKeyRecord(
        key_id=int(row["key_id"]),
        name=str(row["name"]),
        fingerprint=str(row["fingerprint"]),
        scope=str(row["scope"]),
        enabled=bool(row["enabled"]),
        created_at=str(row["created_at"]),
        last_used_at=row["last_used_at"],
        revoked_at=row["revoked_at"],
    )
