"""Small SQLite migration runner."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import sqlite3
from typing import Iterable


class MigrationError(ValueError):
    """Raised when migration definitions are invalid."""


@dataclass(frozen=True)
class Migration:
    """A single ordered SQLite schema migration."""

    version: int
    name: str
    statements: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.version < 1:
            raise MigrationError("Migration version must be a positive integer.")
        if not self.name.strip():
            raise MigrationError("Migration name must not be empty.")
        if not self.statements:
            raise MigrationError("Migration must include at least one SQL statement.")


def ensure_migration_table(connection: sqlite3.Connection) -> None:
    """Create the migration metadata table if needed."""

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TEXT NOT NULL
        )
        """
    )


def get_applied_versions(connection: sqlite3.Connection) -> set[int]:
    """Return all applied migration versions."""

    ensure_migration_table(connection)
    rows = connection.execute("SELECT version FROM schema_migrations").fetchall()
    return {int(row[0]) for row in rows}


def apply_migrations(
    connection: sqlite3.Connection,
    migrations: Iterable[Migration],
) -> list[int]:
    """Apply pending migrations in version order and return applied versions."""

    ordered = _validate_migrations(tuple(migrations))
    ensure_migration_table(connection)
    applied = get_applied_versions(connection)
    newly_applied: list[int] = []

    for migration in ordered:
        if migration.version in applied:
            continue
        _apply_one(connection, migration)
        applied.add(migration.version)
        newly_applied.append(migration.version)

    return newly_applied


def _validate_migrations(migrations: tuple[Migration, ...]) -> tuple[Migration, ...]:
    versions = [migration.version for migration in migrations]
    if len(versions) != len(set(versions)):
        raise MigrationError("Migration versions must be unique.")
    return tuple(sorted(migrations, key=lambda item: item.version))


def _apply_one(connection: sqlite3.Connection, migration: Migration) -> None:
    started_transaction = not connection.in_transaction
    if started_transaction:
        connection.execute("BEGIN")
    try:
        for statement in migration.statements:
            connection.execute(statement)
        connection.execute(
            """
            INSERT INTO schema_migrations (version, name, applied_at)
            VALUES (?, ?, ?)
            """,
            (
                migration.version,
                migration.name,
                datetime.now(UTC).isoformat(),
            ),
        )
    except Exception:
        if started_transaction:
            connection.rollback()
        raise
    else:
        if started_transaction:
            connection.commit()
