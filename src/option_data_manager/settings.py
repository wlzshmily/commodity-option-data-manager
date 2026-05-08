"""Application settings repository."""

from __future__ import annotations

import base64
import ctypes
from ctypes import wintypes
from dataclasses import dataclass
from datetime import UTC, datetime
import os
import sqlite3
from typing import Protocol

from .storage import Migration, apply_migrations


TQSDK_ACCOUNT_KEY = "tqsdk.account"
TQSDK_PASSWORD_KEY = "tqsdk.password"


APP_SETTINGS_MIGRATION = Migration(
    50,
    "create app_settings",
    (
        """
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            secret_value TEXT,
            is_secret INTEGER NOT NULL CHECK (is_secret IN (0, 1)),
            updated_at TEXT NOT NULL
        )
        """,
    ),
)


class SecretProtector(Protocol):
    """Transforms secret values before storage and after retrieval."""

    def protect(self, value: str) -> str:
        """Return a storage-safe representation of a secret."""

    def unprotect(self, value: str) -> str:
        """Return the original secret value."""


class PlainTextProtector:
    """Development-only protector for tests and explicit local experiments."""

    def protect(self, value: str) -> str:
        return value

    def unprotect(self, value: str) -> str:
        return value


class WindowsDpapiProtector:
    """Protect secrets with the current Windows user profile."""

    _prefix = "dpapi:"

    def __init__(self) -> None:
        if os.name != "nt":
            raise RuntimeError("Windows DPAPI protection is only available on Windows.")

    def protect(self, value: str) -> str:
        protected = _crypt_protect_data(value.encode("utf-8"))
        return f"{self._prefix}{base64.b64encode(protected).decode('ascii')}"

    def unprotect(self, value: str) -> str:
        if not value.startswith(self._prefix):
            raise ValueError("Unsupported protected secret format.")
        protected = base64.b64decode(value[len(self._prefix) :].encode("ascii"))
        return _crypt_unprotect_data(protected).decode("utf-8")


class _DataBlob(ctypes.Structure):
    _fields_ = [
        ("cbData", wintypes.DWORD),
        ("pbData", ctypes.POINTER(ctypes.c_char)),
    ]


def _crypt_protect_data(data: bytes) -> bytes:
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    data_buffer = ctypes.create_string_buffer(data)
    blob_in = _DataBlob(len(data), ctypes.cast(data_buffer, ctypes.POINTER(ctypes.c_char)))
    blob_out = _DataBlob()
    if not crypt32.CryptProtectData(
        ctypes.byref(blob_in),
        None,
        None,
        None,
        None,
        0,
        ctypes.byref(blob_out),
    ):
        raise ctypes.WinError()
    try:
        return ctypes.string_at(blob_out.pbData, blob_out.cbData)
    finally:
        kernel32.LocalFree(blob_out.pbData)


def _crypt_unprotect_data(data: bytes) -> bytes:
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    data_buffer = ctypes.create_string_buffer(data)
    blob_in = _DataBlob(len(data), ctypes.cast(data_buffer, ctypes.POINTER(ctypes.c_char)))
    blob_out = _DataBlob()
    if not crypt32.CryptUnprotectData(
        ctypes.byref(blob_in),
        None,
        None,
        None,
        None,
        0,
        ctypes.byref(blob_out),
    ):
        raise ctypes.WinError()
    try:
        return ctypes.string_at(blob_out.pbData, blob_out.cbData)
    finally:
        kernel32.LocalFree(blob_out.pbData)


@dataclass(frozen=True)
class SettingMetadata:
    """Safe setting metadata for API/UI listing."""

    key: str
    is_secret: bool
    updated_at: str
    display_value: str | None


class SettingsRepository:
    """SQLite-backed app settings repository."""

    def __init__(
        self,
        connection: sqlite3.Connection,
        protector: SecretProtector,
    ) -> None:
        connection.row_factory = sqlite3.Row
        self._connection = connection
        self._protector = protector
        apply_migrations(connection, (APP_SETTINGS_MIGRATION,))

    def set_value(self, key: str, value: str) -> None:
        self._validate_key(key)
        self._upsert(key=key, value=value, secret_value=None, is_secret=False)

    def get_value(self, key: str) -> str | None:
        row = self._get_row(key)
        if row is None or bool(row["is_secret"]):
            return None
        return str(row["value"]) if row["value"] is not None else None

    def set_secret(self, key: str, value: str) -> None:
        self._validate_key(key)
        protected_value = self._protector.protect(value)
        self._upsert(key=key, value=None, secret_value=protected_value, is_secret=True)

    def get_secret(self, key: str) -> str | None:
        row = self._get_row(key)
        if row is None or not bool(row["is_secret"]) or row["secret_value"] is None:
            return None
        return self._protector.unprotect(str(row["secret_value"]))

    def list_metadata(self) -> list[SettingMetadata]:
        rows = self._connection.execute(
            """
            SELECT key, value, is_secret, updated_at
            FROM app_settings
            ORDER BY key
            """
        ).fetchall()
        metadata: list[SettingMetadata] = []
        for row in rows:
            is_secret = bool(row["is_secret"])
            metadata.append(
                SettingMetadata(
                    key=str(row["key"]),
                    is_secret=is_secret,
                    updated_at=str(row["updated_at"]),
                    display_value="******" if is_secret else row["value"],
                )
            )
        return metadata

    def _upsert(
        self,
        *,
        key: str,
        value: str | None,
        secret_value: str | None,
        is_secret: bool,
    ) -> None:
        self._connection.execute(
            """
            INSERT INTO app_settings (key, value, secret_value, is_secret, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                secret_value = excluded.secret_value,
                is_secret = excluded.is_secret,
                updated_at = excluded.updated_at
            """,
            (
                key,
                value,
                secret_value,
                1 if is_secret else 0,
                datetime.now(UTC).isoformat(),
            ),
        )
        self._connection.commit()

    def _get_row(self, key: str) -> sqlite3.Row | None:
        self._validate_key(key)
        return self._connection.execute(
            """
            SELECT key, value, secret_value, is_secret, updated_at
            FROM app_settings
            WHERE key = ?
            """,
            (key,),
        ).fetchone()

    @staticmethod
    def _validate_key(key: str) -> None:
        if not key.strip():
            raise ValueError("Setting key must not be empty.")
