"""Standalone TQSDK credential and connectivity check."""

from __future__ import annotations

import argparse
import os
from collections.abc import Callable, Mapping
from pathlib import Path
import sqlite3
from typing import Any

from option_data_manager.settings import (
    SecretProtector,
    TQSDK_ACCOUNT_KEY,
    TQSDK_PASSWORD_KEY,
    default_secret_protector,
)
from option_data_manager.tqsdk_connection import create_tqsdk_api_with_retries


DEFAULT_DATABASE_PATH = Path("data/option-data-current.sqlite3")


def main(argv: list[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    """Run a live TQSDK credential check without printing secrets."""

    args = _parse_args(argv)
    runtime_env = os.environ if env is None else env
    credentials = resolve_credentials(
        runtime_env,
        database_path=Path(args.database),
        protector=default_secret_protector(),
    )
    if credentials is None:
        print("blocked: TQSDK credentials are not configured.")
        return 2
    result = check_connection(
        credentials[0],
        credentials[1],
        attempts=args.attempts,
        retry_delay_seconds=args.retry_delay,
    )
    print(result["message"])
    return 0 if result["status"] == "ok" else 1


def resolve_credentials(
    env: Mapping[str, str],
    *,
    database_path: Path,
    protector: SecretProtector,
) -> tuple[str, str] | None:
    """Resolve credentials from transient env vars or local app settings."""

    account = env.get("TQSDK_ACCOUNT", "").strip()
    password = env.get("TQSDK_PASSWORD", "")
    if account and password:
        return account, password
    if not database_path.exists():
        return None
    try:
        connection = sqlite3.connect(database_path)
        connection.row_factory = sqlite3.Row
        rows = {
            str(row["key"]): row
            for row in connection.execute(
                """
                SELECT key, value, secret_value, is_secret
                FROM app_settings
                WHERE key IN (?, ?)
                """,
                (TQSDK_ACCOUNT_KEY, TQSDK_PASSWORD_KEY),
            ).fetchall()
        }
    except sqlite3.Error:
        return None
    finally:
        try:
            connection.close()
        except UnboundLocalError:
            pass
    account_row = rows.get(TQSDK_ACCOUNT_KEY)
    password_row = rows.get(TQSDK_PASSWORD_KEY)
    if account_row is None or password_row is None:
        return None
    if bool(account_row["is_secret"]) or not bool(password_row["is_secret"]):
        return None
    account = str(account_row["value"] or "").strip()
    protected_password = password_row["secret_value"]
    if not account or protected_password is None:
        return None
    try:
        password = protector.unprotect(str(protected_password))
    except (RuntimeError, ValueError):
        return None
    return (account, password) if password else None


def check_connection(
    account: str,
    password: str,
    *,
    attempts: int | None = None,
    retry_delay_seconds: float | None = None,
    api_factory: Callable[[str, str], Any] | None = None,
) -> dict[str, str]:
    """Open and close TQSDK to validate auth and network connectivity."""

    try:
        api = _create_tqsdk_api(
            account,
            password,
            attempts=attempts,
            retry_delay_seconds=retry_delay_seconds,
            api_factory=api_factory,
        )
        close = getattr(api, "close", None)
        if callable(close):
            close()
    except Exception as exc:
        return {"status": "failed", "message": f"failed: {type(exc).__name__}: {exc}"}
    return {"status": "ok", "message": "ok: TQSDK connection test completed."}


def _create_tqsdk_api(
    account: str,
    password: str,
    *,
    attempts: int | None,
    retry_delay_seconds: float | None,
    api_factory: Callable[[str, str], Any] | None,
) -> Any:
    return create_tqsdk_api_with_retries(
        account,
        password,
        attempts=attempts,
        retry_delay_seconds=retry_delay_seconds,
        api_factory=api_factory,
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate TQSDK credentials from env vars or the runtime database.",
    )
    parser.add_argument("--database", default=str(DEFAULT_DATABASE_PATH))
    parser.add_argument("--attempts", type=int, default=None)
    parser.add_argument("--retry-delay", type=float, default=None)
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
