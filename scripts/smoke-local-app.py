#!/usr/bin/env python3
"""Smoke-test the local API and WebUI factories without external services."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sqlite3
import tempfile

from fastapi.testclient import TestClient

from option_data_manager.api.app import create_app as create_api_app
from option_data_manager.settings import PlainTextProtector
from option_data_manager.webui.app import create_webui_app


API_PATHS = ("/api/health", "/api/status", "/api/settings", "/api/logs", "/docs")
WEBUI_PATHS = (
    "/",
    "/api/settings",
    "/api/webui/overview",
    "/api/webui/runs",
    "/assets/webui.js",
    "/assets/webui.css",
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Smoke-test local Option Data Manager API and WebUI endpoints."
    )
    parser.add_argument(
        "--database",
        help=(
            "SQLite database path for the smoke run. Defaults to a temporary "
            "file outside the repository."
        ),
    )
    args = parser.parse_args()
    if args.database:
        database_path = Path(args.database)
        database_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        handle = tempfile.NamedTemporaryFile(prefix="odm-smoke-", suffix=".sqlite3")
        handle.close()
        database_path = Path(handle.name)

    os.environ.setdefault("ODM_SECRET_PROTECTOR", "plaintext")
    connection = sqlite3.connect(database_path, check_same_thread=False)
    try:
        _check_api(connection, str(database_path))
        _check_webui(connection, str(database_path))
    finally:
        connection.close()
    print(f"local api/webui smoke passed: {database_path}")
    return 0


def _check_api(connection: sqlite3.Connection, database_path: str) -> None:
    client = TestClient(
        create_api_app(
            connection,
            database_path=database_path,
            protector=PlainTextProtector(),
        )
    )
    for path in API_PATHS:
        _assert_ok(client, path)


def _check_webui(connection: sqlite3.Connection, database_path: str) -> None:
    client = TestClient(create_webui_app(connection, database_path=database_path))
    for path in WEBUI_PATHS:
        _assert_ok(client, path)


def _assert_ok(client: TestClient, path: str) -> None:
    response = client.get(path)
    if response.status_code != 200:
        body = response.text[:500].replace("\n", " ")
        raise RuntimeError(f"{path} returned {response.status_code}: {body}")


if __name__ == "__main__":
    raise SystemExit(main())
