from pathlib import Path
import sqlite3

from option_data_manager.cli.check_tqsdk import check_connection, resolve_credentials
from option_data_manager.settings import (
    PlainTextProtector,
    SettingsRepository,
    TQSDK_ACCOUNT_KEY,
    TQSDK_PASSWORD_KEY,
)


def test_connection_closes_api_without_exposing_secret() -> None:
    closed = False

    class FakeApi:
        def close(self) -> None:
            nonlocal closed
            closed = True

    result = check_connection("demo", "super-secret", api_factory=lambda *_: FakeApi())

    assert result == {"status": "ok", "message": "ok: TQSDK connection test completed."}
    assert closed is True
    assert "super-secret" not in str(result)


def test_connection_failure_does_not_expose_password() -> None:
    def fail(account: str, password: str):
        raise RuntimeError("network unavailable")

    result = check_connection("demo", "super-secret", api_factory=fail)

    assert result["status"] == "failed"
    assert "super-secret" not in result["message"]


def test_resolve_credentials_from_database(tmp_path: Path) -> None:
    database_path = tmp_path / "runtime.sqlite3"
    connection = sqlite3.connect(database_path)
    repository = SettingsRepository(connection, PlainTextProtector())
    repository.set_value(TQSDK_ACCOUNT_KEY, "demo")
    repository.set_secret(TQSDK_PASSWORD_KEY, "super-secret")
    connection.close()

    assert resolve_credentials(
        {},
        database_path=database_path,
        protector=PlainTextProtector(),
    ) == ("demo", "super-secret")
