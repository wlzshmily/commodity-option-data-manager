from pathlib import Path
import sqlite3

from option_data_manager.cli.check_tqsdk import check_connection, resolve_credentials
from option_data_manager.settings import (
    PlainTextProtector,
    SettingsRepository,
    TQSDK_ACCOUNT_KEY,
    TQSDK_PASSWORD_KEY,
)
from option_data_manager.tqsdk_connection import (
    TqsdkConnectionError,
    create_tqsdk_api_with_retries,
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


def test_tqsdk_connection_retries_transient_failures() -> None:
    attempts = 0

    class FakeApi:
        pass

    def flaky(account: str, password: str):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("temporary TLS failure")
        return FakeApi()

    api = create_tqsdk_api_with_retries(
        "demo",
        "super-secret",
        attempts=3,
        retry_delay_seconds=0,
        api_factory=flaky,
    )

    assert isinstance(api, FakeApi)
    assert attempts == 3


def test_tqsdk_connection_retry_error_does_not_expose_password() -> None:
    def fail(account: str, password: str):
        raise RuntimeError(f"network unavailable for {account}")

    try:
        create_tqsdk_api_with_retries(
            "demo",
            "super-secret",
            attempts=2,
            retry_delay_seconds=0,
            api_factory=fail,
        )
    except TqsdkConnectionError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected TqsdkConnectionError")

    assert "super-secret" not in message
    assert "2 attempt" in message


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
