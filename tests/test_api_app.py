import sqlite3

from fastapi.testclient import TestClient

from option_data_manager.api.app import API_AUTH_REQUIRED_KEY, create_app
from option_data_manager.settings import PlainTextProtector, SettingsRepository


def test_api_status_and_settings_do_not_require_key_by_default() -> None:
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    app = create_app(connection, database_path=":memory:", protector=PlainTextProtector())
    client = TestClient(app)

    assert client.get("/api/health").status_code == 200
    status = client.get("/api/status").json()
    assert status["collection"]["active_batches"] == 0
    settings = client.get("/api/settings").json()
    assert settings["api"]["auth_required"] is False
    assert settings["tqsdk"]["password_configured"] is False
    assert settings["collection"]["wait_cycles"] == 1


def test_api_key_required_when_setting_enabled() -> None:
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    repository = SettingsRepository(connection, PlainTextProtector())
    repository.set_value(API_AUTH_REQUIRED_KEY, "true")
    app = create_app(connection, database_path=":memory:", protector=PlainTextProtector())
    client = TestClient(app)

    assert client.get("/api/health").status_code == 401
    created = client.post("/api/api-keys", json={"name": "monitor"}).json()
    response = client.get(
        "/api/health",
        headers={"Authorization": f"Bearer {created['secret']}"},
    )

    assert response.status_code == 200
    assert "secret" not in client.get(
        "/api/api-keys",
        headers={"Authorization": f"Bearer {created['secret']}"},
    ).text


def test_saving_tqsdk_credentials_masks_password_in_settings() -> None:
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    app = create_app(connection, database_path=":memory:", protector=PlainTextProtector())
    client = TestClient(app)

    response = client.put(
        "/api/settings/tqsdk-credentials",
        json={"account": "demo", "password": "super-secret"},
    )

    assert response.status_code == 200
    settings = client.get("/api/settings").json()
    assert settings["tqsdk"] == {"account": "demo", "password_configured": True}
    assert "super-secret" not in str(settings)


def test_runtime_settings_are_validated_before_persisting() -> None:
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    app = create_app(connection, database_path=":memory:", protector=PlainTextProtector())
    client = TestClient(app)

    port_response = client.put("/api/settings/api.port", json={"value": "70000"})
    assert port_response.status_code == 400

    secret_response = client.put(
        "/api/settings/tqsdk.password",
        json={"value": "must-not-be-stored-here"},
    )
    assert secret_response.status_code == 400
    assert "must-not-be-stored-here" not in str(client.get("/api/settings").json())

    ok_response = client.put(
        "/api/settings/collection.wait_cycles",
        json={"value": "0"},
    )
    assert ok_response.status_code == 200
    assert ok_response.json()["value"] == "0"
    assert client.get("/api/settings").json()["collection"]["wait_cycles"] == 0
