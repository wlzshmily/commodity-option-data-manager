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


def test_service_logs_capture_safe_settings_events() -> None:
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    app = create_app(connection, database_path=":memory:", protector=PlainTextProtector())
    client = TestClient(app)

    response = client.put(
        "/api/settings/tqsdk-credentials",
        json={"account": "demo", "password": "super-secret"},
    )
    logs = client.get("/api/logs").json()

    assert response.status_code == 200
    assert logs[0]["category"] == "settings"
    assert logs[0]["message"] == "TQSDK credentials updated."
    assert "super-secret" not in str(logs)


def test_quote_stream_controls_start_and_stop_workers(monkeypatch) -> None:
    class FakeProcess:
        _next_pid = 32000

        def __init__(self, command, **kwargs) -> None:
            self.command = command
            self.kwargs = kwargs
            self.pid = FakeProcess._next_pid
            FakeProcess._next_pid += 1
            self.terminated = False
            created.append(self)

        def poll(self):
            return 0 if self.terminated else None

        def terminate(self) -> None:
            self.terminated = True

        def wait(self, timeout=None):
            self.terminated = True
            return 0

        def kill(self) -> None:
            self.terminated = True

    created = []
    monkeypatch.setattr("option_data_manager.api.app.subprocess.Popen", FakeProcess)

    connection = sqlite3.connect(":memory:", check_same_thread=False)
    app = create_app(connection, database_path=":memory:", protector=PlainTextProtector())
    client = TestClient(app)
    client.put(
        "/api/settings/tqsdk-credentials",
        json={"account": "demo", "password": "super-secret"},
    )

    started = client.post(
        "/api/quote-stream/start",
        json={"workers": 2, "quote_shard_size": 50, "max_symbols": 100},
    )

    assert started.status_code == 200
    payload = started.json()
    assert payload["running"] is True
    assert payload["worker_count"] == 2
    assert len(payload["pids"]) == 2
    assert len(created) == 2
    assert "--worker-index" in created[0].command
    assert "super-secret" not in str(created[0].command)

    stopped = client.post("/api/quote-stream/stop")

    assert stopped.status_code == 200
    assert stopped.json()["running"] is False
    assert all(process.terminated for process in created)


def test_quote_stream_start_blocks_without_credentials() -> None:
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    app = create_app(connection, database_path=":memory:", protector=PlainTextProtector())
    client = TestClient(app)

    response = client.post("/api/quote-stream/start", json={"workers": 1})

    assert response.status_code == 200
    assert response.json()["status"] == "blocked"
    assert response.json()["running"] is False
