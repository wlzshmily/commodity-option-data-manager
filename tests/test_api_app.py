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


def test_api_key_can_be_deleted() -> None:
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    app = create_app(connection, database_path=":memory:", protector=PlainTextProtector())
    client = TestClient(app)
    created = client.post("/api/api-keys", json={"name": "monitor"}).json()

    response = client.delete(f"/api/api-keys/{created['key_id']}")

    assert response.status_code == 200
    assert response.json() == {"key_id": created["key_id"], "deleted": True}
    assert client.get("/api/api-keys").json() == []


def test_options_api_exposes_tqsdk_date_fields_and_derived_days() -> None:
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    app = create_app(connection, database_path=":memory:", protector=PlainTextProtector())
    client = TestClient(app)
    connection.execute(
        """
        INSERT INTO instruments (
            symbol,
            exchange_id,
            product_id,
            instrument_id,
            instrument_name,
            ins_class,
            underlying_symbol,
            option_class,
            strike_price,
            expire_datetime,
            last_exercise_datetime,
            price_tick,
            volume_multiple,
            expired,
            active,
            inactive_reason,
            last_seen_at,
            raw_payload_json
        )
        VALUES (
            'SHFE.cu2606C70000',
            'SHFE',
            'cu',
            'cu2606C70000',
            NULL,
            'OPTION',
            'SHFE.cu2606',
            'CALL',
            70000,
            '2026-06-10',
            '2026-06-09',
            NULL,
            NULL,
            0,
            1,
            NULL,
            '2026-05-08T00:00:00+00:00',
            '{}'
        )
        """
    )

    row = client.get("/api/options?underlying=SHFE.cu2606").json()[0]

    assert row["expire_datetime"] == "2026-06-10"
    assert row["last_exercise_datetime"] == "2026-06-09"
    assert "expire_date" not in row
    assert "days_to_expiry" not in row
    assert "days_to_expire_datetime" in row
    assert "days_to_last_exercise_datetime" in row
