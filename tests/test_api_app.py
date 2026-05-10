import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from option_data_manager.api.app import API_AUTH_REQUIRED_KEY, create_app
from option_data_manager.instruments import (
    InstrumentRepository,
    normalize_option_chain_discovery,
)
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
    assert settings["quote_stream"]["kline_batch_size"] == 1
    assert settings["quote_stream"]["kline_data_length"] == 3
    assert settings["quote_stream"]["prioritize_near_expiry"] is True
    assert settings["quote_stream"]["near_expiry_months"] == 2


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
    InstrumentRepository(connection).upsert_instruments(
        normalize_option_chain_discovery(
            underlying_symbol="DCE.a2601",
            call_symbols=("DCE.a2601C100",),
            put_symbols=("DCE.a2601P100",),
            last_seen_at="2026-05-11T00:00:00+08:00",
        )
    )
    client = TestClient(app)
    client.put(
        "/api/settings/tqsdk-credentials",
        json={"account": "demo", "password": "super-secret"},
    )

    started = client.post(
        "/api/quote-stream/start",
        json={
            "workers": 2,
            "quote_shard_size": 50,
            "kline_batch_size": 2,
            "max_symbols": 100,
        },
    )

    assert started.status_code == 200
    payload = started.json()
    assert payload["running"] is True
    assert payload["worker_count"] == 2
    assert len(payload["pids"]) == 2
    assert len(created) == 3
    assert "--worker-index" in created[0].command
    assert "--no-klines" not in created[0].command
    assert "--kline-data-length" in created[0].command
    assert created[0].command[
        created[0].command.index("--kline-data-length") + 1
    ] == "3"
    assert "--kline-batch-size" in created[0].command
    assert created[0].command[
        created[0].command.index("--kline-batch-size") + 1
    ] == "2"
    assert "--near-expiry-months" in created[0].command
    assert created[0].command[
        created[0].command.index("--near-expiry-months") + 1
    ] == "2"
    assert "--no-prioritize-near-expiry" not in created[0].command
    assert "option_data_manager.cli.metrics_worker" in created[2].command
    assert "--min-interval-seconds" in created[2].command
    assert "super-secret" not in str(created[0].command)
    assert "super-secret" not in str(created[2].command)

    stopped = client.post("/api/quote-stream/stop")

    assert stopped.status_code == 200
    assert stopped.json()["running"] is False
    assert all(process.terminated for process in created)


def test_quote_stream_start_initializes_empty_contract_universe(monkeypatch) -> None:
    class FakeProcess:
        _next_pid = 32100

        def __init__(self, command, **kwargs) -> None:
            self.command = command
            self.kwargs = kwargs
            self.pid = FakeProcess._next_pid
            FakeProcess._next_pid += 1
            created.append(self)

        def poll(self):
            return None

    class FakeApi:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    created = []
    fake_api = FakeApi()
    monkeypatch.setattr("option_data_manager.api.app.subprocess.Popen", FakeProcess)
    monkeypatch.setattr(
        "option_data_manager.api.app.create_tqsdk_api_with_retries",
        lambda account, password: fake_api,
    )

    def fake_discover(api, connection):
        assert api is fake_api
        InstrumentRepository(connection).upsert_instruments(
            normalize_option_chain_discovery(
                underlying_symbol="DCE.a2601",
                call_symbols=("DCE.a2601C100",),
                put_symbols=("DCE.a2601P100",),
                last_seen_at="2026-05-11T00:00:00+08:00",
            )
        )
        return None

    monkeypatch.setattr(
        "option_data_manager.api.app._discover_and_persist_market",
        fake_discover,
    )

    connection = sqlite3.connect(":memory:", check_same_thread=False)
    app = create_app(connection, database_path=":memory:", protector=PlainTextProtector())
    client = TestClient(app)
    client.put(
        "/api/settings/tqsdk-credentials",
        json={"account": "demo", "password": "super-secret"},
    )

    response = client.post("/api/quote-stream/start", json={"workers": 1})
    payload = response.json()

    assert response.status_code == 200
    assert payload["running"] is True
    assert payload["message"] == "已初始化合约列表并启动实时订阅 worker。"
    assert fake_api.closed is True
    assert len(created) == 2
    assert "--no-discover" in created[0].command
    assert connection.execute(
        "SELECT COUNT(*) FROM instruments WHERE option_class IN ('CALL', 'PUT')"
    ).fetchone()[0] == 2


def test_quote_stream_status_aggregates_runtime_subscription_progress(
    tmp_path: Path,
) -> None:
    class FakeProcess:
        def __init__(self) -> None:
            self.pid = 33000

        def poll(self):
            return None

    report_dir = tmp_path / "quote-stream-runtime"
    report_dir.mkdir()
    (report_dir / "worker-00-of-01.json").write_text(
        """
        {
          "status": "subscribing",
          "progress": {
            "status": "subscribing",
            "started_at": "2026-05-10T00:00:00+00:00",
            "updated_at": "2026-05-10T00:00:08+00:00",
            "quote_started_at": "2026-05-10T00:00:00+00:00",
            "quote_finished_at": "2026-05-10T00:00:04+00:00",
            "kline_started_at": "2026-05-10T00:00:04+00:00",
            "quote_subscribed": 4,
            "quote_total": 4,
            "kline_subscribed": 2,
            "kline_total": 4,
            "cycle_count": 3,
            "wait_update_count": 2,
            "quotes_written": 8,
            "last_wait_update_at": "2026-05-10T00:00:07+00:00",
            "last_quote_write_at": "2026-05-10T00:00:07+00:00",
            "last_tqsdk_notify_at": "2026-05-10T00:00:06+00:00",
            "last_tqsdk_notify_code": 2019112902,
            "last_tqsdk_notify_level": "WARNING",
            "last_tqsdk_notify_content": "与行情服务器的网络连接已恢复",
            "tqsdk_connection_status": "connected",
            "last_tqsdk_disconnect_at": "2026-05-10T00:00:02+00:00",
            "last_tqsdk_restore_at": "2026-05-10T00:00:06+00:00",
            "tqsdk_notify_count": 2,
            "near_expiry_months": 2,
            "near_expiry_quote_subscribed": 4,
            "near_expiry_quote_total": 4,
            "near_expiry_kline_subscribed": 1,
            "near_expiry_kline_total": 2,
            "near_expiry_subscribed": 5,
            "near_expiry_total": 6,
            "subscribed_objects": 6,
            "total_objects": 8,
            "completion_ratio": 0.75
          }
        }
        """,
        encoding="utf-8",
    )

    connection = sqlite3.connect(":memory:", check_same_thread=False)
    app = create_app(connection, database_path=":memory:", protector=PlainTextProtector())
    app.state.quote_stream_processes["processes"] = [FakeProcess()]
    app.state.service_state.set_value("quote_stream.running", "true")
    app.state.service_state.set_value("quote_stream.worker_count", "1")
    app.state.service_state.set_value("quote_stream.report_dir", str(report_dir))
    app.state.service_state.set_value("quote_stream.pids", "[33000]")
    client = TestClient(app)

    payload = client.get("/api/quote-stream").json()

    assert payload["running"] is True
    assert payload["progress"]["status"] == "subscribing"
    assert payload["progress"]["subscribed_objects"] == 6
    assert payload["progress"]["total_objects"] == 8
    assert payload["progress"]["quote_subscribed"] == 4
    assert payload["progress"]["kline_subscribed"] == 2
    assert payload["progress"]["near_expiry_months"] == 2
    assert payload["progress"]["near_expiry_subscribed"] == 5
    assert payload["progress"]["near_expiry_total"] == 6
    assert payload["progress"]["elapsed_seconds"] == 8
    assert payload["progress"]["active_stage"] == "kline"
    assert payload["progress"]["stage_label"] == "K线"
    assert payload["progress"]["stage_average_seconds_per_object"] == 2.0
    assert payload["progress"]["stage_estimated_remaining_seconds"] == 4.0
    assert payload["progress"]["estimated_remaining_seconds"] == 4.0
    assert payload["progress"]["estimated_remaining_is_total"] is True
    assert payload["progress"]["waiting_for_kline_eta"] is False
    assert payload["progress"]["wait_update_count"] == 2
    assert payload["progress"]["last_wait_update_at"] == "2026-05-10T00:00:07+00:00"
    assert payload["progress"]["tqsdk_connection_status"] == "connected"
    assert payload["progress"]["last_tqsdk_notify_code"] == 2019112902
    assert payload["progress"]["tqsdk_notify_count"] == 2
    assert payload["health"]["status"] in {
        "subscribing",
        "session_closed",
        "awaiting_market_evidence",
        "opening_grace",
    }


def test_quote_stream_progress_resets_when_stopped(tmp_path: Path) -> None:
    report_dir = tmp_path / "quote-stream-runtime"
    report_dir.mkdir()
    (report_dir / "worker-00-of-01.json").write_text(
        """
        {
          "status": "success",
          "result": {
            "symbol_count": 4,
            "kline_symbol_count": 4,
            "subscribed_quote_count": 4,
            "subscribed_kline_count": 4,
            "finished_at": "2026-05-10T00:00:00+00:00"
          }
        }
        """,
        encoding="utf-8",
    )

    connection = sqlite3.connect(":memory:", check_same_thread=False)
    app = create_app(connection, database_path=":memory:", protector=PlainTextProtector())
    app.state.service_state.set_value("quote_stream.running", "false")
    app.state.service_state.set_value("quote_stream.report_dir", str(report_dir))
    client = TestClient(app)

    payload = client.get("/api/quote-stream").json()

    assert payload["running"] is False
    assert payload["progress"]["subscribed_objects"] == 0
    assert payload["progress"]["total_objects"] == 0


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
