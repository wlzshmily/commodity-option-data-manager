import sqlite3
from datetime import UTC, datetime, timedelta

from option_data_manager.log_retention import (
    TelemetryRetentionPolicy,
    cleanup_runtime_log_files,
    cleanup_telemetry,
)
from option_data_manager.service_state import ServiceLogRepository, ServiceStateRepository


def test_cleanup_telemetry_bounds_logs_and_request_metrics() -> None:
    connection = sqlite3.connect(":memory:")
    logs = ServiceLogRepository(connection)
    state = ServiceStateRepository(connection)
    now = datetime(2026, 5, 17, tzinfo=UTC)
    old = (now - timedelta(days=30)).isoformat()
    fresh = now.isoformat()

    connection.execute(
        """
        INSERT INTO service_logs (created_at, level, category, message, context_json)
        VALUES (?, 'info', 'test', 'old', '{}')
        """,
        (old,),
    )
    logs.append(level="info", category="test", message="fresh")
    connection.execute(
        """
        INSERT INTO api_request_metrics (path, method, status_code, latency_ms, created_at)
        VALUES ('/old', 'GET', 200, 1.0, ?)
        """,
        (old,),
    )
    connection.execute(
        """
        INSERT INTO api_request_metrics (path, method, status_code, latency_ms, created_at)
        VALUES ('/fresh', 'GET', 200, 1.0, ?)
        """,
        (fresh,),
    )
    connection.commit()

    result = cleanup_telemetry(
        connection,
        policy=TelemetryRetentionPolicy(
            service_log_retention_days=14,
            service_log_max_rows=10,
            api_metric_retention_days=7,
            api_metric_max_rows=10,
        ),
        now=now,
    )

    assert result["status"] == "ok"
    assert result["service_logs"]["after"] == 1
    assert result["api_request_metrics"]["after"] == 1
    assert state.api_summary().request_count == 1


def test_cleanup_runtime_log_files_keeps_active_runtime_files(tmp_path) -> None:
    old_log = tmp_path / "webui-8765.out.log.1"
    old_log.write_text("x" * 10, encoding="utf-8")
    active_log = tmp_path / "webui-8765.out.log"
    active_log.write_text("active", encoding="utf-8")
    worker_report = tmp_path / "worker-1.json"
    worker_report.write_text("{}", encoding="utf-8")
    old_time = datetime(2026, 4, 1, tzinfo=UTC).timestamp()
    old_log.touch()
    active_log.touch()
    worker_report.touch()
    import os

    os.utime(old_log, (old_time, old_time))
    os.utime(active_log, (old_time, old_time))
    os.utime(worker_report, (old_time, old_time))

    result = cleanup_runtime_log_files(
        tmp_path,
        policy=TelemetryRetentionPolicy(runtime_log_retention_days=14),
        now=datetime(2026, 5, 17, tzinfo=UTC),
    )

    assert result["deleted_files"] == 1
    assert not old_log.exists()
    assert active_log.exists()
    assert worker_report.exists()
