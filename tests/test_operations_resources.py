from option_data_manager.operations_monitor import build_operations_monitor


def test_operations_monitor_alerts_on_resource_pressure() -> None:
    monitor = build_operations_monitor(
        overview={"collection": {}, "underlyings": []},
        resources={
            "disk": {
                "status": "ok",
                "path": "/data",
                "free_percent": 5.0,
                "free_bytes": 1024,
            },
            "memory": {"status": "ok", "available_percent": 9.0},
            "cpu": {"status": "ok", "load_5m_ratio": 2.1},
            "sqlite": {"total_wal_size_bytes": 256 * 1024 * 1024},
            "runtime_logs": {"total_bytes": 700 * 1024 * 1024},
        },
        sqlite_runtime={
            "retry_count": 3,
            "last_error": "OperationalError: database is locked",
        },
        telemetry_cleanup={"status": "failed", "error": "database is locked"},
    )

    codes = {alert["code"] for alert in monitor["alerts"]}
    assert monitor["status"] == "critical"
    assert "disk_space_critical" in codes
    assert "memory_available_critical" in codes
    assert "cpu_load_critical" in codes
    assert "sqlite_wal_large" in codes
    assert "runtime_logs_large" in codes
    assert "sqlite_retryable_lock_seen" in codes
    assert "telemetry_cleanup_failed" in codes
