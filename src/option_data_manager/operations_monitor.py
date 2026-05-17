"""Operational readiness checks for deployment and runtime monitoring."""

from __future__ import annotations

from typing import Any


_SEVERITY_RANK = {"info": 0, "warn": 1, "critical": 2}
_TWO_GIB = 2 * 1024 * 1024 * 1024
_RUNTIME_LOG_WARN_BYTES = 512 * 1024 * 1024
_WAL_WARN_BYTES = 128 * 1024 * 1024


def build_operations_monitor(
    *,
    overview: dict[str, Any],
    quote_stream: dict[str, Any] | None = None,
    metrics_worker: dict[str, Any] | None = None,
    resources: dict[str, Any] | None = None,
    sqlite_runtime: dict[str, Any] | None = None,
    telemetry_cleanup: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build operator-facing alerts from current runtime/readiness state."""

    alerts: list[dict[str, Any]] = []
    quote_stream = quote_stream or {}
    metrics_worker = metrics_worker or {}
    resources = resources or {}
    sqlite_runtime = sqlite_runtime or {}
    telemetry_cleanup = telemetry_cleanup or {}
    collection = overview.get("collection") or {}
    rows = overview.get("underlyings") or []
    progress = quote_stream.get("progress") or {}
    quote_running = bool(quote_stream.get("running"))
    subscription_complete = _subscription_complete(progress)

    stale_running = int(collection.get("stale_running_batches") or 0)
    if stale_running:
        samples = [
            _batch_label(item)
            for item in (collection.get("recent_stale_running_batches") or [])[:5]
        ]
        _append_alert(
            alerts,
            severity="critical",
            code="stale_running_batches",
            title="批量补采卡住",
            message=(
                f"{stale_running} 个 current-slice batch 长时间停在 running，"
                "需要重置或续跑后才能确认数据完整。"
            ),
            samples=samples,
        )

    active_batches = int(collection.get("active_batches") or 0)
    remaining_batches = int(collection.get("remaining_batches") or 0)
    failed_batches = int(collection.get("failed_batches") or 0)
    if active_batches > 0 and remaining_batches > 0:
        severity = "critical" if subscription_complete or failed_batches else "warn"
        _append_alert(
            alerts,
            severity=severity,
            code="current_slice_incomplete",
            title="批量 current-slice 未完成",
            message=(
                f"批量补采 {int(collection.get('success_batches') or 0)}/"
                f"{active_batches} 完成，仍有 {remaining_batches} 个 batch "
                "未成功落库。"
            ),
        )

    if quote_running and not bool(metrics_worker.get("running")):
        _append_alert(
            alerts,
            severity="critical",
            code="metrics_worker_not_running",
            title="指标 worker 未运行",
            message=(
                "实时订阅正在运行，但 IV/Greeks 指标 worker 不在运行态，"
                "后续指标可能不会刷新。"
            ),
            detail=metrics_worker.get("message"),
        )

    if str(metrics_worker.get("status") or "") == "failed":
        _append_alert(
            alerts,
            severity="critical",
            code="metrics_worker_failed",
            title="指标 worker 已失败",
            message="指标 worker 报告失败，需要查看 runtime report 和服务日志。",
            detail=metrics_worker.get("message"),
        )

    if subscription_complete:
        kline_gap_rows = _rows_with_zero(rows, "kline_count")
        if kline_gap_rows:
            _append_alert(
                alerts,
                severity="critical",
                code="subscribed_kline_data_missing",
                title="订阅完成但 K线缺口",
                message=(
                    f"{len(kline_gap_rows)} 个已订阅标的没有 K线 current-slice 行，"
                    "订阅完成不能作为数据已落库的证明。"
                ),
                samples=_row_samples(kline_gap_rows),
            )

        metric_gap_rows = _rows_with_zero(rows, "metrics_count")
        if metric_gap_rows:
            _append_alert(
                alerts,
                severity="critical",
                code="subscribed_metrics_data_missing",
                title="订阅完成但 IV/Greeks 缺口",
                message=(
                    f"{len(metric_gap_rows)} 个已订阅标的没有 metrics current-slice 行，"
                    "需要确认 metrics worker 和补采任务。"
                ),
                samples=_row_samples(metric_gap_rows),
            )

    _append_resource_alerts(alerts, resources)
    _append_sqlite_runtime_alerts(alerts, sqlite_runtime)
    _append_telemetry_cleanup_alerts(alerts, telemetry_cleanup)

    status = "ok"
    if alerts:
        worst = max(alerts, key=lambda item: _SEVERITY_RANK.get(item["severity"], 0))
        status = "critical" if worst["severity"] == "critical" else "warn"
    return {
        "status": status,
        "healthy": status == "ok",
        "alert_count": len(alerts),
        "critical_count": sum(1 for item in alerts if item["severity"] == "critical"),
        "warn_count": sum(1 for item in alerts if item["severity"] == "warn"),
        "alerts": alerts,
        "resources": resources,
        "sqlite_runtime": sqlite_runtime,
        "telemetry_cleanup": telemetry_cleanup,
    }


def _append_alert(
    alerts: list[dict[str, Any]],
    *,
    severity: str,
    code: str,
    title: str,
    message: str,
    detail: Any = None,
    samples: list[str] | None = None,
) -> None:
    alert = {
        "severity": severity,
        "code": code,
        "title": title,
        "message": message,
    }
    if detail:
        alert["detail"] = str(detail)
    if samples:
        alert["samples"] = samples
    alerts.append(alert)


def _subscription_complete(progress: dict[str, Any]) -> bool:
    total = int(progress.get("total_objects") or 0)
    subscribed = int(progress.get("subscribed_objects") or 0)
    if total <= 0:
        quote_total = int(progress.get("quote_total") or 0)
        kline_total = int(progress.get("kline_total") or 0)
        quote_subscribed = int(progress.get("quote_subscribed") or 0)
        kline_subscribed = int(progress.get("kline_subscribed") or 0)
        total = quote_total + kline_total
        subscribed = quote_subscribed + kline_subscribed
    return total > 0 and subscribed >= total


def _rows_with_zero(rows: list[Any], key: str) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if int(row.get(key) or 0) == 0:
            result.append(row)
    return result


def _row_samples(rows: list[dict[str, Any]], *, limit: int = 8) -> list[str]:
    return [
        str(row.get("underlying_symbol") or row.get("symbol") or "")
        for row in rows[:limit]
        if row.get("underlying_symbol") or row.get("symbol")
    ]


def _batch_label(item: Any) -> str:
    if not isinstance(item, dict):
        return str(item)
    underlying = item.get("underlying_symbol") or "unknown"
    batch = item.get("batch_index")
    updated_at = item.get("updated_at")
    if batch is None:
        return f"{underlying}@{updated_at}"
    return f"{underlying}#{batch}@{updated_at}"


def _append_resource_alerts(alerts: list[dict[str, Any]], resources: dict[str, Any]) -> None:
    disk = resources.get("disk") or {}
    free_percent = _float_value(disk.get("free_percent"))
    free_bytes = _int_value(disk.get("free_bytes"))
    if disk.get("status") == "ok":
        if free_percent < 10.0 or free_bytes < _TWO_GIB:
            _append_alert(
                alerts,
                severity="critical",
                code="disk_space_critical",
                title="磁盘空间不足",
                message=(
                    f"数据目录所在磁盘剩余 {free_percent:.1f}% / "
                    f"{_format_bytes(free_bytes)}，需要释放空间后再继续运行。"
                ),
                detail=disk.get("path"),
            )
        elif free_percent < 15.0:
            _append_alert(
                alerts,
                severity="warn",
                code="disk_space_low",
                title="磁盘空间偏低",
                message=(
                    f"数据目录所在磁盘剩余 {free_percent:.1f}% / "
                    f"{_format_bytes(free_bytes)}，请关注日志和 SQLite 文件增长。"
                ),
                detail=disk.get("path"),
            )

    memory = resources.get("memory") or {}
    available_percent = _float_value(memory.get("available_percent"))
    if memory.get("status") == "ok":
        if available_percent < 10.0:
            _append_alert(
                alerts,
                severity="critical",
                code="memory_available_critical",
                title="内存可用率过低",
                message=f"系统可用内存约 {available_percent:.1f}%，worker 可能不稳定。",
            )
        elif available_percent < 15.0:
            _append_alert(
                alerts,
                severity="warn",
                code="memory_available_low",
                title="内存可用率偏低",
                message=f"系统可用内存约 {available_percent:.1f}%，建议观察 worker。"
            )

    cpu = resources.get("cpu") or {}
    load_ratio = _float_value(cpu.get("load_5m_ratio"))
    if cpu.get("status") == "ok":
        if load_ratio >= 2.0:
            _append_alert(
                alerts,
                severity="critical",
                code="cpu_load_critical",
                title="CPU 负载过高",
                message=f"5 分钟负载约为 CPU 核数的 {load_ratio:.2f} 倍。",
            )
        elif load_ratio >= 1.5:
            _append_alert(
                alerts,
                severity="warn",
                code="cpu_load_high",
                title="CPU 负载偏高",
                message=f"5 分钟负载约为 CPU 核数的 {load_ratio:.2f} 倍。",
            )

    sqlite_files = resources.get("sqlite") or {}
    wal_bytes = _int_value(sqlite_files.get("total_wal_size_bytes"))
    if wal_bytes > _WAL_WARN_BYTES:
        _append_alert(
            alerts,
            severity="warn",
            code="sqlite_wal_large",
            title="SQLite WAL 文件偏大",
            message=f"SQLite WAL 总体积约 {_format_bytes(wal_bytes)}，需要关注 checkpoint。"
        )

    runtime_logs = resources.get("runtime_logs") or {}
    runtime_log_bytes = _int_value(runtime_logs.get("total_bytes"))
    if runtime_log_bytes > _RUNTIME_LOG_WARN_BYTES:
        _append_alert(
            alerts,
            severity="warn",
            code="runtime_logs_large",
            title="运行日志体积偏大",
            message=(
                f"运行日志目录约 {_format_bytes(runtime_log_bytes)}，"
                "需要执行日志轮转或清理旧日志。"
            ),
        )


def _append_sqlite_runtime_alerts(
    alerts: list[dict[str, Any]],
    sqlite_runtime: dict[str, Any],
) -> None:
    retry_count = _int_value(sqlite_runtime.get("retry_count"))
    if retry_count <= 0:
        return
    _append_alert(
        alerts,
        severity="warn",
        code="sqlite_retryable_lock_seen",
        title="SQLite 出现可重试锁等待",
        message=(
            f"当前进程记录到 {retry_count} 次可重试 SQLite busy/locked 事件，"
            "说明写入争用仍需观察。"
        ),
        detail=sqlite_runtime.get("last_error"),
    )


def _append_telemetry_cleanup_alerts(
    alerts: list[dict[str, Any]],
    telemetry_cleanup: dict[str, Any],
) -> None:
    if telemetry_cleanup and telemetry_cleanup.get("status") == "failed":
        _append_alert(
            alerts,
            severity="critical",
            code="telemetry_cleanup_failed",
            title="日志保留清理失败",
            message="SQLite telemetry 日志清理失败，日志表可能继续增长。",
            detail=telemetry_cleanup.get("error"),
        )


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _float_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _format_bytes(value: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    amount = float(max(value, 0))
    for unit in units:
        if amount < 1024.0 or unit == units[-1]:
            return f"{amount:.1f} {unit}"
        amount /= 1024.0
    return f"{amount:.1f} B"
