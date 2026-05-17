"""Operational readiness checks for deployment and runtime monitoring."""

from __future__ import annotations

from typing import Any


_SEVERITY_RANK = {"info": 0, "warn": 1, "critical": 2}


def build_operations_monitor(
    *,
    overview: dict[str, Any],
    quote_stream: dict[str, Any] | None = None,
    metrics_worker: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build operator-facing alerts from current runtime/readiness state."""

    alerts: list[dict[str, Any]] = []
    quote_stream = quote_stream or {}
    metrics_worker = metrics_worker or {}
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
