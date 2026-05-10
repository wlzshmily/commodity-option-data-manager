"""Asynchronous IV/Greeks refresh worker."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import sqlite3
import time
import traceback
from typing import Any

from option_data_manager.chain_collector import collect_persisted_option_chain
from option_data_manager.cli.collect_market import (
    DEFAULT_DATABASE_PATH,
    DEFAULT_SOURCE_DATABASE_PATH,
    _connect_database,
    _resolve_credentials,
)
from option_data_manager.metrics_dirty_queue import (
    MetricsDirtyQueueRepository,
    MetricsDirtyTask,
    next_time_after,
)
from option_data_manager.option_metrics import OptionMetricsRepository
from option_data_manager.tqsdk_connection import create_tqsdk_api_with_retries


DEFAULT_REPORT_PATH = Path("docs/qa/sdk-contract-reports/latest-metrics-worker-report.json")
DEFAULT_MIN_INTERVAL_SECONDS = 30
DEFAULT_RETRY_DELAY_SECONDS = 30


@dataclass(frozen=True)
class MetricsWorkerResult:
    """Summary of one metrics worker window."""

    started_at: str
    finished_at: str
    cycles: int
    claimed_count: int
    completed_count: int
    postponed_count: int
    failed_count: int
    pending_count: int
    error_count: int


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    database_path = Path(args.database)
    report_path = Path(args.report)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    credentials = _resolve_credentials(
        os.environ,
        database_path=database_path,
        source_database_path=Path(args.source_database),
    )
    if credentials is None:
        _write_json(
            report_path,
            {
                "status": "blocked",
                "finished_at": datetime.now(UTC).isoformat(),
                "message": "TQSDK credentials are not configured.",
                "secret_handling": "No credential value was written to this report.",
            },
        )
        print(f"blocked: wrote metrics worker report: {report_path}")
        return 2

    api = None
    try:
        api = create_tqsdk_api_with_retries(credentials.account, credentials.password)
        connection = _connect_database(database_path)
        try:
            result = run_metrics_worker(
                api,
                connection,
                cycles=args.cycles,
                duration_seconds=args.duration_seconds,
                poll_interval_seconds=args.poll_interval_seconds,
                claim_limit=args.claim_limit,
                min_interval_seconds=args.min_interval_seconds,
                retry_delay_seconds=args.retry_delay_seconds,
                report_path=report_path,
            )
        finally:
            connection.close()
    except KeyboardInterrupt:
        _write_json(
            report_path,
            {
                "status": "interrupted",
                "finished_at": datetime.now(UTC).isoformat(),
                "secret_handling": "Credential values were not written to this report.",
            },
        )
        print(f"interrupted: wrote metrics worker report: {report_path}")
        return 130
    except Exception as exc:
        _write_json(
            report_path,
            {
                "status": "failed",
                "finished_at": datetime.now(UTC).isoformat(),
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc(),
                "secret_handling": "Credential values were not written to this report.",
            },
        )
        print(f"failed: {type(exc).__name__}: {exc}")
        print(f"Wrote metrics worker report: {report_path}")
        return 1
    finally:
        if api is not None:
            close = getattr(api, "close", None)
            if callable(close):
                close()

    _write_json(
        report_path,
        {
            "status": "success" if result.error_count == 0 else "partial_failure",
            "result": asdict(result),
            "credential_source": credentials.source,
            "database": str(database_path),
            "secret_handling": "No credential value was written to this report.",
        },
    )
    print(f"Wrote metrics worker report: {report_path}")
    return 0 if result.error_count == 0 else 1


def run_metrics_worker(
    api: Any,
    connection: sqlite3.Connection,
    *,
    cycles: int | None = None,
    duration_seconds: float | None = None,
    poll_interval_seconds: float = 1.0,
    claim_limit: int = 20,
    min_interval_seconds: int = DEFAULT_MIN_INTERVAL_SECONDS,
    retry_delay_seconds: int = DEFAULT_RETRY_DELAY_SECONDS,
    report_path: Path | None = None,
) -> MetricsWorkerResult:
    """Process due dirty metrics tasks without blocking quote ingestion."""

    if cycles is not None and cycles < 1:
        raise ValueError("cycles must be positive when provided.")
    if duration_seconds is not None and duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive when provided.")
    if poll_interval_seconds < 0:
        raise ValueError("poll_interval_seconds must not be negative.")
    if claim_limit < 1:
        raise ValueError("claim_limit must be positive.")
    if min_interval_seconds < 1:
        raise ValueError("min_interval_seconds must be positive.")
    if retry_delay_seconds < 1:
        raise ValueError("retry_delay_seconds must be positive.")

    queue = MetricsDirtyQueueRepository(connection)
    metrics_repo = OptionMetricsRepository(connection)
    started_at = datetime.now(UTC).isoformat()
    deadline_at = time.monotonic() + duration_seconds if duration_seconds else None
    cycle_count = 0
    claimed_count = 0
    completed_count = 0
    postponed_count = 0
    failed_count = 0
    error_count = 0
    iv_calculator = _create_option_impv_calculator()

    while True:
        if cycles is not None and cycle_count >= cycles:
            break
        if deadline_at is not None and time.monotonic() >= deadline_at:
            break
        cycle_count += 1
        now = datetime.now(UTC).isoformat()
        tasks = queue.claim_due_tasks(now=now, limit=claim_limit)
        claimed_count += len(tasks)
        if not tasks and poll_interval_seconds:
            _write_progress(
                report_path,
                started_at=started_at,
                cycles=cycle_count,
                claimed_count=claimed_count,
                completed_count=completed_count,
                postponed_count=postponed_count,
                failed_count=failed_count,
                pending_count=queue.pending_count(),
                error_count=error_count,
            )
            time.sleep(poll_interval_seconds)
            continue
        ready_tasks: list[MetricsDirtyTask] = []
        for task in tasks:
            postpone_until = _fresh_metrics_retry_at(
                metrics_repo,
                task,
                min_interval_seconds=min_interval_seconds,
                now=now,
            )
            if postpone_until is not None:
                queue.postpone_recently_refreshed(
                    task=task,
                    next_attempt_at=postpone_until,
                    now=now,
                )
                postponed_count += 1
                continue
            ready_tasks.append(task)
        for underlying_symbol, grouped_tasks in _group_tasks_by_underlying(
            ready_tasks
        ).items():
            try:
                result = collect_persisted_option_chain(
                    api,
                    connection,
                    underlying_symbol=underlying_symbol,
                    option_symbols=tuple(task.symbol for task in grouped_tasks),
                    batch_size=len(grouped_tasks),
                    wait_cycles=1,
                    iv_calculator=iv_calculator,
                )
                if result.error_count:
                    raise MetricsRefreshError(
                        f"Metrics refresh completed with {result.error_count} error(s)."
                    )
                for task in grouped_tasks:
                    queue.complete_task(task)
                completed_count += len(grouped_tasks)
            except Exception as exc:
                failed_count += len(grouped_tasks)
                error_count += len(grouped_tasks)
                retry_at = next_time_after(retry_delay_seconds, now=now)
                failed_at = datetime.now(UTC).isoformat()
                for task in grouped_tasks:
                    queue.fail_task(
                        task,
                        error_type=type(exc).__name__,
                        message=str(exc),
                        retry_at=retry_at,
                        now=failed_at,
                    )
        _write_progress(
            report_path,
            started_at=started_at,
            cycles=cycle_count,
            claimed_count=claimed_count,
            completed_count=completed_count,
            postponed_count=postponed_count,
            failed_count=failed_count,
            pending_count=queue.pending_count(),
            error_count=error_count,
        )

    return MetricsWorkerResult(
        started_at=started_at,
        finished_at=datetime.now(UTC).isoformat(),
        cycles=cycle_count,
        claimed_count=claimed_count,
        completed_count=completed_count,
        postponed_count=postponed_count,
        failed_count=failed_count,
        pending_count=queue.pending_count(),
        error_count=error_count,
    )


class MetricsRefreshError(RuntimeError):
    """Raised when a task did not produce a clean metrics refresh."""


def _group_tasks_by_underlying(
    tasks: list[MetricsDirtyTask],
) -> dict[str, list[MetricsDirtyTask]]:
    grouped: dict[str, list[MetricsDirtyTask]] = {}
    for task in tasks:
        grouped.setdefault(task.underlying_symbol, []).append(task)
    return grouped


def _fresh_metrics_retry_at(
    metrics_repo: OptionMetricsRepository,
    task: MetricsDirtyTask,
    *,
    min_interval_seconds: int,
    now: str,
) -> str | None:
    existing = metrics_repo.get_metrics(task.symbol)
    if existing is None:
        return None
    received_at = _parse_datetime(existing.received_at)
    eligible_at = received_at + timedelta(seconds=min_interval_seconds)
    if eligible_at <= _parse_datetime(now):
        return None
    return eligible_at.isoformat()


def _create_option_impv_calculator():
    from tqsdk.ta import OPTION_IMPV

    return lambda klines, quote: OPTION_IMPV(klines, quote, r=0.025)


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _write_progress(
    report_path: Path | None,
    *,
    started_at: str,
    cycles: int,
    claimed_count: int,
    completed_count: int,
    postponed_count: int,
    failed_count: int,
    pending_count: int,
    error_count: int,
) -> None:
    if report_path is None:
        return
    _write_json(
        report_path,
        {
            "status": "running",
            "progress": {
                "started_at": started_at,
                "updated_at": datetime.now(UTC).isoformat(),
                "cycles": cycles,
                "claimed_count": claimed_count,
                "completed_count": completed_count,
                "postponed_count": postponed_count,
                "failed_count": failed_count,
                "pending_count": pending_count,
                "error_count": error_count,
            },
            "secret_handling": "No credential value was written to this report.",
        },
    )


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    temp_path = path.with_name(f"{path.name}.tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_path.replace(path)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run an asynchronous IV/Greeks dirty-queue worker.",
    )
    parser.add_argument("--database", default=str(DEFAULT_DATABASE_PATH))
    parser.add_argument("--source-database", default=str(DEFAULT_SOURCE_DATABASE_PATH))
    parser.add_argument("--report", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--cycles", type=int, default=None)
    parser.add_argument("--duration-seconds", type=float, default=None)
    parser.add_argument("--poll-interval-seconds", type=float, default=1.0)
    parser.add_argument("--claim-limit", type=int, default=20)
    parser.add_argument("--min-interval-seconds", type=int, default=DEFAULT_MIN_INTERVAL_SECONDS)
    parser.add_argument("--retry-delay-seconds", type=int, default=DEFAULT_RETRY_DELAY_SECONDS)
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
