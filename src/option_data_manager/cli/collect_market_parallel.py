"""Process-level sharded full-market catch-up command."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import time
import traceback
from typing import Any

from option_data_manager.cli.collect_market import (
    DEFAULT_DATABASE_PATH,
    DEFAULT_SOURCE_DATABASE_PATH,
    _connect_database,
    _discover_and_persist_market,
    _resolve_credentials,
)
from option_data_manager.tqsdk_connection import create_tqsdk_api_with_retries


DEFAULT_REPORT_DIR = Path("docs/qa/live-evidence/parallel-catchup")
DEFAULT_SCOPE_PREFIX = "parallel-market-current-slice"


@dataclass(frozen=True)
class ShardSpec:
    """One disjoint contiguous underlying shard."""

    worker_index: int
    worker_count: int
    start_after_underlying: str | None
    end_before_underlying: str | None
    underlying_count: int
    first_underlying: str | None
    last_underlying: str | None


@dataclass(frozen=True)
class WorkerResult:
    """Completed child process summary."""

    worker_index: int
    exit_code: int
    seconds: float
    report_path: str
    stdout_path: str
    stderr_path: str
    selected_batches: int | None
    option_count: int | None
    quotes_written: int | None
    kline_rows_written: int | None
    metrics_written: int | None
    error_count: int | None


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    database_path = Path(args.database)
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path = Path(args.summary)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    credentials = _resolve_credentials(
        os.environ,
        database_path=database_path,
        source_database_path=Path(args.source_database),
    )
    if credentials is None:
        _write_summary(
            summary_path,
            {
                "status": "blocked",
                "message": "TQSDK credentials are not configured.",
                "finished_at": datetime.now(UTC).isoformat(),
                "secret_handling": "No credential value was written to this report.",
            },
        )
        print(f"blocked: wrote parallel collection summary: {summary_path}")
        return 2

    try:
        started_at = datetime.now(UTC).isoformat()
        if args.discover:
            _run_discovery_once(database_path, credentials.account, credentials.password)
        with _connect_database(database_path) as connection:
            underlyings = _active_underlyings(connection)
        shards = build_contiguous_shards(underlyings, worker_count=args.workers)
        waves: list[dict[str, Any]] = []
        exit_code = 0
        for wave_index in range(1, args.max_waves + 1):
            wave_started = time.perf_counter()
            worker_results = run_parallel_wave(
                shards,
                database_path=database_path,
                report_dir=report_dir,
                wave_index=wave_index,
                option_batch_size=args.option_batch_size,
                max_batches_per_worker=args.max_batches_per_worker,
                wait_cycles=args.wait_cycles,
                scope_prefix=args.scope_prefix,
            )
            wave_seconds = time.perf_counter() - wave_started
            waves.append(
                {
                    "wave_index": wave_index,
                    "seconds": round(wave_seconds, 3),
                    "workers": [asdict(result) for result in worker_results],
                }
            )
            if any(result.exit_code != 0 for result in worker_results):
                exit_code = 1
                break
            selected_total = sum(result.selected_batches or 0 for result in worker_results)
            if selected_total == 0 or not args.until_complete:
                break
        summary = _summary_payload(
            args=args,
            shards=shards,
            waves=waves,
            exit_code=exit_code,
            credential_source=credentials.source,
            started_at=started_at,
        )
        _write_summary(summary_path, summary)
    except Exception as exc:
        _write_summary(
            summary_path,
            {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc(),
                "finished_at": datetime.now(UTC).isoformat(),
                "secret_handling": "Credential values were not written to this report.",
            },
        )
        print(f"failed: {type(exc).__name__}: {exc}")
        print(f"Wrote parallel collection summary: {summary_path}")
        return 1

    print(f"Wrote parallel collection summary: {summary_path}")
    return exit_code


def build_contiguous_shards(
    underlyings: list[str],
    *,
    worker_count: int,
) -> list[ShardSpec]:
    """Split ordered underlyings into disjoint contiguous shard ranges."""

    if worker_count < 1:
        raise ValueError("worker_count must be positive.")
    if not underlyings:
        return [
            ShardSpec(index, worker_count, None, None, 0, None, None)
            for index in range(worker_count)
        ]
    shards: list[ShardSpec] = []
    total = len(underlyings)
    for index in range(worker_count):
        start = (total * index) // worker_count
        end = (total * (index + 1)) // worker_count
        chunk = underlyings[start:end]
        previous_symbol = underlyings[start - 1] if start > 0 else None
        next_symbol = underlyings[end] if end < total else None
        shards.append(
            ShardSpec(
                worker_index=index,
                worker_count=worker_count,
                start_after_underlying=previous_symbol,
                end_before_underlying=next_symbol,
                underlying_count=len(chunk),
                first_underlying=chunk[0] if chunk else None,
                last_underlying=chunk[-1] if chunk else None,
            )
        )
    return shards


def run_parallel_wave(
    shards: list[ShardSpec],
    *,
    database_path: Path,
    report_dir: Path,
    wave_index: int,
    option_batch_size: int,
    max_batches_per_worker: int,
    wait_cycles: int,
    scope_prefix: str,
) -> list[WorkerResult]:
    """Run one concurrent wave, one independent TQSDK API process per shard."""

    processes: list[tuple[ShardSpec, float, Path, Path, Path, subprocess.Popen[str]]] = []
    for shard in shards:
        report_path = report_dir / (
            f"wave-{wave_index:03d}-worker-{shard.worker_index:02d}.md"
        )
        stdout_path = report_dir / (
            f"wave-{wave_index:03d}-worker-{shard.worker_index:02d}.stdout.log"
        )
        stderr_path = report_dir / (
            f"wave-{wave_index:03d}-worker-{shard.worker_index:02d}.stderr.log"
        )
        command = [
            sys.executable,
            "-m",
            "option_data_manager.cli.collect_market",
            "--database",
            str(database_path),
            "--report",
            str(report_path),
            "--max-underlyings",
            "1000000",
            "--max-batches",
            str(max_batches_per_worker),
            "--option-batch-size",
            str(option_batch_size),
            "--wait-cycles",
            str(wait_cycles),
            "--scope",
            f"{scope_prefix}-worker-{shard.worker_index:02d}-of-{shard.worker_count:02d}",
            "--skip-discovery",
        ]
        if shard.start_after_underlying is not None:
            command.extend(["--start-after-underlying", shard.start_after_underlying])
        if shard.end_before_underlying is not None:
            command.extend(["--end-before-underlying", shard.end_before_underlying])
        stdout_handle = stdout_path.open("w", encoding="utf-8")
        stderr_handle = stderr_path.open("w", encoding="utf-8")
        started = time.perf_counter()
        process = subprocess.Popen(
            command,
            stdout=stdout_handle,
            stderr=stderr_handle,
            text=True,
        )
        stdout_handle.close()
        stderr_handle.close()
        processes.append((shard, started, report_path, stdout_path, stderr_path, process))

    results: list[WorkerResult] = []
    for shard, started, report_path, stdout_path, stderr_path, process in processes:
        exit_code = process.wait()
        seconds = time.perf_counter() - started
        metrics = _read_worker_report(report_path)
        results.append(
            WorkerResult(
                worker_index=shard.worker_index,
                exit_code=exit_code,
                seconds=round(seconds, 3),
                report_path=str(report_path),
                stdout_path=str(stdout_path),
                stderr_path=str(stderr_path),
                selected_batches=metrics.get("selected_batches"),
                option_count=metrics.get("option_count"),
                quotes_written=metrics.get("quotes_written"),
                kline_rows_written=metrics.get("kline_rows_written"),
                metrics_written=metrics.get("metrics_written"),
                error_count=metrics.get("error_count"),
            )
        )
    return results


def _run_discovery_once(database_path: Path, account: str, password: str) -> None:
    api = create_tqsdk_api_with_retries(account, password)
    try:
        with _connect_database(database_path) as connection:
            _discover_and_persist_market(api, connection)
    finally:
        close = getattr(api, "close", None)
        if callable(close):
            close()


def _active_underlyings(connection: sqlite3.Connection) -> list[str]:
    rows = connection.execute(
        """
        SELECT DISTINCT future.symbol
        FROM instruments AS future
        JOIN instruments AS option
          ON option.underlying_symbol = future.symbol
         AND option.active = 1
         AND option.option_class IN ('CALL', 'PUT')
        WHERE future.active = 1
          AND future.ins_class = 'FUTURE'
        ORDER BY future.symbol
        """
    ).fetchall()
    return [str(row["symbol"] if isinstance(row, sqlite3.Row) else row[0]) for row in rows]


def _read_worker_report(report_path: Path) -> dict[str, int | None]:
    if not report_path.exists():
        return {}
    text = report_path.read_text(encoding="utf-8")
    blocks = re.findall(r"```json\n(.*?)\n```", text, flags=re.S)
    if len(blocks) < 4:
        return {}
    state = json.loads(blocks[2])
    batches = json.loads(blocks[3])
    return {
        "selected_batches": int(state.get("pending_or_failed_selected", 0)),
        "option_count": sum(int(batch.get("option_count") or 0) for batch in batches),
        "quotes_written": sum(int(batch.get("quotes_written") or 0) for batch in batches),
        "kline_rows_written": sum(
            int(batch.get("kline_rows_written") or 0) for batch in batches
        ),
        "metrics_written": sum(int(batch.get("metrics_written") or 0) for batch in batches),
        "error_count": sum(int(batch.get("error_count") or 0) for batch in batches),
    }


def _summary_payload(
    *,
    args: argparse.Namespace,
    shards: list[ShardSpec],
    waves: list[dict[str, Any]],
    exit_code: int,
    credential_source: str,
    started_at: str,
) -> dict[str, Any]:
    worker_results = [
        worker
        for wave in waves
        for worker in wave["workers"]
    ]
    total_options = sum(worker.get("option_count") or 0 for worker in worker_results)
    total_seconds = sum(float(wave["seconds"]) for wave in waves)
    return {
        "status": "success" if exit_code == 0 else "failed",
        "started_at": started_at,
        "finished_at": datetime.now(UTC).isoformat(),
        "settings": {
            "database": args.database,
            "workers": args.workers,
            "option_batch_size": args.option_batch_size,
            "wait_cycles": args.wait_cycles,
            "max_batches_per_worker": args.max_batches_per_worker,
            "until_complete": args.until_complete,
            "max_waves": args.max_waves,
            "scope_prefix": args.scope_prefix,
        },
        "shards": [asdict(shard) for shard in shards],
        "waves": waves,
        "totals": {
            "waves": len(waves),
            "worker_runs": len(worker_results),
            "selected_batches": sum(worker.get("selected_batches") or 0 for worker in worker_results),
            "option_count": total_options,
            "quotes_written": sum(worker.get("quotes_written") or 0 for worker in worker_results),
            "kline_rows_written": sum(worker.get("kline_rows_written") or 0 for worker in worker_results),
            "metrics_written": sum(worker.get("metrics_written") or 0 for worker in worker_results),
            "error_count": sum(worker.get("error_count") or 0 for worker in worker_results),
            "seconds": round(total_seconds, 3),
            "options_per_second": round(total_options / max(total_seconds, 0.001), 3),
        },
        "credential_source": credential_source,
        "secret_handling": "No credential value was written to this report.",
    }


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run process-level sharded full-market current-slice collection."
    )
    parser.add_argument("--database", default=str(DEFAULT_DATABASE_PATH))
    parser.add_argument("--source-database", default=str(DEFAULT_SOURCE_DATABASE_PATH))
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument(
        "--summary",
        default=str(DEFAULT_REPORT_DIR / "latest-parallel-summary.json"),
    )
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--option-batch-size", type=int, default=40)
    parser.add_argument("--wait-cycles", type=int, default=1)
    parser.add_argument("--max-batches-per-worker", type=int, default=50)
    parser.add_argument("--scope-prefix", default=DEFAULT_SCOPE_PREFIX)
    parser.add_argument("--until-complete", action="store_true")
    parser.add_argument("--max-waves", type=int, default=1)
    parser.add_argument("--no-discover", dest="discover", action="store_false")
    parser.set_defaults(discover=True)
    args = parser.parse_args(argv)
    if args.workers < 1:
        parser.error("--workers must be positive.")
    if args.max_batches_per_worker < 1:
        parser.error("--max-batches-per-worker must be positive.")
    if args.max_waves < 1:
        parser.error("--max-waves must be positive.")
    return args


def _write_summary(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
