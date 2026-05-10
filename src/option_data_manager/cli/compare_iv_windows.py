"""Compare OPTION_IMPV results across different daily K-line windows."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sqlite3
import time
from typing import Any

from option_data_manager.cli.collect_market import (
    DEFAULT_DATABASE_PATH,
    DEFAULT_SOURCE_DATABASE_PATH,
    _resolve_credentials,
)
from option_data_manager.instruments import InstrumentRepository
from option_data_manager.source_values import finite_float, json_safe
from option_data_manager.tqsdk_connection import create_tqsdk_api_with_retries


SECONDS_PER_DAY = 24 * 60 * 60
DEFAULT_WINDOWS = (1, 2, 5, 20)
DEFAULT_REPORT_PATH = Path("docs/qa/live-evidence/iv-window-comparison/latest.json")


@dataclass(frozen=True)
class OptionPair:
    """One option symbol and its underlying future symbol."""

    option_symbol: str
    underlying_symbol: str


@dataclass(frozen=True)
class WindowResult:
    """Latest IV result for one option/window combination."""

    option_symbol: str
    underlying_symbol: str
    window: int
    iv: float | None
    elapsed_seconds: float
    status: str
    error_type: str | None = None
    message: str | None = None


def main(argv: list[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    """Run the live IV window comparison without printing credential values."""

    args = _parse_args(argv)
    runtime_env = os.environ if env is None else env
    database_path = Path(args.database)
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    credentials = _resolve_credentials(
        runtime_env,
        database_path=database_path,
        source_database_path=Path(args.source_database),
    )
    if credentials is None:
        _write_json_report(
            report_path,
            {
                "status": "blocked",
                "generated_at": datetime.now(UTC).isoformat(),
                "message": "TQSDK credentials are not configured.",
                "secret_handling": "No credential value was written to this report.",
            },
        )
        print(f"blocked: TQSDK credentials are not configured. Report: {report_path}")
        return 2

    try:
        result = run_iv_window_comparison_command(
            account=credentials.account,
            password=credentials.password,
            database_path=database_path,
            windows=_parse_windows(args.windows),
            max_options=args.max_options,
            option_symbols=_parse_symbols(args.symbols),
            wait_cycles=max(args.wait_cycles, 0),
            risk_free_rate=args.risk_free_rate,
        )
        result["credential_source"] = credentials.source
        result["secret_handling"] = "No credential value was written to this report."
        _write_json_report(report_path, result)
    except Exception as exc:
        _write_json_report(
            report_path,
            {
                "status": "failed",
                "generated_at": datetime.now(UTC).isoformat(),
                "error_type": type(exc).__name__,
                "message": str(exc),
                "secret_handling": "Credential values were not written to this report.",
            },
        )
        print(f"failed: {type(exc).__name__}: {exc}. Report: {report_path}")
        return 1

    print(f"ok: IV window comparison report written to {report_path}")
    return 0


def run_iv_window_comparison_command(
    *,
    account: str,
    password: str,
    database_path: Path,
    windows: Sequence[int] = DEFAULT_WINDOWS,
    max_options: int = 6,
    option_symbols: Sequence[str] = (),
    wait_cycles: int = 2,
    risk_free_rate: float = 0.025,
    api_factory: Callable[[str, str], Any] | None = None,
    iv_calculator: Callable[[Any, Any, float], Any] | None = None,
) -> dict[str, Any]:
    """Open TQSDK, compare IV windows, and return a secret-safe report."""

    selected_windows = _clean_windows(windows)
    if max_options < 1:
        raise ValueError("max_options must be positive.")
    if wait_cycles < 0:
        raise ValueError("wait_cycles must not be negative.")

    connection = _connect_database(database_path)
    try:
        pairs = select_option_pairs(
            connection,
            max_options=max_options,
            option_symbols=tuple(option_symbols),
        )
    finally:
        connection.close()
    if not pairs:
        raise ValueError("No active option pairs were found in the runtime database.")

    create_api = api_factory or create_tqsdk_api_with_retries
    calculate_iv = iv_calculator or _calculate_option_impv
    api = create_api(account, password)
    started = time.monotonic()
    generated_at = datetime.now(UTC).isoformat()
    try:
        results = compare_iv_windows(
            api,
            pairs=pairs,
            windows=selected_windows,
            wait_cycles=wait_cycles,
            risk_free_rate=risk_free_rate,
            iv_calculator=calculate_iv,
        )
    finally:
        close = getattr(api, "close", None)
        if callable(close):
            close()

    return {
        "status": "success",
        "generated_at": generated_at,
        "database": str(database_path),
        "settings": {
            "windows": list(selected_windows),
            "max_options": max_options,
            "requested_option_symbols": list(option_symbols),
            "wait_cycles": wait_cycles,
            "risk_free_rate": risk_free_rate,
        },
        "option_count": len(pairs),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "results": [asdict(result) for result in results],
        "summary": summarize_results(results, baseline_window=max(selected_windows)),
    }


def select_option_pairs(
    connection: sqlite3.Connection,
    *,
    max_options: int,
    option_symbols: tuple[str, ...] = (),
) -> list[OptionPair]:
    """Select active option/underlying pairs from the runtime database."""

    InstrumentRepository(connection)
    connection.row_factory = sqlite3.Row
    if option_symbols:
        placeholders = ",".join("?" for _ in option_symbols)
        rows = connection.execute(
            f"""
            SELECT symbol, underlying_symbol
            FROM instruments
            WHERE active = 1
              AND underlying_symbol IS NOT NULL
              AND symbol IN ({placeholders})
            ORDER BY symbol
            """,
            option_symbols,
        ).fetchall()
        found = {str(row["symbol"]) for row in rows}
        missing = [symbol for symbol in option_symbols if symbol not in found]
        if missing:
            raise ValueError(f"Option symbols are not active: {', '.join(missing)}")
    else:
        rows = connection.execute(
            """
            SELECT symbol, underlying_symbol
            FROM instruments
            WHERE active = 1
              AND underlying_symbol IS NOT NULL
              AND option_class IN ('CALL', 'PUT')
            ORDER BY exchange_id, product_id, underlying_symbol, strike_price, option_class, symbol
            LIMIT ?
            """,
            (max_options,),
        ).fetchall()
    return [
        OptionPair(
            option_symbol=str(row["symbol"]),
            underlying_symbol=str(row["underlying_symbol"]),
        )
        for row in rows
    ]


def compare_iv_windows(
    api: Any,
    *,
    pairs: Sequence[OptionPair],
    windows: Sequence[int],
    wait_cycles: int,
    risk_free_rate: float,
    iv_calculator: Callable[[Any, Any, float], Any],
) -> list[WindowResult]:
    """Compare latest OPTION_IMPV outputs for each requested K-line window."""

    results: list[WindowResult] = []
    quote_refs: dict[str, Any] = {}
    for pair in pairs:
        try:
            quote_refs[pair.option_symbol] = api.get_quote(pair.option_symbol)
        except Exception as exc:
            for window in windows:
                results.append(
                    WindowResult(
                        option_symbol=pair.option_symbol,
                        underlying_symbol=pair.underlying_symbol,
                        window=window,
                        iv=None,
                        elapsed_seconds=0.0,
                        status="failed",
                        error_type=type(exc).__name__,
                        message=str(exc),
                    )
                )
    _wait_updates(api, wait_cycles=wait_cycles)

    for pair in pairs:
        quote = quote_refs.get(pair.option_symbol)
        if quote is None:
            continue
        for window in windows:
            started = time.monotonic()
            try:
                klines = api.get_kline_serial(
                    [pair.option_symbol, pair.underlying_symbol],
                    duration_seconds=SECONDS_PER_DAY,
                    data_length=window,
                )
                _wait_updates(api, wait_cycles=wait_cycles)
                iv_payload = iv_calculator(klines, quote, risk_free_rate)
                iv = _latest_impv(iv_payload)
                results.append(
                    WindowResult(
                        option_symbol=pair.option_symbol,
                        underlying_symbol=pair.underlying_symbol,
                        window=window,
                        iv=iv,
                        elapsed_seconds=round(time.monotonic() - started, 3),
                        status="success" if iv is not None else "empty",
                    )
                )
            except Exception as exc:
                results.append(
                    WindowResult(
                        option_symbol=pair.option_symbol,
                        underlying_symbol=pair.underlying_symbol,
                        window=window,
                        iv=None,
                        elapsed_seconds=round(time.monotonic() - started, 3),
                        status="failed",
                        error_type=type(exc).__name__,
                        message=str(exc),
                    )
                )
    return results


def summarize_results(
    results: Sequence[WindowResult],
    *,
    baseline_window: int,
) -> dict[str, Any]:
    """Summarize per-window IV differences against the largest window."""

    by_symbol: dict[str, dict[int, WindowResult]] = {}
    for result in results:
        by_symbol.setdefault(result.option_symbol, {})[result.window] = result

    comparisons: list[dict[str, Any]] = []
    max_abs_diff = 0.0
    comparable_count = 0
    for symbol, per_window in sorted(by_symbol.items()):
        baseline = per_window.get(baseline_window)
        baseline_iv = baseline.iv if baseline is not None else None
        for window, result in sorted(per_window.items()):
            diff = (
                None
                if baseline_iv is None or result.iv is None
                else result.iv - baseline_iv
            )
            abs_diff = None if diff is None else abs(diff)
            if abs_diff is not None:
                comparable_count += 1
                max_abs_diff = max(max_abs_diff, abs_diff)
            comparisons.append(
                {
                    "option_symbol": symbol,
                    "underlying_symbol": result.underlying_symbol,
                    "window": window,
                    "baseline_window": baseline_window,
                    "iv": result.iv,
                    "baseline_iv": baseline_iv,
                    "diff_from_baseline": diff,
                    "abs_diff_from_baseline": abs_diff,
                    "status": result.status,
                }
            )

    return {
        "baseline_window": baseline_window,
        "comparable_count": comparable_count,
        "max_abs_diff_from_baseline": max_abs_diff if comparable_count else None,
        "successful_points": sum(1 for result in results if result.status == "success"),
        "empty_points": sum(1 for result in results if result.status == "empty"),
        "failed_points": sum(1 for result in results if result.status == "failed"),
        "comparisons": comparisons,
    }


def _calculate_option_impv(klines: Any, quote: Any, risk_free_rate: float) -> Any:
    from tqsdk.ta import OPTION_IMPV

    return OPTION_IMPV(klines, quote, r=risk_free_rate)


def _latest_impv(payload: Any) -> float | None:
    if payload is None:
        return None
    data = dict(payload.to_dict()) if hasattr(payload, "to_dict") else dict(payload)
    values = data.get("impv")
    if isinstance(values, Mapping):
        values = list(values.values())
    if isinstance(values, list | tuple):
        return finite_float(values[-1]) if values else None
    return finite_float(values)


def _connect_database(database_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database_path, timeout=30)
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


def _wait_updates(api: Any, *, wait_cycles: int) -> None:
    for _ in range(max(wait_cycles, 0)):
        api.wait_update(deadline=time.time() + 1)


def _parse_windows(value: str) -> tuple[int, ...]:
    return _clean_windows(int(part.strip()) for part in value.split(",") if part.strip())


def _clean_windows(windows: Sequence[int] | Any) -> tuple[int, ...]:
    cleaned = tuple(sorted(set(int(window) for window in windows)))
    if not cleaned or any(window < 1 for window in cleaned):
        raise ValueError("windows must contain positive integers.")
    return cleaned


def _parse_symbols(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(symbol.strip() for symbol in value.split(",") if symbol.strip())


def _write_json_report(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(json_safe(payload), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare TQSDK OPTION_IMPV outputs across K-line data_length windows.",
    )
    parser.add_argument("--database", default=str(DEFAULT_DATABASE_PATH))
    parser.add_argument("--source-database", default=str(DEFAULT_SOURCE_DATABASE_PATH))
    parser.add_argument("--report", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--windows", default=",".join(str(item) for item in DEFAULT_WINDOWS))
    parser.add_argument("--max-options", type=int, default=6)
    parser.add_argument("--symbols", default=None)
    parser.add_argument("--wait-cycles", type=int, default=2)
    parser.add_argument("--risk-free-rate", type=float, default=0.025)
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
