from pathlib import Path
import sqlite3

from option_data_manager.cli.compare_iv_windows import (
    OptionPair,
    compare_iv_windows,
    main,
    run_iv_window_comparison_command,
    select_option_pairs,
    summarize_results,
)
from option_data_manager.instruments import InstrumentRecord, InstrumentRepository
from option_data_manager.settings import PlainTextProtector, SettingsRepository
from option_data_manager.settings import TQSDK_ACCOUNT_KEY, TQSDK_PASSWORD_KEY


def test_select_option_pairs_uses_active_option_pairs(tmp_path: Path) -> None:
    database = tmp_path / "runtime.sqlite3"
    connection = sqlite3.connect(database)
    repository = InstrumentRepository(connection)
    repository.upsert_instruments(
        [
            _instrument("DCE.m2601-C-3000", "DCE.m2601", "CALL", True),
            _instrument("DCE.m2601-P-3000", "DCE.m2601", "PUT", True),
            _instrument("DCE.m2601-C-3100", "DCE.m2601", "CALL", False),
        ]
    )

    pairs = select_option_pairs(connection, max_options=10)

    assert pairs == [
        OptionPair("DCE.m2601-C-3000", "DCE.m2601"),
        OptionPair("DCE.m2601-P-3000", "DCE.m2601"),
    ]


def test_compare_iv_windows_records_deterministic_differences() -> None:
    class FakeApi:
        def __init__(self) -> None:
            self.wait_updates = 0

        def get_quote(self, symbol: str) -> dict[str, str]:
            return {"symbol": symbol}

        def get_kline_serial(
            self,
            symbols: list[str],
            *,
            duration_seconds: int,
            data_length: int,
        ) -> dict[str, object]:
            return {"symbols": symbols, "window": data_length}

        def wait_update(self, *, deadline: float) -> bool:
            self.wait_updates += 1
            return True

    def fake_iv(klines: dict[str, object], quote: dict[str, str], r: float) -> dict[str, list[float]]:
        return {"impv": [0.1 + int(klines["window"]) / 1000]}

    results = compare_iv_windows(
        FakeApi(),
        pairs=[OptionPair("DCE.m2601-C-3000", "DCE.m2601")],
        windows=(1, 20),
        wait_cycles=1,
        risk_free_rate=0.025,
        iv_calculator=fake_iv,
    )

    assert [item.window for item in results] == [1, 20]
    assert [round(item.iv or 0.0, 3) for item in results] == [0.101, 0.12]
    summary = summarize_results(results, baseline_window=20)
    assert summary["successful_points"] == 2
    assert round(summary["max_abs_diff_from_baseline"], 3) == 0.019


def test_run_iv_window_comparison_command_closes_api_and_hides_secret(tmp_path: Path) -> None:
    database = tmp_path / "runtime.sqlite3"
    connection = sqlite3.connect(database)
    InstrumentRepository(connection).upsert_instruments(
        [_instrument("DCE.m2601-C-3000", "DCE.m2601", "CALL", True)]
    )
    connection.close()
    closed = False

    class FakeApi:
        def close(self) -> None:
            nonlocal closed
            closed = True

        def get_quote(self, symbol: str) -> dict[str, str]:
            return {"symbol": symbol}

        def get_kline_serial(
            self,
            symbols: list[str],
            *,
            duration_seconds: int,
            data_length: int,
        ) -> dict[str, object]:
            return {"symbols": symbols, "window": data_length}

        def wait_update(self, *, deadline: float) -> bool:
            return True

    def fake_iv(klines: dict[str, object], quote: dict[str, str], r: float) -> dict[str, list[float]]:
        return {"impv": [float(klines["window"])]}

    report = run_iv_window_comparison_command(
        account="demo",
        password="super-secret",
        database_path=database,
        windows=(1, 2),
        max_options=1,
        wait_cycles=0,
        api_factory=lambda *_: FakeApi(),
        iv_calculator=fake_iv,
    )

    assert closed is True
    assert report["status"] == "success"
    assert "super-secret" not in str(report)
    assert report["summary"]["max_abs_diff_from_baseline"] == 1.0


def test_main_writes_blocked_report_without_secret(tmp_path: Path) -> None:
    report_path = tmp_path / "blocked.json"

    exit_code = main(
        [
            "--database",
            str(tmp_path / "missing.sqlite3"),
            "--report",
            str(report_path),
        ],
        env={},
    )

    assert exit_code == 2
    report_text = report_path.read_text(encoding="utf-8")
    assert "blocked" in report_text
    assert "TQSDK_PASSWORD" not in report_text


def test_main_uses_database_credentials_without_printing_secret(
    monkeypatch,
    tmp_path: Path,
) -> None:
    database = tmp_path / "runtime.sqlite3"
    connection = sqlite3.connect(database)
    SettingsRepository(connection, PlainTextProtector()).set_value(TQSDK_ACCOUNT_KEY, "demo")
    SettingsRepository(connection, PlainTextProtector()).set_secret(
        TQSDK_PASSWORD_KEY,
        "super-secret",
    )
    connection.close()
    report_path = tmp_path / "report.json"

    monkeypatch.setattr(
        "option_data_manager.cli.compare_iv_windows.run_iv_window_comparison_command",
        lambda **kwargs: {"status": "success", "password_seen": kwargs["password"] == "super-secret"},
    )
    monkeypatch.setattr(
        "option_data_manager.cli.compare_iv_windows._resolve_credentials",
        lambda *_, **__: type(
            "Credentials",
            (),
            {
                "account": "demo",
                "password": "super-secret",
                "source": "test database",
            },
        )(),
    )

    exit_code = main(["--database", str(database), "--report", str(report_path)], env={})

    assert exit_code == 0
    report_text = report_path.read_text(encoding="utf-8")
    assert "super-secret" not in report_text
    assert "password_seen" in report_text


def _instrument(
    symbol: str,
    underlying: str,
    option_class: str,
    active: bool,
) -> InstrumentRecord:
    return InstrumentRecord(
        symbol=symbol,
        exchange_id="DCE",
        product_id="m",
        instrument_id=symbol.split(".")[-1],
        instrument_name=None,
        ins_class="OPTION",
        underlying_symbol=underlying,
        option_class=option_class,
        strike_price=3000.0,
        expire_datetime=None,
        price_tick=None,
        volume_multiple=None,
        expired=not active,
        active=active,
        inactive_reason=None if active else "test",
        last_seen_at="2026-05-10T00:00:00+00:00",
        raw_payload_json="{}",
    )
