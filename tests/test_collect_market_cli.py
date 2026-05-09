from option_data_manager.cli import collect_market
from option_data_manager.cli.collect_market import TqsdkCredentials, _collection_status


def test_collection_status_uses_batch_state_not_metric_warning_run_status() -> None:
    result = {
        "results": [
            {
                "collection_batch_status": "success",
                "run": {"status": "partial_failure"},
            }
        ]
    }

    assert _collection_status(result) == "success"


def test_collect_market_main_returns_nonzero_on_failed_connection(
    monkeypatch,
    tmp_path,
) -> None:
    database_path = tmp_path / "runtime.sqlite3"
    report_path = tmp_path / "report.md"
    monkeypatch.setattr(
        collect_market,
        "_resolve_credentials",
        lambda *_, **__: TqsdkCredentials("demo", "super-secret", "test"),
    )

    def fail_collection(**kwargs):
        raise RuntimeError("network unavailable")

    monkeypatch.setattr(collect_market, "run_collection_command", fail_collection)

    exit_code = collect_market.main(
        [
            "--database",
            str(database_path),
            "--report",
            str(report_path),
        ]
    )

    assert exit_code == 1
    assert "Failed" in report_path.read_text(encoding="utf-8")
    assert "super-secret" not in report_path.read_text(encoding="utf-8")
