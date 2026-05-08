from option_data_manager.cli.collect_market import _collection_status


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
