import sqlite3

from option_data_manager.webui.read_model import WebuiReadModel


def test_overview_includes_collection_progress() -> None:
    connection = sqlite3.connect(":memory:")
    read_model = WebuiReadModel(connection)
    rows = [
        ("routine-market-current-slice", "SHFE.cu2606", 1, "SHFE", "cu", 5, "success", 1, None, "2026-05-08T00:01:00+00:00", "2026-05-08T00:02:00+00:00", 0),
        ("routine-market-current-slice", "SHFE.cu2606", 2, "SHFE", "cu", 5, "pending", 0, None, "2026-05-08T00:03:00+00:00", None, 0),
        ("routine-market-current-slice", "SHFE.cu2606", 3, "SHFE", "cu", 4, "failed", 2, "source_unavailable", "2026-05-08T00:04:00+00:00", None, 0),
    ]
    connection.executemany(
        """
        INSERT INTO collection_plan_batches (
            plan_scope,
            underlying_symbol,
            batch_index,
            exchange_id,
            product_id,
            option_symbols_json,
            option_count,
            status,
            attempt_count,
            last_error,
            created_at,
            updated_at,
            completed_at,
            stale
        )
        VALUES (?, ?, ?, ?, ?, '[]', ?, ?, ?, ?, '2026-05-08T00:00:00+00:00', ?, ?, ?)
        """,
        rows,
    )

    progress = read_model.overview()["collection"]

    assert progress["active_batches"] == 3
    assert progress["success_batches"] == 1
    assert progress["pending_batches"] == 1
    assert progress["failed_batches"] == 1
    assert progress["remaining_batches"] == 2
    assert progress["completion_ratio"] == 0.3333
    assert progress["recent_failures"][0]["last_error"] == "source_unavailable"
