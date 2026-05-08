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


def test_underlying_rows_expose_market_time_separately_from_received_at() -> None:
    connection = sqlite3.connect(":memory:")
    read_model = WebuiReadModel(connection)
    connection.executemany(
        """
        INSERT INTO instruments (
            symbol,
            exchange_id,
            product_id,
            instrument_id,
            instrument_name,
            ins_class,
            underlying_symbol,
            option_class,
            strike_price,
            expire_datetime,
            price_tick,
            volume_multiple,
            expired,
            active,
            inactive_reason,
            last_seen_at,
            raw_payload_json
        )
        VALUES (?, 'SHFE', 'cu', ?, NULL, ?, ?, ?, ?, NULL, NULL, NULL, 0, 1, NULL, '2026-05-08T00:00:00+00:00', '{}')
        """,
        [
            ("SHFE.cu2606", "cu2606", "FUTURE", None, None, None),
            ("SHFE.cu2606C70000", "cu2606C70000", "OPTION", "SHFE.cu2606", "CALL", 70000),
        ],
    )
    connection.executemany(
        """
        INSERT INTO quote_current (
            symbol,
            source_datetime,
            received_at,
            raw_payload_json
        )
        VALUES (?, ?, ?, '{}')
        """,
        [
            (
                "SHFE.cu2606",
                "2026-05-08 22:59:59.000000",
                "2026-05-08T16:25:24+00:00",
            ),
            (
                "SHFE.cu2606C70000",
                "2026-05-08 22:59:58.000000",
                "2026-05-08T16:25:24+00:00",
            ),
        ],
    )
    connection.execute(
        """
        INSERT INTO kline_20d_current (
            symbol,
            bar_datetime,
            received_at,
            raw_payload_json
        )
        VALUES ('SHFE.cu2606C70000', '2026-05-08T00:00:00+08:00', '2026-05-08T16:25:24+00:00', '{}')
        """
    )

    row = read_model.overview()["underlyings"][0]

    assert row["latest_update"] == "2026-05-08T16:25:24+00:00"
    assert row["display_market_time"] == "2026-05-08 22:59:59.000000"
    assert row["display_kline_time"] is not None
