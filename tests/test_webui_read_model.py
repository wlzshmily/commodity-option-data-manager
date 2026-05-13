from datetime import date, timedelta
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


def test_overview_can_prefer_parallel_collection_progress() -> None:
    connection = sqlite3.connect(":memory:")
    read_model = WebuiReadModel(connection)
    rows = [
        ("routine-market-current-slice", "SHFE.cu2606", 1, "SHFE", "cu", 5, "success", 1, None, "2026-05-08T00:01:00+00:00", "2026-05-08T00:02:00+00:00", 0),
        ("parallel-market-current-slice-worker-00-of-02", "SHFE.cu2606", 1, "SHFE", "cu", 5, "success", 1, None, "2026-05-08T00:03:00+00:00", "2026-05-08T00:04:00+00:00", 0),
        ("parallel-market-current-slice-worker-01-of-02", "SHFE.al2606", 1, "SHFE", "al", 5, "pending", 0, None, "2026-05-08T00:05:00+00:00", None, 0),
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

    overview = read_model.overview(prefer_parallel_collection=True)

    assert overview["collection"]["scope"] == "parallel-market-current-slice"
    assert overview["collection"]["active_batches"] == 2
    assert overview["collection"]["success_batches"] == 1
    assert overview["collection"]["remaining_batches"] == 1
    assert [group["scope"] for group in overview["collection_groups"]] == [
        "routine-market-current-slice",
        "parallel-market-current-slice",
    ]


def test_overview_can_filter_to_current_realtime_quote_rows() -> None:
    connection = sqlite3.connect(":memory:")
    read_model = WebuiReadModel(connection)
    cutoff = "2026-05-12T01:00:00+00:00"
    _insert_simple_chain(
        connection,
        underlying_symbol="SHFE.cu2606",
        exchange_id="SHFE",
        product_id="cu",
        delivery_year=2026,
        delivery_month=6,
        received_at="2026-05-09T01:00:00+00:00",
    )
    _insert_simple_chain(
        connection,
        underlying_symbol="SHFE.al2606",
        exchange_id="SHFE",
        product_id="al",
        delivery_year=2026,
        delivery_month=6,
        received_at="2026-05-12T01:05:00+00:00",
    )

    rows = read_model.overview(current_quote_after=cutoff)["underlyings"]

    assert [row["underlying_symbol"] for row in rows] == ["SHFE.al2606"]
    assert rows[0]["current_quote_count"] == 2
    assert rows[0]["latest_current_quote_received_at"] == "2026-05-12T01:05:00+00:00"


def test_overview_can_skip_underlying_rows_when_realtime_has_not_started() -> None:
    connection = sqlite3.connect(":memory:")
    read_model = WebuiReadModel(connection)
    _insert_simple_chain(
        connection,
        underlying_symbol="SHFE.cu2606",
        exchange_id="SHFE",
        product_id="cu",
        delivery_year=2026,
        delivery_month=6,
        received_at="2026-05-09T01:00:00+00:00",
    )

    overview = read_model.overview(require_current_quote=True)

    assert overview["summary"]["active_options"] == 2
    assert overview["underlyings"] == []


def test_overview_sorts_underlyings_by_product_then_delivery_month() -> None:
    connection = sqlite3.connect(":memory:")
    read_model = WebuiReadModel(connection)
    cutoff = "2026-05-12T01:00:00+00:00"
    for symbol, product, month in (
        ("DCE.b2604", "b", 4),
        ("DCE.a2606", "a", 6),
        ("DCE.a2605", "a", 5),
    ):
        _insert_simple_chain(
            connection,
            underlying_symbol=symbol,
            exchange_id="DCE",
            product_id=product,
            delivery_year=2026,
            delivery_month=month,
            received_at="2026-05-12T01:05:00+00:00",
        )

    rows = read_model.overview(current_quote_after=cutoff)["underlyings"]

    assert [row["underlying_symbol"] for row in rows] == [
        "DCE.a2605",
        "DCE.a2606",
        "DCE.b2604",
    ]


def test_overview_subscription_scope_matches_realtime_contract_selection() -> None:
    connection = sqlite3.connect(":memory:")
    read_model = WebuiReadModel(connection)
    cutoff = "2026-05-12T01:00:00+00:00"
    today = date.today()
    for symbol, product, month, option_expiry, received_at in (
        ("CZCE.FG606", "FG", 6, today, "2026-05-12T01:05:00+00:00"),
        ("CZCE.FG607", "FG", 7, today + timedelta(days=30), "2026-05-09T01:00:00+00:00"),
        ("CZCE.FG608", "FG", 8, today + timedelta(days=60), "2026-05-09T01:00:00+00:00"),
        ("CZCE.MA606", "MA", 6, today, "2026-05-12T01:05:00+00:00"),
        ("CZCE.MA607", "MA", 7, today + timedelta(days=30), "2026-05-12T01:05:00+00:00"),
        ("CZCE.MA608", "MA", 8, today + timedelta(days=60), "2026-05-12T01:05:00+00:00"),
    ):
        _insert_simple_chain(
            connection,
            underlying_symbol=symbol,
            exchange_id="CZCE",
            product_id=product,
            delivery_year=2026,
            delivery_month=month,
            received_at=received_at,
        )
        connection.execute(
            """
            UPDATE instruments
            SET expire_datetime = ?
            WHERE underlying_symbol = ? AND option_class IN ('CALL', 'PUT')
            """,
            (option_expiry.isoformat(), symbol),
        )
    connection.commit()

    rows = read_model.overview(
        current_quote_after=cutoff,
        require_current_quote=True,
        subscription_scope_enabled=True,
        subscription_contract_month_limit=2,
        subscription_min_days_to_expiry=1,
    )["underlyings"]

    assert [row["underlying_symbol"] for row in rows] == [
        "CZCE.FG607",
        "CZCE.FG608",
        "CZCE.MA607",
        "CZCE.MA608",
    ]
    assert {row["underlying_symbol"]: row["status"] for row in rows} == {
        "CZCE.FG607": "待订阅",
        "CZCE.FG608": "待订阅",
        "CZCE.MA607": "已订阅",
        "CZCE.MA608": "已订阅",
    }
    assert all(row["days_to_expiry"] >= 1 for row in rows)


def test_overview_subscription_status_can_follow_worker_lifecycle() -> None:
    connection = sqlite3.connect(":memory:")
    read_model = WebuiReadModel(connection)
    cutoff = "2026-05-12T01:00:00+00:00"
    today = date.today()
    for symbol, product, month in (
        ("CZCE.FG607", "FG", 7),
        ("CZCE.FG608", "FG", 8),
    ):
        _insert_simple_chain(
            connection,
            underlying_symbol=symbol,
            exchange_id="CZCE",
            product_id=product,
            delivery_year=2026,
            delivery_month=month,
            received_at="2026-05-09T01:00:00+00:00",
        )
        connection.execute(
            """
            UPDATE instruments
            SET expire_datetime = ?
            WHERE underlying_symbol = ? AND option_class IN ('CALL', 'PUT')
            """,
            ((today + timedelta(days=30)).isoformat(), symbol),
        )
    connection.commit()

    rows = read_model.overview(
        current_quote_after=cutoff,
        require_current_quote=True,
        subscription_scope_enabled=True,
        subscription_contract_month_limit=2,
        subscription_min_days_to_expiry=1,
        subscription_lifecycle_status="subscribed",
    )["underlyings"]

    assert [row["underlying_symbol"] for row in rows] == [
        "CZCE.FG607",
        "CZCE.FG608",
    ]
    assert {row["status"] for row in rows} == {"已订阅"}
    assert all(row["current_quote_count"] == 0 for row in rows)


def test_tquote_marks_historical_cache_as_unsubscribed_until_realtime_refresh() -> None:
    connection = sqlite3.connect(":memory:")
    read_model = WebuiReadModel(connection)
    cutoff = "2026-05-12T01:00:00+00:00"
    _insert_simple_chain(
        connection,
        underlying_symbol="SHFE.cu2606",
        exchange_id="SHFE",
        product_id="cu",
        delivery_year=2026,
        delivery_month=6,
        received_at="2026-05-09T01:00:00+00:00",
    )

    stale_quote = read_model.tquote(
        underlying_symbol="SHFE.cu2606",
        include_selectors=False,
        realtime_started_at=cutoff,
    )

    assert stale_quote["underlying"]["realtime_status"] == "未订阅"
    assert stale_quote["underlying"]["realtime_subscribed"] is False
    assert stale_quote["underlying"]["realtime_latest_quote_received_at"] is None

    connection.execute(
        """
        UPDATE quote_current
        SET received_at = '2026-05-12T01:05:00+00:00',
            source_datetime = '2026-05-12 09:05:00.000000'
        WHERE symbol = 'SHFE.cu2606C100'
        """
    )
    fresh_quote = read_model.tquote(
        underlying_symbol="SHFE.cu2606",
        include_selectors=False,
        realtime_started_at=cutoff,
    )

    assert fresh_quote["underlying"]["realtime_status"] == "正常"
    assert fresh_quote["underlying"]["realtime_subscribed"] is True
    assert (
        fresh_quote["underlying"]["realtime_latest_quote_received_at"]
        == "2026-05-12T01:05:00+00:00"
    )


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
            bid_price1,
            raw_payload_json
        )
        VALUES (?, ?, ?, ?, '{}')
        """,
        [
            (
                "SHFE.cu2606",
                "2026-05-08 22:59:59.000000",
                "2026-05-08T16:25:24+00:00",
                70100,
            ),
            (
                "SHFE.cu2606C70000",
                "2026-05-08 22:59:58.000000",
                "2026-05-08T16:25:24+00:00",
                100,
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


def test_empty_quote_source_time_is_not_displayed_as_market_time() -> None:
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
        VALUES (?, 'CZCE', 'AP', ?, NULL, ?, ?, ?, ?, NULL, NULL, NULL, 0, 1, NULL, '2026-05-08T00:00:00+00:00', '{}')
        """,
        [
            ("CZCE.AP610", "AP610", "FUTURE", None, None, None),
            ("CZCE.AP610C6500", "AP610C6500", "OPTION", "CZCE.AP610", "CALL", 6500),
        ],
    )
    connection.executemany(
        """
        INSERT INTO quote_current (
            symbol,
            source_datetime,
            received_at,
            volume,
            raw_payload_json
        )
        VALUES (?, ?, ?, 0, '{}')
        """,
        [
            ("CZCE.AP610", "2026-05-08 19:05:00.000000", "2026-05-09T06:38:36+00:00"),
            (
                "CZCE.AP610C6500",
                "2026-05-08 19:05:00.000000",
                "2026-05-09T06:38:02+00:00",
            ),
        ],
    )

    overview = read_model.overview()
    row = overview["underlyings"][0]

    assert overview["summary"]["option_quote_rows"] == 0
    assert row["quote_coverage"] == 0
    assert row["display_market_time"] is None


def test_expiry_days_are_exposed_for_overview_and_tquote() -> None:
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
            last_exercise_datetime,
            price_tick,
            volume_multiple,
            expired,
            active,
            inactive_reason,
            last_seen_at,
            raw_payload_json
        )
        VALUES (?, 'SHFE', 'cu', ?, NULL, ?, ?, ?, ?, ?, ?, NULL, NULL, 0, 1, NULL, '2026-05-08T00:00:00+00:00', '{}')
        """,
        [
            ("SHFE.cu2606", "cu2606", "FUTURE", None, None, None, "2026-06-15", None),
            (
                "SHFE.cu2606C70000",
                "cu2606C70000",
                "OPTION",
                "SHFE.cu2606",
                "CALL",
                70000,
                "2026-06-10",
                "2026-06-09",
            ),
            (
                "SHFE.cu2606P70000",
                "cu2606P70000",
                "OPTION",
                "SHFE.cu2606",
                "PUT",
                70000,
                "2026-06-10",
                "2026-06-09",
            ),
        ],
    )

    row = read_model.overview()["underlyings"][0]
    quote = read_model.tquote(underlying_symbol="SHFE.cu2606")

    assert row["future_expire_datetime"] == "2026-06-15"
    assert row["option_expire_datetime"] == "2026-06-10"
    assert row["option_last_exercise_datetime"] == "2026-06-09"
    assert row["days_to_option_expire_datetime"] == (
        date(2026, 6, 10) - date.today()
    ).days
    assert row["days_to_option_last_exercise_datetime"] == (
        date(2026, 6, 9) - date.today()
    ).days
    assert quote["underlying"]["future_expire_datetime"] == "2026-06-15"
    assert quote["underlying"]["option_expire_datetime"] == "2026-06-10"
    assert quote["strikes"][0]["CALL"]["expire_datetime"] == "2026-06-10"
    assert quote["strikes"][0]["CALL"]["last_exercise_datetime"] == "2026-06-09"
    assert quote["strikes"][0]["CALL"]["days_to_expire_datetime"] == (
        date(2026, 6, 10) - date.today()
    ).days

    assert row["expire_datetime"] == "2026-06-10"
    assert row["days_to_expiry"] == row["days_to_option_expire_datetime"]
    assert quote["underlying"]["expire_datetime"] == "2026-06-10"
    assert quote["underlying"]["days_to_expiry"] == row["days_to_expiry"]
    assert quote["selectors"][0]["days_to_expiry"] == row["days_to_expiry"]


def test_expiry_days_fall_back_to_quote_payload() -> None:
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
            last_exercise_datetime,
            price_tick,
            volume_multiple,
            expired,
            active,
            inactive_reason,
            last_seen_at,
            raw_payload_json
        )
        VALUES (?, 'SHFE', 'zn', ?, NULL, ?, ?, ?, ?, NULL, NULL, NULL, NULL, 0, 1, NULL, '2026-05-08T00:00:00+00:00', '{}')
        """,
        [
            ("SHFE.zn2606", "zn2606", "FUTURE", None, None, None),
            (
                "SHFE.zn2606C24000",
                "zn2606C24000",
                "OPTION",
                "SHFE.zn2606",
                "CALL",
                24000,
            ),
            (
                "SHFE.zn2606P24000",
                "zn2606P24000",
                "OPTION",
                "SHFE.zn2606",
                "PUT",
                24000,
            ),
        ],
    )
    connection.executemany(
        """
        INSERT INTO quote_current (
            symbol,
            source_datetime,
            received_at,
            last_price,
            raw_payload_json
        )
        VALUES (?, '2026-05-09 00:59:59.000001', '2026-05-09T12:36:13+00:00', ?, ?)
        """,
        [
            (
                "SHFE.zn2606",
                24030,
                '{"expire_datetime":1781506800.0,"expire_rest_days":37}',
            ),
            (
                "SHFE.zn2606C24000",
                62,
                '{"expire_datetime":1779692400.0,"last_exercise_datetime":1779692400.0,"expire_rest_days":16}',
            ),
            (
                "SHFE.zn2606P24000",
                235,
                '{"expire_datetime":1779692400.0,"last_exercise_datetime":1779692400.0,"expire_rest_days":16}',
            ),
        ],
    )

    overview_row = read_model.overview()["underlyings"][0]
    quote = read_model.tquote(
        underlying_symbol="SHFE.zn2606",
        include_selectors=False,
    )

    assert overview_row["expire_datetime"] == "2026-05-25"
    assert overview_row["days_to_expiry"] == (date(2026, 5, 25) - date.today()).days
    assert quote["selectors"] == []
    assert quote["underlying"]["expire_datetime"] == "2026-05-25"
    assert quote["underlying"]["days_to_expiry"] == overview_row["days_to_expiry"]
    assert quote["strikes"][0]["CALL"]["expire_datetime"] == "2026-05-25"
    assert quote["strikes"][0]["CALL"]["days_to_expire_datetime"] == overview_row[
        "days_to_expiry"
    ]


def test_runs_include_service_logs() -> None:
    connection = sqlite3.connect(":memory:")
    read_model = WebuiReadModel(connection)
    connection.execute(
        """
        INSERT INTO service_logs (created_at, level, category, message, context_json)
        VALUES ('2026-05-09T00:00:00+00:00', 'info', 'collection', 'window started', '{}')
        """
    )

    logs = read_model.runs()["service_logs"]

    assert logs[0]["category"] == "collection"
    assert logs[0]["message"] == "window started"


def _insert_simple_chain(
    connection: sqlite3.Connection,
    *,
    underlying_symbol: str,
    exchange_id: str,
    product_id: str,
    delivery_year: int,
    delivery_month: int,
    received_at: str,
) -> None:
    instrument_id = underlying_symbol.split(".", maxsplit=1)[-1]
    call_symbol = f"{underlying_symbol}C100"
    put_symbol = f"{underlying_symbol}P100"
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
            delivery_year,
            delivery_month,
            price_tick,
            volume_multiple,
            expired,
            active,
            inactive_reason,
            last_seen_at,
            raw_payload_json
        )
        VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, NULL, NULL, 0, 1, NULL, '2026-05-12T00:00:00+00:00', '{}')
        """,
        [
            (
                underlying_symbol,
                exchange_id,
                product_id,
                instrument_id,
                "FUTURE",
                None,
                None,
                None,
                delivery_year,
                delivery_month,
            ),
            (
                call_symbol,
                exchange_id,
                product_id,
                f"{instrument_id}C100",
                "OPTION",
                underlying_symbol,
                "CALL",
                100,
                None,
                None,
            ),
            (
                put_symbol,
                exchange_id,
                product_id,
                f"{instrument_id}P100",
                "OPTION",
                underlying_symbol,
                "PUT",
                100,
                None,
                None,
            ),
        ],
    )
    connection.executemany(
        """
        INSERT INTO quote_current (
            symbol,
            source_datetime,
            received_at,
            last_price,
            raw_payload_json
        )
        VALUES (?, '2026-05-12 09:00:00.000000', ?, 1, '{}')
        """,
        [
            (underlying_symbol, received_at),
            (call_symbol, received_at),
            (put_symbol, received_at),
        ],
    )
