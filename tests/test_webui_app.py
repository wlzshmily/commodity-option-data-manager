import sqlite3

from fastapi.testclient import TestClient

from option_data_manager.webui.app import (
    INDEX_HTML,
    WEBUI_JS,
    create_webui_app,
    _quote_stream_lifecycle_status,
)
from option_data_manager.webui.read_model import SubscriptionLifecycleStatus


def test_quote_stream_lifecycle_allows_runtime_running_state() -> None:
    status = _quote_stream_lifecycle_status(
        {
            "status": "running",
            "running": True,
            "progress": {
                "quote_total": 2,
                "quote_subscribed": 2,
                "kline_total": 2,
                "kline_subscribed": 2,
            },
        }
    )

    assert status == SubscriptionLifecycleStatus.SUBSCRIBED


def test_tquote_metric_coverage_uses_subscription_scope_denominator() -> None:
    assert "const subscribedOptions = data.strikes.reduce" in WEBUI_JS
    assert "isSubscribedQuoteOption(row.CALL)" in WEBUI_JS
    assert "withIv / subscribedOptions" in WEBUI_JS
    assert "withGreeks / subscribedOptions" in WEBUI_JS
    assert "option?.iv !== null && option?.iv !== undefined" in WEBUI_JS


def test_tquote_coloring_uses_backend_moneyness_classification() -> None:
    assert "function optionMoneynessClass(option)" in WEBUI_JS
    assert "option?.moneyness" in WEBUI_JS
    assert "row.is_atm ? \" atm\" : \"\"" in WEBUI_JS
    assert "function moneynessClass" not in WEBUI_JS


def test_overview_call_put_counts_use_subscription_kline_scope_when_running() -> None:
    assert "function overviewSideCount(row, side)" in WEBUI_JS
    assert "subscription_kline_call_total" in WEBUI_JS
    assert "subscription_kline_put_total" in WEBUI_JS
    assert "overviewSideCount(row, \"CALL\")" in WEBUI_JS
    assert "overviewSideCount(row, \"PUT\")" in WEBUI_JS


def test_webui_product_label_prefers_tqsdk_display_name_and_ad_fallback_is_alloy() -> None:
    assert 'ad: "铝合金"' in WEBUI_JS
    assert "function productLabelForRow(row)" in WEBUI_JS
    assert "row?.product_display_name" in WEBUI_JS


def test_api_page_is_api_status_and_access_page_not_placeholder() -> None:
    assert "接入信息" in INDEX_HTML
    assert "常用端点" in INDEX_HTML
    assert "运行摘要" in INDEX_HTML
    assert "manage-api-keys" in INDEX_HTML
    assert "当前 DEV-024 只交付展示切片" not in INDEX_HTML
    assert "async function renderApiPage()" in WEBUI_JS
    assert 'fetchJson("/api/webui/api-summary")' in WEBUI_JS


def test_api_page_summary_endpoint_is_lightweight() -> None:
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    client = TestClient(create_webui_app(connection, database_path=":memory:"))

    response = client.get("/api/webui/api-summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["api"]["request_count"] >= 0
    assert payload["summary"]["active_options"] == 0
