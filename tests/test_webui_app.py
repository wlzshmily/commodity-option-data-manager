from option_data_manager.webui.app import WEBUI_JS, _quote_stream_lifecycle_status
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


def test_webui_product_label_prefers_tqsdk_display_name_and_ad_fallback_is_alloy() -> None:
    assert 'ad: "铝合金"' in WEBUI_JS
    assert "function productLabelForRow(row)" in WEBUI_JS
    assert "row?.product_display_name" in WEBUI_JS
