from option_data_manager.webui.app import _quote_stream_lifecycle_status
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
