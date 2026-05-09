import sqlite3

from option_data_manager.option_metrics import (
    OptionMetricsRepository,
    normalize_option_metrics,
)
from option_data_manager.quotes import QuoteRepository, normalize_quote


def test_empty_quote_update_preserves_last_valid_market_fields() -> None:
    connection = sqlite3.connect(":memory:")
    repository = QuoteRepository(connection)
    repository.upsert_quote(
        normalize_quote(
            "SHFE.cu2606C80000",
            {
                "datetime": "2026-05-08 23:00:00.000000",
                "last_price": 100.0,
                "bid_price1": 99.0,
                "ask_price1": 101.0,
                "volume": 12,
            },
            received_at="2026-05-08T15:00:00+00:00",
        )
    )

    repository.upsert_quote(
        normalize_quote(
            "SHFE.cu2606C80000",
            {
                "datetime": None,
                "last_price": None,
                "bid_price1": None,
                "ask_price1": None,
                "volume": None,
            },
            received_at="2026-05-08T16:00:00+00:00",
        )
    )

    quote = repository.get_quote("SHFE.cu2606C80000")
    assert quote is not None
    assert quote.source_datetime == "2026-05-08 23:00:00.000000"
    assert quote.last_price == 100.0
    assert quote.bid_price1 == 99.0
    assert quote.ask_price1 == 101.0
    assert quote.volume == 12
    assert quote.received_at == "2026-05-08T16:00:00+00:00"


def test_empty_metrics_update_preserves_last_valid_greeks_and_iv() -> None:
    connection = sqlite3.connect(":memory:")
    repository = OptionMetricsRepository(connection)
    repository.upsert_metrics(
        normalize_option_metrics(
            "SHFE.cu2606C80000",
            received_at="2026-05-08T15:00:00+00:00",
            greeks_payload={"delta": 0.5, "gamma": 0.02, "theta": -1.0, "vega": 3.0},
            iv_payload={"impv": [0.21]},
        )
    )

    repository.upsert_metrics(
        normalize_option_metrics(
            "SHFE.cu2606C80000",
            received_at="2026-05-08T16:00:00+00:00",
            greeks_payload={},
            iv_payload={},
        )
    )

    metrics = repository.get_metrics("SHFE.cu2606C80000")
    assert metrics is not None
    assert metrics.delta == 0.5
    assert metrics.gamma == 0.02
    assert metrics.theta == -1.0
    assert metrics.vega == 3.0
    assert metrics.iv == 0.21
    assert metrics.received_at == "2026-05-08T15:00:00+00:00"
