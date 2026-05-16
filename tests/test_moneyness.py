from option_data_manager.moneyness import (
    MONEYNESS_ATM,
    MONEYNESS_ITM,
    MONEYNESS_OTM,
    build_moneyness_classification,
    classify_option_moneyness,
    normalize_moneyness_filter,
    underlying_reference_price,
)


def test_classify_option_moneyness_for_call_and_put_sides() -> None:
    rows = [
        _row("DCE.a2601C90", "CALL", 90),
        _row("DCE.a2601C100", "CALL", 100),
        _row("DCE.a2601C110", "CALL", 110),
        _row("DCE.a2601P90", "PUT", 90),
        _row("DCE.a2601P100", "PUT", 100),
        _row("DCE.a2601P110", "PUT", 110),
    ]

    result = classify_option_moneyness(
        rows,
        underlying_prices={"DCE.a2601": 100},
    )

    assert result["DCE.a2601C90"] == MONEYNESS_ITM
    assert result["DCE.a2601C100"] == MONEYNESS_ATM
    assert result["DCE.a2601C110"] == MONEYNESS_OTM
    assert result["DCE.a2601P90"] == MONEYNESS_OTM
    assert result["DCE.a2601P100"] == MONEYNESS_ATM
    assert result["DCE.a2601P110"] == MONEYNESS_ITM


def test_atm_tie_uses_one_upper_strike_for_both_sides() -> None:
    rows = [
        _row("DCE.a2601C100", "CALL", 100),
        _row("DCE.a2601C110", "CALL", 110),
        _row("DCE.a2601P100", "PUT", 100),
        _row("DCE.a2601P110", "PUT", 110),
    ]

    result = classify_option_moneyness(
        rows,
        underlying_prices={"DCE.a2601": 105},
    )

    assert result["DCE.a2601C110"] == MONEYNESS_ATM
    assert result["DCE.a2601P110"] == MONEYNESS_ATM
    assert result["DCE.a2601C100"] == MONEYNESS_ITM
    assert result["DCE.a2601P100"] == MONEYNESS_OTM


def test_atm_strike_uses_nearest_strike_with_upper_tie() -> None:
    rows = [
        _row("DCE.a2601C100", "CALL", 100),
        _row("DCE.a2601C110", "CALL", 110),
        _row("DCE.a2601P100", "PUT", 100),
        _row("DCE.a2601P110", "PUT", 110),
    ]

    result = classify_option_moneyness(
        rows,
        underlying_prices={"DCE.a2601": 103},
    )

    assert result["DCE.a2601C100"] == MONEYNESS_ATM
    assert result["DCE.a2601P100"] == MONEYNESS_ATM
    assert result["DCE.a2601C110"] == MONEYNESS_OTM
    assert result["DCE.a2601P110"] == MONEYNESS_ITM


def test_build_moneyness_classification_returns_display_context() -> None:
    rows = [
        _row("DCE.a2601C100", "CALL", 100),
        _row("DCE.a2601C110", "CALL", 110),
        _row("DCE.a2601P100", "PUT", 100),
        _row("DCE.a2601P110", "PUT", 110),
    ]

    result = build_moneyness_classification(
        rows,
        underlying_prices={"DCE.a2601": 105},
    )

    assert result.primary_atm_strike == 110
    assert result.classifications["DCE.a2601C110"] == MONEYNESS_ATM
    assert result.symbol_atm_strikes["DCE.a2601P100"] == 110


def test_atm_strike_is_scoped_to_option_family() -> None:
    rows = [
        _row("DCE.a2601-C-100", "CALL", 100),
        _row("DCE.a2601-C-110", "CALL", 110),
        _row("DCE.a2601-P-100", "PUT", 100),
        _row("DCE.a2601-P-110", "PUT", 110),
        _row("DCE.a2601-MS-C-90", "CALL", 90),
        _row("DCE.a2601-MS-C-100", "CALL", 100),
        _row("DCE.a2601-MS-P-90", "PUT", 90),
        _row("DCE.a2601-MS-P-100", "PUT", 100),
    ]

    result = classify_option_moneyness(
        rows,
        underlying_prices={"DCE.a2601": 95},
    )

    assert result["DCE.a2601-C-100"] == MONEYNESS_ATM
    assert result["DCE.a2601-P-100"] == MONEYNESS_ATM
    assert result["DCE.a2601-MS-C-100"] == MONEYNESS_ATM
    assert result["DCE.a2601-MS-P-100"] == MONEYNESS_ATM


def test_normalize_moneyness_filter_defaults_to_all_for_empty_values() -> None:
    assert normalize_moneyness_filter(None) == {"itm", "atm", "otm"}
    assert normalize_moneyness_filter("") == {"itm", "atm", "otm"}
    assert normalize_moneyness_filter("otm,atm") == {"otm", "atm"}


def test_underlying_reference_price_falls_back_to_quote_fields() -> None:
    assert underlying_reference_price({"last_price": 101, "pre_settlement": 99}) == 101
    assert (
        underlying_reference_price(
            {
                "last_price": None,
                "bid_price1": 99,
                "ask_price1": 101,
                "pre_settlement": 98,
            }
        )
        == 100
    )
    assert (
        underlying_reference_price(
            {
                "last_price": None,
                "bid_price1": None,
                "ask_price1": None,
                "raw_payload_json": '{"pre_settlement":7650,"pre_close":7659}',
            }
        )
        == 7650
    )


def _row(symbol: str, option_class: str, strike: float) -> dict:
    return {
        "symbol": symbol,
        "underlying_symbol": "DCE.a2601",
        "option_class": option_class,
        "strike_price": strike,
    }
