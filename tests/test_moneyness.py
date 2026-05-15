from option_data_manager.moneyness import (
    MONEYNESS_ATM,
    MONEYNESS_ITM,
    MONEYNESS_OTM,
    classify_option_moneyness,
    normalize_moneyness_filter,
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


def test_atm_tie_prefers_out_of_the_money_side() -> None:
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
    assert result["DCE.a2601P100"] == MONEYNESS_ATM
    assert result["DCE.a2601C100"] == MONEYNESS_ITM
    assert result["DCE.a2601P110"] == MONEYNESS_ITM


def test_normalize_moneyness_filter_defaults_to_all_for_empty_values() -> None:
    assert normalize_moneyness_filter(None) == {"itm", "atm", "otm"}
    assert normalize_moneyness_filter("") == {"itm", "atm", "otm"}
    assert normalize_moneyness_filter("otm,atm") == {"otm", "atm"}


def _row(symbol: str, option_class: str, strike: float) -> dict:
    return {
        "symbol": symbol,
        "underlying_symbol": "DCE.a2601",
        "option_class": option_class,
        "strike_price": strike,
    }
