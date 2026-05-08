from option_data_manager.market_discovery import (
    derive_underlying_symbol,
    group_option_symbols_by_underlying,
)


def test_groups_only_commodity_option_exchanges() -> None:
    grouped = group_option_symbols_by_underlying(
        [
            "SHFE.cu2606C70000",
            "SHFE.cu2606P70000",
            "DCE.m2605-C-3000",
            "CFFEX.IO2606-C-3000",
        ]
    )

    assert set(grouped) == {"DCE.m2605", "SHFE.cu2606"}
    assert grouped["SHFE.cu2606"]["CALL"] == ["SHFE.cu2606C70000"]
    assert grouped["SHFE.cu2606"]["PUT"] == ["SHFE.cu2606P70000"]


def test_derives_underlying_from_supported_symbol_shapes() -> None:
    assert derive_underlying_symbol("SHFE.cu2606C70000") == "SHFE.cu2606"
    assert derive_underlying_symbol("DCE.m2605-C-3000") == "DCE.m2605"

