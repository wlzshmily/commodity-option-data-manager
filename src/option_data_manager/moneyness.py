"""Option moneyness classification helpers."""

from __future__ import annotations

from collections import defaultdict
from typing import Any


MONEYNESS_ITM = "itm"
MONEYNESS_ATM = "atm"
MONEYNESS_OTM = "otm"
MONEYNESS_ALL = frozenset({MONEYNESS_ITM, MONEYNESS_ATM, MONEYNESS_OTM})


def normalize_moneyness_filter(value: str | set[str] | list[str] | tuple[str, ...] | None) -> set[str]:
    """Return a validated moneyness filter; empty/invalid values preserve all."""

    if value is None:
        return set(MONEYNESS_ALL)
    if isinstance(value, str):
        raw_items = [item.strip().lower() for item in value.split(",")]
    else:
        raw_items = [str(item).strip().lower() for item in value]
    parsed = {item for item in raw_items if item in MONEYNESS_ALL}
    return parsed or set(MONEYNESS_ALL)


def is_all_moneyness(selected: set[str]) -> bool:
    return set(selected) == set(MONEYNESS_ALL)


def classify_option_moneyness(
    rows: list[dict[str, Any]],
    *,
    underlying_prices: dict[str, float],
) -> dict[str, str]:
    """Classify option rows as itm/atm/otm using their own underlying price."""

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        symbol = str(row.get("symbol") or "").strip()
        underlying = str(row.get("underlying_symbol") or "").strip()
        option_class = str(row.get("option_class") or "").strip().upper()
        strike = _float_or_none(row.get("strike_price"))
        if not symbol or not underlying or option_class not in {"CALL", "PUT"}:
            continue
        if strike is None or underlying not in underlying_prices:
            continue
        grouped[(underlying, option_class)].append(row)

    result: dict[str, str] = {}
    for (underlying, option_class), side_rows in grouped.items():
        price = underlying_prices[underlying]
        atm_symbol = _atm_symbol(side_rows, price=price, option_class=option_class)
        for row in side_rows:
            symbol = str(row["symbol"])
            strike = float(row["strike_price"])
            if symbol == atm_symbol:
                result[symbol] = MONEYNESS_ATM
            elif _is_itm(str(row["option_class"]), strike=strike, price=price):
                result[symbol] = MONEYNESS_ITM
            else:
                result[symbol] = MONEYNESS_OTM
    return result


def _atm_symbol(rows: list[dict[str, Any]], *, price: float, option_class: str) -> str | None:
    if not rows:
        return None
    ranked = sorted(
        rows,
        key=lambda row: (
            abs(float(row["strike_price"]) - price),
            0
            if _is_otm(option_class, strike=float(row["strike_price"]), price=price)
            else 1,
            float(row["strike_price"]),
            str(row.get("symbol") or ""),
        ),
    )
    return str(ranked[0].get("symbol") or "") or None


def _is_itm(option_class: str, *, strike: float, price: float) -> bool:
    normalized = option_class.strip().upper()
    if normalized == "CALL":
        return strike < price
    if normalized == "PUT":
        return strike > price
    return False


def _is_otm(option_class: str, *, strike: float, price: float) -> bool:
    normalized = option_class.strip().upper()
    if normalized == "CALL":
        return strike > price
    if normalized == "PUT":
        return strike < price
    return False


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
