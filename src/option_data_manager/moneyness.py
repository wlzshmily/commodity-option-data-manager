"""Option moneyness classification helpers."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import json
import math
import re
from collections.abc import Mapping
from typing import Any


MONEYNESS_ITM = "itm"
MONEYNESS_ATM = "atm"
MONEYNESS_OTM = "otm"
MONEYNESS_ALL = frozenset({MONEYNESS_ITM, MONEYNESS_ATM, MONEYNESS_OTM})


@dataclass(frozen=True)
class MoneynessClassification:
    """Shared option-chain moneyness result for subscription and display code."""

    classifications: dict[str, str]
    atm_strikes: dict[tuple[str, str], float]
    symbol_atm_strikes: dict[str, float]

    @property
    def primary_atm_strike(self) -> float | None:
        if not self.atm_strikes:
            return None
        return self.atm_strikes[sorted(self.atm_strikes)[0]]


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

    return build_moneyness_classification(
        rows,
        underlying_prices=underlying_prices,
    ).classifications


def build_moneyness_classification(
    rows: list[dict[str, Any]],
    *,
    underlying_prices: dict[str, float],
) -> MoneynessClassification:
    """Build one moneyness snapshot shared by Kline filtering and T quote display."""

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
        grouped[(underlying, _option_family_key(row))].append(row)

    result: dict[str, str] = {}
    atm_strikes: dict[tuple[str, str], float] = {}
    symbol_atm_strikes: dict[str, float] = {}
    for (underlying, _family), underlying_rows in grouped.items():
        price = underlying_prices[underlying]
        atm_strike = _atm_strike(underlying_rows, price=price)
        if atm_strike is None:
            continue
        atm_strikes[(underlying, _family)] = atm_strike
        for row in underlying_rows:
            symbol = str(row["symbol"])
            strike = float(row["strike_price"])
            symbol_atm_strikes[symbol] = atm_strike
            if strike == atm_strike:
                result[symbol] = MONEYNESS_ATM
            elif _is_itm(str(row["option_class"]), strike=strike, price=price):
                result[symbol] = MONEYNESS_ITM
            else:
                result[symbol] = MONEYNESS_OTM
    return MoneynessClassification(
        classifications=result,
        atm_strikes=atm_strikes,
        symbol_atm_strikes=symbol_atm_strikes,
    )


def underlying_reference_price(fields: Mapping[str, Any]) -> float | None:
    """Return the best available underlying price for moneyness classification."""

    payload = _raw_payload(fields.get("raw_payload_json"))
    last_price = _positive_float_or_none(_field_value(fields, payload, "last_price"))
    if last_price is not None:
        return last_price

    bid_price = _positive_float_or_none(_field_value(fields, payload, "bid_price1"))
    ask_price = _positive_float_or_none(_field_value(fields, payload, "ask_price1"))
    if bid_price is not None and ask_price is not None:
        return (bid_price + ask_price) / 2

    for key in (
        "close_price",
        "close",
        "settlement",
        "pre_settlement",
        "pre_close",
        "average_price",
        "average",
    ):
        price = _positive_float_or_none(_field_value(fields, payload, key))
        if price is not None:
            return price
    return None


def _atm_strike(rows: list[dict[str, Any]], *, price: float) -> float | None:
    if not rows:
        return None
    strikes = sorted({float(row["strike_price"]) for row in rows})
    return min(strikes, key=lambda strike: (abs(strike - price), -strike))


def _option_family_key(row: dict[str, Any]) -> str:
    symbol = str(row.get("symbol") or "").strip()
    underlying = str(row.get("underlying_symbol") or "").strip()
    option_class = str(row.get("option_class") or "").strip().upper()
    marker = "C" if option_class == "CALL" else "P" if option_class == "PUT" else ""
    if not symbol or not marker:
        return underlying
    delimited_marker = f"-{marker}-"
    if delimited_marker in symbol:
        return symbol.split(delimited_marker, maxsplit=1)[0]
    match = re.match(rf"^(.*){marker}\d", symbol)
    if match:
        return match.group(1)
    return underlying


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
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _positive_float_or_none(value: Any) -> float | None:
    number = _float_or_none(value)
    if number is None or number <= 0:
        return None
    return number


def _raw_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        payload = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _field_value(
    fields: Mapping[str, Any],
    payload: Mapping[str, Any],
    key: str,
) -> Any:
    value = fields.get(key)
    if value is not None:
        return value
    return payload.get(key)
