import option_data_manager.chain_collector as chain_collector
from option_data_manager.chain_collector import _quote_refs, _wait_updates
from option_data_manager.instruments import InstrumentRecord


def test_quote_refs_prefers_batch_quote_subscription() -> None:
    calls: list[object] = []

    class FakeApi:
        def get_quote_list(self, symbols: list[str]) -> list[str]:
            calls.append(("list", tuple(symbols)))
            return [f"quote:{symbol}" for symbol in symbols]

        def get_quote(self, symbol: str) -> str:
            calls.append(("single", symbol))
            return f"single:{symbol}"

    refs = _quote_refs(
        FakeApi(),
        "CZCE.AP610",
        [
            _instrument("CZCE.AP610C8000"),
            _instrument("CZCE.AP610P8000"),
        ],
    )

    assert refs == {
        "CZCE.AP610": "quote:CZCE.AP610",
        "CZCE.AP610C8000": "quote:CZCE.AP610C8000",
        "CZCE.AP610P8000": "quote:CZCE.AP610P8000",
    }
    assert calls == [("list", ("CZCE.AP610", "CZCE.AP610C8000", "CZCE.AP610P8000"))]


def test_quote_refs_falls_back_to_single_quote_subscription() -> None:
    calls: list[str] = []

    class FakeApi:
        def get_quote(self, symbol: str) -> str:
            calls.append(symbol)
            return f"single:{symbol}"

    refs = _quote_refs(FakeApi(), "CZCE.AP610", [_instrument("CZCE.AP610C8000")])

    assert refs == {
        "CZCE.AP610": "single:CZCE.AP610",
        "CZCE.AP610C8000": "single:CZCE.AP610C8000",
    }
    assert calls == ["CZCE.AP610", "CZCE.AP610C8000"]


def test_wait_updates_uses_absolute_deadline(monkeypatch) -> None:
    deadlines: list[float] = []

    class FakeApi:
        def wait_update(self, *, deadline: float) -> bool:
            deadlines.append(deadline)
            return True

    monkeypatch.setattr(chain_collector.time, "time", lambda: 1000.0)

    _wait_updates(FakeApi(), wait_cycles=2)

    assert deadlines == [1001.0, 1001.0]


def _instrument(symbol: str) -> InstrumentRecord:
    return InstrumentRecord(
        symbol=symbol,
        exchange_id="CZCE",
        product_id="AP",
        instrument_id=symbol.split(".", maxsplit=1)[1],
        instrument_name=None,
        ins_class="OPTION",
        underlying_symbol="CZCE.AP610",
        option_class="CALL",
        strike_price=8000,
        expire_datetime=None,
        price_tick=None,
        volume_multiple=None,
        expired=False,
        active=True,
        inactive_reason=None,
        last_seen_at="2026-05-09T00:00:00+00:00",
        raw_payload_json="{}",
    )
