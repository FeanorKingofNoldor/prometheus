"""Tests for the FOP broker + selection plumbing.

Covers the parts of Phase 1 that don't need a live IBKR connection:
  * instrument_id format round-trip
  * spec-map completeness for the 6 sleeve commodities
  * _build_contracts producing FuturesOption with correct fields
  * OptionsStrategyManager._submit_directives picking the .FOP suffix
    for commodity symbols
"""

from __future__ import annotations

import re

from prometheus.derivatives.selection import _build_contracts
from prometheus.execution.futures_option_specs import (
    FOP_SPECS,
    FuturesOptionSpec,
    get_fop_spec,
    is_commodity_fop_symbol,
)
from prometheus.execution.instrument_mapper import InstrumentMapper

# Match the _FOP_RE in run_derivatives_daily.py — keep these in sync.
_FOP_RE = re.compile(r'^([A-Z0-9]+)_(\d{6}|\d{8})_([\d.]+)([CP])\.FOP$')


# ── spec map ───────────────────────────────────────────────────────────


def test_fop_specs_cover_all_sleeve_commodities():
    expected = {"CL", "BZ", "NG", "ZW", "GC", "HG"}
    assert expected.issubset(set(FOP_SPECS.keys()))


def test_get_fop_spec_known_symbol():
    spec = get_fop_spec("CL")
    assert isinstance(spec, FuturesOptionSpec)
    assert spec.exchange == "NYMEX"
    assert spec.trading_class == "LO"
    assert spec.multiplier == "1000"
    assert spec.currency == "USD"


def test_get_fop_spec_case_insensitive():
    assert get_fop_spec("cl") is get_fop_spec("CL")


def test_get_fop_spec_unknown_returns_none():
    assert get_fop_spec("XXXX") is None


def test_is_commodity_fop_symbol_known():
    for sym in ("CL", "BZ", "NG", "ZW", "GC", "HG"):
        assert is_commodity_fop_symbol(sym)
    assert is_commodity_fop_symbol("cl")  # case-insensitive


def test_is_commodity_fop_symbol_unknown():
    assert not is_commodity_fop_symbol("SPY")
    assert not is_commodity_fop_symbol("VIX")
    assert not is_commodity_fop_symbol("")


def test_fop_spec_zw_multiplier_matches_futures_manager():
    """Cross-check: ZW FOP multiplier (5000) matches the futures-side
    multiplier we corrected in futures_manager.py after the 2026-06-06
    probe found the original 50 was wrong.
    """
    from prometheus.execution.futures_manager import PRODUCTS
    zw_fut = PRODUCTS["ZW"]
    zw_fop = get_fop_spec("ZW")
    assert zw_fut.multiplier == 5000.0
    assert zw_fop.multiplier == "5000"


# ── instrument_id round-trip ───────────────────────────────────────────


def test_futures_option_instrument_id_format():
    iid = InstrumentMapper.futures_option_instrument_id("CL", "20260622", 75.0, "C")
    assert iid == "CL_260622_75C.FOP"


def test_futures_option_instrument_id_decimal_strike():
    iid = InstrumentMapper.futures_option_instrument_id("ZW", "20260626", 410.5, "P")
    assert iid == "ZW_260626_410.5P.FOP"


def test_futures_option_instrument_id_short_expiry():
    iid = InstrumentMapper.futures_option_instrument_id("GC", "260622", 3500.0, "C")
    assert iid == "GC_260622_3500C.FOP"


def test_fop_regex_parses_iid():
    iid = InstrumentMapper.futures_option_instrument_id("BZ", "20260825", 70.0, "P")
    m = _FOP_RE.match(iid)
    assert m is not None
    assert m.group(1) == "BZ"
    assert m.group(2) == "260825"
    assert float(m.group(3)) == 70.0
    assert m.group(4) == "P"


def test_fop_regex_rejects_equity_format():
    assert _FOP_RE.match("SPY_260418_560P.US") is None


def test_us_regex_rejects_fop_format():
    us_re = re.compile(r'^([A-Z0-9]+)_(\d{6}|\d{8})_([\d.]+)([CP])\.US$')
    assert us_re.match("CL_260622_75C.FOP") is None


# ── _build_contracts FOP path ──────────────────────────────────────────


def test_build_contracts_fop_returns_futures_option_with_spec_fields():
    from prometheus.execution.ib_compat import FuturesOption
    contracts = _build_contracts(
        underlying="CL",
        expiry="20260622",
        strikes=[70.0, 75.0, 80.0],
        right="C",
        exchange="NYMEX",
        trading_class=None,   # ignored on FOP path; spec wins
        sec_type="FOP",
    )
    assert len(contracts) == 3
    for c in contracts:
        assert isinstance(c, FuturesOption)
        assert c.symbol == "CL"
        assert c.lastTradeDateOrContractMonth == "20260622"
        assert c.right == "C"
        assert c.exchange == "NYMEX"
        assert c.multiplier == "1000"
        assert c.tradingClass == "LO"
        assert c.currency == "USD"
    assert [c.strike for c in contracts] == [70.0, 75.0, 80.0]


def test_build_contracts_fop_uses_spec_exchange_even_if_arg_differs():
    contracts = _build_contracts(
        underlying="GC",
        expiry="20260825",
        strikes=[3500.0],
        right="P",
        exchange="WRONG",
        trading_class="WRONG",
        sec_type="FOP",
    )
    assert len(contracts) == 1
    assert contracts[0].exchange == "COMEX"
    assert contracts[0].tradingClass == "OG"
    assert contracts[0].multiplier == "100"


def test_build_contracts_fop_unknown_symbol_returns_empty():
    contracts = _build_contracts(
        underlying="UNKNOWN",
        expiry="20260622",
        strikes=[100.0],
        right="C",
        exchange="X",
        trading_class=None,
        sec_type="FOP",
    )
    assert contracts == []


def test_build_contracts_equity_path_unchanged():
    from prometheus.execution.ib_compat import Option
    contracts = _build_contracts(
        underlying="SPY",
        expiry="20260418",
        strikes=[560.0],
        right="P",
        exchange="SMART",
        trading_class="SPY",
        sec_type="STK",
    )
    assert len(contracts) == 1
    assert isinstance(contracts[0], Option)
    assert contracts[0].strike == 560.0
    assert contracts[0].tradingClass == "SPY"


# ── OptionsStrategyManager picks FOP suffix for commodities ────────────


def test_submit_directives_uses_fop_suffix_for_commodity():
    """End-to-end check: a directive on CL routes through the FOP
    instrument_id format, while a directive on SPY uses the equity
    format. Captures the dispatch we added to _submit_directives."""
    from prometheus.execution.broker_interface import BrokerInterface
    from prometheus.execution.instrument_mapper import InstrumentMapper
    from prometheus.execution.options_strategy import (
        OptionsStrategyManager,
        OptionTradeDirective,
        TradeAction,
    )

    captured: list[str] = []

    class _RecordingBroker(BrokerInterface):
        def submit_order(self, order):
            captured.append(order.instrument_id)
            return "stub-order-id"
        def cancel_order(self, order_id): return False
        def get_positions(self): return {}
        def get_order_status(self, order_id): return None
        def get_account_state(self): return {}
        def get_fills(self, since=None): return []
        def sync(self): pass

    mapper = InstrumentMapper.__new__(InstrumentMapper)
    mgr = OptionsStrategyManager(
        broker=_RecordingBroker(),
        mapper=mapper,
        strategies=[],   # bypass the legacy strategy fan-out
    )

    cl_directive = OptionTradeDirective(
        strategy="commodity.crude_chokepoint_call",
        action=TradeAction.OPEN,
        symbol="CL", right="C", expiry="20260622",
        strike=75.0, quantity=1, limit_price=2.50,
    )
    spy_directive = OptionTradeDirective(
        strategy="protective_put",
        action=TradeAction.OPEN,
        symbol="SPY", right="P", expiry="20260418",
        strike=560.0, quantity=1, limit_price=10.0,
    )

    mgr._submit_directives([cl_directive, spy_directive])

    assert "CL_260622_75C.FOP" in captured
    assert "SPY_260418_560P.US" in captured
