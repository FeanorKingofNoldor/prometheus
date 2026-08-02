"""Tests for the commodity-FOP CLOSE-at-DTE branch in PositionLifecycleManager.

The legacy lifecycle emits ROLL at STRATEGY_ROLL_DTE, but FOPs aren't
rollable by our broker layer — for commodity templates the directive
must be CLOSE instead.
"""

from __future__ import annotations

from datetime import date, timedelta

from prometheus.execution.options_strategy import TradeAction
from prometheus.execution.position_lifecycle import PositionLifecycleManager


def _pos(strategy: str, symbol: str, dte: int, *, qty: int = 1) -> dict:
    today = date.today()
    expiry = (today + timedelta(days=dte)).strftime("%Y%m%d")
    return {
        "strategy": strategy,
        "symbol": symbol,
        "right": "C",
        "expiry": expiry,
        "strike": 75.0,
        "quantity": qty,
        "entry_price": 1.0,
        "current_price": 1.0,
    }


def test_commodity_template_emits_close_at_dte_threshold():
    mgr = PositionLifecycleManager()
    # commodity.crude_chokepoint_call has roll_dte=14; position at 10 DTE.
    pos = _pos("commodity.crude_chokepoint_call", "BZ", dte=10)
    directives = mgr.check_rolls([pos])
    assert len(directives) == 1
    assert directives[0].action == TradeAction.CLOSE
    assert directives[0].metadata["lifecycle"] == "close"
    assert "Lifecycle close" in directives[0].reason


def test_commodity_symbol_without_template_prefix_still_closes():
    """Even if the strategy name is generic, a commodity FOP symbol routes CLOSE."""
    mgr = PositionLifecycleManager()
    pos = _pos("some_legacy_strategy", "CL", dte=5)
    # Strategy unknown → falls to default_roll_dte=14; CL is a commodity symbol.
    directives = mgr.check_rolls([pos])
    assert len(directives) == 1
    assert directives[0].action == TradeAction.CLOSE


def test_equity_option_still_emits_roll():
    mgr = PositionLifecycleManager()
    pos = _pos("protective_put", "SPY", dte=10)   # roll_dte=14
    directives = mgr.check_rolls([pos])
    assert len(directives) == 1
    assert directives[0].action == TradeAction.ROLL
    assert directives[0].metadata["lifecycle"] == "roll"


def test_far_from_expiry_no_directive():
    mgr = PositionLifecycleManager()
    pos = _pos("commodity.natgas_supply_call", "NG", dte=30)
    directives = mgr.check_rolls([pos])
    assert directives == []


def test_commodity_gold_uses_21_dte_threshold():
    """commodity.gold_sanctions_call has roll_dte=21 — closes earlier."""
    mgr = PositionLifecycleManager()
    pos_at_18 = _pos("commodity.gold_sanctions_call", "GC", dte=18)
    pos_at_25 = _pos("commodity.gold_sanctions_call", "GC", dte=25)
    assert len(mgr.check_rolls([pos_at_18])) == 1
    assert mgr.check_rolls([pos_at_18])[0].action == TradeAction.CLOSE
    assert mgr.check_rolls([pos_at_25]) == []


def test_short_commodity_position_close_quantity_sign():
    """Closing a short FOP position: directive qty should be +abs(qty)."""
    mgr = PositionLifecycleManager()
    pos = _pos("commodity.crude_chokepoint_call", "BZ", dte=10, qty=-5)
    directives = mgr.check_rolls([pos])
    assert len(directives) == 1
    assert directives[0].action == TradeAction.CLOSE
    assert directives[0].quantity == 5  # -(-5) = +5, closing the short
