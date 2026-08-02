"""Tests for OptionsStrategyManager._apply_long_debit_cap.

Independent of the rest of the manager — exercises the cap logic with
synthetic directives + existing positions to confirm the book-level
guard works without needing live strategies.
"""

from __future__ import annotations

from prometheus.execution.options_strategy import (
    OptionsStrategyManager,
    OptionTradeDirective,
    TradeAction,
)


def _open_long(symbol: str, qty: int, price: float, strategy: str = "test_strategy") -> OptionTradeDirective:
    return OptionTradeDirective(
        strategy=strategy,
        action=TradeAction.OPEN,
        symbol=symbol,
        right="C",
        expiry="20260717",
        strike=100.0,
        quantity=qty,
        limit_price=price,
        reason="test",
    )


def _open_short(symbol: str, qty: int, price: float) -> OptionTradeDirective:
    # Short = negative quantity (credit collected, not premium paid).
    return OptionTradeDirective(
        strategy="test_short",
        action=TradeAction.OPEN,
        symbol=symbol,
        right="C",
        expiry="20260717",
        strike=100.0,
        quantity=-qty,
        limit_price=price,
        reason="test",
    )


def test_cap_passes_through_when_under_budget():
    # NAV = 100k → cap = 30k. Existing 5k + proposed 10k = 15k < 30k.
    existing = [{"quantity": 10, "entry_price": 5.0}]   # 5,000
    proposed = [_open_long("SPY", 20, 5.0)]              # 10,000
    out = OptionsStrategyManager._apply_long_debit_cap(
        proposed, existing, {"nav": 100_000.0},
    )
    assert len(out) == 1


def test_cap_drops_directive_that_exceeds_budget():
    # NAV = 100k → cap = 30k. Existing 25k + proposed 10k = 35k > 30k.
    existing = [{"quantity": 50, "entry_price": 5.0}]    # 25,000
    proposed = [_open_long("SPY", 20, 5.0)]              # 10,000
    out = OptionsStrategyManager._apply_long_debit_cap(
        proposed, existing, {"nav": 100_000.0},
    )
    assert out == []


def test_cap_keeps_first_drops_overflow_in_order():
    # NAV = 100k → cap = 30k. Two 20k proposals — first fits, second doesn't.
    proposed = [
        _open_long("SPY", 40, 5.0),   # 20,000 → fits (running=20k)
        _open_long("QQQ", 40, 5.0),   # 20,000 → would push to 40k > 30k → drop
    ]
    out = OptionsStrategyManager._apply_long_debit_cap(
        proposed, [], {"nav": 100_000.0},
    )
    assert len(out) == 1
    assert out[0].symbol == "SPY"


def test_cap_ignores_short_premium_directives():
    # Short = quantity < 0, doesn't consume long-debit budget.
    proposed = [
        _open_short("SPY", 40, 5.0),  # short, ignored
        _open_long("SPY", 40, 5.0),   # 20k long, fits in 30k cap
    ]
    out = OptionsStrategyManager._apply_long_debit_cap(
        proposed, [], {"nav": 100_000.0},
    )
    assert len(out) == 2


def test_cap_passes_close_and_roll_directives_unchecked():
    close_d = OptionTradeDirective(
        strategy="t", action=TradeAction.CLOSE, symbol="SPY", right="C",
        expiry="20260717", strike=100.0, quantity=-10, limit_price=5.0,
    )
    roll_d = OptionTradeDirective(
        strategy="t", action=TradeAction.ROLL, symbol="SPY", right="C",
        expiry="20260717", strike=100.0, quantity=-10, limit_price=5.0,
    )
    # Even with existing budget already maxed, CLOSE/ROLL pass.
    existing = [{"quantity": 100, "entry_price": 5.0}]   # 50k = over 30k
    out = OptionsStrategyManager._apply_long_debit_cap(
        [close_d, roll_d], existing, {"nav": 100_000.0},
    )
    assert len(out) == 2


def test_cap_passes_through_when_nav_unknown():
    # No NAV → can't enforce, fall back to permissive.
    proposed = [_open_long("SPY", 1000, 100.0)]   # 10M premium
    out = OptionsStrategyManager._apply_long_debit_cap(
        proposed, [], {"nav": 0.0},
    )
    assert len(out) == 1


def test_cap_keeps_no_price_directive():
    # Directive with no limit_price can't be sized; passes through.
    d = OptionTradeDirective(
        strategy="t", action=TradeAction.OPEN, symbol="SPY", right="C",
        expiry="20260717", strike=100.0, quantity=10, limit_price=None,
    )
    out = OptionsStrategyManager._apply_long_debit_cap(
        [d], [], {"nav": 100_000.0},
    )
    assert len(out) == 1


def test_existing_short_positions_dont_count_against_cap():
    # quantity < 0 in existing = short premium; not long-debit.
    existing = [{"quantity": -100, "entry_price": 5.0}]  # short, ignored
    proposed = [_open_long("SPY", 50, 5.0)]              # 25,000 long
    out = OptionsStrategyManager._apply_long_debit_cap(
        proposed, existing, {"nav": 100_000.0},
    )
    assert len(out) == 1


def test_repro_spy_7_17_failure_mode():
    """The exact 2026-06-05 failure: 73% NAV in long debit on one expiry.

    NAV $570k. Cap = $171k (30%). Existing positions already at $416k.
    Any new long-debit OPEN should be rejected.
    """
    nav = 570_000.0
    # Six existing long SPY 7/17 positions summing to ~$416k.
    existing = [
        {"quantity": 85, "entry_price": 13.0},   # ~110.5k
        {"quantity": 85, "entry_price": 9.2},    # ~78.2k
        {"quantity": 68, "entry_price": 10.9},   # ~74.1k
        {"quantity": 51, "entry_price": 10.4},   # ~53.0k
        {"quantity": 51, "entry_price": 10.3},   # ~52.5k
        {"quantity": 48, "entry_price": 10.0},   # ~48.0k
    ]
    proposed = [_open_long("SPY", 10, 5.0)]      # 5k tiny new long
    out = OptionsStrategyManager._apply_long_debit_cap(
        proposed, existing, {"nav": nav},
    )
    assert out == [], "any new long-debit OPEN should be rejected past cap"
