"""Wheel decision engine — every validated rule pinned as a test.

Each test names the backtest evidence it encodes (see
prometheus/scripts/research/wheel_validation/README.md).
"""

from __future__ import annotations

from datetime import date

from prometheus.wheel.engine import (
    BlockPhase,
    BlockState,
    IntentKind,
    MarketInputs,
    OpenOption,
    WheelParams,
    apply_expiry,
    decide_block,
    next_expiry_on_or_after,
    round_strike,
)

P = WheelParams()
D = date(2026, 8, 3)  # a Monday


def _cash_block(cash: float = 100_000.0) -> BlockState:
    return BlockState(block_id="b1", phase=BlockPhase.CASH)


def _mkt(spot=630.0, vix=17.0, mark=None, cash=100_000.0) -> MarketInputs:
    return MarketInputs(as_of=D, spot=spot, vix=vix, option_mark_per_share=mark, available_cash=cash)


# -- CASH phase: CSP entry ---------------------------------------------------


def test_normal_vol_sells_2pct_otm_put():
    intents = decide_block(_cash_block(), _mkt(spot=630.0, vix=17.0), P)
    assert len(intents) == 1
    i = intents[0]
    assert i.kind == IntentKind.SELL_PUT
    assert i.strike == round_strike(630.0 * 0.98)  # 617
    assert i.manage_with_profit_take is False


def test_rich_vol_widens_put_and_arms_profit_take():
    """VIXCOND rule: VIX>25 -> 5% OTM + PT50 (wheel_sim_v2 robust winner)."""
    intents = decide_block(_cash_block(), _mkt(spot=630.0, vix=31.0), P)
    i = intents[0]
    assert i.strike == round_strike(630.0 * 0.95)  # 599
    assert i.manage_with_profit_take is True


def test_cash_secured_or_nothing():
    """No cash, no put — margin-secured wheels are the blow-up mode."""
    intents = decide_block(_cash_block(), _mkt(cash=10_000.0), P)
    assert intents == []


def test_expiry_targets_friday_at_least_30d_out():
    e = next_expiry_on_or_after(D, 30)
    assert e.weekday() == 4
    assert (e - D).days >= 30
    assert (e - D).days < 37


# -- CSP_OPEN phase: hold vs profit-take -------------------------------------


def _csp_block(managed: bool, credit=6.0) -> BlockState:
    return BlockState(
        block_id="b1",
        phase=BlockPhase.CSP_OPEN,
        cash_reserved=61_700.0,
        open_option=OpenOption(
            right="P", strike=617.0, expiry=date(2026, 9, 4),
            credit_per_share=credit, managed=managed,
        ),
    )


def test_unmanaged_csp_held_to_expiry_even_at_90pct_profit():
    """PT50-always tested as a wash (wheel_sim_managed) — normal opens hold."""
    intents = decide_block(_csp_block(managed=False), _mkt(mark=0.5), P)
    assert intents == []


def test_managed_csp_profit_takes_at_half_credit():
    intents = decide_block(_csp_block(managed=True, credit=6.0), _mkt(mark=2.9), P)
    assert len(intents) == 1
    assert intents[0].kind == IntentKind.BUY_TO_CLOSE


def test_managed_csp_holds_above_half_credit():
    intents = decide_block(_csp_block(managed=True, credit=6.0), _mkt(mark=3.2), P)
    assert intents == []


def test_managed_csp_without_mark_holds():
    intents = decide_block(_csp_block(managed=True), _mkt(mark=None), P)
    assert intents == []


# -- SHARES phase: covered call or skip --------------------------------------


def _shares_block() -> BlockState:
    return BlockState(
        block_id="b1", phase=BlockPhase.SHARES, shares=100, share_cost_basis=617.0,
    )


def test_normal_vol_sells_8pct_otm_call():
    intents = decide_block(_shares_block(), _mkt(spot=630.0, vix=17.0), P)
    i = intents[0]
    assert i.kind == IntentKind.SELL_CALL
    assert i.strike == round_strike(630.0 * 1.08)  # 680


def test_dead_vol_skips_the_call():
    """VIXCOND rule: VIX<13 -> don't sell cheap options."""
    intents = decide_block(_shares_block(), _mkt(vix=12.2), P)
    assert intents == []


def test_open_covered_call_never_defensively_closed():
    """7-DTE avoidance tested destructive: assignment IS the trim."""
    state = BlockState(
        block_id="b1", phase=BlockPhase.CC_OPEN, shares=100,
        open_option=OpenOption(
            right="C", strike=680.0, expiry=date(2026, 8, 7),  # expiry this week
            credit_per_share=4.0, managed=False,
        ),
    )
    # deep ITM, expiry days away — still no intent: let it be called.
    intents = decide_block(state, _mkt(spot=700.0, mark=20.5), P)
    assert intents == []


# -- Expiry state transitions -------------------------------------------------


def test_put_assignment_transitions_to_shares_at_strike_basis():
    s = apply_expiry(_csp_block(managed=False), settlement_spot=600.0)
    assert s.phase == BlockPhase.SHARES
    assert s.shares == 100
    assert s.share_cost_basis == 617.0
    assert s.open_option is None


def test_put_expiring_otm_returns_to_cash():
    s = apply_expiry(_csp_block(managed=False), settlement_spot=630.0)
    assert s.phase == BlockPhase.CASH
    assert s.shares == 0


def test_call_away_returns_block_to_cash():
    state = BlockState(
        block_id="b1", phase=BlockPhase.CC_OPEN, shares=100, share_cost_basis=617.0,
        open_option=OpenOption(
            right="C", strike=680.0, expiry=date(2026, 9, 4),
            credit_per_share=4.0, managed=False,
        ),
    )
    s = apply_expiry(state, settlement_spot=695.0)
    assert s.phase == BlockPhase.CASH
    assert s.shares == 0


def test_call_expiring_otm_keeps_shares():
    state = BlockState(
        block_id="b1", phase=BlockPhase.CC_OPEN, shares=100, share_cost_basis=617.0,
        open_option=OpenOption(
            right="C", strike=680.0, expiry=date(2026, 9, 4),
            credit_per_share=4.0, managed=False,
        ),
    )
    s = apply_expiry(state, settlement_spot=650.0)
    assert s.phase == BlockPhase.SHARES
    assert s.shares == 100
    assert s.share_cost_basis == 617.0
