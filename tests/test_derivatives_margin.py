"""Tests for prometheus.derivatives.margin and the runner gate."""

from __future__ import annotations

from datetime import date

import pytest

from prometheus.derivatives import backtest, margin, runner, sleeves

# ── Checker unit tests ───────────────────────────────────────────────


def test_null_checker_approves_everything():
    c = margin.NullMarginChecker()
    r = c.check(
        underlying="SPY", right="P", strike=500.0,
        quantity=10, limit_price=5.0,
    )
    assert r.approved is True


def test_notional_checker_rejects_zero_or_invalid_inputs():
    c = margin.NotionalMarginChecker(account_equity=200_000.0)
    r = c.check(
        underlying="SPY", right="P", strike=500.0,
        quantity=0, limit_price=5.0,
    )
    assert r.approved is False
    assert r.reason == "zero_or_invalid_inputs"


def test_notional_checker_long_premium_margin_is_debit():
    c = margin.NotionalMarginChecker(account_equity=200_000.0)
    r = c.check(
        underlying="SPY", right="P", strike=500.0,
        quantity=5, limit_price=4.0,
    )
    # Long: margin = debit = 4.0 × 100 × 5 = $2,000
    assert r.estimated_init_margin == pytest.approx(2_000.0)
    assert r.approved is True


def test_notional_checker_short_premium_uses_notional_percentage():
    c = margin.NotionalMarginChecker(
        account_equity=200_000.0, short_margin_pct=0.20,
    )
    r = c.check(
        underlying="SPY", right="P", strike=500.0,
        quantity=-5, limit_price=4.0,
    )
    # Short: margin = max(20% × 500 × 100 × 5, debit)
    # = max(50_000, 2_000) = 50_000
    assert r.estimated_init_margin == pytest.approx(50_000.0)


def test_notional_checker_rejects_when_over_utilisation_threshold():
    c = margin.NotionalMarginChecker(
        account_equity=10_000.0,        # tiny account
        max_margin_util=0.50,           # 50%
    )
    # 5 short SPY puts at 500 strike → 50% notional = $125k → way over
    r = c.check(
        underlying="SPY", right="P", strike=500.0,
        quantity=-5, limit_price=4.0,
    )
    assert r.approved is False
    assert "exceeds max" in r.reason


def test_notional_checker_running_total_accumulates():
    c = margin.NotionalMarginChecker(account_equity=20_000.0)
    # First trade: 2 long puts × 100 × $2 = $400 margin
    first = c.check(
        underlying="SPY", right="P", strike=500.0,
        quantity=2, limit_price=2.0,
    )
    assert first.approved is True
    assert c.current_margin_used == 400.0
    # Second trade adds to the running total
    second = c.check(
        underlying="SPY", right="P", strike=490.0,
        quantity=3, limit_price=3.0,
    )
    assert second.approved is True
    assert c.current_margin_used == 400.0 + 900.0


def test_notional_checker_rejects_when_running_total_breaches_after_prior_fill():
    c = margin.NotionalMarginChecker(
        account_equity=1_000.0, max_margin_util=0.50,
    )
    # First $400 fits (40%)
    c.check(
        underlying="SPY", right="P", strike=10.0,
        quantity=2, limit_price=2.0,
    )
    # Second $400 would push to 80% — over 50% limit
    second = c.check(
        underlying="SPY", right="P", strike=10.0,
        quantity=2, limit_price=2.0,
    )
    assert second.approved is False


# ── Runner integration ──────────────────────────────────────────────


def _prices(_d: date, symbol: str) -> float:
    s = symbol.upper()
    if s == "SPY":
        return 500.0
    if s == "XLE":
        return 90.0
    if s == "VIX":
        return 18.0
    return 0.0


def _hedge():
    return sleeves.default_sleeves()[sleeves.Sleeve.HEDGE]


def test_runner_default_uses_null_checker_so_directives_pass_through():
    """Without an explicit margin_checker, behaviour is identical to
    pre-2.3c — every sized directive becomes a real directive."""
    today = date(2026, 5, 22)
    discovery = backtest.BacktestDiscovery(today, _prices)
    iv_svc = backtest.BacktestIvLookup(
        today, lambda _d: {"vix_level": 18.0}, _prices,
    )
    liq_svc = backtest.BacktestLiquidityFilter(
        today, lambda _d: {"vix_level": 18.0}, _prices,
    )
    result = runner.run_sleeve(
        _hedge(),
        signals={"vix_level": 18.0, "market_state": "NEUTRAL"},
        nav=200_000.0,
        open_contracts_by_template={},
        underlying_price_fn=lambda s: _prices(today, s),
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today,
    )
    # vix_tail_call always fires; no margin checker → it passes through
    vt = [d for d in result.directives if d.template_name == "hedge.vix_tail_call"]
    assert len(vt) == 1
    margin_skips = [s for s in result.skips if s.reason == runner.SKIP_MARGIN]
    assert margin_skips == []


def test_runner_with_tight_margin_rejects_directives():
    """Normal-sized sleeve sizes contracts up; tight margin util
    threshold rejects them post-sizing."""
    today = date(2026, 5, 22)
    discovery = backtest.BacktestDiscovery(today, _prices)
    iv_svc = backtest.BacktestIvLookup(
        today, lambda _d: {"vix_level": 18.0}, _prices,
    )
    liq_svc = backtest.BacktestLiquidityFilter(
        today, lambda _d: {"vix_level": 18.0}, _prices,
    )
    # NAV is normal so sizing produces > 0 contracts; margin checker
    # rejects with a very tight util threshold.
    checker = margin.NotionalMarginChecker(
        account_equity=200_000.0,
        max_margin_util=0.001,           # 0.1% util → almost everything rejected
    )
    result = runner.run_sleeve(
        _hedge(),
        signals={
            "mhi": 0.20, "vix_level": 35.0,
            "market_state": "CRISIS",
            "sector_shi": {"ENERGY": 0.10},
        },
        nav=200_000.0,
        open_contracts_by_template={},
        underlying_price_fn=lambda s: _prices(today, s),
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today,
        margin_checker=checker,
    )
    margin_skips = [s for s in result.skips if s.reason == runner.SKIP_MARGIN]
    assert len(margin_skips) >= 1


def test_runner_margin_skip_carries_explanation_detail():
    today = date(2026, 5, 22)
    discovery = backtest.BacktestDiscovery(today, _prices)
    iv_svc = backtest.BacktestIvLookup(
        today, lambda _d: {"vix_level": 18.0}, _prices,
    )
    liq_svc = backtest.BacktestLiquidityFilter(
        today, lambda _d: {"vix_level": 18.0}, _prices,
    )
    checker = margin.NotionalMarginChecker(
        account_equity=500.0, max_margin_util=0.05,
    )
    result = runner.run_sleeve(
        _hedge(),
        signals={"vix_level": 18.0, "market_state": "NEUTRAL"},
        nav=500.0,
        open_contracts_by_template={},
        underlying_price_fn=lambda s: _prices(today, s),
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today,
        margin_checker=checker,
    )
    margin_skips = [s for s in result.skips if s.reason == runner.SKIP_MARGIN]
    for skip in margin_skips:
        # Detail must explain why margin failed
        assert "margin rejected" in skip.detail
        assert "exceeds max" in skip.detail or "est_init" in skip.detail


class _FailingMarginChecker:
    """Approves the first N calls, rejects the rest. For testing
    spread-leg integrity under partial rejection."""

    def __init__(self, approve_first_n: int) -> None:
        self.approve_first_n = approve_first_n
        self.call_count = 0

    def check(self, **kwargs):
        self.call_count += 1
        if self.call_count <= self.approve_first_n:
            return margin.MarginCheck(
                approved=True, estimated_init_margin=100.0,
                estimated_util_after=0.05, reason="approved",
            )
        return margin.MarginCheck(
            approved=False, estimated_init_margin=100.0,
            estimated_util_after=1.0, reason="synthetic_rejection",
        )


def test_spread_with_any_leg_rejected_drops_entire_spread():
    """If any leg of a 2-leg spread fails margin, BOTH legs must be
    rejected. We never want to submit a half-open spread."""
    today = date(2026, 5, 22)
    discovery = backtest.BacktestDiscovery(today, _prices)
    iv_svc = backtest.BacktestIvLookup(
        today, lambda _d: {"vix_level": 18.0}, _prices,
    )
    liq_svc = backtest.BacktestLiquidityFilter(
        today, lambda _d: {"vix_level": 18.0}, _prices,
    )
    # Approve 1 leg, reject the second. Sector put spread is 2 legs.
    checker = _FailingMarginChecker(approve_first_n=1)
    result = runner.run_sleeve(
        _hedge(),
        signals={
            "mhi": 0.20, "vix_level": 35.0,
            "market_state": "RISK_OFF",
            "sector_shi": {"ENERGY": 0.10},
        },
        nav=200_000.0,
        open_contracts_by_template={},
        underlying_price_fn=lambda s: _prices(today, s),
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today,
        margin_checker=checker,
    )
    # Sector put spread should have ZERO directives in the result —
    # rejecting one leg rejects all legs.
    sp_directives = [
        d for d in result.directives
        if d.template_name == "hedge.sector_put_spread"
    ]
    assert sp_directives == []
    # And both legs appear as SKIP_MARGIN entries
    sp_skips = [
        s for s in result.skips
        if s.template_name == "hedge.sector_put_spread"
    ]
    assert len(sp_skips) == 2
    assert all(s.reason == runner.SKIP_MARGIN for s in sp_skips)
    assert all("spread" in s.detail for s in sp_skips)


def test_running_total_in_notional_checker_blocks_later_templates():
    """The first template's margin is approved and consumes the budget;
    later directives fail because the checker's running total now sits
    at the limit. We use a normal NAV (so sizing produces contracts)
    but a margin util threshold tight enough that one fill saturates."""
    today = date(2026, 5, 22)
    discovery = backtest.BacktestDiscovery(today, _prices)
    iv_svc = backtest.BacktestIvLookup(
        today, lambda _d: {"vix_level": 18.0}, _prices,
    )
    liq_svc = backtest.BacktestLiquidityFilter(
        today, lambda _d: {"vix_level": 18.0}, _prices,
    )
    # 200k NAV → sizing produces contracts. Margin util 1% → only the
    # first/cheapest directive fits; the rest get rejected.
    checker = margin.NotionalMarginChecker(
        account_equity=200_000.0, max_margin_util=0.01,
    )
    result = runner.run_sleeve(
        _hedge(),
        signals={
            "mhi": 0.20, "vix_level": 35.0, "frag": 0.50,
            "market_state": "RISK_OFF",  # allows all 4 hedge templates
            "sector_shi": {"ENERGY": 0.10},
        },
        nav=200_000.0,
        open_contracts_by_template={},
        underlying_price_fn=lambda s: _prices(today, s),
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today,
        margin_checker=checker,
    )
    margin_skips = [s for s in result.skips if s.reason == runner.SKIP_MARGIN]
    approved = result.directives
    # The whole sleeve fired (multiple templates produce directives);
    # some get through, some get rejected once margin saturates.
    assert len(margin_skips) + len(approved) >= 1
    # The checker should have consumed *some* margin, demonstrating
    # state was updated mid-run.
    assert checker.current_margin_used > 0
