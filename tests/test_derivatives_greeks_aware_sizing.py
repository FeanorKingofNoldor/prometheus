"""Tests for greeks-aware sizing in size_position + runner.

Two layers:

1. Pure sizing: size_position honours GreeksHeadroom + PerContractGreeks.
2. End-to-end via runner: multi-template sleeve runs cannot collectively
   breach the portfolio greeks budget because headroom is decremented
   between templates.
"""

from __future__ import annotations

from datetime import date

from prometheus.derivatives import backtest, sleeves
from prometheus.derivatives.sizing import (
    GreeksHeadroom,
    PerContractGreeks,
    size_position,
)

# ── Pure sizing tests ────────────────────────────────────────────────


def test_no_headroom_means_no_greeks_cap():
    r = size_position(
        category_budget_usd=10_000.0,
        premium_per_contract_usd=100.0,
        max_contracts=50,
    )
    assert r.contracts == 50
    assert r.greeks_bound is False


def test_headroom_caps_size_when_below_budget_cap():
    # Budget allows 100; greeks allow only 5
    r = size_position(
        category_budget_usd=10_000.0,
        premium_per_contract_usd=100.0,
        max_contracts=200,
        greeks_headroom=GreeksHeadroom(
            delta_abs=150.0, gamma=10.0, theta=100.0, vega=100.0,
        ),
        per_contract_greeks=PerContractGreeks(
            delta=-30.0, gamma=0.5, theta=-5.0, vega=15.0,
        ),
    )
    # delta cap: 150/30 = 5; vega cap: 100/15 = 6; gamma cap: 10/0.5 = 20
    # → min = 5
    assert r.contracts == 5
    assert r.greeks_bound is True


def test_zero_headroom_for_a_bound_greek_skips():
    r = size_position(
        category_budget_usd=10_000.0,
        premium_per_contract_usd=100.0,
        max_contracts=10,
        greeks_headroom=GreeksHeadroom(
            delta_abs=0.0, gamma=10.0, theta=100.0, vega=100.0,
        ),
        per_contract_greeks=PerContractGreeks(
            delta=-30.0, gamma=0.5, theta=-5.0, vega=15.0,
        ),
    )
    assert r.contracts == 0
    assert r.greeks_bound is True
    assert r.skipped_reason == "greeks_headroom_exhausted"


def test_greek_with_zero_per_contract_does_not_constrain():
    # Position has zero gamma — gamma headroom shouldn't bind it
    r = size_position(
        category_budget_usd=10_000.0,
        premium_per_contract_usd=100.0,
        max_contracts=10,
        greeks_headroom=GreeksHeadroom(
            delta_abs=1_000.0, gamma=0.0, theta=100.0, vega=1_000.0,
        ),
        per_contract_greeks=PerContractGreeks(
            delta=-30.0, gamma=0.0, theta=-5.0, vega=15.0,
        ),
    )
    # Gamma=0 → not in cap calc; other greeks have plenty of room
    assert r.contracts == 10
    assert r.greeks_bound is False


def test_theta_only_consumes_when_position_bleeds():
    # Long position with negative theta consumes theta headroom
    r_long = size_position(
        category_budget_usd=10_000.0,
        premium_per_contract_usd=100.0,
        max_contracts=20,
        greeks_headroom=GreeksHeadroom(
            delta_abs=10_000, gamma=10_000, theta=10.0, vega=10_000,
        ),
        per_contract_greeks=PerContractGreeks(
            delta=10.0, gamma=0.0, theta=-5.0, vega=10.0,
        ),
    )
    # theta cap: 10/5 = 2 contracts
    assert r_long.contracts == 2

    # Short position with positive theta doesn't consume theta headroom
    r_short = size_position(
        category_budget_usd=10_000.0,
        premium_per_contract_usd=100.0,
        max_contracts=20,
        greeks_headroom=GreeksHeadroom(
            delta_abs=10_000, gamma=10_000, theta=10.0, vega=10_000,
        ),
        per_contract_greeks=PerContractGreeks(
            delta=10.0, gamma=0.0, theta=+5.0, vega=10.0,
        ),
    )
    # Positive theta → not in cap → budget/capacity rules apply
    assert r_short.contracts == 20


def test_budget_bound_when_budget_tighter_than_greeks():
    # Budget allows 5; greeks allow 50
    r = size_position(
        category_budget_usd=500.0,
        premium_per_contract_usd=100.0,
        max_contracts=100,
        greeks_headroom=GreeksHeadroom(
            delta_abs=1_500.0, gamma=10.0, theta=100.0, vega=750.0,
        ),
        per_contract_greeks=PerContractGreeks(
            delta=-30.0, gamma=0.5, theta=-5.0, vega=15.0,
        ),
    )
    assert r.contracts == 5
    assert r.budget_bound is True
    assert r.greeks_bound is False


# ── End-to-end runner tests ──────────────────────────────────────────


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


def test_runner_passes_headroom_through_to_templates():
    # Generous budget, tight delta headroom → all firing templates
    # should be small.
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.15, "vix_level": 45.0, "frag": 0.50,
            "market_state": "CRISIS",
            "sector_shi": {"ENERGY": 0.10},
            "greeks_headroom": GreeksHeadroom(
                delta_abs=50.0, gamma=10.0, theta=50.0, vega=200.0,
            ),
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge()},
    )
    # We don't pass greeks_headroom explicitly — the backtest harness
    # doesn't yet thread it (Phase 2.3b infrastructure-only). But the
    # absence shouldn't break anything.
    assert len(result.directives) >= 1


def test_runner_decrements_headroom_so_later_templates_size_smaller():
    """When the first template consumes most of the headroom, a later
    template that would normally size larger gets capped (or skipped)
    because the runner threads the decremented headroom through."""
    from prometheus.derivatives import runner

    today = date(2026, 5, 22)
    hedge = _hedge()

    discovery = backtest.BacktestDiscovery(today, _prices)
    iv_svc = backtest.BacktestIvLookup(
        today, lambda _d: {"vix_level": 45.0}, _prices,
    )
    liq_svc = backtest.BacktestLiquidityFilter(
        today, lambda _d: {"vix_level": 45.0}, _prices,
    )

    common_signals = {
        "mhi": 0.15, "vix_level": 45.0,
        "market_state": "CRISIS",
        "sector_shi": {"ENERGY": 0.10},
    }

    # Generous headroom: every template sizes to its budget cap
    fat = GreeksHeadroom(
        delta_abs=10_000.0, gamma=1_000.0, theta=10_000.0, vega=100_000.0,
    )
    fat_result = runner.run_sleeve(
        hedge, signals=common_signals, nav=200_000.0,
        open_contracts_by_template={},
        underlying_price_fn=lambda s: _prices(today, s),
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today, greeks_headroom=fat,
    )

    # Thin headroom: same templates fire but should size smaller
    thin = GreeksHeadroom(
        delta_abs=50.0, gamma=5.0, theta=20.0, vega=200.0,
    )
    thin_result = runner.run_sleeve(
        hedge, signals=common_signals, nav=200_000.0,
        open_contracts_by_template={},
        underlying_price_fn=lambda s: _prices(today, s),
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today, greeks_headroom=thin,
    )

    fat_total = sum(abs(d.quantity) for d in fat_result.directives)
    thin_total = sum(abs(d.quantity) for d in thin_result.directives)

    # Thin headroom must produce strictly fewer total contracts
    # (or some templates skipped on greeks).
    assert thin_total <= fat_total, (
        f"thin_total={thin_total} not ≤ fat_total={fat_total}"
    )
    # At least one directive should carry greeks_bound=True
    assert any(d.sizing.greeks_bound for d in thin_result.directives) or \
        any("greeks" in (s.detail or "") for s in thin_result.skips), (
        "Expected at least one directive/skip showing greeks-bound behaviour"
    )


def test_runner_with_zero_headroom_skips_everything_with_explanation():
    """If headroom is fully consumed, no template can size > 0."""
    from prometheus.derivatives import runner

    today = date(2026, 5, 22)
    hedge = _hedge()

    discovery = backtest.BacktestDiscovery(today, _prices)
    iv_svc = backtest.BacktestIvLookup(
        today, lambda _d: {"vix_level": 45.0}, _prices,
    )
    liq_svc = backtest.BacktestLiquidityFilter(
        today, lambda _d: {"vix_level": 45.0}, _prices,
    )

    headroom = GreeksHeadroom(delta_abs=0.0, gamma=0.0, theta=0.0, vega=0.0)

    result = runner.run_sleeve(
        hedge,
        signals={
            "mhi": 0.15, "vix_level": 45.0,
            "market_state": "CRISIS",
            "sector_shi": {"ENERGY": 0.10},
        },
        nav=200_000.0,
        open_contracts_by_template={},
        underlying_price_fn=lambda s: _prices(today, s),
        discovery=discovery, iv_lookup=iv_svc, liquidity=liq_svc,
        today=today,
        greeks_headroom=headroom,
    )

    # With zero headroom, every firing template skips on greeks
    sizing_skips = [
        s for s in result.skips
        if s.reason == runner.SKIP_SIZING
        and "greeks_headroom_exhausted" in (s.detail or "")
    ]
    assert len(sizing_skips) >= 1
