"""Tests for prometheus.derivatives.allocator."""

from __future__ import annotations

import pytest

from prometheus.derivatives import allocator
from prometheus.derivatives.sleeves import Sleeve

# ── SleeveCutoverState ───────────────────────────────────────────────


def test_cutover_state_defaults_to_all_false():
    s = allocator.SleeveCutoverState()
    assert s.hedge is False
    assert s.income is False
    assert s.convex is False
    assert s.is_active(Sleeve.HEDGE) is False
    assert s.is_active(Sleeve.INCOME) is False
    assert s.is_active(Sleeve.CONVEX) is False


def test_cutover_state_from_env_reads_each_sleeve():
    s = allocator.SleeveCutoverState.from_env({
        "PROMETHEUS_DERIVATIVES_HEDGE_CUTOVER": "1",
        "PROMETHEUS_DERIVATIVES_INCOME_CUTOVER": "false",
        "PROMETHEUS_DERIVATIVES_CONVEX_CUTOVER": "yes",
    })
    assert s.hedge is True
    assert s.income is False
    assert s.convex is True


def test_cutover_state_from_empty_env():
    s = allocator.SleeveCutoverState.from_env({})
    assert s == allocator.SleeveCutoverState()


@pytest.mark.parametrize("truthy", ["1", "true", "TRUE", "Yes", " on "])
def test_cutover_state_truthy_values(truthy):
    s = allocator.SleeveCutoverState.from_env({
        "PROMETHEUS_DERIVATIVES_HEDGE_CUTOVER": truthy,
    })
    assert s.hedge is True


@pytest.mark.parametrize("falsy", ["0", "false", "no", "off", "", "garbage"])
def test_cutover_state_falsy_values(falsy):
    s = allocator.SleeveCutoverState.from_env({
        "PROMETHEUS_DERIVATIVES_HEDGE_CUTOVER": falsy,
    })
    assert s.hedge is False


# ── SleeveBudgetPlanner ──────────────────────────────────────────────


def test_planner_emits_per_sleeve_dollar_budgets():
    p = allocator.SleeveBudgetPlanner()
    plan = p.plan(nav=200_000.0)
    assert plan.sleeve_budgets[Sleeve.HEDGE] == pytest.approx(20_000.0)
    assert plan.sleeve_budgets[Sleeve.INCOME] == pytest.approx(30_000.0)
    assert plan.sleeve_budgets[Sleeve.CONVEX] == pytest.approx(10_000.0)
    assert plan.sleeve_budgets[Sleeve.COMMODITY] == pytest.approx(10_000.0)


def test_planner_sleeve_budgets_sum_to_35pct_of_nav():
    p = allocator.SleeveBudgetPlanner()
    plan = p.plan(nav=200_000.0)
    total = sum(plan.sleeve_budgets.values())
    assert total == pytest.approx(70_000.0)


def test_planner_clamps_negative_nav():
    p = allocator.SleeveBudgetPlanner()
    plan = p.plan(nav=-100.0)
    assert all(b == 0.0 for b in plan.sleeve_budgets.values())


def test_planner_no_cutover_leaves_all_legacy_categories_intact():
    p = allocator.SleeveBudgetPlanner()
    plan = p.plan(nav=200_000.0)
    # All five known categories pass through
    for cat in ("DIRECTIONAL", "INCOME", "HEDGE", "VOLATILITY", "FUTURES"):
        assert plan.legacy_multiplier(cat) == 1.0
    assert plan.active_sleeves == frozenset()


def test_planner_hedge_cutover_zeros_hedge_category_only():
    p = allocator.SleeveBudgetPlanner()
    plan = p.plan(
        nav=200_000.0,
        cutover_state=allocator.SleeveCutoverState(hedge=True),
    )
    assert plan.legacy_multiplier("HEDGE") == 0.0
    # Other categories untouched
    assert plan.legacy_multiplier("INCOME") == 1.0
    assert plan.legacy_multiplier("DIRECTIONAL") == 1.0
    assert plan.legacy_multiplier("FUTURES") == 1.0
    assert plan.active_sleeves == frozenset({Sleeve.HEDGE})


def test_planner_income_cutover_zeros_income_and_volatility():
    """INCOME sleeve absorbs both INCOME and VOLATILITY legacy
    categories (calendar/straddle live in INCOME's scope)."""
    p = allocator.SleeveBudgetPlanner()
    plan = p.plan(
        nav=200_000.0,
        cutover_state=allocator.SleeveCutoverState(income=True),
    )
    assert plan.legacy_multiplier("INCOME") == 0.0
    assert plan.legacy_multiplier("VOLATILITY") == 0.0
    # HEDGE / DIRECTIONAL / FUTURES untouched
    assert plan.legacy_multiplier("HEDGE") == 1.0
    assert plan.legacy_multiplier("DIRECTIONAL") == 1.0


def test_planner_convex_cutover_does_not_zero_any_legacy_category():
    """CONVEX is net-new exposure — doesn't replace a legacy category."""
    p = allocator.SleeveBudgetPlanner()
    plan = p.plan(
        nav=200_000.0,
        cutover_state=allocator.SleeveCutoverState(convex=True),
    )
    for cat in ("DIRECTIONAL", "INCOME", "HEDGE", "VOLATILITY", "FUTURES"):
        assert plan.legacy_multiplier(cat) == 1.0
    assert plan.active_sleeves == frozenset({Sleeve.CONVEX})


def test_planner_full_cutover_leaves_only_directional_and_futures_in_legacy():
    """When all three sleeves are cut over, only DIRECTIONAL and
    FUTURES remain in legacy (DIRECTIONAL stays until Phase 5
    deletes it; FUTURES isn't part of options sleeves)."""
    p = allocator.SleeveBudgetPlanner()
    plan = p.plan(
        nav=200_000.0,
        cutover_state=allocator.SleeveCutoverState(
            hedge=True, income=True, convex=True,
        ),
    )
    assert plan.legacy_multiplier("HEDGE") == 0.0
    assert plan.legacy_multiplier("INCOME") == 0.0
    assert plan.legacy_multiplier("VOLATILITY") == 0.0
    assert plan.legacy_multiplier("DIRECTIONAL") == 1.0
    assert plan.legacy_multiplier("FUTURES") == 1.0
    assert plan.active_sleeves == {Sleeve.HEDGE, Sleeve.INCOME, Sleeve.CONVEX}


def test_adjusted_legacy_budget_applies_multiplier():
    p = allocator.SleeveBudgetPlanner()
    plan = p.plan(
        nav=200_000.0,
        cutover_state=allocator.SleeveCutoverState(hedge=True),
    )
    # HEDGE cutover → multiplier 0.0 → budget zeroed
    assert plan.adjusted_legacy_budget("HEDGE", 12_000.0) == 0.0
    # INCOME unaffected → pass through
    assert plan.adjusted_legacy_budget("INCOME", 24_000.0) == 24_000.0


def test_adjusted_legacy_budget_handles_unknown_category():
    """An unknown category defaults to multiplier 1.0 (no change)."""
    p = allocator.SleeveBudgetPlanner()
    plan = p.plan(nav=200_000.0)
    assert plan.adjusted_legacy_budget("UNKNOWN_CATEGORY", 5_000.0) == 5_000.0


def test_plan_carries_cutover_state_for_audit():
    p = allocator.SleeveBudgetPlanner()
    state = allocator.SleeveCutoverState(hedge=True, income=False)
    plan = p.plan(nav=200_000.0, cutover_state=state)
    assert plan.cutover_state is state


def test_silenced_strategies_empty_when_no_cutover():
    state = allocator.SleeveCutoverState()
    assert allocator.silenced_strategies(state) == frozenset()


def test_silenced_strategies_for_hedge_cutover():
    state = allocator.SleeveCutoverState(hedge=True)
    silenced = allocator.silenced_strategies(state)
    assert "protective_put" in silenced
    assert "sector_put_spread" in silenced
    assert "vix_tail_hedge" in silenced
    assert "crisis_alpha" in silenced
    # Income/convex strategies not silenced
    assert "iron_condor" not in silenced
    assert "covered_call" not in silenced


def test_silenced_strategies_unions_across_active_sleeves():
    state = allocator.SleeveCutoverState(hedge=True, income=True)
    silenced = allocator.silenced_strategies(state)
    # Includes both HEDGE strategies and INCOME strategies
    assert "protective_put" in silenced
    assert "iron_condor" in silenced
    assert "covered_call" in silenced


def test_silenced_strategies_convex_cutover_adds_nothing():
    """CONVEX is net-new exposure with no legacy class to silence."""
    state = allocator.SleeveCutoverState(convex=True)
    assert allocator.silenced_strategies(state) == frozenset()


def test_custom_sleeve_replaces_map_overrides_default():
    """Phase 4 (or future redesign) can re-wire which sleeve owns
    which legacy categories."""
    custom_map = {
        Sleeve.HEDGE: ("HEDGE", "VOLATILITY"),
        Sleeve.INCOME: ("INCOME",),
        Sleeve.CONVEX: ("DIRECTIONAL",),
    }
    p = allocator.SleeveBudgetPlanner(sleeve_replaces=custom_map)
    plan = p.plan(
        nav=200_000.0,
        cutover_state=allocator.SleeveCutoverState(hedge=True),
    )
    # Custom map: HEDGE absorbs HEDGE + VOLATILITY
    assert plan.legacy_multiplier("HEDGE") == 0.0
    assert plan.legacy_multiplier("VOLATILITY") == 0.0
    assert plan.legacy_multiplier("INCOME") == 1.0
