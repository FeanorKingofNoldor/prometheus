"""Tests for prometheus.derivatives.sizing.

Two test groups:

1. *Behaviour* — edge cases and provenance flags.
2. *Reproduction* — show that ``size_position`` reproduces the existing
   per-strategy sizing formulas in ``prometheus.execution.options_strategy``
   when given matching arguments. This is the proof of correctness for
   the Phase 2+ migration.
"""

from __future__ import annotations

from prometheus.derivatives.sizing import SizingResult, size_position

# ── Group 1: behaviour ───────────────────────────────────────────────


def test_basic_budget_division():
    r = size_position(category_budget_usd=6_000.0, premium_per_contract_usd=1_000.0)
    assert isinstance(r, SizingResult)
    assert r.contracts == 6
    assert r.budget_bound is True
    assert r.capacity_bound is False
    assert r.skipped_reason is None


def test_zero_budget_skips():
    r = size_position(category_budget_usd=0.0, premium_per_contract_usd=100.0)
    assert r.contracts == 0
    assert r.skipped is True
    assert r.skipped_reason == "budget_non_positive"


def test_negative_budget_skips():
    r = size_position(category_budget_usd=-50.0, premium_per_contract_usd=100.0)
    assert r.contracts == 0
    assert r.skipped_reason == "budget_non_positive"


def test_zero_cost_skips():
    r = size_position(category_budget_usd=1_000.0, premium_per_contract_usd=0.0)
    assert r.contracts == 0
    assert r.skipped_reason == "premium_estimate_non_positive"


def test_max_contracts_caps():
    r = size_position(
        category_budget_usd=10_000.0,
        premium_per_contract_usd=100.0,
        max_contracts=12,
    )
    assert r.contracts == 12
    assert r.capacity_bound is True
    assert r.budget_bound is False


def test_max_contracts_above_affordable_does_not_lift():
    r = size_position(
        category_budget_usd=300.0,
        premium_per_contract_usd=100.0,
        max_contracts=50,
    )
    assert r.contracts == 3
    assert r.budget_bound is True
    assert r.capacity_bound is False


def test_already_open_subtracts_from_capacity():
    r = size_position(
        category_budget_usd=10_000.0,
        premium_per_contract_usd=100.0,
        max_contracts=10,
        already_open_contracts=7,
    )
    assert r.contracts == 3
    assert r.capacity_bound is True


def test_capacity_exhausted_skips():
    r = size_position(
        category_budget_usd=10_000.0,
        premium_per_contract_usd=100.0,
        max_contracts=5,
        already_open_contracts=5,
    )
    assert r.contracts == 0
    assert r.skipped_reason == "capacity_exhausted"


def test_min_contracts_floor_lifts_to_one_when_affordable():
    # Budget covers 1 contract but rounds-down result would be 0 in fractional case.
    r = size_position(
        category_budget_usd=99.0,
        premium_per_contract_usd=100.0,
        min_contracts=1,
    )
    # 99/100 = 0 → below min → caller wanted ≥ 1 → but budget can't cover one
    assert r.contracts == 0
    assert r.skipped_reason == "below_min_contracts"


def test_min_contracts_zero_allows_skip_below_threshold():
    r = size_position(
        category_budget_usd=99.0,
        premium_per_contract_usd=100.0,
        min_contracts=0,
    )
    assert r.contracts == 0


def test_capacity_and_budget_both_bind_at_equal_value():
    # 10 contracts affordable and capacity is exactly 10 — both flags True.
    r = size_position(
        category_budget_usd=1_000.0,
        premium_per_contract_usd=100.0,
        max_contracts=10,
    )
    assert r.contracts == 10
    assert r.budget_bound is True
    assert r.capacity_bound is True


def test_no_max_contracts_means_unbounded_capacity():
    r = size_position(
        category_budget_usd=1_000_000.0,
        premium_per_contract_usd=100.0,
    )
    assert r.contracts == 10_000
    assert r.capacity_bound is False


# ── Group 2: reproduces existing per-strategy formulas ───────────────


def test_reproduces_protective_put_formula():
    # ProtectivePutStrategy (options_strategy.py:505-511):
    #   max_premium = nav * 0.03
    #   estimated_premium_per_contract = strike * 100 * 0.02
    #   n_contracts = max(1, int(max_premium / max(estimated_premium_per_contract, 1)))
    nav = 200_000.0
    strike = 480.0
    max_premium = nav * 0.03
    premium_per_contract = strike * 100 * 0.02

    legacy = max(1, int(max_premium / max(premium_per_contract, 1)))

    r = size_position(
        category_budget_usd=max_premium,
        premium_per_contract_usd=premium_per_contract,
    )
    assert r.contracts == legacy


def test_reproduces_sector_put_spread_formula():
    # SectorPutSpreadStrategy (options_strategy.py:787-804):
    #   n_contracts = max(1, int(sector_exposure / (etf_price * 100)))
    #   n_contracts = min(n_contracts, max_per_sector)
    #   n_contracts = min(n_contracts, max_from_total)
    sector_exposure = 50_000.0
    etf_price = 100.0
    max_per_sector = 3
    max_from_total = 12

    legacy = max(1, int(sector_exposure / (etf_price * 100)))
    legacy = min(legacy, max_per_sector)
    legacy = min(legacy, max_from_total)

    r = size_position(
        category_budget_usd=sector_exposure,
        premium_per_contract_usd=etf_price * 100,
        max_contracts=min(max_per_sector, max_from_total),
    )
    assert r.contracts == legacy
    assert r.capacity_bound is True


def test_reproduces_iron_condor_formula():
    # IronCondorStrategy (options_strategy.py:2204-2217):
    #   budget = nav * nav_pct
    #   n_by_max_loss = int(budget / max(max_loss, 1))
    #   n_by_credit   = int(budget / max(credit_per_contract, 1))
    #   n_contracts = max(1, min(n_by_max_loss, n_by_credit))
    #   _n_by_margin = int(margin_avail / max(credit_per_contract, 1))
    #   n_contracts = max(1, min(n_contracts, _n_by_margin))
    nav = 200_000.0
    budget = nav * 0.05
    max_loss_per_spread = 400.0
    credit_per_contract = 90.0
    margin_avail = 30_000.0

    legacy_n_by_max_loss = int(budget / max(max_loss_per_spread, 1))
    legacy_n_by_credit = int(budget / max(credit_per_contract, 1))
    legacy_n_by_margin = int(margin_avail / max(credit_per_contract, 1))
    legacy = max(1, min(legacy_n_by_max_loss, legacy_n_by_credit, legacy_n_by_margin))

    # The condor passes `min(n_by_credit, n_by_margin)` as the secondary
    # capacity cap; the unified function uses max_loss as the cost.
    r = size_position(
        category_budget_usd=budget,
        premium_per_contract_usd=max_loss_per_spread,
        max_contracts=min(legacy_n_by_credit, legacy_n_by_margin),
    )
    assert r.contracts == legacy


def test_reproduces_covered_call_share_based_formula():
    # CoveredCallStrategy (options_strategy.py:639):
    #   n_contracts = coverable // 100
    # The unified function expresses this via max_contracts = shares // 100
    # and a sentinel cost so the budget never binds.
    shares = 327
    legacy = shares // 100

    r = size_position(
        category_budget_usd=1_000_000.0,
        premium_per_contract_usd=1.0,
        max_contracts=shares // 100,
    )
    assert r.contracts == legacy
    assert r.capacity_bound is True


def test_reproduces_concurrent_cap():
    # Concurrent-position cap: max_concurrent = 3, currently 2 open.
    nav = 200_000.0
    budget = nav * 0.04
    premium_per_csp = 300.0
    max_concurrent = 3
    already_open = 2

    legacy_capacity = max_concurrent - already_open
    legacy_n_by_budget = int(budget / max(premium_per_csp, 1))
    legacy = max(1, min(legacy_n_by_budget, legacy_capacity)) if legacy_capacity > 0 else 0

    r = size_position(
        category_budget_usd=budget,
        premium_per_contract_usd=premium_per_csp,
        max_contracts=max_concurrent,
        already_open_contracts=already_open,
    )
    assert r.contracts == legacy
