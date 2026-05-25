"""Tests for per-strategy dampener overrides in dynamic_constraints."""

from __future__ import annotations

import pytest

from prometheus.risk.dynamic_constraints import (
    _strategy_dampener_disabled,
    _strategy_floor,
    compute_dampener,
    get_dynamic_strategy_risk_config,
)


def test_disabled_strategies_register() -> None:
    assert _strategy_dampener_disabled("US_EQ_HEDGE_ETF") is True
    assert _strategy_dampener_disabled("US_EQ_TAIL_HEDGE") is True


def test_default_strategy_not_disabled() -> None:
    assert _strategy_dampener_disabled("US_EQ_CORE_LONG_EQ") is False
    assert _strategy_dampener_disabled("UNKNOWN_STRAT") is False


def test_strategy_floor_default() -> None:
    assert _strategy_floor("US_EQ_CORE_LONG_EQ") == 0.40


def test_strategy_floor_override() -> None:
    """Allocator has an 80% floor — it can't be dampened past that."""
    assert _strategy_floor("US_EQ_ALLOCATOR") == 0.80


def test_disabled_strategy_returns_unity_dampener() -> None:
    """Hedge books opt out; dampener is 1.0 regardless of signal severity."""
    mult, inputs = compute_dampener(
        portfolio_id="P",
        strategy_id="US_EQ_HEDGE_ETF",
        geo_score=95.0,
        compound_severities=["CRITICAL"],
        exposed_isos=set(),
    )
    assert mult == 1.0
    assert inputs.dampener == 1.0


def test_strategy_floor_clamps_dampener() -> None:
    """Allocator with a high floor doesn't dampen as aggressively as default."""
    # Same severe inputs as test_compute_dampener_clamped_to_floor in
    # test_dynamic_constraints.py, but with a strategy_id whose floor is 0.80.
    mult, _inputs = compute_dampener(
        portfolio_id="P",
        strategy_id="US_EQ_ALLOCATOR",
        geo_score=95.0,                  # would be 0.40
        compound_severities=["CRITICAL"], # 0.70
        exposed_isos=set(),
    )
    # Floor is 0.80 — multiplier can't go below that even with severe inputs.
    assert mult == pytest.approx(0.80)


def test_dynamic_config_disabled_for_hedge_books() -> None:
    cfg, inputs = get_dynamic_strategy_risk_config(
        "US_EQ_HEDGE_ETF",
        portfolio_id="IBKR_PAPER",
    )
    assert inputs is None  # No dampener data because dampener is bypassed
    # Static config flows through unchanged
    assert cfg.max_abs_weight_per_name == 1.0


def test_default_strategies_still_dampen_when_severe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sanity: a non-disabled, non-overridden strategy still receives the
    standard dampener treatment."""

    def _fake_compute(**kwargs):
        from prometheus.risk.dynamic_constraints import DampenerInputs

        return 0.5, DampenerInputs(
            overall_geo_risk=70.0,
            compound_severities=("HIGH",),
            portfolio_exposed_isos=("USA",),
            base_max_weight=0.0,
            dampener=0.5,
        )

    monkeypatch.setattr(
        "prometheus.risk.dynamic_constraints.compute_dampener",
        _fake_compute,
    )
    cfg, inputs = get_dynamic_strategy_risk_config(
        "US_EQ_CORE_LONG_EQ",
        portfolio_id="IBKR_PAPER",
    )
    assert inputs is not None
    assert inputs.dampener == 0.5
    assert cfg.max_abs_weight_per_name == pytest.approx(0.025)
