"""Tests for prometheus.risk.dynamic_constraints."""

from __future__ import annotations

from datetime import date

import pytest

from prometheus.risk.dynamic_constraints import (
    _interpolate_geo_dampener,
    _compound_dampener,
    compute_dampener,
    get_dynamic_strategy_risk_config,
)


def test_geo_dampener_no_score() -> None:
    assert _interpolate_geo_dampener(None) == 1.0


def test_geo_dampener_below_threshold() -> None:
    assert _interpolate_geo_dampener(20.0) == 1.0
    assert _interpolate_geo_dampener(30.0) == 1.0


def test_geo_dampener_at_high_threshold() -> None:
    # At GEO_HIGH (80) we should hit the floor
    assert _interpolate_geo_dampener(80.0) == pytest.approx(0.40)
    assert _interpolate_geo_dampener(95.0) == pytest.approx(0.40)


def test_geo_dampener_interpolates() -> None:
    # Halfway between 30 and 80 → halfway between 1.0 and 0.4
    mid = _interpolate_geo_dampener(55.0)
    assert mid == pytest.approx(0.7)


def test_compound_dampener_picks_worst() -> None:
    assert _compound_dampener([]) == 1.0
    assert _compound_dampener(["LOW", "MODERATE"]) == 1.0
    assert _compound_dampener(["HIGH"]) == 0.85
    assert _compound_dampener(["MODERATE", "HIGH"]) == 0.85
    # Worst (lowest) wins
    assert _compound_dampener(["HIGH", "CRITICAL"]) == 0.70


def test_compute_dampener_combines_signals() -> None:
    """Geo + compound multiply, with floor at 0.40."""
    mult, inputs = compute_dampener(
        portfolio_id="P",
        geo_score=55.0,             # → 0.7
        compound_severities=["HIGH"],  # → 0.85
        exposed_isos=set(),
    )
    expected = max(0.40, 0.7 * 0.85)
    assert mult == pytest.approx(expected)
    assert inputs.overall_geo_risk == 55.0
    assert inputs.compound_severities == ("HIGH",)


def test_compute_dampener_clamped_to_floor() -> None:
    """Even with extreme inputs, dampener floors at 0.40."""
    mult, _ = compute_dampener(
        portfolio_id="P",
        geo_score=95.0,                  # 0.40
        compound_severities=["CRITICAL"], # 0.70
        exposed_isos=set(),
    )
    # 0.40 * 0.70 = 0.28, but floor is 0.40
    assert mult == pytest.approx(0.40)


def test_compute_dampener_no_signals() -> None:
    """Without any signals the dampener is 1.0 (no shrinking)."""
    mult, inputs = compute_dampener(
        portfolio_id="P",
        geo_score=None,
        compound_severities=[],
        exposed_isos=set(),
    )
    assert mult == 1.0
    assert inputs.dampener == 1.0


def test_dynamic_config_without_portfolio_returns_static() -> None:
    cfg, inputs = get_dynamic_strategy_risk_config("US_EQ_CORE_LONG_EQ")
    assert inputs is None
    assert cfg.max_abs_weight_per_name == 0.05  # default static


def test_dynamic_config_disabled_via_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROMETHEUS_DYNAMIC_RISK_DAMPENER", "0")
    cfg, inputs = get_dynamic_strategy_risk_config(
        "US_EQ_CORE_LONG_EQ",
        portfolio_id="IBKR_PAPER",
    )
    assert inputs is None
    assert cfg.max_abs_weight_per_name == 0.05


def test_dynamic_config_dampens_when_severe(monkeypatch: pytest.MonkeyPatch) -> None:
    """When the signals are severe, the per-name cap is reduced."""

    def _fake_compute(**_kwargs):
        from prometheus.risk.dynamic_constraints import DampenerInputs

        return 0.5, DampenerInputs(
            overall_geo_risk=70.0,
            compound_severities=("HIGH",),
            portfolio_exposed_isos=("USA", "ISR"),
            base_max_weight=0.0,
            dampener=0.5,
        )

    monkeypatch.setattr("prometheus.risk.dynamic_constraints.compute_dampener", _fake_compute)
    cfg, inputs = get_dynamic_strategy_risk_config(
        "US_EQ_CORE_LONG_EQ",
        portfolio_id="IBKR_PAPER",
        as_of_date=date(2026, 5, 5),
    )
    assert inputs is not None
    assert inputs.dampener == 0.5
    assert cfg.max_abs_weight_per_name == pytest.approx(0.025)  # 0.05 * 0.5
    assert inputs.base_max_weight == 0.05  # the static value before dampening


def test_dynamic_config_no_op_when_dampener_unity(monkeypatch: pytest.MonkeyPatch) -> None:
    """When dampener is 1.0, the static config is returned unchanged."""

    def _fake_compute(**_kwargs):
        from prometheus.risk.dynamic_constraints import DampenerInputs

        return 1.0, DampenerInputs(
            overall_geo_risk=15.0,
            compound_severities=(),
            portfolio_exposed_isos=(),
            base_max_weight=0.0,
            dampener=1.0,
        )

    monkeypatch.setattr("prometheus.risk.dynamic_constraints.compute_dampener", _fake_compute)
    cfg, inputs = get_dynamic_strategy_risk_config(
        "US_EQ_CORE_LONG_EQ",
        portfolio_id="IBKR_PAPER",
    )
    assert cfg.max_abs_weight_per_name == 0.05  # unchanged
    assert inputs is not None
    assert inputs.dampener == 1.0
