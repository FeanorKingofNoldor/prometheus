"""Tests for prometheus.execution.intel_signals."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from prometheus.execution.intel_signals import (
    IntelSignals,
    options_sizing_multiplier,
)


def test_empty_intel_signals_serialize() -> None:
    intel = IntelSignals()
    out = intel.as_signals_dict()
    assert out["divergence_alerts"] == []
    assert out["convergence_timelines"] == []
    assert out["compound_pressure"] == []
    assert out["portfolio_geo_risk"] is None


def test_extreme_divergence_detected() -> None:
    intel = IntelSignals(
        divergence_alerts=[{"severity": "EXTREME", "trading_signal": "FADE_NARRATIVE"}],
    )
    assert intel.has_extreme_divergence() is True


def test_critical_compound_detected() -> None:
    intel = IntelSignals(
        compound_pressure=[{"severity": "CRITICAL"}],
    )
    assert intel.has_critical_compound_pressure() is True


def test_sizing_multiplier_baseline() -> None:
    """No signals → multiplier = 1.0."""
    assert options_sizing_multiplier(IntelSignals()) == 1.0


def test_sizing_multiplier_extreme_divergence_boosts() -> None:
    intel = IntelSignals(
        divergence_alerts=[{"severity": "EXTREME"}],
    )
    assert options_sizing_multiplier(intel) == pytest.approx(1.20)


def test_sizing_multiplier_critical_compound_boosts() -> None:
    intel = IntelSignals(
        compound_pressure=[{"severity": "CRITICAL"}],
    )
    assert options_sizing_multiplier(intel) == pytest.approx(1.15)


def test_sizing_multiplier_high_geo_caps() -> None:
    intel = IntelSignals(
        portfolio_geo_risk={"overall_risk_score": 75.0},
    )
    assert options_sizing_multiplier(intel) == pytest.approx(0.85)


def test_sizing_multiplier_combined() -> None:
    """All three triggers compose multiplicatively, then clamp to [0.5, 1.5]."""
    intel = IntelSignals(
        divergence_alerts=[{"severity": "EXTREME"}],
        compound_pressure=[{"severity": "CRITICAL"}],
        portfolio_geo_risk={"overall_risk_score": 75.0},
    )
    expected = 1.20 * 1.15 * 0.85
    assert options_sizing_multiplier(intel) == pytest.approx(expected)


def test_sizing_multiplier_clamps() -> None:
    """Even if math would push above 1.5, the multiplier clamps."""
    intel = IntelSignals(
        divergence_alerts=[
            {"severity": "EXTREME"},
            {"severity": "EXTREME"},
            {"severity": "EXTREME"},
        ],
        compound_pressure=[{"severity": "CRITICAL"}],
    )
    mult = options_sizing_multiplier(intel)
    assert mult <= 1.5
