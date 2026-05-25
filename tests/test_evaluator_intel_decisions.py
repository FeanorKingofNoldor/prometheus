"""Tests for the new intel-decision branches in OutcomeEvaluator."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from prometheus.decisions.evaluator import OutcomeEvaluator


@pytest.fixture
def evaluator() -> OutcomeEvaluator:
    """Build an evaluator with a stubbed DB manager — none of the unit
    tests below touch the DB; we exercise pure-logic methods only."""

    class _StubDB:
        def get_runtime_connection(self):  # pragma: no cover - not used
            raise RuntimeError("unit tests must not hit the DB")

    return OutcomeEvaluator(db_manager=_StubDB())


def test_resolve_known_chokepoint_maps_to_oil_etf(evaluator: OutcomeEvaluator) -> None:
    assert evaluator._resolve_entity_instrument("chokepoint", "hormuz") == "USO.US"


def test_resolve_known_conflict_maps_to_defense(evaluator: OutcomeEvaluator) -> None:
    assert evaluator._resolve_entity_instrument("conflict", "russia_ukraine") == "ITA.US"


def test_resolve_sovereign_uses_country_etf(evaluator: OutcomeEvaluator) -> None:
    assert evaluator._resolve_entity_instrument("SOVEREIGN", "CHN") == "FXI.US"


def test_resolve_unknown_falls_back_to_spy(evaluator: OutcomeEvaluator) -> None:
    assert evaluator._resolve_entity_instrument("conflict", "unknown_xyz") == "SPY.US"
    assert evaluator._resolve_entity_instrument(None, None) == "SPY.US"


def test_divergence_fade_is_short(evaluator: OutcomeEvaluator) -> None:
    direction, _reason = evaluator._intel_predicted_direction(
        engine_name="DIVERGENCE",
        output_refs={"trading_signal": "FADE_NARRATIVE"},
        input_refs={},
    )
    assert direction == -1


def test_divergence_front_run_is_long(evaluator: OutcomeEvaluator) -> None:
    direction, _ = evaluator._intel_predicted_direction(
        engine_name="DIVERGENCE",
        output_refs={"trading_signal": "FRONT_RUN_REALITY"},
        input_refs={},
    )
    assert direction == +1


def test_divergence_no_signal_is_neutral(evaluator: OutcomeEvaluator) -> None:
    direction, _ = evaluator._intel_predicted_direction(
        engine_name="DIVERGENCE",
        output_refs={"trading_signal": "NONE"},
        input_refs={},
    )
    assert direction == 0


def test_compound_pressure_is_short(evaluator: OutcomeEvaluator) -> None:
    direction, _ = evaluator._intel_predicted_direction(
        engine_name="COMPOUND_PRESSURE",
        output_refs={"severity": "HIGH"},
        input_refs={"target_entity_id": "IRN"},
    )
    assert direction == -1


def test_geo_risk_is_short(evaluator: OutcomeEvaluator) -> None:
    direction, _ = evaluator._intel_predicted_direction(
        engine_name="GEO_RISK",
        output_refs={"overall_risk_score": 70.0},
        input_refs={"portfolio_id": "IBKR_PAPER"},
    )
    assert direction == -1


def test_beneficiary_is_neutral(evaluator: OutcomeEvaluator) -> None:
    """Beneficiary asymmetry doesn't yet map to a single instrument
    direction — score 0 so it doesn't pollute hit-rate."""
    direction, _ = evaluator._intel_predicted_direction(
        engine_name="BENEFICIARY",
        output_refs={"asymmetry_detected": True},
        input_refs={"victim_entity_id": "iran_war_2026"},
    )
    assert direction == 0


def test_scenario_is_long(evaluator: OutcomeEvaluator) -> None:
    direction, _ = evaluator._intel_predicted_direction(
        engine_name="SCENARIO",
        output_refs={"probability": 0.6},
        input_refs={},
    )
    assert direction == +1


def test_unknown_engine_is_neutral(evaluator: OutcomeEvaluator) -> None:
    direction, reason = evaluator._intel_predicted_direction(
        engine_name="UNKNOWN_ENGINE",
        output_refs={},
        input_refs={},
    )
    assert direction == 0
    assert "no scoring rule" in reason.lower()
