"""Prometheus v2 – Decision tracking and outcome evaluation.

This module provides services for recording engine decisions and
evaluating their realized outcomes at specified horizons. It enables
data-driven diagnosis of strategy performance by tracking what decisions
were made, why, and how they performed.

Key components:
- DecisionTracker: Records decisions from engines (universe, assessment, portfolio, execution)
- OutcomeEvaluator: Computes realized metrics at horizons and stores in decision_outcomes
- DecisionAnalyzer: Query and analyze decision performance patterns

The module uses the existing `engine_decisions` and `decision_outcomes`
tables (Layer 3) but provides higher-level interfaces than direct SQL access.
"""

from __future__ import annotations

# Import services
from prometheus.decisions.evaluator import OutcomeEvaluator
from prometheus.decisions.tracker import DecisionTracker

# Re-export types from meta module for convenience
from prometheus.meta.types import DecisionOutcome, EngineDecision

__all__ = [
    "DecisionOutcome",
    "DecisionTracker",
    "EngineDecision",
    "OutcomeEvaluator",
]
