"""Prometheus v2 – Risk Management Service package.

This package implements a minimal Risk Management Service that applies
simple constraints (e.g. per-name weight caps) to proposed positions and
optionally logs risk actions into the ``risk_actions`` table.
"""

from __future__ import annotations

from prometheus.risk.constraints import StrategyRiskConfig, get_strategy_risk_config
from prometheus.risk.engine import RiskActionType
from prometheus.risk.api import apply_risk_constraints

__all__ = [
    "StrategyRiskConfig",
    "RiskActionType",
    "get_strategy_risk_config",
    "apply_risk_constraints",
]
