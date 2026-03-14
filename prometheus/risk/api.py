"""Prometheus v2 – Risk Management Service public API.

This module exposes a small, dictionary-based API for applying risk
constraints to proposed decisions. It is intentionally simple and does
not depend on any particular Assessment or Portfolio implementation.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List

from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from prometheus.risk.constraints import StrategyRiskConfig, get_strategy_risk_config
from prometheus.risk.engine import apply_risk_to_decision
from prometheus.risk.storage import RiskAction, insert_risk_actions


logger = get_logger(__name__)


def apply_risk_constraints(
    decisions: Iterable[Dict[str, Any]],
    *,
    strategy_id: str,
    db_manager: DatabaseManager | None = None,
) -> List[Dict[str, Any]]:
    """Apply basic risk constraints to a batch of decisions.

    Args:
        decisions: Iterable of decision dictionaries. Each decision is
            expected to contain ``instrument_id`` and ``target_weight``
            fields; unknown fields are preserved.
        strategy_id: Logical strategy identifier used to look up
            :class:`StrategyRiskConfig`.
        db_manager: Optional database manager. If provided, risk actions
            are logged into the ``risk_actions`` table; otherwise
            constraints are applied in-memory only.

    Returns:
        A list of updated decision dictionaries with adjusted
        ``target_weight`` values and ``risk_*`` annotations.
    """

    config: StrategyRiskConfig = get_strategy_risk_config(strategy_id)

    updated: List[Dict[str, Any]] = []
    actions: List[RiskAction] = []

    for decision in decisions:
        new_decision, result = apply_risk_to_decision(decision, config)
        updated.append(new_decision)

        if db_manager is not None:
            actions.append(
                RiskAction(
                    strategy_id=strategy_id,
                    instrument_id=result.instrument_id,
                    decision_id=new_decision.get("decision_id"),
                    action_type=result.action_type,
                    details={
                        "original_weight": result.original_weight,
                        "adjusted_weight": result.adjusted_weight,
                        "reason": result.reason,
                    },
                )
            )

    if db_manager is not None and actions:
        try:
            insert_risk_actions(db_manager, actions)
        except Exception:  # pragma: no cover - defensive logging
            logger.exception("apply_risk_constraints: failed to insert risk_actions")

    return updated
