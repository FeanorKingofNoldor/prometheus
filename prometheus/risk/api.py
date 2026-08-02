"""Prometheus v2 – Risk Management Service public API.

This module exposes a small, dictionary-based API for applying risk
constraints to proposed decisions. It is intentionally simple and does
not depend on any particular Assessment or Portfolio implementation.
"""

from __future__ import annotations

from datetime import date as _date
from typing import Any, Dict, Iterable, List

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger

from prometheus.risk.constraints import get_strategy_risk_config
from prometheus.risk.dynamic_constraints import get_dynamic_strategy_risk_config
from prometheus.risk.engine import apply_risk_to_decision
from prometheus.risk.storage import RiskAction, insert_risk_actions

logger = get_logger(__name__)


def apply_risk_constraints(
    decisions: Iterable[Dict[str, Any]],
    *,
    strategy_id: str,
    db_manager: DatabaseManager | None = None,
    portfolio_id: str | None = None,
    as_of_date: _date | None = None,
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
        portfolio_id: Optional portfolio identifier — when supplied (and a
            DB manager is available), per-name caps are dampened by the
            current geo-risk + compound-pressure dampener.
        as_of_date: Decision date — feeds the dampener's compound-pressure
            lookup so we read alerts that were active when the decisions
            were made (not whatever's in the DB right now).

    Returns:
        A list of updated decision dictionaries with adjusted
        ``target_weight`` values and ``risk_*`` annotations.
    """

    if portfolio_id is not None and db_manager is not None:
        config, dampener_inputs = get_dynamic_strategy_risk_config(
            strategy_id,
            portfolio_id=portfolio_id,
            as_of_date=as_of_date,
            db_manager=db_manager,
        )
    else:
        config = get_strategy_risk_config(strategy_id)
        dampener_inputs = None

    updated: List[Dict[str, Any]] = []
    actions: List[RiskAction] = []

    for decision in decisions:
        new_decision, result = apply_risk_to_decision(decision, config)
        if dampener_inputs is not None and dampener_inputs.dampener < 0.999:
            new_decision["risk_dampener"] = round(dampener_inputs.dampener, 4)
            new_decision["risk_dampener_geo_score"] = dampener_inputs.overall_geo_risk
        updated.append(new_decision)

        if db_manager is not None:
            details: Dict[str, Any] = {
                "original_weight": result.original_weight,
                "adjusted_weight": result.adjusted_weight,
                "reason": result.reason,
            }
            if dampener_inputs is not None and dampener_inputs.dampener < 0.999:
                details["dampener"] = dampener_inputs.dampener
                details["dampener_geo_score"] = dampener_inputs.overall_geo_risk
                details["dampener_compound_severities"] = list(
                    dampener_inputs.compound_severities,
                )
            actions.append(
                RiskAction(
                    strategy_id=strategy_id,
                    instrument_id=result.instrument_id,
                    decision_id=new_decision.get("decision_id"),
                    action_type=result.action_type,
                    details=details,
                )
            )

    if db_manager is not None and actions:
        try:
            insert_risk_actions(db_manager, actions)
        except Exception:  # pragma: no cover - defensive logging
            logger.exception("apply_risk_constraints: failed to insert risk_actions")

    return updated
