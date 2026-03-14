"""Prometheus v2 – Risk storage helpers.

This module provides small helpers for persisting risk actions into the
``risk_actions`` table in the runtime database.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable

from psycopg2.extras import Json

from apathis.core.database import DatabaseManager
from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger
from prometheus.risk.engine import RiskActionType


logger = get_logger(__name__)


@dataclass(frozen=True)
class RiskAction:
    """Logical record of a risk action taken on a decision."""

    strategy_id: str | None
    instrument_id: str | None
    decision_id: str | None
    action_type: RiskActionType
    details: Dict[str, object]


def insert_risk_actions(db_manager: DatabaseManager, actions: Iterable[RiskAction]) -> None:
    """Insert multiple :class:`RiskAction` rows into ``risk_actions``.

    This helper is intended to be called with a small number of actions
    per batch (e.g. per assessment cycle or per book rebalance).
    """

    actions = list(actions)
    if not actions:
        return

    sql = """
        INSERT INTO risk_actions (
            action_id,
            strategy_id,
            instrument_id,
            decision_id,
            action_type,
            details_json,
            created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            for action in actions:
                cursor.execute(
                    sql,
                    (
                        generate_uuid(),
                        action.strategy_id,
                        action.instrument_id,
                        action.decision_id,
                        action.action_type.value,
                        Json(action.details),
                    ),
                )
            conn.commit()
        finally:
            cursor.close()
