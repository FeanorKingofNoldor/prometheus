"""Prometheus v2 – executed_actions helpers.

This module provides small helpers for mapping low-level execution
artifacts (fills) into the Meta-Orchestrator friendly ``executed_actions``
Table defined in migration 0018.

The goal is to keep the execution storage (orders/fills/positions) and
meta logging (engine_decisions/executed_actions/decision_outcomes)
loosely coupled while providing a convenience function that can be used
from backtests and, later, from live/paper execution flows.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Mapping, Sequence

from apatheon.core.database import DatabaseManager
from apatheon.core.ids import generate_uuid
from apatheon.core.logging import get_logger
from psycopg2.extras import Json

from prometheus.execution.broker_interface import Fill

logger = get_logger(__name__)


@dataclass(frozen=True)
class ExecutedActionContext:
    """Context used when recording executed_actions rows.

    Attributes:
        run_id: Optional backtest or live run identifier.
        portfolio_id: Optional logical portfolio identifier.
        decision_id: Optional engine decision id that led to these trades.
        mode: Optional execution mode (LIVE/PAPER/BACKTEST); stored in
            metadata for now since ``executed_actions`` does not have a
            dedicated mode column.
    """

    run_id: str | None = None
    portfolio_id: str | None = None
    decision_id: str | None = None
    mode: str | None = None


def record_executed_actions_for_fills(
    db_manager: DatabaseManager,
    *,
    fills: Sequence[Fill],
    context: ExecutedActionContext,
) -> None:
    """Insert one ``executed_actions`` row per fill.

    For now this helper:

    * Uses ``fill.timestamp.date()`` as ``trade_date``.
    * Stores ``fill.commission`` as ``fees`` and leaves ``slippage`` NULL.
    * Stores a small metadata payload containing ``fill_id`` and
      optional ``mode``.

    Args:
        db_manager: Runtime database manager.
        fills: Sequence of :class:`Fill` objects to persist.
        context: :class:`ExecutedActionContext` carrying optional
            ``run_id``, ``portfolio_id``, ``decision_id``, and ``mode``.
    """

    if not fills:
        return

    sql = """
        INSERT INTO executed_actions (
            action_id,
            decision_id,
            run_id,
            portfolio_id,
            instrument_id,
            trade_date,
            side,
            quantity,
            price,
            slippage,
            fees,
            metadata
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            for fill in fills:
                trade_date: date = fill.timestamp.date()
                meta: Mapping[str, object] = {
                    "fill_id": fill.fill_id,
                    "mode": context.mode,
                }
                cursor.execute(
                    sql,
                    (
                        generate_uuid(),
                        context.decision_id,
                        context.run_id,
                        context.portfolio_id,
                        fill.instrument_id,
                        trade_date,
                        fill.side.value,
                        float(fill.quantity),
                        float(fill.price),
                        None,  # slippage (not modelled yet)
                        float(fill.commission),
                        Json(meta),
                    ),
                )
            conn.commit()
        finally:
            cursor.close()

    logger.info(
        "Recorded %d executed_actions rows (run_id=%s, portfolio_id=%s, mode=%s)",
        len(fills),
        context.run_id,
        context.portfolio_id,
        context.mode,
    )
