"""Prometheus v2 – Execution API.

This module provides a small, mode-agnostic helper for applying an
execution plan given target positions and a :class:`BrokerInterface`.

The core entrypoint :func:`apply_execution_plan`:

- Computes required orders via :func:`order_planner.plan_orders`.
- Submits them through the provided broker.
- In BACKTEST mode, calls ``BacktestBroker.process_fills`` for the
  current date and records fills.
- Persists orders, fills, and an optional positions snapshot into the
  runtime database using :mod:`prometheus.execution.storage`.

This helper is designed to be used by both backtesting code and future
live/paper execution flows.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Mapping

from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from prometheus.execution.backtest_broker import BacktestBroker
from prometheus.execution.broker_interface import BrokerInterface, OrderSide
from prometheus.execution.order_planner import plan_orders
from prometheus.execution.storage import (
    ExecutionMode,
    record_fills,
    record_orders,
    record_positions_snapshot,
)


logger = get_logger(__name__)


@dataclass(frozen=True)
class ExecutionSummary:
    """Lightweight summary of an execution step."""

    num_orders: int
    num_fills: int


def apply_execution_plan(
    db_manager: DatabaseManager,
    *,
    broker: BrokerInterface,
    portfolio_id: str | None,
    target_positions: Mapping[str, float],
    mode: str,
    as_of_date: date | None = None,
    decision_id: str | None = None,
    record_positions: bool = True,
    sells_first: bool = False,
) -> ExecutionSummary:
    """Apply an execution plan for ``target_positions`` via ``broker``.

    Args:
        db_manager: Runtime database manager.
        broker: Concrete :class:`BrokerInterface` implementation.
        portfolio_id: Logical portfolio identifier associated with the
            orders (may be ``None`` for some strategies).
        target_positions: Mapping from instrument_id to desired absolute
            quantity.
        mode: Execution mode (``"LIVE"``, ``"PAPER"``, or ``"BACKTEST"``).
        as_of_date: Optional trading date for the step. Required for
            BACKTEST mode to process fills at the correct date.
        decision_id: Optional engine decision id that produced the
            orders.
        record_positions: If True, also persist a positions snapshot
            after fills are processed.

    Returns:
        :class:`ExecutionSummary` with counts of orders and fills.
    """

    # 1) Compute orders from current vs target positions.
    current_positions = broker.get_positions()
    orders = plan_orders(current_positions=current_positions, target_positions=target_positions)

    if not orders:
        logger.info("apply_execution_plan: no orders generated; nothing to do")
        # Optionally record a positions snapshot even if no orders.
        if record_positions and portfolio_id is not None and current_positions and as_of_date is not None:
            record_positions_snapshot(
                db_manager=db_manager,
                portfolio_id=portfolio_id,
                positions=current_positions,
                as_of_date=as_of_date,
                mode=mode,
            )
        return ExecutionSummary(num_orders=0, num_fills=0)

    if sells_first:
        orders = sorted(
            orders,
            key=lambda o: (
                0 if o.side == OrderSide.SELL else 1,
                o.instrument_id,
            ),
        )

    # 2) Submit orders via broker.
    for order in orders:
        broker.submit_order(order)

    # 3) Persist orders to DB.
    record_orders(
        db_manager=db_manager,
        portfolio_id=portfolio_id,
        orders=orders,
        mode=mode,
        decision_id=decision_id,
        as_of_date=as_of_date,
    )

    # 4) In BACKTEST mode, process fills synchronously using the
    # BacktestBroker + MarketSimulator.
    fills = []
    if mode.upper() == ExecutionMode.BACKTEST and isinstance(broker, BacktestBroker):
        if as_of_date is None:
            raise ValueError("apply_execution_plan: as_of_date is required for BACKTEST mode")
        fills = broker.process_fills(as_of_date)
        if fills:
            record_fills(db_manager=db_manager, fills=fills, mode=mode)

    # 5) Optionally record a positions snapshot after execution.
    if record_positions and portfolio_id is not None and as_of_date is not None:
        positions_after = broker.get_positions()
        if positions_after:
            record_positions_snapshot(
                db_manager=db_manager,
                portfolio_id=portfolio_id,
                positions=positions_after,
                as_of_date=as_of_date,
                mode=mode,
            )

    logger.info(
        "apply_execution_plan: mode=%s portfolio_id=%s orders=%d fills=%d",
        mode,
        portfolio_id,
        len(orders),
        len(fills),
    )

    return ExecutionSummary(num_orders=len(orders), num_fills=len(fills))
