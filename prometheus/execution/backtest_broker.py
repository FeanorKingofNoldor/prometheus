"""Prometheus v2 – BacktestBroker implementation.

This module implements :class:`BacktestBroker`, a concrete
:class:`~prometheus.execution.broker_interface.BrokerInterface` used for
backtesting. It delegates pricing and position management to a
:class:`~prometheus.execution.market_simulator.MarketSimulator` backed by
:class:`~prometheus.execution.time_machine.TimeMachine`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List

from apatheon.core.logging import get_logger

from prometheus.execution.broker_interface import (
    BrokerInterface,
    Fill,
    Order,
    OrderStatus,
    Position,
)
from prometheus.execution.market_simulator import MarketSimulator
from prometheus.execution.time_machine import TimeMachine

logger = get_logger(__name__)


@dataclass
class BacktestBroker(BrokerInterface):
    """BrokerInterface implementation for BACKTEST mode.

    The broker stores orders and fills in memory and relies on a
    :class:`MarketSimulator` for pricing and portfolio evolution. It is
    deliberately minimal and synchronous: all fills are generated when
    :meth:`process_fills` is called, typically once per simulated trading
    day.
    """

    time_machine: TimeMachine
    simulator: MarketSimulator

    _orders: Dict[str, Order] = field(default_factory=dict, init=False)
    _statuses: Dict[str, OrderStatus] = field(default_factory=dict, init=False)
    _fills: List[Fill] = field(default_factory=list, init=False)

    # ------------------------------------------------------------------
    # BrokerInterface methods
    # ------------------------------------------------------------------

    def submit_order(self, order: Order) -> str:
        if order.order_id in self._orders:
            raise ValueError(f"Order with id {order.order_id} already exists")

        self._orders[order.order_id] = order
        # In this implementation orders are considered immediately
        # submitted; fills are produced later by ``process_fills``.
        self._statuses[order.order_id] = OrderStatus.SUBMITTED
        logger.debug(
            "BacktestBroker.submit_order: submitted order %s %s x %.4f",
            order.instrument_id,
            order.side,
            order.quantity,
        )
        return order.order_id

    def cancel_order(self, order_id: str) -> bool:
        status = self._statuses.get(order_id)
        if status is None or status in {
            OrderStatus.FILLED,
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
        }:
            return False

        self._statuses[order_id] = OrderStatus.CANCELLED
        logger.debug("BacktestBroker.cancel_order: cancelled order %s", order_id)
        return True

    def get_order_status(self, order_id: str) -> OrderStatus:
        return self._statuses.get(order_id, OrderStatus.REJECTED)

    def get_fills(self, since: datetime | None = None) -> List[Fill]:
        if since is None:
            return list(self._fills)
        return [f for f in self._fills if f.timestamp > since]

    def get_positions(self) -> Dict[str, Position]:
        as_of = self.time_machine.current_date
        return self.simulator.get_positions(as_of)

    def get_account_state(self) -> Dict:
        as_of = self.time_machine.current_date
        return self.simulator.get_account_state(as_of)

    def sync(self) -> None:  # pragma: no cover - trivial wiring
        """Recompute valuations for the current date.

        For the backtest broker this simply triggers a repricing of
        existing positions via the underlying :class:`MarketSimulator`.
        """

        as_of = self.time_machine.current_date
        self.simulator.get_positions(as_of)

    # ------------------------------------------------------------------
    # Backtest-specific helpers
    # ------------------------------------------------------------------

    def process_fills(self, as_of_date: date) -> List[Fill]:
        """Generate fills for all open orders on ``as_of_date``.

        This method is specific to the BACKTEST broker and is intentionally
        not part of the abstract :class:`BrokerInterface`. It is invoked
        by backtesting code once per simulated trading day.
        """

        # Ensure the TimeMachine is aligned with the date we are
        # processing.
        self.time_machine.set_date(as_of_date)

        open_orders = [
            order
            for oid, order in self._orders.items()
            if self._statuses.get(oid) in {OrderStatus.PENDING, OrderStatus.SUBMITTED}
        ]
        if not open_orders:
            return []

        fills = self.simulator.simulate_fills(as_of_date, open_orders)
        if not fills:
            return []

        for fill in fills:
            self._fills.append(fill)
            self._statuses[fill.order_id] = OrderStatus.FILLED

        logger.debug(
            "BacktestBroker.process_fills: processed %d orders, generated %d fills on %s",
            len(open_orders),
            len(fills),
            as_of_date,
        )

        return fills
