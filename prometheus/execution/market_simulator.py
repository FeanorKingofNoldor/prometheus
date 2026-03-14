"""Prometheus v2 – Simple market simulator for backtesting.

This module implements a minimal :class:`MarketSimulator` used by the
``BacktestBroker`` to generate fills and maintain simulated positions and
account state in BACKTEST mode.

The implementation is intentionally conservative and EOD-only for the
initial iteration: all supported orders are filled using the daily close
price (plus optional slippage), and volume constraints are optional.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Dict, Iterable, List, Sequence

import pandas as pd

from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger
from prometheus.execution.broker_interface import Fill, Order, OrderSide, Position
from prometheus.execution.time_machine import TimeMachine


logger = get_logger(__name__)


@dataclass
class FillConfig:
    """Configuration for simple fill modelling.

    Attributes:
        market_slippage_bps: Slippage to apply to market orders in basis
            points. Positive values worsen the price for buys and improve
            it for sells.
        use_volume_constraints: When True, respect a simple participation
            cap based on ``max_participation_rate`` and the day's
            historical volume.
        max_participation_rate: Maximum fraction of daily volume allowed
            for any single order when ``use_volume_constraints`` is True.
    """

    market_slippage_bps: float = 0.0
    use_volume_constraints: bool = False
    max_participation_rate: float = 1.0


@dataclass
class MarketSimulator:
    """EOD market simulator backed by :class:`TimeMachine`.

    The simulator maintains an internal cash balance and per-instrument
    :class:`Position` objects. It relies on ``TimeMachine.get_data`` over
    ``prices_daily`` to obtain close prices (and optionally volumes) for a
    given date.
    """

    time_machine: TimeMachine
    initial_cash: float
    fill_config: FillConfig = field(default_factory=FillConfig)

    def __post_init__(self) -> None:  # pragma: no cover - trivial wiring
        self._cash: float = float(self.initial_cash)
        self._positions: Dict[str, Position] = {}

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------

    def simulate_fills(self, as_of_date: date, orders: Sequence[Order]) -> List[Fill]:
        """Simulate fills for a batch of orders on ``as_of_date``.

        For Iteration 1 all supported orders are filled at the EOD close
        price (plus optional slippage). Limit/stop semantics and partial
        fills can be added in later iterations.
        """

        if not orders:
            return []

        instrument_ids = sorted({o.instrument_id for o in orders})
        df = self.time_machine.get_data(
            "prices_daily",
            {
                "instrument_ids": instrument_ids,
                "start_date": as_of_date,
                "end_date": as_of_date,
            },
        )

        if df.empty:
            logger.warning(
                "MarketSimulator.simulate_fills: no prices for instruments %s on %s",
                instrument_ids,
                as_of_date,
            )

        price_map: Dict[str, float] = {}
        volume_map: Dict[str, float] = {}
        for _, row in df.iterrows():
            inst_id = str(row["instrument_id"])
            price_map[inst_id] = float(row["close"])
            volume_map[inst_id] = float(row.get("volume", 0.0))

        fills: List[Fill] = []
        ts = datetime.combine(as_of_date, time(23, 59, 0))

        for order in orders:
            base_price = price_map.get(order.instrument_id)
            if base_price is None or base_price <= 0.0:
                logger.warning(
                    "MarketSimulator.simulate_fills: missing or non-positive price for %s on %s, skipping order %s",
                    order.instrument_id,
                    as_of_date,
                    order.order_id,
                )
                continue

            qty = float(order.quantity)
            if qty <= 0.0:
                continue

            # Optional simple volume constraint.
            if self.fill_config.use_volume_constraints:
                daily_vol = volume_map.get(order.instrument_id, 0.0)
                max_qty = daily_vol * self.fill_config.max_participation_rate
                if daily_vol <= 0.0 or max_qty <= 0.0:
                    logger.warning(
                        "MarketSimulator.simulate_fills: zero volume for %s on %s, skipping order %s",
                        order.instrument_id,
                        as_of_date,
                        order.order_id,
                    )
                    continue
                if qty > max_qty:
                    qty = max_qty

            # Simple slippage model in basis points.
            slip_mult = 1.0
            if self.fill_config.market_slippage_bps != 0.0:
                bps = self.fill_config.market_slippage_bps / 10_000.0
                if order.side == OrderSide.BUY:
                    slip_mult = 1.0 + bps
                else:
                    slip_mult = 1.0 - bps

            exec_price = base_price * slip_mult

            # Update positions and cash balance.
            pos = self._positions.get(order.instrument_id)
            if order.side == OrderSide.BUY:
                notional = exec_price * qty
                if pos is None:
                    new_qty = qty
                    avg_cost = exec_price
                else:
                    new_qty = pos.quantity + qty
                    avg_cost = (
                        (pos.avg_cost * pos.quantity + notional) / new_qty
                        if new_qty != 0.0
                        else exec_price
                    )
                self._cash -= notional
            else:  # SELL
                notional = exec_price * qty
                if pos is None:
                    # Allow simple shorting: start from zero.
                    new_qty = -qty
                    avg_cost = exec_price
                else:
                    new_qty = pos.quantity - qty
                    avg_cost = pos.avg_cost
                self._cash += notional

            market_value = new_qty * exec_price
            unrealized_pnl = (exec_price - avg_cost) * new_qty
            self._positions[order.instrument_id] = Position(
                instrument_id=order.instrument_id,
                quantity=new_qty,
                avg_cost=avg_cost,
                market_value=market_value,
                unrealized_pnl=unrealized_pnl,
            )

            fills.append(
                Fill(
                    fill_id=generate_uuid(),
                    order_id=order.order_id,
                    instrument_id=order.instrument_id,
                    side=order.side,
                    quantity=qty,
                    price=exec_price,
                    timestamp=ts,
                    commission=0.0,
                    metadata=None,
                )
            )

        return fills

    # ------------------------------------------------------------------
    # State inspection helpers
    # ------------------------------------------------------------------

    def _reprice_positions(self, as_of_date: date) -> None:
        """Reprice existing positions using close prices on ``as_of_date``."""

        if not self._positions:
            return

        instrument_ids = sorted(self._positions.keys())
        df = self.time_machine.get_data(
            "prices_daily",
            {
                "instrument_ids": instrument_ids,
                "start_date": as_of_date,
                "end_date": as_of_date,
            },
        )
        if df.empty:
            return

        price_map = {str(row["instrument_id"]): float(row["close"]) for _, row in df.iterrows()}

        for inst_id, pos in list(self._positions.items()):
            price = price_map.get(inst_id)
            if price is None:
                continue
            market_value = pos.quantity * price
            unrealized_pnl = (price - pos.avg_cost) * pos.quantity
            self._positions[inst_id] = Position(
                instrument_id=inst_id,
                quantity=pos.quantity,
                avg_cost=pos.avg_cost,
                market_value=market_value,
                unrealized_pnl=unrealized_pnl,
            )

    def get_positions(self, as_of_date: date) -> Dict[str, Position]:
        """Return current positions repriced to ``as_of_date``."""

        self._reprice_positions(as_of_date)
        return dict(self._positions)

    def get_account_state(self, as_of_date: date) -> Dict[str, float]:
        """Return a simple account state dict for ``as_of_date``."""

        self._reprice_positions(as_of_date)
        equity = self._cash + sum(p.market_value for p in self._positions.values())
        return {"cash": float(self._cash), "equity": float(equity)}