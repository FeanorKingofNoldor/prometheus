"""Prometheus v2 – Execution broker interface abstractions.

This module defines the core broker-facing data structures and an
abstract :class:`BrokerInterface` used by both live/paper trading and the
backtesting ``BacktestBroker``.

The API is intentionally minimal and mode-agnostic so that portfolio and
backtesting code can interact with the same interface regardless of
whether orders are routed to a real broker or a simulated market.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List


class OrderSide(str, Enum):
    """Side of an order (buy or sell)."""

    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    """Supported order types.

    For Iteration 1 of the execution layer only ``MARKET`` orders are
    exercised in tests, but the enum is future-proofed for limits and
    stops.
    """

    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"


class OrderStatus(str, Enum):
    """Lifecycle status for an order."""

    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class OrderTimeInForce(str, Enum):
    """Time-in-force for an order."""

    DAY = "DAY"      # Expires at end of trading day
    GTC = "GTC"      # Good till cancelled
    IOC = "IOC"      # Immediate or cancel
    GTD = "GTD"      # Good till date (requires expiry)


@dataclass
class Order:
    """Logical order to buy or sell an instrument.

    Attributes:
        order_id: Unique identifier for the order.
        instrument_id: Identifier of the instrument to trade.
        side: Buy or sell.
        order_type: Order type (market/limit/stop/etc.).
        quantity: Absolute quantity in instrument units (e.g. shares).
        limit_price: Limit price for limit/stop-limit orders.
        stop_price: Trigger price for stop/stop-limit orders.
        tif: Time-in-force (DAY, GTC, IOC, GTD).
        combo_legs: For multi-leg orders (BAG), list of leg dicts with
            ``con_id``, ``action``, ``ratio``, ``exchange``.
        oca_group: One-cancels-all group name.  Orders with the same
            ``oca_group`` are linked so that filling one cancels the rest.
        metadata: Optional free-form metadata for diagnostics.
    """

    order_id: str
    instrument_id: str
    side: OrderSide
    order_type: OrderType
    quantity: float
    limit_price: float | None = None
    stop_price: float | None = None
    tif: OrderTimeInForce = OrderTimeInForce.DAY
    combo_legs: List[Dict] | None = None
    oca_group: str | None = None
    metadata: Dict | None = None


@dataclass
class Fill:
    """Execution fill for part or all of an order."""

    fill_id: str
    order_id: str
    instrument_id: str
    side: OrderSide
    quantity: float
    price: float
    timestamp: datetime
    commission: float = 0.0
    metadata: Dict | None = None


@dataclass
class Position:
    """Aggregated position for an instrument in a portfolio/account."""

    instrument_id: str
    quantity: float
    avg_cost: float
    market_value: float
    unrealized_pnl: float


class BrokerInterface(ABC):
    """Abstract interface for broker interactions.

    Concrete implementations include:

    * ``LiveBroker`` – routes orders to a live IBKR account.
    * ``PaperBroker`` – routes orders to an IBKR paper account.
    * ``BacktestBroker`` – simulates execution using historical data.
    """

    @abstractmethod
    def submit_order(self, order: Order) -> str:
        """Submit an order and return its ``order_id``."""

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """Attempt to cancel a pending order.

        Returns ``True`` if the order was successfully cancelled.
        """

    @abstractmethod
    def get_order_status(self, order_id: str) -> OrderStatus:
        """Return the current status of an order."""

    @abstractmethod
    def get_fills(self, since: datetime | None = None) -> List[Fill]:
        """Return fills since the given timestamp (or all fills if ``None``)."""

    @abstractmethod
    def get_positions(self) -> Dict[str, Position]:
        """Return current positions keyed by ``instrument_id``."""

    @abstractmethod
    def get_account_state(self) -> Dict:
        """Return account-level state (cash, equity, margin, etc.)."""

    @abstractmethod
    def sync(self) -> None:
        """Synchronise local state with the underlying broker.

        For live/paper brokers this typically pulls latest positions and
        fills from the external broker. For the backtest broker this is a
        no-op beyond recomputing valuations.
        """