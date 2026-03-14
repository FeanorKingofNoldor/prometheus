"""Prometheus v2 – IBKR client abstraction.

This module defines a small abstraction layer for Interactive Brokers
(IBKR) connectivity that can be used by :class:`LiveBroker` and
:class:`PaperBroker`.

The intent is to keep all IBKR-specific logic (connection management,
contract/order translation, event handling) behind a thin
:class:`IbkrClient` interface so that the rest of the execution layer can
remain broker-agnostic and easy to test.

For this iteration, :class:`IbkrClient` is an abstract base class; no
concrete implementation is provided. Integrations can implement this
interface using either ``ib_insync`` or the official ``ibapi`` package.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from prometheus.execution.broker_interface import Fill, Order, OrderStatus, Position


@dataclass(frozen=True)
class IbkrConnectionConfig:
    """Connection configuration for an IBKR client.

    Attributes
    ----------
    host:
        IBKR Gateway/TWS host, typically ``127.0.0.1``.
    port:
        TCP port (e.g. 7496 for live TWS, 4001 for paper by default).
    client_id:
        Client id for the API session; must be unique per client
        connection.
    account_id:
        Optional broker account identifier (e.g. ``U1234567``). Some
        setups route orders based on this.
    connect_timeout_sec:
        Timeout (in seconds) for establishing the API connection.
    readonly:
        When True, the client should avoid sending any order-modifying
        requests and only query state. This is useful for diagnostics.
    """

    host: str = "127.0.0.1"
    port: int = 7496
    client_id: int = 1
    account_id: Optional[str] = None
    connect_timeout_sec: int = 60
    readonly: bool = False


class IbkrClient(ABC):
    """Abstract IBKR client used by LIVE/PAPER brokers.

    A concrete implementation is responsible for:

    * Establishing and maintaining a connection to IBKR.
    * Translating Prometheus :class:`Order` objects into IBKR contracts
      + orders.
    * Submitting and cancelling orders.
    * Polling or streaming fills, positions, and account state.

    This interface deliberately mirrors the Prometheus-level
    :class:`BrokerInterface` operations rather than exposing raw IBKR API
    primitives.
    """

    def __init__(self, config: IbkrConnectionConfig) -> None:
        self._config = config

    @property
    def config(self) -> IbkrConnectionConfig:
        """Return the connection configuration."""

        return self._config

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    def connect(self) -> None:
        """Establish a connection to IBKR.

        Implementations should be idempotent: calling ``connect`` when
        already connected should be a no-op.
        """

    @abstractmethod
    def disconnect(self) -> None:
        """Close the current connection (if any)."""

    @abstractmethod
    def is_connected(self) -> bool:
        """Return True if the client is currently connected."""

    # ------------------------------------------------------------------
    # Order and execution API
    # ------------------------------------------------------------------

    @abstractmethod
    def submit_order(self, order: Order) -> str:
        """Submit an order and return the broker-assigned order id."""

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """Attempt to cancel an existing order.

        Returns True if the cancel request was accepted.
        """

    @abstractmethod
    def get_order_status(self, order_id: str) -> OrderStatus:
        """Return the broker's best-effort status for an order."""

    @abstractmethod
    def get_fills(self, since: Optional[datetime] = None) -> List[Fill]:
        """Return fills since the given timestamp (or all fills if None)."""

    @abstractmethod
    def get_positions(self) -> Dict[str, Position]:
        """Return current positions keyed by instrument_id."""

    @abstractmethod
    def get_account_state(self) -> Dict:
        """Return account-level information (cash, equity, margin, etc.)."""

    @abstractmethod
    def sync(self) -> None:
        """Synchronise local caches with IBKR (positions, orders, fills)."""
