"""Prometheus v2 – LiveBroker stub implementation.

This module defines :class:`LiveBroker`, a concrete
:class:`~prometheus.execution.broker_interface.BrokerInterface` intended
for LIVE trading against a real broker (e.g. IBKR Gateway/TWS).

For now this implementation is a *safe stub*:

* It provides the full BrokerInterface surface.
* All broker-facing methods raise ``NotImplementedError`` with a clear
  message indicating that real connectivity must be implemented in a
  deployment-specific adapter.

The goal is to:

* Keep the architecture and type surface consistent with the execution
  spec (015).
* Avoid accidentally routing real money orders through an incomplete
  implementation.

In later passes this module should be extended to wrap a concrete IBKR
adapter (or other broker) and to integrate tightly with the execution
storage helpers (orders/fills/executed_actions).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List

from apatheon.core.logging import get_logger

from prometheus.execution.broker_interface import (
    BrokerInterface,
    Fill,
    Order,
    OrderStatus,
    Position,
)
from prometheus.execution.ibkr_client import IbkrClient

logger = get_logger(__name__)


@dataclass
class LiveBroker(BrokerInterface):
    """Stub BrokerInterface implementation for LIVE trading.

    Parameters
    ----------
    account_id:
        Optional broker account identifier. Stored for logging only in
        this stub implementation.

    Notes
    -----
    All broker-facing methods currently raise ``NotImplementedError``.
    This is intentional to prevent accidental use in production before a
    real broker adapter has been integrated.
    """

    account_id: str | None = None
    client: IbkrClient | None = None

    # Local caches are defined for interface completeness and future
    # extensions but are not populated in this stub. They may be used by
    # future IbkrClient implementations if desired.
    _orders: Dict[str, Order] = field(default_factory=dict, init=False)
    _statuses: Dict[str, OrderStatus] = field(default_factory=dict, init=False)
    _fills: List[Fill] = field(default_factory=list, init=False)
    _positions: Dict[str, Position] = field(default_factory=dict, init=False)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_client(self) -> IbkrClient:
        """Return the configured IbkrClient or raise if missing.

        This keeps the default behaviour safe: unless a concrete
        IbkrClient is explicitly injected, LiveBroker cannot be used to
        route real orders.
        """

        if self.client is None:
            msg = (
                "LiveBroker has no IbkrClient configured. "
                "Inject a concrete IbkrClient implementation before use."
            )
            logger.error(msg)
            raise NotImplementedError(msg)
        return self.client

    # ------------------------------------------------------------------
    # BrokerInterface API (delegating to IbkrClient)
    # ------------------------------------------------------------------

    def submit_order(self, order: Order) -> str:  # pragma: no cover - stub
        client = self._require_client()
        return client.submit_order(order)

    def cancel_order(self, order_id: str) -> bool:  # pragma: no cover - stub
        client = self._require_client()
        return client.cancel_order(order_id)

    def get_order_status(self, order_id: str) -> OrderStatus:  # pragma: no cover - thin wrapper
        client = self._require_client()
        return client.get_order_status(order_id)

    def get_fills(self, since: datetime | None = None) -> List[Fill]:  # pragma: no cover - stub
        client = self._require_client()
        return client.get_fills(since)

    def get_positions(self) -> Dict[str, Position]:  # pragma: no cover - stub
        client = self._require_client()
        return client.get_positions()

    def get_account_state(self) -> Dict:
        client = self._require_client()
        return client.get_account_state()

    def sync(self) -> None:  # pragma: no cover - stub
        client = self._require_client()
        client.sync()
