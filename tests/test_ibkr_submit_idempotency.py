"""Broker-side idempotency for IbkrClientImpl.submit_order.

If a non-terminal trade with this orderRef already exists at IBKR (a
previous attempt crashed or timed out after placeOrder but before
persisting), submit_order must adopt the existing trade instead of
placing the order a second time.  Terminal trades (Filled/Cancelled/
Inactive) do NOT suppress a fresh submission.
"""

from __future__ import annotations

from types import SimpleNamespace

from prometheus.execution.broker_interface import (
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
)
from prometheus.execution.ibkr_client import IbkrConnectionConfig
from prometheus.execution.ibkr_client_impl import IbkrClientImpl


def _trade(order_ref: str, *, status: str, filled: float = 0.0, remaining: float = 10.0):
    return SimpleNamespace(
        order=SimpleNamespace(orderRef=order_ref, orderId=7, permId=99001122),
        orderStatus=SimpleNamespace(status=status, filled=filled, remaining=remaining),
    )


class _FakeIB:
    """Minimal IB stand-in: connected, with a fixed trade book."""

    def __init__(self, trades) -> None:
        self._trades = list(trades)
        self.placed: list = []

    def isConnected(self) -> bool:
        return True

    def trades(self):
        return list(self._trades)

    def qualifyContracts(self, contract):
        return [contract]

    def placeOrder(self, contract, ib_order):
        self.placed.append((contract, ib_order))
        return _trade(ib_order.orderRef, status="Submitted")


def _client(trades) -> IbkrClientImpl:
    client = IbkrClientImpl(config=IbkrConnectionConfig(client_id=999))
    client._ib = _FakeIB(trades)
    client._connected = True
    return client


def _order(order_id: str = "ord-2026-07-02-AAPL-1") -> Order:
    return Order(
        order_id=order_id,
        instrument_id="AAPL.US",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        # Pre-built contract skips the DB-backed instrument mapper.
        metadata={"contract": SimpleNamespace(symbol="AAPL", secType="STK")},
    )


def test_submit_order_suppresses_resubmission_for_working_trade():
    order = _order()
    client = _client([_trade(order.order_id, status="Submitted")])

    returned = client.submit_order(order)

    assert returned == order.order_id
    assert client._ib.placed == []  # nothing re-sent to the broker
    assert client._order_statuses[order.order_id] == OrderStatus.SUBMITTED
    assert client._trades_by_ref[order.order_id] is not None
    assert order.metadata["resubmission_suppressed"] is True
    assert order.metadata["ibkr"]["orderId"] == 7


def test_submit_order_places_when_existing_trade_is_terminal():
    order = _order()
    client = _client(
        [
            _trade(order.order_id, status="Cancelled"),
            _trade("someone-else", status="Submitted"),
        ]
    )

    returned = client.submit_order(order)

    assert returned == order.order_id
    assert len(client._ib.placed) == 1
    _, placed_order = client._ib.placed[0]
    assert placed_order.orderRef == order.order_id


def test_find_active_trade_ignores_terminal_statuses():
    order_id = "ord-x"
    for status in ("Filled", "Cancelled", "ApiCancelled", "Inactive"):
        client = _client([_trade(order_id, status=status)])
        assert client._find_active_trade(order_id) is None
    for status in ("Submitted", "PreSubmitted", "PendingSubmit"):
        client = _client([_trade(order_id, status=status)])
        assert client._find_active_trade(order_id) is not None
