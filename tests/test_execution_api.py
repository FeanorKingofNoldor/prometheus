"""Unit tests for apply_execution_plan.

Tests cover the full execution flow: order generation, submission,
status polling, fill handling, backtest mode, and DB persistence calls.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Dict, List
from unittest.mock import MagicMock, call, patch

import pytest

from prometheus.execution.broker_interface import (
    BrokerInterface,
    Fill,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
)
from prometheus.execution.api import ExecutionSummary, apply_execution_plan


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class StubBroker(BrokerInterface):
    """Configurable in-memory broker for testing apply_execution_plan."""

    def __init__(
        self,
        positions: Dict[str, Position] | None = None,
        account_state: Dict | None = None,
        fills: List[Fill] | None = None,
        order_statuses: Dict[str, OrderStatus] | None = None,
    ):
        self._positions = positions or {}
        self._account_state = account_state or {"equity": 100_000.0}
        self._fills = fills or []
        self._order_statuses = order_statuses or {}
        self.submitted: list[Order] = []
        self.cancelled: list[str] = []
        self.synced = False

    def submit_order(self, order: Order) -> str:
        self.submitted.append(order)
        return order.order_id

    def cancel_order(self, order_id: str) -> bool:
        self.cancelled.append(order_id)
        return True

    def get_order_status(self, order_id: str) -> OrderStatus:
        return self._order_statuses.get(order_id, OrderStatus.FILLED)

    def get_fills(self, since: datetime | None = None) -> List[Fill]:
        return list(self._fills)

    def get_positions(self) -> Dict[str, Position]:
        return dict(self._positions)

    def get_account_state(self) -> Dict:
        return dict(self._account_state)

    def sync(self) -> None:
        self.synced = True


def _make_fill(
    fill_id: str = "fill-1",
    order_id: str = "ord-1",
    instrument_id: str = "AAPL",
    side: OrderSide = OrderSide.BUY,
    quantity: float = 10.0,
    price: float = 150.0,
) -> Fill:
    return Fill(
        fill_id=fill_id,
        order_id=order_id,
        instrument_id=instrument_id,
        side=side,
        quantity=quantity,
        price=price,
        timestamp=datetime(2025, 6, 1, 16, 0, 0, tzinfo=timezone.utc),
    )


def _make_order(
    order_id: str = "ord-1",
    instrument_id: str = "AAPL",
    side: OrderSide = OrderSide.BUY,
    quantity: float = 10.0,
) -> Order:
    return Order(
        order_id=order_id,
        instrument_id=instrument_id,
        side=side,
        order_type=OrderType.MARKET,
        quantity=quantity,
    )


def _mock_db_manager():
    """Create a mock DatabaseManager with a mock runtime connection."""
    db = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    db.get_runtime_connection.return_value = mock_conn
    return db


# ---------------------------------------------------------------------------
# Tests: no orders
# ---------------------------------------------------------------------------


class TestNoOrders:

    @patch("prometheus.execution.api.plan_orders", return_value=[])
    @patch("prometheus.execution.api.record_positions_snapshot")
    def test_no_orders_returns_zero_counts(self, mock_snap, mock_plan):
        broker = StubBroker()
        db = _mock_db_manager()
        result = apply_execution_plan(
            db,
            broker=broker,
            portfolio_id="port-1",
            target_positions={},
            mode="BACKTEST",
            as_of_date=date(2025, 6, 1),
        )
        assert result == ExecutionSummary(num_orders=0, num_fills=0)

    @patch("prometheus.execution.api.plan_orders", return_value=[])
    @patch("prometheus.execution.api.record_positions_snapshot")
    def test_no_orders_still_records_positions_snapshot(self, mock_snap, mock_plan):
        """When record_positions=True and there are current positions, snapshot is recorded."""
        positions = {"AAPL": Position("AAPL", 100, 150.0, 15000.0, 0.0)}
        broker = StubBroker(positions=positions)
        db = _mock_db_manager()
        apply_execution_plan(
            db,
            broker=broker,
            portfolio_id="port-1",
            target_positions={},
            mode="BACKTEST",
            as_of_date=date(2025, 6, 1),
        )
        mock_snap.assert_called_once()

    @patch("prometheus.execution.api.plan_orders", return_value=[])
    @patch("prometheus.execution.api.record_positions_snapshot")
    def test_no_orders_no_snapshot_when_no_portfolio(self, mock_snap, mock_plan):
        broker = StubBroker()
        db = _mock_db_manager()
        apply_execution_plan(
            db,
            broker=broker,
            portfolio_id=None,
            target_positions={},
            mode="BACKTEST",
            as_of_date=date(2025, 6, 1),
        )
        mock_snap.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: order submission
# ---------------------------------------------------------------------------


class TestOrderSubmission:

    @patch("prometheus.execution.api.record_executed_actions_for_fills")
    @patch("prometheus.execution.api.update_order_statuses")
    @patch("prometheus.execution.api.record_positions_snapshot")
    @patch("prometheus.execution.api.record_fills")
    @patch("prometheus.execution.api.record_orders")
    @patch("prometheus.execution.api.plan_orders")
    def test_orders_submitted_to_broker(
        self, mock_plan, mock_rec_orders, mock_rec_fills, mock_snap, mock_update, mock_exec_actions
    ):
        orders = [_make_order("ord-1"), _make_order("ord-2", instrument_id="MSFT")]
        mock_plan.return_value = orders

        fills = [
            _make_fill("fill-1", "ord-1"),
            _make_fill("fill-2", "ord-2", instrument_id="MSFT"),
        ]
        broker = StubBroker(fills=fills)
        db = _mock_db_manager()

        result = apply_execution_plan(
            db,
            broker=broker,
            portfolio_id="port-1",
            target_positions={"AAPL": 10, "MSFT": 10},
            mode="PAPER",
            as_of_date=date(2025, 6, 1),
            status_poll_timeout_sec=0.1,
            status_poll_interval_sec=0.05,
        )
        assert len(broker.submitted) == 2
        assert result.num_orders == 2

    @patch("prometheus.execution.api.record_executed_actions_for_fills")
    @patch("prometheus.execution.api.update_order_statuses")
    @patch("prometheus.execution.api.record_positions_snapshot")
    @patch("prometheus.execution.api.record_fills")
    @patch("prometheus.execution.api.record_orders")
    @patch("prometheus.execution.api.plan_orders")
    def test_orders_persisted_to_db(
        self, mock_plan, mock_rec_orders, mock_rec_fills, mock_snap, mock_update, mock_exec_actions
    ):
        orders = [_make_order("ord-1")]
        mock_plan.return_value = orders
        broker = StubBroker(fills=[_make_fill("fill-1", "ord-1")])
        db = _mock_db_manager()

        apply_execution_plan(
            db,
            broker=broker,
            portfolio_id="port-1",
            target_positions={"AAPL": 10},
            mode="LIVE",
            as_of_date=date(2025, 6, 1),
            status_poll_timeout_sec=0.1,
            status_poll_interval_sec=0.05,
        )
        mock_rec_orders.assert_called_once()
        assert mock_rec_orders.call_args.kwargs["orders"] == orders

    @patch("prometheus.execution.api.record_executed_actions_for_fills")
    @patch("prometheus.execution.api.update_order_statuses")
    @patch("prometheus.execution.api.record_positions_snapshot")
    @patch("prometheus.execution.api.record_fills")
    @patch("prometheus.execution.api.record_orders")
    @patch("prometheus.execution.api.plan_orders")
    def test_broker_sync_called(
        self, mock_plan, mock_rec_orders, mock_rec_fills, mock_snap, mock_update, mock_exec_actions
    ):
        mock_plan.return_value = []
        broker = StubBroker()
        db = _mock_db_manager()

        apply_execution_plan(
            db,
            broker=broker,
            portfolio_id="port-1",
            target_positions={},
            mode="PAPER",
        )
        assert broker.synced is True


# ---------------------------------------------------------------------------
# Tests: status polling (LIVE/PAPER)
# ---------------------------------------------------------------------------


class TestStatusPolling:

    @patch("prometheus.execution.api.record_executed_actions_for_fills")
    @patch("prometheus.execution.api.update_order_statuses")
    @patch("prometheus.execution.api.record_positions_snapshot")
    @patch("prometheus.execution.api.record_fills")
    @patch("prometheus.execution.api.record_orders")
    @patch("prometheus.execution.api.plan_orders")
    def test_terminal_status_stops_polling(
        self, mock_plan, mock_rec_orders, mock_rec_fills, mock_snap, mock_update, mock_exec_actions
    ):
        orders = [_make_order("ord-1")]
        mock_plan.return_value = orders
        broker = StubBroker(
            order_statuses={"ord-1": OrderStatus.FILLED},
            fills=[_make_fill("fill-1", "ord-1")],
        )
        db = _mock_db_manager()

        result = apply_execution_plan(
            db,
            broker=broker,
            portfolio_id="port-1",
            target_positions={"AAPL": 10},
            mode="PAPER",
            status_poll_timeout_sec=0.5,
            status_poll_interval_sec=0.05,
        )
        assert result.num_fills == 1
        mock_update.assert_called_once()

    @patch("prometheus.execution.api.record_executed_actions_for_fills")
    @patch("prometheus.execution.api.update_order_statuses")
    @patch("prometheus.execution.api.record_positions_snapshot")
    @patch("prometheus.execution.api.record_fills")
    @patch("prometheus.execution.api.record_orders")
    @patch("prometheus.execution.api.plan_orders")
    def test_cancelled_status_is_terminal(
        self, mock_plan, mock_rec_orders, mock_rec_fills, mock_snap, mock_update, mock_exec_actions
    ):
        orders = [_make_order("ord-1")]
        mock_plan.return_value = orders
        broker = StubBroker(
            order_statuses={"ord-1": OrderStatus.CANCELLED},
            fills=[],
        )
        db = _mock_db_manager()

        result = apply_execution_plan(
            db,
            broker=broker,
            portfolio_id="port-1",
            target_positions={"AAPL": 10},
            mode="PAPER",
            status_poll_timeout_sec=0.5,
            status_poll_interval_sec=0.05,
        )
        assert result.num_fills == 0

    @patch("prometheus.execution.api.record_executed_actions_for_fills")
    @patch("prometheus.execution.api.update_order_statuses")
    @patch("prometheus.execution.api.record_positions_snapshot")
    @patch("prometheus.execution.api.record_fills")
    @patch("prometheus.execution.api.record_orders")
    @patch("prometheus.execution.api.plan_orders")
    def test_pending_orders_cancelled_after_timeout(
        self, mock_plan, mock_rec_orders, mock_rec_fills, mock_snap, mock_update, mock_exec_actions
    ):
        """Orders that remain PENDING after timeout are cancelled."""
        orders = [_make_order("ord-1")]
        mock_plan.return_value = orders
        broker = StubBroker(
            order_statuses={"ord-1": OrderStatus.SUBMITTED},  # Never goes terminal
            fills=[],
        )
        db = _mock_db_manager()

        apply_execution_plan(
            db,
            broker=broker,
            portfolio_id="port-1",
            target_positions={"AAPL": 10},
            mode="PAPER",
            status_poll_timeout_sec=0.1,
            status_poll_interval_sec=0.05,
        )
        assert "ord-1" in broker.cancelled


# ---------------------------------------------------------------------------
# Tests: fill handling
# ---------------------------------------------------------------------------


class TestFillHandling:

    @patch("prometheus.execution.api.record_executed_actions_for_fills")
    @patch("prometheus.execution.api.update_order_statuses")
    @patch("prometheus.execution.api.record_positions_snapshot")
    @patch("prometheus.execution.api.record_fills")
    @patch("prometheus.execution.api.record_orders")
    @patch("prometheus.execution.api.plan_orders")
    def test_fills_recorded(
        self, mock_plan, mock_rec_orders, mock_rec_fills, mock_snap, mock_update, mock_exec_actions
    ):
        orders = [_make_order("ord-1")]
        mock_plan.return_value = orders
        fills = [_make_fill("fill-1", "ord-1")]
        broker = StubBroker(fills=fills)
        db = _mock_db_manager()

        apply_execution_plan(
            db,
            broker=broker,
            portfolio_id="port-1",
            target_positions={"AAPL": 10},
            mode="PAPER",
            status_poll_timeout_sec=0.1,
            status_poll_interval_sec=0.05,
        )
        mock_rec_fills.assert_called_once()

    @patch("prometheus.execution.api.record_executed_actions_for_fills")
    @patch("prometheus.execution.api.update_order_statuses")
    @patch("prometheus.execution.api.record_positions_snapshot")
    @patch("prometheus.execution.api.record_fills")
    @patch("prometheus.execution.api.record_orders")
    @patch("prometheus.execution.api.plan_orders")
    def test_fills_filtered_to_submission_batch(
        self, mock_plan, mock_rec_orders, mock_rec_fills, mock_snap, mock_update, mock_exec_actions
    ):
        """Fills from other order batches are excluded."""
        orders = [_make_order("ord-1")]
        mock_plan.return_value = orders
        fills = [
            _make_fill("fill-1", "ord-1"),
            _make_fill("fill-other", "ord-other"),  # not in this batch
        ]
        broker = StubBroker(fills=fills)
        db = _mock_db_manager()

        result = apply_execution_plan(
            db,
            broker=broker,
            portfolio_id="port-1",
            target_positions={"AAPL": 10},
            mode="PAPER",
            status_poll_timeout_sec=0.1,
            status_poll_interval_sec=0.05,
        )
        # Only the fill matching ord-1 should be counted.
        assert result.num_fills == 1

    @patch("prometheus.execution.api.record_executed_actions_for_fills")
    @patch("prometheus.execution.api.update_order_statuses")
    @patch("prometheus.execution.api.record_positions_snapshot")
    @patch("prometheus.execution.api.record_fills")
    @patch("prometheus.execution.api.record_orders")
    @patch("prometheus.execution.api.plan_orders")
    def test_duplicate_fills_deduplicated(
        self, mock_plan, mock_rec_orders, mock_rec_fills, mock_snap, mock_update, mock_exec_actions
    ):
        orders = [_make_order("ord-1")]
        mock_plan.return_value = orders
        fill = _make_fill("fill-1", "ord-1")
        broker = StubBroker(fills=[fill, fill])  # same fill_id twice
        db = _mock_db_manager()

        result = apply_execution_plan(
            db,
            broker=broker,
            portfolio_id="port-1",
            target_positions={"AAPL": 10},
            mode="PAPER",
            status_poll_timeout_sec=0.1,
            status_poll_interval_sec=0.05,
        )
        assert result.num_fills == 1


# ---------------------------------------------------------------------------
# Tests: backtest mode
# ---------------------------------------------------------------------------


class TestBacktestMode:

    @patch("prometheus.execution.api.update_order_statuses")
    @patch("prometheus.execution.api.record_positions_snapshot")
    @patch("prometheus.execution.api.record_fills")
    @patch("prometheus.execution.api.record_orders")
    @patch("prometheus.execution.api.plan_orders")
    def test_backtest_calls_process_fills(
        self, mock_plan, mock_rec_orders, mock_rec_fills, mock_snap, mock_update
    ):
        orders = [_make_order("ord-1")]
        mock_plan.return_value = orders

        mock_bt_broker = MagicMock()
        mock_bt_broker.__class__ = type("BacktestBroker", (), {})
        # Make isinstance check work by patching the import.
        from prometheus.execution import backtest_broker

        fills = [_make_fill("fill-1", "ord-1")]
        mock_bt_broker.process_fills.return_value = fills
        mock_bt_broker.get_positions.return_value = {}
        mock_bt_broker.get_order_status.return_value = OrderStatus.FILLED

        db = _mock_db_manager()

        with patch("prometheus.execution.api.isinstance", side_effect=lambda obj, cls: True):
            # Use actual isinstance by checking directly
            pass

        # Instead, let's use a proper mock that passes isinstance
        bt_broker = MagicMock(spec=backtest_broker.BacktestBroker)
        bt_broker.process_fills.return_value = fills
        bt_broker.get_positions.return_value = {}
        bt_broker.get_order_status.return_value = OrderStatus.FILLED

        result = apply_execution_plan(
            db,
            broker=bt_broker,
            portfolio_id="port-1",
            target_positions={"AAPL": 10},
            mode="BACKTEST",
            as_of_date=date(2025, 6, 1),
        )
        bt_broker.process_fills.assert_called_once_with(date(2025, 6, 1))
        assert result.num_fills == 1

    @patch("prometheus.execution.api.update_order_statuses")
    @patch("prometheus.execution.api.record_positions_snapshot")
    @patch("prometheus.execution.api.record_fills")
    @patch("prometheus.execution.api.record_orders")
    @patch("prometheus.execution.api.plan_orders")
    def test_backtest_requires_as_of_date(
        self, mock_plan, mock_rec_orders, mock_rec_fills, mock_snap, mock_update
    ):
        orders = [_make_order("ord-1")]
        mock_plan.return_value = orders

        from prometheus.execution import backtest_broker

        bt_broker = MagicMock(spec=backtest_broker.BacktestBroker)
        bt_broker.get_positions.return_value = {}

        db = _mock_db_manager()

        with pytest.raises(ValueError, match="as_of_date is required"):
            apply_execution_plan(
                db,
                broker=bt_broker,
                portfolio_id="port-1",
                target_positions={"AAPL": 10},
                mode="BACKTEST",
                as_of_date=None,
            )

    @patch("prometheus.execution.api.update_order_statuses")
    @patch("prometheus.execution.api.record_positions_snapshot")
    @patch("prometheus.execution.api.record_fills")
    @patch("prometheus.execution.api.record_orders")
    @patch("prometheus.execution.api.plan_orders")
    def test_backtest_no_fills(
        self, mock_plan, mock_rec_orders, mock_rec_fills, mock_snap, mock_update
    ):
        orders = [_make_order("ord-1")]
        mock_plan.return_value = orders

        from prometheus.execution import backtest_broker

        bt_broker = MagicMock(spec=backtest_broker.BacktestBroker)
        bt_broker.process_fills.return_value = []
        bt_broker.get_positions.return_value = {}
        bt_broker.get_order_status.return_value = OrderStatus.SUBMITTED

        db = _mock_db_manager()
        result = apply_execution_plan(
            db,
            broker=bt_broker,
            portfolio_id="port-1",
            target_positions={"AAPL": 10},
            mode="BACKTEST",
            as_of_date=date(2025, 6, 1),
        )
        assert result.num_fills == 0
        mock_rec_fills.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: positions snapshot
# ---------------------------------------------------------------------------


class TestPositionsSnapshot:

    @patch("prometheus.execution.api.record_executed_actions_for_fills")
    @patch("prometheus.execution.api.update_order_statuses")
    @patch("prometheus.execution.api.record_positions_snapshot")
    @patch("prometheus.execution.api.record_fills")
    @patch("prometheus.execution.api.record_orders")
    @patch("prometheus.execution.api.plan_orders")
    def test_positions_snapshot_recorded_after_execution(
        self, mock_plan, mock_rec_orders, mock_rec_fills, mock_snap, mock_update, mock_exec_actions
    ):
        orders = [_make_order("ord-1")]
        mock_plan.return_value = orders
        fills = [_make_fill("fill-1", "ord-1")]
        positions = {"AAPL": Position("AAPL", 10, 150.0, 1500.0, 0.0)}
        broker = StubBroker(fills=fills, positions=positions)
        db = _mock_db_manager()

        apply_execution_plan(
            db,
            broker=broker,
            portfolio_id="port-1",
            target_positions={"AAPL": 10},
            mode="PAPER",
            as_of_date=date(2025, 6, 1),
            record_positions=True,
            status_poll_timeout_sec=0.1,
            status_poll_interval_sec=0.05,
        )
        mock_snap.assert_called_once()
        assert mock_snap.call_args.kwargs["portfolio_id"] == "port-1"

    @patch("prometheus.execution.api.record_executed_actions_for_fills")
    @patch("prometheus.execution.api.update_order_statuses")
    @patch("prometheus.execution.api.record_positions_snapshot")
    @patch("prometheus.execution.api.record_fills")
    @patch("prometheus.execution.api.record_orders")
    @patch("prometheus.execution.api.plan_orders")
    def test_no_snapshot_when_record_positions_false(
        self, mock_plan, mock_rec_orders, mock_rec_fills, mock_snap, mock_update, mock_exec_actions
    ):
        orders = [_make_order("ord-1")]
        mock_plan.return_value = orders
        broker = StubBroker(fills=[_make_fill("fill-1", "ord-1")])
        db = _mock_db_manager()

        apply_execution_plan(
            db,
            broker=broker,
            portfolio_id="port-1",
            target_positions={"AAPL": 10},
            mode="PAPER",
            as_of_date=date(2025, 6, 1),
            record_positions=False,
            status_poll_timeout_sec=0.1,
            status_poll_interval_sec=0.05,
        )
        mock_snap.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: sells_first ordering
# ---------------------------------------------------------------------------


class TestSellsFirst:

    @patch("prometheus.execution.api.record_executed_actions_for_fills")
    @patch("prometheus.execution.api.update_order_statuses")
    @patch("prometheus.execution.api.record_positions_snapshot")
    @patch("prometheus.execution.api.record_fills")
    @patch("prometheus.execution.api.record_orders")
    @patch("prometheus.execution.api.plan_orders")
    def test_sells_submitted_before_buys(
        self, mock_plan, mock_rec_orders, mock_rec_fills, mock_snap, mock_update, mock_exec_actions
    ):
        buy = _make_order("ord-buy", instrument_id="MSFT", side=OrderSide.BUY)
        sell = _make_order("ord-sell", instrument_id="AAPL", side=OrderSide.SELL)
        mock_plan.return_value = [buy, sell]

        broker = StubBroker(fills=[])
        db = _mock_db_manager()

        apply_execution_plan(
            db,
            broker=broker,
            portfolio_id="port-1",
            target_positions={"AAPL": 0, "MSFT": 10},
            mode="PAPER",
            sells_first=True,
            status_poll_timeout_sec=0.1,
            status_poll_interval_sec=0.05,
        )
        # Sells should be submitted first.
        assert broker.submitted[0].side == OrderSide.SELL
        assert broker.submitted[1].side == OrderSide.BUY
