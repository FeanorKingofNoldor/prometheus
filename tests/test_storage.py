"""Unit tests for execution storage helpers.

Tests cover record_orders, record_fills, record_positions_snapshot,
and update_order_statuses with mocked database connections.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

from prometheus.execution.broker_interface import (
    Fill,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
)
from prometheus.execution.storage import (
    ExecutionMode,
    _default_timestamp,
    record_fills,
    record_orders,
    record_positions_snapshot,
    update_order_statuses,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_db_manager():
    """Create a mock DatabaseManager with cursor tracking."""
    db = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    db.get_runtime_connection.return_value = mock_conn
    return db, mock_conn, mock_cursor


def _make_order(
    order_id: str = "ord-1",
    instrument_id: str = "AAPL",
    side: OrderSide = OrderSide.BUY,
    quantity: float = 10.0,
    limit_price: float | None = None,
    stop_price: float | None = None,
    metadata: dict | None = None,
) -> Order:
    return Order(
        order_id=order_id,
        instrument_id=instrument_id,
        side=side,
        order_type=OrderType.MARKET,
        quantity=quantity,
        limit_price=limit_price,
        stop_price=stop_price,
        metadata=metadata,
    )


def _make_fill(
    fill_id: str = "fill-1",
    order_id: str = "ord-1",
    instrument_id: str = "AAPL",
    side: OrderSide = OrderSide.BUY,
    quantity: float = 10.0,
    price: float = 150.0,
    commission: float = 1.0,
    metadata: dict | None = None,
) -> Fill:
    return Fill(
        fill_id=fill_id,
        order_id=order_id,
        instrument_id=instrument_id,
        side=side,
        quantity=quantity,
        price=price,
        timestamp=datetime(2025, 6, 1, 16, 0, 0, tzinfo=timezone.utc),
        commission=commission,
        metadata=metadata,
    )


# ---------------------------------------------------------------------------
# Tests: _default_timestamp
# ---------------------------------------------------------------------------


class TestDefaultTimestamp:

    def test_with_as_of_date(self):
        ts = _default_timestamp(date(2025, 6, 15))
        assert ts.year == 2025
        assert ts.month == 6
        assert ts.day == 15
        assert ts.hour == 23
        assert ts.minute == 59
        assert ts.tzinfo == timezone.utc

    def test_without_as_of_date(self):
        before = datetime.utcnow()
        ts = _default_timestamp(None)
        after = datetime.utcnow()
        assert before <= ts.replace(tzinfo=None) <= after


# ---------------------------------------------------------------------------
# Tests: record_orders
# ---------------------------------------------------------------------------


class TestRecordOrders:

    def test_inserts_single_order(self):
        db, conn, cursor = _mock_db_manager()
        order = _make_order()
        record_orders(
            db, portfolio_id="port-1", orders=[order], mode="LIVE"
        )
        cursor.execute.assert_called_once()
        args = cursor.execute.call_args[0][1]
        assert args[0] == "ord-1"  # order_id
        assert args[2] == "AAPL"  # instrument_id
        assert args[3] == "BUY"  # side
        assert args[8] == "SUBMITTED"  # initial status
        assert args[9] == "LIVE"  # mode
        assert args[10] == "port-1"  # portfolio_id
        conn.commit.assert_called_once()

    def test_inserts_multiple_orders(self):
        db, conn, cursor = _mock_db_manager()
        orders = [
            _make_order("ord-1", "AAPL"),
            _make_order("ord-2", "MSFT"),
            _make_order("ord-3", "GOOG"),
        ]
        record_orders(
            db, portfolio_id="port-1", orders=orders, mode="PAPER"
        )
        assert cursor.execute.call_count == 3
        conn.commit.assert_called_once()

    def test_empty_orders_noop(self):
        db, conn, cursor = _mock_db_manager()
        record_orders(
            db, portfolio_id="port-1", orders=[], mode="LIVE"
        )
        cursor.execute.assert_not_called()
        conn.commit.assert_not_called()

    def test_backtest_mode_uses_as_of_date_timestamp(self):
        db, conn, cursor = _mock_db_manager()
        order = _make_order()
        record_orders(
            db,
            portfolio_id="port-1",
            orders=[order],
            mode="BACKTEST",
            as_of_date=date(2025, 3, 15),
        )
        args = cursor.execute.call_args[0][1]
        ts = args[1]  # timestamp
        assert ts.year == 2025
        assert ts.month == 3
        assert ts.day == 15
        assert ts.hour == 23

    def test_custom_timestamp_in_metadata(self):
        db, conn, cursor = _mock_db_manager()
        order = _make_order(metadata={"timestamp": "2025-01-10T12:30:00+00:00"})
        record_orders(
            db, portfolio_id="port-1", orders=[order], mode="LIVE"
        )
        args = cursor.execute.call_args[0][1]
        ts = args[1]
        assert ts.year == 2025
        assert ts.month == 1
        assert ts.day == 10

    def test_decision_id_passed_through(self):
        db, conn, cursor = _mock_db_manager()
        order = _make_order()
        record_orders(
            db,
            portfolio_id="port-1",
            orders=[order],
            mode="LIVE",
            decision_id="dec-42",
        )
        args = cursor.execute.call_args[0][1]
        assert args[11] == "dec-42"

    def test_limit_and_stop_prices(self):
        db, conn, cursor = _mock_db_manager()
        order = _make_order(limit_price=155.0, stop_price=145.0)
        record_orders(
            db, portfolio_id="port-1", orders=[order], mode="LIVE"
        )
        args = cursor.execute.call_args[0][1]
        assert args[6] == 155.0  # limit_price
        assert args[7] == 145.0  # stop_price

    def test_none_prices_when_market_order(self):
        db, conn, cursor = _mock_db_manager()
        order = _make_order()
        record_orders(
            db, portfolio_id="port-1", orders=[order], mode="LIVE"
        )
        args = cursor.execute.call_args[0][1]
        assert args[6] is None  # limit_price
        assert args[7] is None  # stop_price

    def test_cursor_closed_after_execution(self):
        db, conn, cursor = _mock_db_manager()
        order = _make_order()
        record_orders(
            db, portfolio_id="port-1", orders=[order], mode="LIVE"
        )
        cursor.close.assert_called_once()


# ---------------------------------------------------------------------------
# Tests: record_fills
# ---------------------------------------------------------------------------


class TestRecordFills:

    def test_inserts_single_fill(self):
        db, conn, cursor = _mock_db_manager()
        fill = _make_fill()
        record_fills(db, fills=[fill], mode="LIVE")
        cursor.execute.assert_called_once()
        args = cursor.execute.call_args[0][1]
        assert args[0] == "fill-1"  # fill_id
        assert args[1] == "ord-1"  # order_id
        assert args[3] == "AAPL"  # instrument_id
        assert args[4] == "BUY"  # side
        assert args[5] == 10.0  # quantity
        assert args[6] == 150.0  # price
        assert args[7] == 1.0  # commission
        conn.commit.assert_called_once()

    def test_inserts_multiple_fills(self):
        db, conn, cursor = _mock_db_manager()
        fills = [
            _make_fill("fill-1", "ord-1"),
            _make_fill("fill-2", "ord-2"),
        ]
        record_fills(db, fills=fills, mode="PAPER")
        assert cursor.execute.call_count == 2

    def test_empty_fills_noop(self):
        db, conn, cursor = _mock_db_manager()
        record_fills(db, fills=[], mode="LIVE")
        cursor.execute.assert_not_called()

    def test_sql_has_on_conflict_do_nothing(self):
        """The INSERT statement uses ON CONFLICT (fill_id) DO NOTHING for dedup."""
        db, conn, cursor = _mock_db_manager()
        fill = _make_fill()
        record_fills(db, fills=[fill], mode="LIVE")
        sql = cursor.execute.call_args[0][0]
        assert "ON CONFLICT" in sql
        assert "DO NOTHING" in sql

    def test_fill_metadata_serialized(self):
        db, conn, cursor = _mock_db_manager()
        fill = _make_fill(metadata={"source": "test"})
        record_fills(db, fills=[fill], mode="LIVE")
        args = cursor.execute.call_args[0][1]
        # metadata is the last arg (index 9), wrapped in Json()
        payload = args[9]
        # psycopg2.extras.Json wraps the dict; check the adapted value
        assert payload.adapted == {"source": "test"}

    def test_fill_none_metadata_becomes_empty_dict(self):
        db, conn, cursor = _mock_db_manager()
        fill = _make_fill(metadata=None)
        record_fills(db, fills=[fill], mode="LIVE")
        args = cursor.execute.call_args[0][1]
        payload = args[9]
        assert payload.adapted == {}

    def test_cursor_closed_after_fills(self):
        db, conn, cursor = _mock_db_manager()
        fill = _make_fill()
        record_fills(db, fills=[fill], mode="LIVE")
        cursor.close.assert_called_once()


# ---------------------------------------------------------------------------
# Tests: record_positions_snapshot
# ---------------------------------------------------------------------------


class TestRecordPositionsSnapshot:

    def test_inserts_single_position(self):
        db, conn, cursor = _mock_db_manager()
        positions = {"AAPL": Position("AAPL", 100, 150.0, 15000.0, 500.0)}
        record_positions_snapshot(
            db,
            portfolio_id="port-1",
            positions=positions,
            as_of_date=date(2025, 6, 1),
            mode="LIVE",
        )
        cursor.execute.assert_called_once()
        args = cursor.execute.call_args[0][1]
        assert args[0] == "port-1"
        assert args[2] == date(2025, 6, 1)
        assert args[3] == "AAPL"
        assert args[4] == 100.0  # quantity
        assert args[5] == 150.0  # avg_cost
        assert args[6] == 15000.0  # market_value
        assert args[7] == 500.0  # unrealized_pnl
        assert args[8] == "LIVE"
        conn.commit.assert_called_once()

    def test_inserts_multiple_positions(self):
        db, conn, cursor = _mock_db_manager()
        positions = {
            "AAPL": Position("AAPL", 100, 150.0, 15000.0, 500.0),
            "MSFT": Position("MSFT", 50, 300.0, 15000.0, 200.0),
        }
        record_positions_snapshot(
            db,
            portfolio_id="port-1",
            positions=positions,
            as_of_date=date(2025, 6, 1),
            mode="PAPER",
        )
        assert cursor.execute.call_count == 2

    def test_empty_positions_noop(self):
        db, conn, cursor = _mock_db_manager()
        record_positions_snapshot(
            db,
            portfolio_id="port-1",
            positions={},
            as_of_date=date(2025, 6, 1),
            mode="LIVE",
        )
        cursor.execute.assert_not_called()

    def test_custom_timestamp(self):
        db, conn, cursor = _mock_db_manager()
        positions = {"AAPL": Position("AAPL", 100, 150.0, 15000.0, 500.0)}
        custom_ts = datetime(2025, 6, 1, 10, 30, 0, tzinfo=timezone.utc)
        record_positions_snapshot(
            db,
            portfolio_id="port-1",
            positions=positions,
            as_of_date=date(2025, 6, 1),
            mode="LIVE",
            timestamp=custom_ts,
        )
        args = cursor.execute.call_args[0][1]
        assert args[1] == custom_ts

    def test_default_timestamp_from_as_of_date(self):
        db, conn, cursor = _mock_db_manager()
        positions = {"AAPL": Position("AAPL", 100, 150.0, 15000.0, 0.0)}
        record_positions_snapshot(
            db,
            portfolio_id="port-1",
            positions=positions,
            as_of_date=date(2025, 3, 20),
            mode="BACKTEST",
        )
        args = cursor.execute.call_args[0][1]
        ts = args[1]
        assert ts.year == 2025
        assert ts.month == 3
        assert ts.day == 20
        assert ts.hour == 23

    def test_cursor_closed(self):
        db, conn, cursor = _mock_db_manager()
        positions = {"AAPL": Position("AAPL", 100, 150.0, 15000.0, 0.0)}
        record_positions_snapshot(
            db,
            portfolio_id="port-1",
            positions=positions,
            as_of_date=date(2025, 6, 1),
            mode="LIVE",
        )
        cursor.close.assert_called_once()


# ---------------------------------------------------------------------------
# Tests: update_order_statuses
# ---------------------------------------------------------------------------


class TestUpdateOrderStatuses:

    def test_updates_single_status(self):
        db, conn, cursor = _mock_db_manager()
        update_order_statuses(
            db, statuses={"ord-1": OrderStatus.FILLED}
        )
        cursor.execute.assert_called_once()
        args = cursor.execute.call_args[0][1]
        assert args[0] == "FILLED"
        assert args[1] == "ord-1"
        conn.commit.assert_called_once()

    def test_updates_multiple_statuses(self):
        db, conn, cursor = _mock_db_manager()
        update_order_statuses(
            db,
            statuses={
                "ord-1": OrderStatus.FILLED,
                "ord-2": OrderStatus.CANCELLED,
                "ord-3": OrderStatus.REJECTED,
            },
        )
        assert cursor.execute.call_count == 3

    def test_empty_statuses_noop(self):
        db, conn, cursor = _mock_db_manager()
        update_order_statuses(db, statuses={})
        cursor.execute.assert_not_called()

    def test_string_status_value(self):
        """Statuses can be passed as raw strings."""
        db, conn, cursor = _mock_db_manager()
        update_order_statuses(
            db, statuses={"ord-1": "FILLED"}
        )
        args = cursor.execute.call_args[0][1]
        assert args[0] == "FILLED"

    def test_cursor_closed(self):
        db, conn, cursor = _mock_db_manager()
        update_order_statuses(
            db, statuses={"ord-1": OrderStatus.FILLED}
        )
        cursor.close.assert_called_once()


# ---------------------------------------------------------------------------
# Tests: ExecutionMode constants
# ---------------------------------------------------------------------------


class TestExecutionMode:

    def test_mode_values(self):
        assert ExecutionMode.LIVE == "LIVE"
        assert ExecutionMode.PAPER == "PAPER"
        assert ExecutionMode.BACKTEST == "BACKTEST"
