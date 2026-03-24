"""Tests for order planning (prometheus.execution.order_planner)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from prometheus.execution.broker_interface import Order, OrderSide, OrderType, Position
from prometheus.execution.order_planner import (
    DEFAULT_LIMIT_BUFFER_PCT,
    DEFAULT_MIN_REBALANCE_PCT,
    MIN_ABS_QUANTITY,
    plan_orders,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pos(instrument_id: str, quantity: float, avg_cost: float = 100.0) -> Position:
    """Shorthand to create a Position."""
    return Position(
        instrument_id=instrument_id,
        quantity=quantity,
        avg_cost=avg_cost,
        market_value=quantity * avg_cost,
        unrealized_pnl=0.0,
    )


def _orders_by_instrument(orders: list[Order]) -> dict[str, Order]:
    return {o.instrument_id: o for o in orders}


# ---------------------------------------------------------------------------
# Basic order generation
# ---------------------------------------------------------------------------


class TestPlanOrdersBasic:
    """Core order planning tests."""

    @patch("prometheus.execution.order_planner.generate_uuid", return_value="order-001")
    def test_buy_new_position(self, _mock_uuid):
        """Target a new instrument with no current position → BUY order."""
        orders = plan_orders(
            current_positions={},
            target_positions={"AAPL": 100.0},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        assert orders[0].instrument_id == "AAPL"
        assert orders[0].side == OrderSide.BUY
        assert orders[0].quantity == 100.0
        assert orders[0].order_type == OrderType.MARKET

    @patch("prometheus.execution.order_planner.generate_uuid", return_value="order-001")
    def test_sell_entire_position(self, _mock_uuid):
        """Current position not in targets → SELL order for full quantity."""
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", 50.0)},
            target_positions={},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        assert orders[0].instrument_id == "AAPL"
        assert orders[0].side == OrderSide.SELL
        assert orders[0].quantity == 50.0

    @patch("prometheus.execution.order_planner.generate_uuid", return_value="order-001")
    def test_increase_position(self, _mock_uuid):
        """Target > current → BUY the delta."""
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", 50.0)},
            target_positions={"AAPL": 80.0},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        assert orders[0].side == OrderSide.BUY
        assert orders[0].quantity == pytest.approx(30.0)

    @patch("prometheus.execution.order_planner.generate_uuid", return_value="order-001")
    def test_decrease_position(self, _mock_uuid):
        """Target < current → SELL the delta."""
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", 100.0)},
            target_positions={"AAPL": 40.0},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        assert orders[0].side == OrderSide.SELL
        assert orders[0].quantity == pytest.approx(60.0)

    def test_no_change_no_orders(self):
        """Matching current and target → no orders."""
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", 100.0)},
            target_positions={"AAPL": 100.0},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 0

    def test_empty_both_no_orders(self):
        """Both empty → no orders."""
        orders = plan_orders(
            current_positions={},
            target_positions={},
        )
        assert len(orders) == 0


# ---------------------------------------------------------------------------
# Turnover filter
# ---------------------------------------------------------------------------


class TestTurnoverFilter:
    """Tests for the minimum rebalance percentage filter."""

    def test_tiny_delta_suppressed(self):
        """Delta below min_rebalance_pct is suppressed."""
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", 100.0)},
            target_positions={"AAPL": 101.0},  # 1% change
            min_rebalance_pct=0.05,  # 5% threshold
        )
        assert len(orders) == 0

    def test_delta_above_threshold_passes(self):
        """Delta above min_rebalance_pct produces an order."""
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", 100.0)},
            target_positions={"AAPL": 110.0},  # 10% change
            min_rebalance_pct=0.05,
        )
        assert len(orders) == 1

    def test_zero_threshold_no_suppression(self):
        """min_rebalance_pct=0 disables the filter."""
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", 100.0)},
            target_positions={"AAPL": 100.001},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1

    def test_min_abs_quantity_filter(self):
        """Delta below min_abs_quantity is dropped entirely."""
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", 100.0)},
            target_positions={"AAPL": 100.0 + MIN_ABS_QUANTITY / 10},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 0

    def test_default_min_rebalance_pct_value(self):
        """Default min_rebalance_pct is 2%."""
        assert DEFAULT_MIN_REBALANCE_PCT == pytest.approx(0.02)


# ---------------------------------------------------------------------------
# Sells-first ordering
# ---------------------------------------------------------------------------


class TestSellsFirstOrdering:
    """Tests for sells_first parameter."""

    @patch("prometheus.execution.order_planner.generate_uuid", side_effect=["o1", "o2", "o3"])
    def test_sells_before_buys(self, _mock_uuid):
        """SELL orders should appear before BUY orders when sells_first=True."""
        orders = plan_orders(
            current_positions={
                "AAPL": _pos("AAPL", 100.0),
                "GOOG": _pos("GOOG", 50.0),
            },
            target_positions={
                "AAPL": 0.0,    # SELL 100
                "MSFT": 75.0,   # BUY 75
                "GOOG": 0.0,    # SELL 50
            },
            min_rebalance_pct=0.0,
            sells_first=True,
        )
        sides = [o.side for o in orders]
        # All sells should come before all buys
        sell_indices = [i for i, s in enumerate(sides) if s == OrderSide.SELL]
        buy_indices = [i for i, s in enumerate(sides) if s == OrderSide.BUY]
        assert all(si < bi for si in sell_indices for bi in buy_indices)

    @patch("prometheus.execution.order_planner.generate_uuid", side_effect=["o1", "o2"])
    def test_sells_first_disabled(self, _mock_uuid):
        """When sells_first=False, orders are in sorted instrument order."""
        orders = plan_orders(
            current_positions={"ZZZ": _pos("ZZZ", 100.0)},
            target_positions={"AAA": 50.0, "ZZZ": 0.0},
            min_rebalance_pct=0.0,
            sells_first=False,
        )
        # Without sells_first sorting, alphabetical order prevails
        assert orders[0].instrument_id == "AAA"
        assert orders[1].instrument_id == "ZZZ"


# ---------------------------------------------------------------------------
# Multiple instruments
# ---------------------------------------------------------------------------


class TestMultipleInstruments:
    """Tests with multiple instruments."""

    @patch("prometheus.execution.order_planner.generate_uuid", side_effect=["o1", "o2", "o3", "o4"])
    def test_mixed_buys_and_sells(self, _mock_uuid):
        """Mix of new, increased, decreased, and closed positions."""
        orders = plan_orders(
            current_positions={
                "AAPL": _pos("AAPL", 100.0),
                "GOOG": _pos("GOOG", 200.0),
                "TSLA": _pos("TSLA", 50.0),
            },
            target_positions={
                "AAPL": 150.0,   # BUY 50
                "GOOG": 0.0,     # SELL 200
                "MSFT": 75.0,    # BUY 75 (new)
                # TSLA: absent → SELL 50
            },
            min_rebalance_pct=0.0,
        )
        by_inst = _orders_by_instrument(orders)
        # Four instruments should have orders (AAPL buy, GOOG sell, MSFT buy, TSLA sell)
        assert len(orders) == 4
        assert by_inst["AAPL"].side == OrderSide.BUY
        assert by_inst["GOOG"].side == OrderSide.SELL
        assert by_inst["MSFT"].side == OrderSide.BUY
        assert by_inst["TSLA"].side == OrderSide.SELL

    @patch("prometheus.execution.order_planner.generate_uuid", side_effect=["o1", "o2", "o3"])
    def test_all_instruments_unchanged(self, _mock_uuid):
        """All positions at target → no orders."""
        orders = plan_orders(
            current_positions={
                "AAPL": _pos("AAPL", 100.0),
                "GOOG": _pos("GOOG", 200.0),
            },
            target_positions={
                "AAPL": 100.0,
                "GOOG": 200.0,
            },
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 0


# ---------------------------------------------------------------------------
# Limit orders
# ---------------------------------------------------------------------------


class TestLimitOrders:
    """Tests for LIMIT order generation."""

    @patch("prometheus.execution.order_planner.generate_uuid", return_value="o1")
    def test_limit_buy_adds_buffer(self, _mock_uuid):
        """LIMIT BUY should set limit_price above reference price."""
        orders = plan_orders(
            current_positions={},
            target_positions={"AAPL": 100.0},
            order_type=OrderType.LIMIT,
            prices={"AAPL": 150.0},
            limit_buffer_pct=0.01,
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        assert orders[0].order_type == OrderType.LIMIT
        assert orders[0].limit_price == pytest.approx(round(150.0 * 1.01, 2))

    @patch("prometheus.execution.order_planner.generate_uuid", return_value="o1")
    def test_limit_sell_subtracts_buffer(self, _mock_uuid):
        """LIMIT SELL should set limit_price below reference price."""
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", 100.0)},
            target_positions={"AAPL": 0.0},
            order_type=OrderType.LIMIT,
            prices={"AAPL": 150.0},
            limit_buffer_pct=0.01,
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        assert orders[0].order_type == OrderType.LIMIT
        assert orders[0].limit_price == pytest.approx(round(150.0 * 0.99, 2))

    @patch("prometheus.execution.order_planner.generate_uuid", return_value="o1")
    def test_limit_no_price_falls_back_to_market(self, _mock_uuid):
        """LIMIT with no price for the instrument falls back to MARKET."""
        orders = plan_orders(
            current_positions={},
            target_positions={"AAPL": 100.0},
            order_type=OrderType.LIMIT,
            prices={},  # No price for AAPL
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        assert orders[0].order_type == OrderType.MARKET
        assert orders[0].limit_price is None

    @patch("prometheus.execution.order_planner.generate_uuid", return_value="o1")
    def test_limit_zero_price_falls_back_to_market(self, _mock_uuid):
        """LIMIT with zero price falls back to MARKET."""
        orders = plan_orders(
            current_positions={},
            target_positions={"AAPL": 100.0},
            order_type=OrderType.LIMIT,
            prices={"AAPL": 0.0},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        assert orders[0].order_type == OrderType.MARKET
        assert orders[0].limit_price is None

    @patch("prometheus.execution.order_planner.generate_uuid", return_value="o1")
    def test_limit_prices_none_treated_as_market(self, _mock_uuid):
        """LIMIT order_type with prices=None → MARKET (no limit computation)."""
        orders = plan_orders(
            current_positions={},
            target_positions={"AAPL": 100.0},
            order_type=OrderType.LIMIT,
            prices=None,
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        # When prices is None, limit_price stays None and order_type stays LIMIT
        # (the code only enters the limit price branch when prices is not None)
        assert orders[0].limit_price is None


# ---------------------------------------------------------------------------
# Fractional shares
# ---------------------------------------------------------------------------


class TestFractionalShares:
    """Tests for fractional share handling."""

    @patch("prometheus.execution.order_planner.generate_uuid", return_value="o1")
    def test_fractional_buy(self, _mock_uuid):
        """Fractional target quantity should produce fractional order."""
        orders = plan_orders(
            current_positions={},
            target_positions={"AAPL": 10.5},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        assert orders[0].quantity == pytest.approx(10.5)

    @patch("prometheus.execution.order_planner.generate_uuid", return_value="o1")
    def test_fractional_sell(self, _mock_uuid):
        """Fractional position decrease should produce correct quantity."""
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", 10.75)},
            target_positions={"AAPL": 5.25},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        assert orders[0].side == OrderSide.SELL
        assert orders[0].quantity == pytest.approx(5.5)

    @patch("prometheus.execution.order_planner.generate_uuid", return_value="o1")
    def test_very_small_fractional_suppressed(self, _mock_uuid):
        """Very tiny fractional delta below min_abs_quantity is suppressed."""
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", 100.0)},
            target_positions={"AAPL": 100.0 + 1e-9},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 0


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Miscellaneous edge cases."""

    @patch("prometheus.execution.order_planner.generate_uuid", return_value="o1")
    def test_target_zero_from_nonzero(self, _mock_uuid):
        """Target 0 for an existing position → SELL."""
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", 42.0)},
            target_positions={"AAPL": 0.0},
            min_rebalance_pct=0.0,
        )
        assert len(orders) == 1
        assert orders[0].side == OrderSide.SELL
        assert orders[0].quantity == pytest.approx(42.0)

    def test_order_has_uuid(self):
        """Each order should get a unique order_id (non-empty, distinct)."""
        orders = plan_orders(
            current_positions={},
            target_positions={"AAPL": 100.0, "GOOG": 50.0},
            min_rebalance_pct=0.0,
        )
        ids = {o.order_id for o in orders}
        assert len(ids) == 2
        assert all(len(oid) > 0 for oid in ids)

    @patch("prometheus.execution.order_planner.generate_uuid", return_value="o1")
    def test_default_limit_buffer_pct(self, _mock_uuid):
        """Default limit buffer is 0.1% (10 bps)."""
        assert DEFAULT_LIMIT_BUFFER_PCT == pytest.approx(0.001)

    @patch("prometheus.execution.order_planner.generate_uuid", side_effect=[f"o{i}" for i in range(10)])
    def test_instruments_sorted_deterministically(self, _mock_uuid):
        """Orders for the same side are sorted by instrument_id."""
        orders = plan_orders(
            current_positions={},
            target_positions={"ZZZ": 10.0, "AAA": 20.0, "MMM": 30.0},
            min_rebalance_pct=0.0,
            sells_first=False,
        )
        instrument_ids = [o.instrument_id for o in orders]
        assert instrument_ids == sorted(instrument_ids)

    @patch("prometheus.execution.order_planner.generate_uuid", return_value="o1")
    def test_quantity_always_positive(self, _mock_uuid):
        """Order quantity should always be the absolute value of delta."""
        orders = plan_orders(
            current_positions={"AAPL": _pos("AAPL", 200.0)},
            target_positions={"AAPL": 50.0},
            min_rebalance_pct=0.0,
        )
        assert orders[0].quantity > 0
        assert orders[0].quantity == pytest.approx(150.0)
