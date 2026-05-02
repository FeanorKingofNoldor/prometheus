"""Unit tests for RiskCheckingBroker.

Tests cover risk limit enforcement, price estimation, delegation to
the inner broker, and the zero-means-disabled semantics.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List
from unittest.mock import MagicMock, patch

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
from prometheus.execution.risk_broker import RiskCheckingBroker, RiskLimitExceeded


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeBroker(BrokerInterface):
    """Minimal in-memory broker for testing."""

    def __init__(
        self,
        positions: Dict[str, Position] | None = None,
        account_state: Dict | None = None,
    ):
        self._positions = positions or {}
        self._account_state = account_state or {"equity": 100_000.0}
        self._submitted: list[Order] = []
        self._cancelled: list[str] = []
        self.synced = False

    def submit_order(self, order: Order) -> str:
        self._submitted.append(order)
        return order.order_id

    def cancel_order(self, order_id: str) -> bool:
        self._cancelled.append(order_id)
        return True

    def get_order_status(self, order_id: str) -> OrderStatus:
        return OrderStatus.FILLED

    def get_fills(self, since: datetime | None = None) -> List[Fill]:
        return []

    def get_positions(self) -> Dict[str, Position]:
        return dict(self._positions)

    def get_account_state(self) -> Dict:
        return dict(self._account_state)

    def sync(self) -> None:
        self.synced = True


def _make_order(
    instrument_id: str = "AAPL",
    side: OrderSide = OrderSide.BUY,
    quantity: float = 10.0,
    order_id: str = "ord-1",
) -> Order:
    return Order(
        order_id=order_id,
        instrument_id=instrument_id,
        side=side,
        order_type=OrderType.MARKET,
        quantity=quantity,
    )


def _make_config(
    enabled: bool = True,
    max_order_notional: float = 0.0,
    max_position_notional: float = 0.0,
    max_leverage: float = 0.0,
    max_drawdown_pct: float = 0.0,
    max_sector_concentration_pct: float = 0.0,
):
    """Build a lightweight ExecutionRiskConfig-like object.

    Keep this in sync with new ExecutionRiskConfig fields — using a
    MagicMock with ``spec_set=False`` would let unset attributes return
    a MagicMock, which fails ``> 0`` comparisons in the broker.
    """
    cfg = MagicMock()
    cfg.enabled = enabled
    cfg.max_order_notional = max_order_notional
    cfg.max_position_notional = max_position_notional
    cfg.max_leverage = max_leverage
    cfg.max_drawdown_pct = max_drawdown_pct
    cfg.max_sector_concentration_pct = max_sector_concentration_pct
    return cfg


# ---------------------------------------------------------------------------
# Tests: delegation
# ---------------------------------------------------------------------------


class TestDelegation:
    """Verify that broker methods are forwarded to the inner broker."""

    def test_sync_delegates(self):
        inner = FakeBroker()
        rb = RiskCheckingBroker(inner, config=_make_config(enabled=False))
        rb.sync()
        assert inner.synced is True

    def test_get_positions_delegates(self):
        positions = {"AAPL": Position("AAPL", 100, 150.0, 15000.0, 500.0)}
        inner = FakeBroker(positions=positions)
        rb = RiskCheckingBroker(inner, config=_make_config(enabled=False))
        result = rb.get_positions()
        assert "AAPL" in result
        assert result["AAPL"].quantity == 100

    def test_get_account_state_delegates(self):
        inner = FakeBroker(account_state={"equity": 42_000.0})
        rb = RiskCheckingBroker(inner, config=_make_config(enabled=False))
        result = rb.get_account_state()
        assert result["equity"] == 42_000.0

    def test_cancel_order_delegates(self):
        inner = FakeBroker()
        rb = RiskCheckingBroker(inner, config=_make_config(enabled=False))
        assert rb.cancel_order("ord-99") is True
        assert "ord-99" in inner._cancelled

    def test_get_order_status_delegates(self):
        inner = FakeBroker()
        rb = RiskCheckingBroker(inner, config=_make_config(enabled=False))
        assert rb.get_order_status("ord-1") == OrderStatus.FILLED

    def test_get_fills_delegates(self):
        inner = FakeBroker()
        rb = RiskCheckingBroker(inner, config=_make_config(enabled=False))
        assert rb.get_fills() == []

    def test_getattr_delegation_to_inner(self):
        """Unknown attributes fall through to inner broker."""
        inner = FakeBroker()
        inner.custom_attr = "hello"  # type: ignore[attr-defined]
        rb = RiskCheckingBroker(inner, config=_make_config(enabled=False))
        assert rb.custom_attr == "hello"  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Tests: risk checks disabled
# ---------------------------------------------------------------------------


class TestRiskDisabled:
    """When config.enabled is False, orders pass through unchecked."""

    def test_submit_bypasses_limits_when_disabled(self):
        inner = FakeBroker()
        cfg = _make_config(enabled=False, max_order_notional=1.0)
        rb = RiskCheckingBroker(inner, config=cfg)
        # Huge order would exceed max_order_notional=1, but risk is disabled.
        order = _make_order(quantity=999_999)
        rb.submit_order(order)
        assert len(inner._submitted) == 1


# ---------------------------------------------------------------------------
# Tests: max_order_notional
# ---------------------------------------------------------------------------


class TestMaxOrderNotional:
    """Per-order notional limit."""

    @patch("prometheus.execution.risk_broker.insert_risk_actions")
    @patch("prometheus.execution.risk_broker.get_db_manager")
    def test_order_within_limit_passes(self, mock_db, mock_insert):
        positions = {"AAPL": Position("AAPL", 50, 150.0, 7500.0, 0.0)}
        inner = FakeBroker(positions=positions)
        cfg = _make_config(max_order_notional=20_000.0)
        rb = RiskCheckingBroker(inner, config=cfg)
        order = _make_order(quantity=10)  # 10 * 150 = 1500 < 20000
        rb.submit_order(order)
        assert len(inner._submitted) == 1

    @patch("prometheus.execution.risk_broker.insert_risk_actions")
    @patch("prometheus.execution.risk_broker.get_db_manager")
    def test_order_exceeding_limit_blocked(self, mock_db, mock_insert):
        positions = {"AAPL": Position("AAPL", 50, 150.0, 7500.0, 0.0)}
        inner = FakeBroker(positions=positions)
        cfg = _make_config(max_order_notional=1000.0)
        rb = RiskCheckingBroker(inner, config=cfg)
        order = _make_order(quantity=10)  # 10 * 150 = 1500 > 1000
        with pytest.raises(RiskLimitExceeded, match="max_order_notional"):
            rb.submit_order(order)
        assert len(inner._submitted) == 0

    @patch("prometheus.execution.risk_broker.insert_risk_actions")
    @patch("prometheus.execution.risk_broker.get_db_manager")
    def test_zero_limit_means_unconstrained(self, mock_db, mock_insert):
        positions = {"AAPL": Position("AAPL", 50, 150.0, 7500.0, 0.0)}
        inner = FakeBroker(positions=positions)
        cfg = _make_config(max_order_notional=0.0)
        rb = RiskCheckingBroker(inner, config=cfg)
        order = _make_order(quantity=999_999)
        rb.submit_order(order)
        assert len(inner._submitted) == 1


# ---------------------------------------------------------------------------
# Tests: max_position_notional
# ---------------------------------------------------------------------------


class TestMaxPositionNotional:
    """Per-position notional limit (after applying the order)."""

    @patch("prometheus.execution.risk_broker.insert_risk_actions")
    @patch("prometheus.execution.risk_broker.get_db_manager")
    def test_position_within_limit(self, mock_db, mock_insert):
        positions = {"AAPL": Position("AAPL", 10, 150.0, 1500.0, 0.0)}
        inner = FakeBroker(positions=positions)
        cfg = _make_config(max_position_notional=50_000.0)
        rb = RiskCheckingBroker(inner, config=cfg)
        order = _make_order(quantity=5, side=OrderSide.BUY)  # (10+5)*150=2250 < 50000
        rb.submit_order(order)
        assert len(inner._submitted) == 1

    @patch("prometheus.execution.risk_broker.insert_risk_actions")
    @patch("prometheus.execution.risk_broker.get_db_manager")
    def test_position_exceeding_limit_blocked(self, mock_db, mock_insert):
        positions = {"AAPL": Position("AAPL", 100, 150.0, 15000.0, 0.0)}
        inner = FakeBroker(positions=positions)
        cfg = _make_config(max_position_notional=20_000.0)
        rb = RiskCheckingBroker(inner, config=cfg)
        order = _make_order(quantity=50, side=OrderSide.BUY)  # (100+50)*150=22500 > 20000
        with pytest.raises(RiskLimitExceeded, match="max_position_notional"):
            rb.submit_order(order)
        assert len(inner._submitted) == 0

    @patch("prometheus.execution.risk_broker.insert_risk_actions")
    @patch("prometheus.execution.risk_broker.get_db_manager")
    def test_sell_reduces_position(self, mock_db, mock_insert):
        """A sell that reduces position notional below limit should pass."""
        positions = {"AAPL": Position("AAPL", 100, 150.0, 15000.0, 0.0)}
        inner = FakeBroker(positions=positions)
        cfg = _make_config(max_position_notional=10_000.0)
        rb = RiskCheckingBroker(inner, config=cfg)
        # Sell 60 shares: (100-60)*150 = 6000 < 10000  -> passes
        order = _make_order(quantity=60, side=OrderSide.SELL)
        rb.submit_order(order)
        assert len(inner._submitted) == 1

    @patch("prometheus.execution.risk_broker.insert_risk_actions")
    @patch("prometheus.execution.risk_broker.get_db_manager")
    def test_zero_position_limit_unconstrained(self, mock_db, mock_insert):
        positions = {"AAPL": Position("AAPL", 100, 150.0, 15000.0, 0.0)}
        inner = FakeBroker(positions=positions)
        cfg = _make_config(max_position_notional=0.0)
        rb = RiskCheckingBroker(inner, config=cfg)
        order = _make_order(quantity=999_999, side=OrderSide.BUY)
        rb.submit_order(order)
        assert len(inner._submitted) == 1


# ---------------------------------------------------------------------------
# Tests: max_leverage
# ---------------------------------------------------------------------------


class TestMaxLeverage:
    """Gross-leverage limit."""

    @patch("prometheus.execution.risk_broker.insert_risk_actions")
    @patch("prometheus.execution.risk_broker.get_db_manager")
    def test_leverage_within_limit(self, mock_db, mock_insert):
        positions = {"AAPL": Position("AAPL", 100, 150.0, 15000.0, 0.0)}
        inner = FakeBroker(positions=positions, account_state={"equity": 100_000.0})
        cfg = _make_config(max_leverage=2.0)
        rb = RiskCheckingBroker(inner, config=cfg)
        # gross = 15000 (existing) + 10*150 (new) = 16500; leverage = 16500/100000 = 0.165
        order = _make_order(quantity=10)
        rb.submit_order(order)
        assert len(inner._submitted) == 1

    @patch("prometheus.execution.risk_broker.insert_risk_actions")
    @patch("prometheus.execution.risk_broker.get_db_manager")
    def test_leverage_exceeding_limit_blocked(self, mock_db, mock_insert):
        positions = {"AAPL": Position("AAPL", 1000, 150.0, 150_000.0, 0.0)}
        inner = FakeBroker(positions=positions, account_state={"equity": 100_000.0})
        cfg = _make_config(max_leverage=1.5)
        rb = RiskCheckingBroker(inner, config=cfg)
        # gross = 150000 + 500*150 = 225000; leverage = 225000/100000 = 2.25 > 1.5
        order = _make_order(quantity=500)
        with pytest.raises(RiskLimitExceeded, match="max_leverage"):
            rb.submit_order(order)
        assert len(inner._submitted) == 0

    @patch("prometheus.execution.risk_broker.insert_risk_actions")
    @patch("prometheus.execution.risk_broker.get_db_manager")
    def test_zero_leverage_limit_unconstrained(self, mock_db, mock_insert):
        positions = {"AAPL": Position("AAPL", 1000, 150.0, 150_000.0, 0.0)}
        inner = FakeBroker(positions=positions, account_state={"equity": 100_000.0})
        cfg = _make_config(max_leverage=0.0)
        rb = RiskCheckingBroker(inner, config=cfg)
        order = _make_order(quantity=999_999)
        rb.submit_order(order)
        assert len(inner._submitted) == 1

    @patch("prometheus.execution.risk_broker.insert_risk_actions")
    @patch("prometheus.execution.risk_broker.get_db_manager")
    def test_zero_equity_skips_leverage_check(self, mock_db, mock_insert):
        """When equity is 0, the leverage check should not divide by zero."""
        positions = {}
        inner = FakeBroker(positions=positions, account_state={"equity": 0.0})
        cfg = _make_config(max_leverage=2.0)
        rb = RiskCheckingBroker(inner, config=cfg)
        order = _make_order(quantity=10)
        # Should not raise (leverage check is skipped when equity <= 0).
        rb.submit_order(order)
        assert len(inner._submitted) == 1


# ---------------------------------------------------------------------------
# Tests: _estimate_price
# ---------------------------------------------------------------------------


class TestEstimatePrice:
    """Price estimation for risk notional calculations."""

    def test_estimate_price_from_position(self):
        positions = {"AAPL": Position("AAPL", 50, 150.0, 7500.0, 0.0)}
        inner = FakeBroker(positions=positions)
        rb = RiskCheckingBroker(inner, config=_make_config())
        # market_value / quantity = 7500 / 50 = 150
        price = rb._estimate_price("AAPL", positions)
        assert price == 150.0

    def test_estimate_price_position_with_negative_market_value(self):
        """Short positions have negative market_value; price should be abs."""
        positions = {"AAPL": Position("AAPL", -50, 150.0, -7500.0, 0.0)}
        inner = FakeBroker(positions=positions)
        rb = RiskCheckingBroker(inner, config=_make_config())
        price = rb._estimate_price("AAPL", positions)
        assert price == 150.0

    @patch("apatheon.core.database.get_db_manager")
    def test_estimate_price_fallback_to_db(self, mock_get_db):
        """When no position exists, fall back to DB lookup."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (200.0,)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_get_db.return_value.get_historical_connection.return_value = mock_conn

        inner = FakeBroker()
        rb = RiskCheckingBroker(inner, config=_make_config())
        price = rb._estimate_price("MSFT", {})
        assert price == 200.0

    @patch("apatheon.core.database.get_db_manager", side_effect=Exception("no DB"))
    def test_estimate_price_conservative_fallback(self, mock_get_db):
        """When DB lookup fails, returns 1000.0 conservative fallback."""
        inner = FakeBroker()
        rb = RiskCheckingBroker(inner, config=_make_config())
        price = rb._estimate_price("UNKNOWN", {})
        assert price == 1000.0

    def test_estimate_price_zero_quantity_position(self):
        """Position with 0 quantity should not be used (would divide by zero)."""
        positions = {"AAPL": Position("AAPL", 0, 150.0, 0.0, 0.0)}
        inner = FakeBroker(positions=positions)
        rb = RiskCheckingBroker(inner, config=_make_config())
        # quantity is 0 -> falsy -> skip to fallback
        with patch("prometheus.execution.risk_broker.get_db_manager", side_effect=Exception("no DB")):
            price = rb._estimate_price("AAPL", positions)
        assert price == 1000.0


# ---------------------------------------------------------------------------
# Tests: _block records risk action
# ---------------------------------------------------------------------------


class TestBlockRecordsRiskAction:

    @patch("prometheus.execution.risk_broker.insert_risk_actions")
    @patch("prometheus.execution.risk_broker.get_db_manager")
    def test_block_inserts_risk_action(self, mock_get_db, mock_insert):
        inner = FakeBroker()
        rb = RiskCheckingBroker(inner, config=_make_config(max_order_notional=1.0))
        rb.strategy_id = "strat-1"
        rb.portfolio_id = "port-1"

        order = _make_order(quantity=100)
        with pytest.raises(RiskLimitExceeded):
            rb.submit_order(order)

        mock_insert.assert_called_once()
        actions = mock_insert.call_args[1].get("actions") or mock_insert.call_args[0][1]
        assert len(actions) == 1
        assert actions[0].strategy_id == "strat-1"

    @patch("prometheus.execution.risk_broker.insert_risk_actions", side_effect=Exception("DB down"))
    @patch("prometheus.execution.risk_broker.get_db_manager")
    def test_block_still_raises_if_risk_action_insert_fails(self, mock_get_db, mock_insert):
        """Even if persisting the risk action fails, the order is still blocked."""
        inner = FakeBroker()
        rb = RiskCheckingBroker(inner, config=_make_config(max_order_notional=1.0))
        order = _make_order(quantity=100)
        with pytest.raises(RiskLimitExceeded):
            rb.submit_order(order)


# ---------------------------------------------------------------------------
# Tests: gross_exposure
# ---------------------------------------------------------------------------


class TestGrossExposure:

    def test_empty_positions(self):
        assert RiskCheckingBroker._gross_exposure({}) == 0.0

    def test_mixed_positions(self):
        positions = {
            "AAPL": Position("AAPL", 100, 150.0, 15000.0, 0.0),
            "TSLA": Position("TSLA", -50, 200.0, -10000.0, 0.0),
        }
        # abs(15000) + abs(-10000) = 25000
        assert RiskCheckingBroker._gross_exposure(positions) == 25_000.0
