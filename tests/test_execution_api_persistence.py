from __future__ import annotations
import importlib
import sys
import types
from datetime import datetime, timezone

from prometheus.execution.broker_interface import Fill, Order, OrderSide, OrderStatus, OrderType


class _DummyDbManager:
    pass


class _StubLogger:
    def debug(self, *_args, **_kwargs) -> None:
        return None

    def info(self, *_args, **_kwargs) -> None:
        return None

    def warning(self, *_args, **_kwargs) -> None:
        return None

    def error(self, *_args, **_kwargs) -> None:
        return None

    def exception(self, *_args, **_kwargs) -> None:
        return None


def _load_execution_api_module(monkeypatch):
    # Minimal stubs so importing execution.api does not require the
    # sibling apatheon package to be installed in the test environment.
    apatheon_mod = types.ModuleType("apatheon")
    apatheon_core_mod = types.ModuleType("apatheon.core")
    apatheon_db_mod = types.ModuleType("apatheon.core.database")
    apatheon_logging_mod = types.ModuleType("apatheon.core.logging")
    apatheon_ids_mod = types.ModuleType("apatheon.core.ids")
    apatheon_time_mod = types.ModuleType("apatheon.core.time")
    apatheon_data_mod = types.ModuleType("apatheon.data")
    apatheon_data_reader_mod = types.ModuleType("apatheon.data.reader")

    class _StubDatabaseManager:
        pass
    class _StubTradingCalendarConfig:
        def __init__(self, market: str = "US_EQ") -> None:
            self.market = market

    class _StubTradingCalendar:
        def __init__(self, _config: _StubTradingCalendarConfig | None = None) -> None:
            self._config = _config

        def is_trading_day(self, _as_of) -> bool:
            return True

    class _StubDataReader:
        pass

    apatheon_db_mod.DatabaseManager = _StubDatabaseManager  # type: ignore[attr-defined]
    apatheon_logging_mod.get_logger = lambda _name: _StubLogger()  # type: ignore[attr-defined]
    apatheon_ids_mod.generate_uuid = lambda: "stub-uuid"  # type: ignore[attr-defined]
    apatheon_time_mod.US_EQ = "US_EQ"  # type: ignore[attr-defined]
    apatheon_time_mod.TradingCalendar = _StubTradingCalendar  # type: ignore[attr-defined]
    apatheon_time_mod.TradingCalendarConfig = _StubTradingCalendarConfig  # type: ignore[attr-defined]
    apatheon_data_reader_mod.DataReader = _StubDataReader  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "apatheon", apatheon_mod)
    monkeypatch.setitem(sys.modules, "apatheon.core", apatheon_core_mod)
    monkeypatch.setitem(sys.modules, "apatheon.core.database", apatheon_db_mod)
    monkeypatch.setitem(sys.modules, "apatheon.core.logging", apatheon_logging_mod)
    monkeypatch.setitem(sys.modules, "apatheon.core.ids", apatheon_ids_mod)
    monkeypatch.setitem(sys.modules, "apatheon.core.time", apatheon_time_mod)
    monkeypatch.setitem(sys.modules, "apatheon.data", apatheon_data_mod)
    monkeypatch.setitem(sys.modules, "apatheon.data.reader", apatheon_data_reader_mod)

    for name in (
        "prometheus.execution.api",
        "prometheus.execution.storage",
        "prometheus.execution.executed_actions",
        "prometheus.execution.order_planner",
        "prometheus.execution.backtest_broker",
    ):
        sys.modules.pop(name, None)

    return importlib.import_module("prometheus.execution.api")


class _FakeBroker:
    def __init__(self, *, statuses: dict[str, OrderStatus], fills: list[Fill]) -> None:
        self._statuses = dict(statuses)
        self._fills = list(fills)
        self.submitted_order_ids: list[str] = []
        self.fills_since_args: list[datetime | None] = []

    def submit_order(self, order: Order) -> str:
        self.submitted_order_ids.append(order.order_id)
        return order.order_id

    def cancel_order(self, order_id: str) -> bool:
        return False

    def get_order_status(self, order_id: str) -> OrderStatus:
        return self._statuses.get(order_id, OrderStatus.SUBMITTED)

    def get_fills(self, since: datetime | None = None) -> list[Fill]:
        self.fills_since_args.append(since)
        return list(self._fills)

    def get_positions(self):
        return {}

    def get_account_state(self):
        return {}

    def sync(self) -> None:
        return None


def _mk_order(order_id: str, *, side: OrderSide) -> Order:
    return Order(
        order_id=order_id,
        instrument_id=f"{order_id}.EQ",
        side=side,
        order_type=OrderType.MARKET,
        quantity=10.0,
    )


def _mk_fill(fill_id: str, order_id: str) -> Fill:
    return Fill(
        fill_id=fill_id,
        order_id=order_id,
        instrument_id=f"{order_id}.EQ",
        side=OrderSide.BUY,
        quantity=5.0,
        price=100.0,
        timestamp=datetime.now(timezone.utc),
        commission=0.25,
    )


def test_apply_execution_plan_paper_persists_statuses_and_filters_batch_fills(monkeypatch) -> None:
    execution_api = _load_execution_api_module(monkeypatch)
    orders = [
        _mk_order("ord-1", side=OrderSide.BUY),
        _mk_order("ord-2", side=OrderSide.SELL),
    ]
    broker = _FakeBroker(
        statuses={
            "ord-1": OrderStatus.FILLED,
            "ord-2": OrderStatus.REJECTED,
        },
        fills=[
            _mk_fill("fill-1", "ord-1"),
            _mk_fill("fill-1", "ord-1"),  # duplicate fill_id (should be deduped)
            _mk_fill("fill-x", "unrelated-order"),  # foreign order (should be filtered)
        ],
    )

    captured: dict[str, object] = {}

    def _fake_plan_orders(**_kwargs):
        return list(orders)

    def _fake_record_orders(**kwargs):
        captured["record_orders"] = kwargs

    def _fake_update_statuses(*, db_manager, statuses):
        captured["statuses"] = {"db_manager": db_manager, "statuses": statuses}

    def _fake_record_fills(*, db_manager, fills, mode):
        captured["record_fills"] = {"db_manager": db_manager, "fills": fills, "mode": mode}

    def _fake_record_executed_actions(db_manager, *, fills, context):
        captured["executed_actions"] = {"db_manager": db_manager, "fills": fills, "context": context}

    monkeypatch.setattr(execution_api, "plan_orders", _fake_plan_orders)
    monkeypatch.setattr(execution_api, "record_orders", _fake_record_orders)
    monkeypatch.setattr(execution_api, "update_order_statuses", _fake_update_statuses)
    monkeypatch.setattr(execution_api, "record_fills", _fake_record_fills)
    monkeypatch.setattr(execution_api, "record_executed_actions_for_fills", _fake_record_executed_actions)

    summary = execution_api.apply_execution_plan(
        db_manager=_DummyDbManager(),
        broker=broker,
        portfolio_id="US_EQ_CORE",
        target_positions={"ord-1.EQ": 10.0, "ord-2.EQ": 0.0},
        mode="PAPER",
        as_of_date=None,
        decision_id="decision-1",
        record_positions=False,
        status_poll_timeout_sec=0.0,
    )

    assert broker.submitted_order_ids == ["ord-1", "ord-2"]
    assert captured["record_orders"]["orders"] == orders
    assert captured["statuses"]["statuses"] == {
        "ord-1": OrderStatus.FILLED,
        "ord-2": OrderStatus.REJECTED,
    }

    persisted_fills = captured["record_fills"]["fills"]
    assert len(persisted_fills) == 1
    assert persisted_fills[0].fill_id == "fill-1"
    assert persisted_fills[0].order_id == "ord-1"

    action_fills = captured["executed_actions"]["fills"]
    assert len(action_fills) == 1
    assert action_fills[0].fill_id == "fill-1"
    assert captured["executed_actions"]["context"].mode == "PAPER"

    assert len(broker.fills_since_args) == 1
    assert broker.fills_since_args[0] is not None
    assert summary.num_orders == 2
    assert summary.num_fills == 1


def test_apply_execution_plan_paper_updates_statuses_even_without_fills(monkeypatch) -> None:
    execution_api = _load_execution_api_module(monkeypatch)
    orders = [
        _mk_order("ord-a", side=OrderSide.BUY),
        _mk_order("ord-b", side=OrderSide.SELL),
    ]
    broker = _FakeBroker(
        statuses={
            "ord-a": OrderStatus.SUBMITTED,
            "ord-b": OrderStatus.PARTIALLY_FILLED,
        },
        fills=[],
    )

    captured_statuses: dict[str, OrderStatus] = {}
    record_fills_called = {"value": False}
    record_actions_called = {"value": False}

    def _fake_plan_orders(**_kwargs):
        return list(orders)

    def _fake_record_orders(**_kwargs):
        return None

    def _fake_update_statuses(*, db_manager, statuses):
        del db_manager
        captured_statuses.update(statuses)

    def _fake_record_fills(*, db_manager, fills, mode):
        del db_manager, fills, mode
        record_fills_called["value"] = True

    def _fake_record_executed_actions(db_manager, *, fills, context):
        del db_manager, fills, context
        record_actions_called["value"] = True

    monkeypatch.setattr(execution_api, "plan_orders", _fake_plan_orders)
    monkeypatch.setattr(execution_api, "record_orders", _fake_record_orders)
    monkeypatch.setattr(execution_api, "update_order_statuses", _fake_update_statuses)
    monkeypatch.setattr(execution_api, "record_fills", _fake_record_fills)
    monkeypatch.setattr(execution_api, "record_executed_actions_for_fills", _fake_record_executed_actions)

    summary = execution_api.apply_execution_plan(
        db_manager=_DummyDbManager(),
        broker=broker,
        portfolio_id="US_EQ_CORE",
        target_positions={"ord-a.EQ": 10.0},
        mode="PAPER",
        record_positions=False,
        status_poll_timeout_sec=0.0,
    )

    assert captured_statuses == {
        "ord-a": OrderStatus.SUBMITTED,
        "ord-b": OrderStatus.PARTIALLY_FILLED,
    }
    assert record_fills_called["value"] is False
    assert record_actions_called["value"] is False
    assert summary.num_orders == 2
    assert summary.num_fills == 0
