"""Run-flow tests for the 2026-07 derivatives audit fixes.

Drives ``run_derivatives_daily`` end-to-end against a fake IB and the
fake options-storage DB (shared with test_options_storage) to pin:

* defect A — strategy tags persisted at submission are restored after a
  sync/reconcile round-trip, and ``vix_tail_hedge`` does NOT re-enter
  when a tagged VIX position exists;
* defect B — a paper-gateway run (port 4002) persists positions under
  US_OPTIONS_PAPER / mode='PAPER' even when ``dry_run=False``;
* defect C — a failed spread wing cancels the already-submitted parent
  leg and lands in the run summary's warnings.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, List

import pytest
from test_options_storage import _FakeDb

from prometheus.execution import options_storage
from prometheus.scripts.run import run_derivatives_daily as rdd

# ── Fake IB (mirrors test_run_derivatives_daily_retry_safety) ────────


class _FakeTicker:
    def __init__(self, px: float) -> None:
        self.last = px
        self.close = px
        self.bid = px
        self.ask = px


def _vix_contract(expiry: str = "20261016") -> SimpleNamespace:
    return SimpleNamespace(
        secType="OPT", symbol="VIX",
        lastTradeDateOrContractMonth=expiry,
        strike=35.0, right="C", conId=4242, multiplier="100",
    )


class _FakeIB:
    """Stands in for the ib_insync/ib_async ``IB`` client, optionally
    holding pre-existing option positions."""

    def __init__(self, option_positions: Any = None) -> None:
        self._option_positions = list(option_positions or [])
        self._open_trades: list = []
        self.placed: list = []
        self.cancel_calls: list = []

    # Connection / event loop
    def connect(self, host: str = "", port: int = 0,
                clientId: int = 0, timeout: int = 0) -> "_FakeIB":
        return self

    def disconnect(self) -> None:
        return None

    def sleep(self, secs: float = 0) -> None:
        return None

    # Account / positions
    def accountValues(self) -> list:
        return [
            SimpleNamespace(tag="NetLiquidation", value="1000000", currency="USD"),
            SimpleNamespace(tag="AvailableFunds", value="500000", currency="USD"),
        ]

    def positions(self) -> list:
        return [
            SimpleNamespace(contract=c, position=qty, avgCost=avg_cost)
            for c, qty, avg_cost in self._option_positions
        ]

    def portfolio(self) -> list:
        return [
            SimpleNamespace(
                contract=c, position=qty, averageCost=avg_cost,
                marketValue=qty * avg_cost, unrealizedPNL=0.0,
                marketPrice=avg_cost / 100.0,
            )
            for c, qty, avg_cost in self._option_positions
        ]

    # Market data
    def reqMarketDataType(self, data_type: int) -> None:
        return None

    def qualifyContracts(self, *contracts: Any) -> list:
        out = []
        for c in contracts:
            c.conId = getattr(c, "conId", 0) or 1234
            out.append(c)
        return out

    def reqMktData(self, contract: Any, *args: Any, **kwargs: Any) -> _FakeTicker:
        return _FakeTicker(20.0)

    def cancelMktData(self, contract: Any) -> None:
        return None

    # Orders
    def trades(self) -> list:
        return list(self._open_trades)

    def placeOrder(self, contract: Any, order: Any) -> Any:
        order.orderId = len(self.placed) + 1
        trade = SimpleNamespace(
            contract=contract,
            order=order,
            orderStatus=SimpleNamespace(status="Submitted"),
        )
        self.placed.append(trade)
        self._open_trades.append(trade)
        return trade

    def cancelOrder(self, order: Any) -> None:
        self.cancel_calls.append(order)


class _WingRejectingIB(_FakeIB):
    """Rejects any order whose contract strike is 490 (the long wing)."""

    def placeOrder(self, contract: Any, order: Any) -> Any:
        if float(getattr(contract, "strike", 0) or 0) == 490.0:
            raise RuntimeError("Error 200: no security definition for wing")
        return super().placeOrder(contract, order)


# ── Shared wiring ────────────────────────────────────────────────────


def _wire(monkeypatch: pytest.MonkeyPatch, fake_ib: _FakeIB,
          db: Any, *, shadow: bool = False) -> None:
    if shadow:
        monkeypatch.setenv("PROMETHEUS_DERIVATIVES_SHADOW", "1")
    else:
        monkeypatch.delenv("PROMETHEUS_DERIVATIVES_SHADOW", raising=False)
    monkeypatch.setattr("prometheus.execution.ib_compat.IB", lambda: fake_ib)
    monkeypatch.setattr("apatheon.core.database.get_db_manager", lambda: db)


def _capture_directives(monkeypatch: pytest.MonkeyPatch) -> dict:
    captured: dict = {}
    orig = rdd._apply_risk_checks

    def _wrapped(directives: list, *args: Any, **kwargs: Any) -> list:
        captured["directives"] = list(directives)
        return orig(directives, *args, **kwargs)

    monkeypatch.setattr(rdd, "_apply_risk_checks", _wrapped)
    return captured


def _seed_vix_submission(db: _FakeDb) -> None:
    options_storage.record_order_submission(
        db,
        portfolio_id="US_OPTIONS_PAPER",
        mode="PAPER",
        instrument_id="VIX_261016_35C.US",
        symbol="VIX",
        right="C",
        expiry="20261016",
        strike=35.0,
        quantity=150,
        strategy="vix_tail_hedge",
        order_id="oid-prev-night",
    )


def _vix_opens(directives: List[Any]) -> List[Any]:
    return [
        d for d in directives
        if d.strategy == "vix_tail_hedge" and d.action.value == "OPEN"
    ]


# ── Defect A: restored tags stop nightly re-entry ────────────────────


def test_vix_tail_hedge_does_not_reenter_when_tagged_position_exists(
    monkeypatch: pytest.MonkeyPatch,
):
    db = _FakeDb()
    _seed_vix_submission(db)
    fake_ib = _FakeIB(option_positions=[(_vix_contract(), 150, 0.85)])
    _wire(monkeypatch, fake_ib, db)
    captured = _capture_directives(monkeypatch)

    result = rdd.run_derivatives_daily(dry_run=True)

    assert result["errors"] == []
    assert result["strategy_tags_restored"] == 1
    assert _vix_opens(captured["directives"]) == []


def test_vix_tail_hedge_reenters_without_tags_control(
    monkeypatch: pytest.MonkeyPatch,
):
    """Control for the test above: with NO persisted provenance the
    strategy is position-blind and emits a fresh OPEN — proving the
    non-re-entry above comes from the restored tag."""
    db = _FakeDb()  # no SUBMIT events
    fake_ib = _FakeIB(option_positions=[(_vix_contract(), 150, 0.85)])
    _wire(monkeypatch, fake_ib, db)
    captured = _capture_directives(monkeypatch)

    result = rdd.run_derivatives_daily(dry_run=True)

    assert result["errors"] == []
    assert len(_vix_opens(captured["directives"])) == 1


def test_tag_restore_failure_is_fatal_when_positions_exist(
    monkeypatch: pytest.MonkeyPatch,
):
    """If the account holds option positions but provenance can't be
    loaded, the run must abort pre-submission (retry-safe error) rather
    than trade position-blind — blindness IS the defect."""
    fake_ib = _FakeIB(option_positions=[(_vix_contract(), 150, 0.85)])
    # DB manager with no get_runtime_connection → restore blows up.
    _wire(monkeypatch, fake_ib, SimpleNamespace())

    result = rdd.run_derivatives_daily(dry_run=False, port=4002)

    assert fake_ib.placed == []
    assert any("strategy_tag_restore" in e for e in result["errors"])


# ── Defect B: paper gateway persists as US_OPTIONS_PAPER / PAPER ─────


def test_paper_port_persists_paper_mode_even_when_submitting(
    monkeypatch: pytest.MonkeyPatch,
):
    """dry_run=False on port 4002 (the nightly paper submission run)
    must persist US_OPTIONS_PAPER / mode='PAPER' — the old code keyed
    the label off dry_run and wrote US_OPTIONS_LIVE / LIVE."""
    from prometheus.execution.options_strategy import OptionsStrategyManager
    from prometheus.execution.position_lifecycle import PositionLifecycleManager

    db = _FakeDb()
    _seed_vix_submission(db)
    fake_ib = _FakeIB(option_positions=[(_vix_contract(), 150, 0.85)])
    _wire(monkeypatch, fake_ib, db, shadow=True)
    # Shadow pass + diff report exercise machinery irrelevant here.
    monkeypatch.setattr(rdd, "_run_shadow_pass",
                        lambda **kwargs: [])
    monkeypatch.setattr(rdd, "_write_daily_diff_report",
                        lambda **kwargs: None)
    # No new directives — this test is about persistence labels.
    monkeypatch.setattr(
        OptionsStrategyManager, "evaluate_all",
        lambda self, portfolio, signals, existing_options=None,
        allocations=None: [],
    )
    monkeypatch.setattr(
        PositionLifecycleManager, "evaluate",
        lambda self, positions, signals: [],
    )

    result = rdd.run_derivatives_daily(dry_run=False, port=4002)

    assert result["errors"] == []
    assert result["trading_mode"] == "PAPER"
    assert result["options_portfolio_id"] == "US_OPTIONS_PAPER"
    assert result["strategy_tags_restored"] == 1

    # The reconciled position row carries the paper namespace AND the
    # restored strategy tag (full defect-A round trip through the run).
    rows = list(db.positions.values())
    assert len(rows) == 1
    assert rows[0]["portfolio_id"] == "US_OPTIONS_PAPER"
    assert rows[0]["mode"] == "PAPER"
    assert rows[0]["strategy"] == "vix_tail_hedge"


def test_derive_trading_context_port_mapping():
    assert rdd._derive_trading_context(port=4001) == ("US_OPTIONS_LIVE", "LIVE")
    assert rdd._derive_trading_context(port=4002) == ("US_OPTIONS_PAPER", "PAPER")


# ── Defect C: failed wing cancels parent + lands in warnings ─────────


def test_failed_wing_cancels_parent_and_records_warning(
    monkeypatch: pytest.MonkeyPatch,
):
    from prometheus.execution.options_strategy import (
        OptionsStrategyManager,
        OptionTradeDirective,
        TradeAction,
    )
    from prometheus.execution.position_lifecycle import PositionLifecycleManager

    fake_ib = _WingRejectingIB()
    _wire(monkeypatch, fake_ib, SimpleNamespace())

    monkeypatch.setattr(
        OptionsStrategyManager, "evaluate_all",
        lambda self, portfolio, signals, existing_options=None,
        allocations=None: [],
    )
    short_put = OptionTradeDirective(
        strategy="iron_condor", action=TradeAction.OPEN,
        symbol="SPY", right="P", expiry="20260918",
        strike=500.0, quantity=-1, limit_price=4.20,
        reason="naked-leg test short",
    )
    wing = OptionTradeDirective(
        strategy="iron_condor", action=TradeAction.OPEN,
        symbol="SPY", right="P", expiry="20260918",
        strike=490.0, quantity=1, limit_price=2.10,
        reason="naked-leg test wing",
    )
    short_put.spread_leg = wing
    monkeypatch.setattr(
        PositionLifecycleManager, "evaluate",
        lambda self, positions, signals: [short_put],
    )

    class _StubTracker:
        def __init__(self, db_manager: Any = None) -> None:
            return None

        def record_options_decision(self, **kwargs: Any) -> None:
            return None

    import prometheus.decisions.tracker as tracker_mod
    monkeypatch.setattr(tracker_mod, "DecisionTracker", _StubTracker)

    result = rdd.run_derivatives_daily(dry_run=False, port=4002)

    # Submission-time failures are post-submission — warnings, never errors.
    assert result["errors"] == []
    # Only the parent went out, and its cancel was issued.
    assert len(fake_ib.placed) == 1
    assert len(fake_ib.cancel_calls) == 1
    assert fake_ib.cancel_calls[0].orderId == 1
    # Both the wing failure and the naked-leg cancel are in the summary.
    submission_warnings = [
        w for w in result["warnings"] if w.startswith("submission:")
    ]
    assert any("submit failed" in w for w in submission_warnings)
    assert any("NAKED-LEG GUARD" in w for w in submission_warnings)
    assert result["submission_failures"] >= 2
