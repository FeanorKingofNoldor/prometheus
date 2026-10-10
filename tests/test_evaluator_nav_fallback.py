"""Tests for OutcomeEvaluator NAV resolution + OPTIONS_SHADOW evaluation.

Covers:
- Historical NAV is scoped to account and date; absent data stays unknown.
- OPTIONS_SHADOW is included in the pending-decision scan and in the
  batch order loader.
- Shadow decisions without contract metadata get an explicit
  'unpriceable' outcome instead of a silent skip.
"""

from __future__ import annotations

import inspect
from datetime import date, timedelta
from typing import Any, List

import pytest

from prometheus.decisions.evaluator import OutcomeEvaluator

# ── Fake DB that dispatches on SQL content ───────────────────────────


class _DispatchCursor:
    def __init__(self, db: "_DispatchDB") -> None:
        self._db = db
        self._last: List[tuple] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self._db.executed.append((sql, params))
        norm = " ".join(sql.split()).lower()
        if "portfolio_equity_history" in norm:
            if self._db.equity_history_error:
                raise RuntimeError("relation portfolio_equity_history does not exist")
            self._last = self._db.equity_rows
        elif "positions_snapshots" in norm:
            if self._db.snapshots_error:
                raise RuntimeError("snapshots query failed")
            self._last = self._db.snapshot_rows
        else:
            self._last = []

    def fetchone(self):
        return self._last[0] if self._last else None

    def fetchall(self) -> List[tuple]:
        return list(self._last)

    def close(self) -> None:
        pass

    def __enter__(self): return self
    def __exit__(self, *args): return False


class _DispatchConn:
    def __init__(self, db: "_DispatchDB") -> None:
        self._db = db

    def cursor(self) -> _DispatchCursor:
        return _DispatchCursor(self._db)

    def __enter__(self) -> "_DispatchConn":
        return self

    def __exit__(self, *a: Any) -> bool:
        return False


class _DispatchDB:
    def __init__(
        self,
        *,
        equity_rows: List[tuple] | None = None,
        snapshot_rows: List[tuple] | None = None,
        equity_history_error: bool = False,
        snapshots_error: bool = False,
    ) -> None:
        self.equity_rows = equity_rows or []
        self.snapshot_rows = snapshot_rows or []
        self.equity_history_error = equity_history_error
        self.snapshots_error = snapshots_error
        self.executed: List[tuple] = []

    def get_runtime_connection(self) -> _DispatchConn:
        return _DispatchConn(self)

    def get_historical_connection(self) -> _DispatchConn:
        return _DispatchConn(self)


def _bare_evaluator(db: Any) -> OutcomeEvaluator:
    ev = OutcomeEvaluator.__new__(OutcomeEvaluator)
    ev.db_manager = db
    ev.calendar = None
    ev._price_cache = {}
    return ev


# ── NAV fallback chain ───────────────────────────────────────────────


def test_nav_is_scoped_to_date_and_portfolio():
    db = _DispatchDB(equity_rows=[(253_411.55,)])
    ev = _bare_evaluator(db)
    day = date(2026, 1, 2)
    assert ev._resolve_portfolio_nav(day) == 253_411.55
    sql, params = db.executed[-1]
    assert "portfolio_id = %s AND as_of_date <= %s" in sql
    assert params == ("IBKR_PAPER", day)


def test_missing_nav_does_not_invent_notional_or_use_position_value():
    ev = _bare_evaluator(_DispatchDB(snapshot_rows=[(247_500.0,)]))
    assert ev._resolve_portfolio_nav(date(2026, 1, 2)) is None


def test_nav_database_failure_is_not_a_successful_evaluation():
    ev = _bare_evaluator(_DispatchDB(equity_history_error=True))
    with pytest.raises(RuntimeError):
        ev._resolve_portfolio_nav(date(2026, 1, 2))


def test_portfolio_outcome_pnl_uses_resolved_nav():
    db = _DispatchDB(equity_rows=[(200_000.0,)])
    ev = _bare_evaluator(db)

    class _Cal:
        def trading_days_between(self, start_date, end_date):
            return [start_date, end_date]

    ev.calendar = _Cal()
    entry_day = date(2026, 1, 2)
    prices = {
        ("SPY.US", entry_day): 100.0,
        ("SPY.US", entry_day + timedelta(days=21)): 110.0,
    }
    ev._get_price = lambda inst, d: prices.get((inst, d))  # type: ignore[method-assign]

    out = ev.evaluate_portfolio_decision_outcome(
        decision_id="d1",
        decision_as_of_date=entry_day,
        horizon_days=21,
        target_weights={"SPY.US": 1.0},
    )
    assert out is not None
    assert out.realized_return == pytest.approx(0.10)
    assert out.realized_pnl == pytest.approx(0.10 * 200_000.0)
    assert out.metadata["notional_nav"] == 200_000.0


# ── OPTIONS_SHADOW inclusion ─────────────────────────────────────────


def test_find_pending_includes_options_shadow_engine():
    src = inspect.getsource(OutcomeEvaluator.find_pending_decisions)
    assert "'OPTIONS_SHADOW'" in src


def test_batch_order_loader_includes_shadow_engine():
    src = inspect.getsource(OutcomeEvaluator._batch_load_decision_orders)
    assert "OPTIONS_SHADOW" in src


def test_unpriceable_shadow_outcome_is_explicit():
    ev = _bare_evaluator(_DispatchDB())
    out = ev._unpriceable_options_outcome(
        decision_id="shadow1",
        decision_as_of_date=date(2026, 1, 2),
        horizon_days=21,
        engine_name="OPTIONS_SHADOW",
    )
    assert out is not None
    assert out.realized_pnl == 0.0
    assert out.metadata["evaluation_error"] == "unpriceable"
    assert out.metadata["engine_name"] == "OPTIONS_SHADOW"
    assert out.metadata["complete"] is False


def test_unpriceable_outcome_respects_lookahead_guard():
    ev = _bare_evaluator(_DispatchDB())
    out = ev._unpriceable_options_outcome(
        decision_id="shadow2",
        decision_as_of_date=date.today() - timedelta(days=1),
        horizon_days=5,  # horizon ends in the future
        engine_name="OPTIONS_SHADOW",
    )
    assert out is None
