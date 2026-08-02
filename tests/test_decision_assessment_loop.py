"""Bulletproofing the decision-assessment loop.

Covers six audit gaps:
 1. OPTIONS_SHADOW excluded from live performance / hedge-effectiveness.
 2. Options legs that can't be priced are flagged (not silently dropped).
 3. save_decision_outcome uses ON CONFLICT (decision_id, horizon_days) DO UPDATE.
 4. Future-horizon decisions are skipped-and-flagged (look-ahead guard).
 5. Min-sample gating flags small-N metrics as insufficient_sample.
 6. UNIVERSE decisions are scored (forward hit-rate of selected names).

All DB access is mocked — no real DB / IBKR.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, List

import pytest

from prometheus.decisions.evaluator import OutcomeEvaluator
from prometheus.decisions.live_performance import LivePerformanceTracker
from prometheus.meta.storage import MetaStorage
from prometheus.meta.types import DecisionOutcome

# ---------------------------------------------------------------------------
# Mock DB plumbing
# ---------------------------------------------------------------------------


class _FakeCursor:
    """Returns queued result sets and records executed SQL."""

    def __init__(self, results: List[List[tuple]]) -> None:
        self._results = list(results)
        self.executed: List[tuple] = []
        self._last: List[tuple] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))
        self._last = self._results.pop(0) if self._results else []

    def fetchall(self) -> List[tuple]:
        return self._last

    def fetchone(self):
        return self._last[0] if self._last else None

    def close(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor
        self.committed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeDB:
    """db_manager whose runtime/historical connections share one cursor."""

    def __init__(self, results: List[List[tuple]]) -> None:
        self.cursor = _FakeCursor(results)
        self.conn = _FakeConn(self.cursor)

    def get_runtime_connection(self):
        return self.conn

    def get_historical_connection(self):
        return self.conn


# ===========================================================================
# 1. Shadow leakage — OPTIONS_SHADOW must not reach live readers
# ===========================================================================


def test_live_guard_sql_excludes_shadow_engine_and_mode():
    # rolling: account_resets (run boundary), PORTFOLIO rows, breakdown rows
    db = _FakeDB([[], [(0.01, 100.0)], []])
    tracker = LivePerformanceTracker(db_manager=db)
    tracker.compute_rolling_performance(as_of_date=date(2026, 6, 10))
    all_sql = " ".join(s for s, _ in db.cursor.executed)
    assert "OPTIONS_SHADOW" in all_sql  # guard names the shadow engine
    assert "input_refs->>'mode'" in all_sql
    assert "<> 'shadow'" in all_sql


def test_hedge_effectiveness_query_carries_live_guard():
    # account_resets (run boundary) first, then the hedge query.
    db = _FakeDB([[], []])
    tracker = LivePerformanceTracker(db_manager=db)
    tracker.compute_hedge_effectiveness(as_of_date=date(2026, 6, 10))
    sql = db.cursor.executed[1][0]
    assert "ed.engine_name <> 'OPTIONS_SHADOW'" in sql
    assert "input_refs->>'mode'" in sql


# ===========================================================================
# 5. Min-sample gating
# ===========================================================================


def test_rolling_perf_flags_insufficient_sample_below_min_n():
    rows = [(0.01, 10.0), (0.02, 20.0), (-0.01, -5.0)]  # n=3
    db = _FakeDB([[], rows, []])  # leading []: account_resets run boundary
    tracker = LivePerformanceTracker(db_manager=db, min_n=20)
    out = tracker.compute_rolling_performance(as_of_date=date(2026, 6, 10))
    assert out["n"] == 3
    assert out["insufficient_sample"] is True
    assert out["min_n"] == 20


def test_rolling_perf_sufficient_sample_clears_flag():
    rows = [(0.001 * i, float(i)) for i in range(1, 26)]  # n=25
    db = _FakeDB([[], rows, []])  # leading []: account_resets run boundary
    tracker = LivePerformanceTracker(db_manager=db, min_n=20)
    out = tracker.compute_rolling_performance(as_of_date=date(2026, 6, 10))
    assert out["n"] == 25
    assert out["insufficient_sample"] is False


# ===========================================================================
# 3. Dedup / idempotency on save_decision_outcome
# ===========================================================================


def test_save_decision_outcome_uses_on_conflict_upsert():
    db = _FakeDB([[]])
    storage = MetaStorage(db_manager=db)
    storage.save_decision_outcome(
        DecisionOutcome(
            decision_id="d1", horizon_days=21, realized_return=0.05,
            realized_pnl=1.0, realized_drawdown=0.0, realized_vol=0.0, metadata={},
        )
    )
    sql = db.cursor.executed[0][0]
    assert "ON CONFLICT (decision_id, horizon_days)" in sql
    assert "DO UPDATE" in sql
    assert db.conn.committed is True


def test_migration_makes_decision_outcomes_index_unique():
    import pathlib
    mig = pathlib.Path(
        "migrations/versions/0102_decision_outcomes_unique_horizon.py"
    ).read_text()
    assert "unique=True" in mig
    assert "uq_decision_outcomes_decision_horizon" in mig


# ===========================================================================
# Evaluator fixtures (no DB; calendar stubbed)
# ===========================================================================


class _StubCalendar:
    def trading_days_between(self, start_date, end_date):
        # Two endpoints is enough for the single-point options/universe eval.
        return [start_date, end_date]


def _make_evaluator(prices: dict) -> OutcomeEvaluator:
    class _NoDB:
        def get_runtime_connection(self):  # pragma: no cover
            raise RuntimeError("must not hit DB")
        def get_historical_connection(self):  # pragma: no cover
            raise RuntimeError("must not hit DB")

    ev = OutcomeEvaluator.__new__(OutcomeEvaluator)
    ev.db_manager = _NoDB()
    ev.calendar = _StubCalendar()
    ev._price_cache = {}
    ev._get_price = lambda inst, d: prices.get(inst)  # type: ignore[assignment]
    ev._get_prices_for_instruments = lambda ids, d: {  # type: ignore[assignment]
        i: prices[i] for i in ids if prices.get(i) is not None
    }
    return ev


# ===========================================================================
# 2. Silent options legs — flag incompleteness, don't drop silently
# ===========================================================================


def test_options_incomplete_legs_are_flagged_not_dropped():
    ev = _make_evaluator({"SPY.US": 500.0})
    orders = [
        # priceable BUY call
        {"underlying_id": "SPY.US", "right": "C", "strike": 450.0,
         "action": "BUY", "quantity": 1, "entry_price": 5.0},
        # zero-premium leg — unpriceable, must be flagged
        {"underlying_id": "SPY.US", "right": "P", "strike": 400.0,
         "action": "SELL", "quantity": 1, "entry_price": 0.0},
    ]
    out = ev.evaluate_options_decision_outcome(
        decision_id="opt1",
        decision_as_of_date=date(2026, 1, 2),
        horizon_days=21,
        orders=orders,
    )
    assert out is not None
    md = out.metadata
    assert md["total_legs"] == 2
    assert md["orders_evaluated"] == 1
    assert md["incomplete_legs"] == 1
    assert md["unpriced_entry_legs"] == 1
    assert md["complete"] is False
    assert "evaluation_error" in md


def test_options_all_legs_priceable_marked_complete():
    ev = _make_evaluator({"SPY.US": 500.0})
    orders = [
        {"underlying_id": "SPY.US", "right": "C", "strike": 450.0,
         "action": "BUY", "quantity": 1, "entry_price": 5.0},
    ]
    out = ev.evaluate_options_decision_outcome(
        decision_id="opt2",
        decision_as_of_date=date(2026, 1, 2),
        horizon_days=21,
        orders=orders,
    )
    assert out is not None
    assert out.metadata["complete"] is True
    assert out.metadata["incomplete_legs"] == 0
    assert "evaluation_error" not in out.metadata


def test_options_no_priceable_legs_records_explicit_incomplete_outcome():
    ev = _make_evaluator({"SPY.US": 500.0})
    orders = [
        {"underlying_id": "SPY.US", "right": "P", "strike": 400.0,
         "action": "SELL", "quantity": 1, "entry_price": 0.0},
    ]
    out = ev.evaluate_options_decision_outcome(
        decision_id="opt3",
        decision_as_of_date=date(2026, 1, 2),
        horizon_days=21,
        orders=orders,
    )
    # Previously returned None (silent vanish). Now an explicit, flagged row.
    assert out is not None
    assert out.metadata["complete"] is False
    assert out.metadata["evaluation_error"] == "no_priceable_legs"
    assert out.realized_pnl == 0.0


# ===========================================================================
# 4. Look-ahead / contamination guard
# ===========================================================================


def test_future_horizon_options_decision_is_skipped():
    ev = _make_evaluator({"SPY.US": 500.0})
    future_date = date.today() - timedelta(days=1)  # horizon ends tomorrow
    orders = [
        {"underlying_id": "SPY.US", "right": "C", "strike": 450.0,
         "action": "BUY", "quantity": 1, "entry_price": 5.0},
    ]
    out = ev.evaluate_options_decision_outcome(
        decision_id="optfut",
        decision_as_of_date=future_date,
        horizon_days=5,
        orders=orders,
    )
    assert out is None


def test_future_horizon_portfolio_decision_is_skipped():
    ev = _make_evaluator({"SPY.US": 500.0})
    future_date = date.today() - timedelta(days=1)
    out = ev.evaluate_portfolio_decision_outcome(
        decision_id="pfut",
        decision_as_of_date=future_date,
        horizon_days=5,
        target_weights={"SPY.US": 1.0},
    )
    assert out is None


def test_elapsed_horizon_guard_allows_evaluation():
    ev = _make_evaluator({"SPY.US": 500.0})
    assert ev._horizon_in_future("x", date.today() - timedelta(days=30), 5) is False
    assert ev._horizon_in_future("x", date.today(), 5) is True


# ===========================================================================
# 6. UNIVERSE decisions are now scored
# ===========================================================================


def test_universe_decision_scored_forward_hit_rate():
    # entry prices captured by stub; exit prices differ -> compute returns.
    # _get_prices_for_instruments uses the same price for entry+exit in the
    # stub, so use distinct entry/exit via a 2-call closure.
    class _StubCal:
        def trading_days_between(self, start_date, end_date):
            return [start_date, end_date]

    entry = {"AAA.US": 100.0, "BBB.US": 50.0, "CCC.US": 20.0}
    exit_ = {"AAA.US": 110.0, "BBB.US": 45.0, "CCC.US": 22.0}

    ev = OutcomeEvaluator.__new__(OutcomeEvaluator)
    ev.db_manager = None
    ev.calendar = _StubCal()
    ev._price_cache = {}

    def _prices(ids, d):
        src = entry if d == date(2026, 1, 2) else exit_
        return {i: src[i] for i in ids if i in src}

    ev._get_prices_for_instruments = _prices  # type: ignore[assignment]

    out = ev.evaluate_universe_decision_outcome(
        decision_id="u1",
        decision_as_of_date=date(2026, 1, 2),
        horizon_days=21,
        included_instruments=["AAA.US", "BBB.US", "CCC.US"],
    )
    assert out is not None
    # 2 of 3 names up (AAA +10%, CCC +10%, BBB -10%)
    assert out.metadata["hit_rate"] == pytest.approx(2 / 3, abs=1e-3)
    assert out.metadata["selected_count"] == 3
    assert out.metadata["priced_count"] == 3
    assert out.metadata["complete"] is True
    # equal-weight basket return ≈ (0.10 - 0.10 + 0.10)/3
    assert out.realized_return == pytest.approx(0.10 / 3, abs=1e-6)


def test_universe_no_priceable_names_records_explicit_incomplete():
    ev = _make_evaluator({})  # no prices
    out = ev.evaluate_universe_decision_outcome(
        decision_id="u2",
        decision_as_of_date=date(2026, 1, 2),
        horizon_days=21,
        included_instruments=["ZZZ.US"],
    )
    assert out is not None
    assert out.metadata["complete"] is False
    assert out.metadata["evaluation_error"] == "no_priceable_names"


def test_find_pending_includes_universe_engine():
    import inspect
    src = inspect.getsource(OutcomeEvaluator.find_pending_decisions)
    assert "'UNIVERSE'" in src
