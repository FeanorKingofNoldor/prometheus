"""Sample-size honesty gates in LivePerformanceTracker.

Verdicts (SIGNAL_VALID / SIGNAL_INVERTED / …) must only be published with
n >= MIN_VERDICT_N; below that the verdict is INSUFFICIENT_DATA and the
observed n is always included in the result payload.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, List

from prometheus.decisions.live_performance import (
    MIN_VERDICT_N,
    LivePerformanceTracker,
)

# ── Fake DB: sequential canned result sets ───────────────────────────


class _FakeCursor:
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


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *a: Any) -> bool:
        return False


class _FakeDB:
    def __init__(self, results: List[List[tuple]]) -> None:
        self.cursor = _FakeCursor(results)

    def get_runtime_connection(self) -> _FakeConn:
        return _FakeConn(self.cursor)


def _fragility_rows(n: int, inverted: bool = False) -> List[tuple]:
    """n day-pairs with perfectly monotone fragility→return relationship."""
    rows = []
    d0 = date(2026, 1, 1)
    for i in range(n):
        frag = float(i)
        ret = float(i) if inverted else float(-i)  # rho = +1 / -1
        rows.append((d0 + timedelta(days=i), frag, ret))
    return rows


# ── Fragility validation gates ───────────────────────────────────────


def test_fragility_small_n_forces_insufficient_data_despite_strong_rho():
    n = 50  # would previously verdict SIGNAL_VALID off rho=-1.0
    db = _FakeDB([[], _fragility_rows(n)])  # account_resets, fragility rows
    tracker = LivePerformanceTracker(db_manager=db)
    out = tracker.validate_fragility_signal(as_of_date=date(2026, 6, 10))
    assert out["n"] == n
    assert out["verdict"] == "INSUFFICIENT_DATA"
    assert out["reliable"] is False
    assert out["min_verdict_n"] == MIN_VERDICT_N
    # rho still reported for transparency
    assert out["spearman_rho"] < -0.9


def test_fragility_large_n_valid_signal():
    n = MIN_VERDICT_N
    db = _FakeDB([[], _fragility_rows(n)])
    tracker = LivePerformanceTracker(db_manager=db)
    out = tracker.validate_fragility_signal(as_of_date=date(2026, 6, 10))
    assert out["n"] == n
    assert out["verdict"] == "SIGNAL_VALID"
    assert out["reliable"] is True


def test_fragility_large_n_inverted_signal():
    n = MIN_VERDICT_N + 20
    db = _FakeDB([[], _fragility_rows(n, inverted=True)])
    tracker = LivePerformanceTracker(db_manager=db)
    out = tracker.validate_fragility_signal(as_of_date=date(2026, 6, 10))
    assert out["verdict"] == "SIGNAL_INVERTED"


def test_fragility_tiny_n_keeps_insufficient_data():
    db = _FakeDB([[], _fragility_rows(2)])
    tracker = LivePerformanceTracker(db_manager=db)
    out = tracker.validate_fragility_signal(as_of_date=date(2026, 6, 10))
    assert out["verdict"] == "INSUFFICIENT_DATA"
    assert out["n"] == 2


# ── Rolling performance: reliable flag + strategy filter ─────────────


def test_rolling_perf_reports_reliable_flag_and_n():
    rows = [(0.01, 10.0)] * 30
    db = _FakeDB([[], rows, []])
    tracker = LivePerformanceTracker(db_manager=db)
    out = tracker.compute_rolling_performance(as_of_date=date(2026, 6, 10))
    assert out["n"] == 30
    assert out["reliable"] is False


def test_rolling_perf_reliable_at_min_verdict_n():
    rows = [(0.01 * ((-1) ** i), 10.0) for i in range(MIN_VERDICT_N)]
    db = _FakeDB([[], rows, []])
    tracker = LivePerformanceTracker(db_manager=db)
    out = tracker.compute_rolling_performance(as_of_date=date(2026, 6, 10))
    assert out["n"] == MIN_VERDICT_N
    assert out["reliable"] is True


def test_rolling_perf_strategy_filter_threads_into_sql_and_params():
    db = _FakeDB([[], [], []])
    tracker = LivePerformanceTracker(db_manager=db)
    out = tracker.compute_rolling_performance(
        as_of_date=date(2026, 6, 10), strategy_id="US_CORE_LONG_EQ",
    )
    assert out["strategy_id"] == "US_CORE_LONG_EQ"
    # Skip the account_resets query; both aggregate queries must filter.
    for sql, params in db.cursor.executed[1:]:
        assert "ed.strategy_id = %s" in sql
        assert params[-1] == "US_CORE_LONG_EQ"


def test_rolling_perf_no_strategy_filter_by_default():
    db = _FakeDB([[], [], []])
    tracker = LivePerformanceTracker(db_manager=db)
    tracker.compute_rolling_performance(as_of_date=date(2026, 6, 10))
    for sql, _params in db.cursor.executed[1:]:
        assert "ed.strategy_id" not in sql


# ── Run-boundary clamping ────────────────────────────────────────────


def test_rolling_perf_window_clamped_to_reset_boundary():
    reset = date(2026, 5, 13)
    db = _FakeDB([[(reset,)], [], []])
    tracker = LivePerformanceTracker(db_manager=db)
    out = tracker.compute_rolling_performance(
        as_of_date=date(2026, 6, 10), lookback_days=90,
    )
    # 90d lookback would reach 2026-03-12; the reset boundary wins.
    assert out["effective_start"] == reset.isoformat()
    _sql, params = db.cursor.executed[1]
    assert reset in params


def test_regime_breakdown_rows_carry_verdict_and_reliable():
    rows = [("expansion", 0.01)] * 10 + [("crisis", -0.02)] * 3
    db = _FakeDB([[], rows])
    tracker = LivePerformanceTracker(db_manager=db)
    out = tracker.compute_regime_breakdown(as_of_date=date(2026, 6, 10))
    assert {r["regime_label"] for r in out} == {"expansion", "crisis"}
    for r in out:
        assert r["verdict"] == "INSUFFICIENT_DATA"  # n far below 100
        assert r["reliable"] is False
        assert isinstance(r["n"], int)
