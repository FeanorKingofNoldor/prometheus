"""Sample-size gates + run-boundary scoping in the meta feedback loop."""

from __future__ import annotations

from datetime import date
from typing import Any, List

from prometheus.meta.feedback import MIN_SEVERITY_N, compute_feedback_report

# ── Fake DB: sequential canned result sets ───────────────────────────
# Query order inside compute_feedback_report:
#   1. account_resets (run boundary)
#   2. PORTFOLIO outcomes
#   3. ASSESSMENT outcomes
#   4. EXECUTION decisions


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

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *a: Any) -> bool:
        return False


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


def _port_rows(n: int, losing: bool = True) -> List[tuple]:
    """(realized_return, realized_drawdown, horizon_days, metadata) rows."""
    ret = -0.01 if losing else 0.01
    return [(ret, -0.01, 21, {}) for _ in range(n)]


# ── Hit-rate severity gates ──────────────────────────────────────────


def test_small_n_hit_rate_is_insufficient_data_not_critical():
    n = 30  # 0% hit rate would previously be critical
    db = _FakeDB([[], _port_rows(n, losing=True), [], []])
    report = compute_feedback_report(db, date(2026, 6, 10))

    hit_insights = [i for i in report.insights
                    if i.metric_name == "portfolio_hit_rate"]
    assert len(hit_insights) == 1
    ins = hit_insights[0]
    assert ins.severity == "info"
    assert "INSUFFICIENT_DATA" in ins.message
    assert f"n={n}" in ins.message
    assert ins.sample_size == n
    # No warning/critical anywhere from this tiny sample
    assert not any(i.severity in ("warning", "critical") for i in report.insights)


def test_large_n_bad_hit_rate_still_critical():
    n = MIN_SEVERITY_N + 50
    db = _FakeDB([[], _port_rows(n, losing=True), [], []])
    report = compute_feedback_report(db, date(2026, 6, 10))

    crit = [i for i in report.insights
            if i.metric_name == "portfolio_hit_rate" and i.severity == "critical"]
    assert len(crit) == 1
    assert crit[0].sample_size == n
    assert f"n={n}" in crit[0].message


def test_large_n_good_hit_rate_no_alarm():
    n = MIN_SEVERITY_N + 10
    db = _FakeDB([[], _port_rows(n, losing=False), [], []])
    report = compute_feedback_report(db, date(2026, 6, 10))
    assert not any(
        i.metric_name == "portfolio_hit_rate"
        and i.severity in ("warning", "critical")
        for i in report.insights
    )
    assert report.portfolio_hit_rate == 1.0


# ── Assessment spread gate ───────────────────────────────────────────


def test_small_n_inverted_assessment_is_insufficient_data():
    # 20 high-score losers + 20 low-score winners → inverted spread,
    # but n=40 < MIN_SEVERITY_N so severity must be info.
    assess = (
        [("0.10", -0.05, date(2026, 5, 1))] * 20
        + [("-0.10", 0.05, date(2026, 5, 1))] * 20
    )
    db = _FakeDB([[], [], assess, []])
    report = compute_feedback_report(db, date(2026, 6, 10))

    spread = [i for i in report.insights if i.metric_name == "assessment_spread"]
    assert len(spread) == 1
    assert spread[0].severity == "info"
    assert "INSUFFICIENT_DATA" in spread[0].message
    assert spread[0].sample_size == 40


def test_large_n_inverted_assessment_is_critical():
    half = MIN_SEVERITY_N  # 2 * MIN → comfortably above gate
    assess = (
        [("0.10", -0.05, date(2026, 5, 1))] * half
        + [("-0.10", 0.05, date(2026, 5, 1))] * half
    )
    db = _FakeDB([[], [], assess, []])
    report = compute_feedback_report(db, date(2026, 6, 10))

    spread = [i for i in report.insights if i.metric_name == "assessment_spread"]
    assert len(spread) == 1
    assert spread[0].severity == "critical"
    assert spread[0].sample_size == 2 * half


# ── Run-boundary scoping ─────────────────────────────────────────────


def test_feedback_window_clamped_to_reset_boundary():
    reset = date(2026, 5, 13)
    db = _FakeDB([[(reset,)], [], [], []])
    compute_feedback_report(db, date(2026, 6, 10), lookback_days=63)

    # Every windowed query (after account_resets) must start at the reset,
    # not at as_of - 2*63 days.
    for _sql, params in db.cursor.executed[1:]:
        assert params[0] == reset
