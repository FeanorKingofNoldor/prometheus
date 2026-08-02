"""Execution-telemetry invariants (prometheus/execution/invariants.py).

Fake-DB unit tests keyed on SQL shape, mirroring test_daemon_health_check.
The module exists because the 2026-07 fill blindness (broker traded, DB
recorded nothing) ran three weeks undetected — these tests pin each
detector plus the notification wiring.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime

from prometheus.execution.invariants import (
    InvariantsResult,
    run_invariants_check,
)

_TODAY = date(2026, 8, 3)


class _FakeCursor:
    """Answers keyed by SQL substring; unknown queries return (0,)."""

    def __init__(self, answers: dict[str, list]) -> None:
        self._answers = answers
        self._rows: list = [(0,)]

    def execute(self, sql: str, params=()) -> None:
        s = " ".join(sql.split()).lower()
        if "from positions_snapshots" in s and "count(*)" in s:
            self._rows = self._answers.get("snapshot_count", [(1,)])
        elif "max(as_of_date)" in s:
            self._rows = self._answers.get("prev_snapshot_date", [(date(2026, 7, 31),)])
        elif "full outer join" in s:
            self._rows = self._answers.get("delta_mismatches", [])
        elif "from portfolio_equity_history" in s:
            self._rows = self._answers.get("equity_rows", [
                (_TODAY, 200_000.0), (date(2026, 7, 31), 199_000.0),
            ])
        elif "status in ('submitted', 'pending')" in s:
            self._rows = self._answers.get("stuck_orders", [(0, None)])
        elif "join fills" in s:
            self._rows = self._answers.get("cancelled_with_fills", [(0,)])
        else:
            self._rows = [(0,)]

    def fetchall(self):
        return list(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeDb:
    def __init__(self, answers: dict[str, list]) -> None:
        self._answers = answers

    @contextmanager
    def get_runtime_connection(self):
        yield self

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._answers)


def _run(answers: dict[str, list], **kwargs) -> InvariantsResult:
    return run_invariants_check(
        _FakeDb(answers), _TODAY, portfolio_id="IBKR_PAPER", mode="paper",
        notify=False, **kwargs,
    )


def test_all_clean_produces_no_violations():
    result = _run({})
    assert result.violations == []
    assert result.errors == []
    assert result.checks_run >= 5


def test_missing_snapshot_is_critical_and_skips_delta_check():
    result = _run({"snapshot_count": [(0,)]})
    checks = [v.check for v in result.violations]
    assert "snapshot_missing" in checks
    assert result.violations[0].severity == "critical"
    # delta check must not have run against two stale days
    assert "position_delta_without_fills" not in checks


def test_position_delta_without_fills_is_critical():
    result = _run({
        "delta_mismatches": [("PFE.US", -196.0, 0.0), ("KO.US", -60.0, 0.0)],
    })
    v = next(v for v in result.violations if v.check == "position_delta_without_fills")
    assert v.severity == "critical"
    assert "PFE.US" in v.detail


def test_first_snapshot_ever_skips_delta_check():
    result = _run({"prev_snapshot_date": [(None,)]})
    assert all(v.check != "position_delta_without_fills" for v in result.violations)


def test_equity_jump_warns():
    result = _run({
        "equity_rows": [(_TODAY, 150_000.0), (date(2026, 7, 31), 200_000.0)],
    })
    v = next(v for v in result.violations if v.check == "equity_jump")
    assert v.severity == "warning"


def test_equity_missing_today_warns():
    result = _run({
        "equity_rows": [(date(2026, 7, 31), 200_000.0), (date(2026, 7, 30), 201_000.0)],
    })
    assert any(v.check == "equity_missing" for v in result.violations)


def test_stuck_orders_warn():
    result = _run({"stuck_orders": [(3, datetime(2026, 7, 28, 21, 35))]})
    v = next(v for v in result.violations if v.check == "orders_stuck_nonterminal")
    assert v.severity == "warning"
    assert "3 order(s)" in v.title


def test_cancelled_with_fills_is_critical():
    result = _run({"cancelled_with_fills": [(5,)]})
    v = next(v for v in result.violations if v.check == "cancelled_order_has_fills")
    assert v.severity == "critical"


def test_check_failure_is_isolated():
    class _BoomDb(_FakeDb):
        @contextmanager
        def get_runtime_connection(self):
            raise RuntimeError("db down")
            yield  # pragma: no cover

    result = run_invariants_check(
        _BoomDb({}), _TODAY, portfolio_id="IBKR_PAPER", mode="paper", notify=False,
    )
    assert result.errors  # every check failed, none raised
    assert result.violations == []


def test_notifications_written_idempotently(monkeypatch):
    calls: list[dict] = []

    import prometheus.meta.notifications as notif

    monkeypatch.setattr(
        notif, "record_notification", lambda db, **kw: calls.append(kw) or True,
    )

    run_invariants_check(
        _FakeDb({"cancelled_with_fills": [(2,)]}),
        _TODAY, portfolio_id="IBKR_PAPER", mode="paper", notify=True,
    )
    assert len(calls) == 1
    kw = calls[0]
    assert kw["kind"] == "invariant_cancelled_order_has_fills"
    assert kw["severity"] == "critical"
    # idempotency key: same day + check → same source_id
    assert kw["source_id"] == f"IBKR_PAPER:{_TODAY}:cancelled_order_has_fills"


# ---------------------------------------------------------------------------
# DAG wiring
# ---------------------------------------------------------------------------


def test_us_dag_has_invariants_check_with_no_dependents():
    from prometheus.orchestration.dag import build_market_dag

    dag = build_market_dag("US_EQ", _TODAY)
    job_id = f"us_eq_invariants_check_{_TODAY}"
    job = dag.jobs[job_id]
    assert job.job_type == "invariants_check"
    assert job.dependencies == (f"us_eq_snapshot_positions_{_TODAY}",)
    # dangles: nothing may depend on it — it must never block finalize
    assert all(job_id not in j.dependencies for j in dag.jobs.values())


def test_non_account_global_dag_has_no_invariants_check():
    from prometheus.orchestration.dag import build_market_dag

    dag = build_market_dag("UK_EQ", _TODAY)
    assert not any(j.job_type == "invariants_check" for j in dag.jobs.values())
