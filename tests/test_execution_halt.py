"""PROMETHEUS_EXECUTION_HALT — the core+wheel transition kill switch.

With the flag set, the daily pipeline must keep computing everything
(signals, universes, books, targets — the passive-scoring path) while
submitting NOTHING to the broker, and the health check must not treat
the resulting zero-order days as failures.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import patch

from prometheus.pipeline.state import EngineRun, RunPhase


def _run(phase: RunPhase = RunPhase.BOOKS_DONE) -> EngineRun:
    now = datetime.now(timezone.utc)
    return EngineRun(
        run_id="run-halt-test",
        as_of_date=date(2026, 8, 3),
        region="US",
        phase=phase,
        error=None,
        created_at=now,
        updated_at=now,
        phase_started_at=now,
        phase_completed_at=None,
    )


def test_equity_execution_halted_skips_broker_and_advances_phase(monkeypatch):
    monkeypatch.setenv("PROMETHEUS_EXECUTION_HALT", "1")
    from prometheus.pipeline import tasks

    advanced = {}

    def _fake_update_phase(db, run_id, phase):
        advanced["phase"] = phase
        return _run(phase)

    # _load_target_weights must NOT be reached — the halt short-circuits
    # before any DB/broker work beyond logging.
    with patch.object(tasks, "update_phase", side_effect=_fake_update_phase), \
         patch.object(tasks, "_load_target_weights", side_effect=AssertionError(
             "halt must return before loading targets")):
        result = tasks.run_execution_for_run(object(), _run())

    assert advanced["phase"] == RunPhase.EXECUTION_DONE
    assert result.phase == RunPhase.EXECUTION_DONE


def test_equity_execution_halt_idempotent_when_already_advanced(monkeypatch):
    monkeypatch.setenv("PROMETHEUS_EXECUTION_HALT", "1")
    from prometheus.pipeline import tasks

    with patch.object(tasks, "update_phase", side_effect=AssertionError(
            "must not touch phase when already past BOOKS_DONE")):
        result = tasks.run_execution_for_run(object(), _run(RunPhase.EXECUTION_DONE))
    assert result.phase == RunPhase.EXECUTION_DONE


def test_equity_execution_runs_normally_without_flag(monkeypatch):
    monkeypatch.delenv("PROMETHEUS_EXECUTION_HALT", raising=False)
    from prometheus.pipeline import tasks

    # Without the flag the function proceeds to target loading (which we
    # fail deliberately to stop before any broker work).
    sentinel = RuntimeError("reached target loading")
    with patch.object(tasks, "_load_target_weights", side_effect=sentinel):
        try:
            tasks.run_execution_for_run(object(), _run())
        except RuntimeError as exc:
            assert exc is sentinel
        else:  # pragma: no cover
            raise AssertionError("expected to reach _load_target_weights")


# -- minimal health-check fakes (tests/ is not a package, so the fakes in
# test_daemon_health_check.py are not importable from here) ----------------


class _HCCursor:
    def __init__(self, answers):
        self._answers = answers
        self._last = 0

    def execute(self, sql, params=()):
        s = sql.lower()
        if "close <= 0" in s:
            self._last = self._answers["nonpos_prices"]
        elif "count(distinct instrument_id) from prices_daily" in s:
            self._last = self._answers["prices"]
        elif "from target_portfolios" in s:
            self._last = self._answers["targets"]
        elif "from orders" in s:
            self._last = self._answers["orders"]
        elif "from sector_health_daily" in s:
            self._last = self._answers["shi"]
        else:
            self._last = 0

    def fetchone(self):
        return (self._last,)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _HCConn:
    def __init__(self, answers):
        self._answers = answers

    def cursor(self):
        return _HCCursor(self._answers)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _HCDb:
    def __init__(self, answers):
        self._answers = answers

    def get_historical_connection(self):
        return _HCConn(self._answers)

    def get_runtime_connection(self):
        return _HCConn(self._answers)


_HC_ANSWERS = {"prices": 660, "nonpos_prices": 0, "targets": 50, "orders": 0, "shi": 11}


def test_health_check_accepts_zero_orders_under_halt(monkeypatch, tmp_path):
    monkeypatch.setenv("PROMETHEUS_EXECUTION_HALT", "1")
    monkeypatch.setenv("PROMETHEUS_HEALTH_REPORT_DIR", str(tmp_path))

    from prometheus.orchestration.market_aware_daemon import _run_health_check

    healthy, err = _run_health_check(
        _HCDb(dict(_HC_ANSWERS)), _run(), date(2026, 8, 3), "US_EQ",
    )
    assert healthy is True
    assert err is None


def test_health_check_still_fails_zero_orders_without_halt(monkeypatch, tmp_path):
    monkeypatch.delenv("PROMETHEUS_EXECUTION_HALT", raising=False)
    monkeypatch.setenv("PROMETHEUS_HEALTH_REPORT_DIR", str(tmp_path))

    from prometheus.orchestration.market_aware_daemon import _run_health_check

    healthy, err = _run_health_check(
        _HCDb(dict(_HC_ANSWERS)), _run(), date(2026, 8, 3), "US_EQ",
    )
    assert healthy is False
    assert "NO ORDERS" in err
