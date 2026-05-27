"""Tests for the daily autopilot orchestrator.

Verifies the four steps fire in order, each is failure-isolated, and the
aggregated result accurately reflects per-step outcomes.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from prometheus.meta.autopilot import AutopilotResult, run_daily_autopilot


def _fake_db() -> MagicMock:
    return MagicMock()


def _stub_analysis(rows: int = 5, errored: bool = False):
    return SimpleNamespace(
        total_persisted=rows,
        steps=["a", "b", "c"],
        any_errors=errored,
    )


def _stub_drift(rows_count: int = 3, warn: int = 0, errors: int = 0):
    return SimpleNamespace(
        rows=[SimpleNamespace(severity="info") for _ in range(rows_count)],
        warning_or_worse=warn,
        errors=[None] * errors,
    )


def _stub_alerts(recorded: int = 0, errored: bool = False):
    return SimpleNamespace(
        total_recorded=recorded,
        evaluations=[1, 2, 3],
        any_errors=errored,
    )


def _stub_weekly(rows: int = 1, error: str | None = None):
    return SimpleNamespace(rows_persisted=rows, error=error)


def test_happy_path_aggregates_everything():
    db = _fake_db()
    with patch("prometheus.meta.daily_analysis.run_daily_meta_analysis",
               return_value=_stub_analysis(rows=42)) as ma, \
         patch("prometheus.meta.drift_monitor.run_daily_drift_check",
               return_value=_stub_drift(rows_count=7, warn=2)) as dr, \
         patch("prometheus.meta.notifications.evaluate_daily_alerts",
               return_value=_stub_alerts(recorded=5)) as al, \
         patch("prometheus.meta.daily_analysis.run_weekly_report",
               return_value=_stub_weekly()) as wk:
        # Wednesday — weekly should NOT run
        result = run_daily_autopilot(db, date(2026, 5, 27))

    assert isinstance(result, AutopilotResult)
    assert result.meta_analysis_rows == 42
    assert result.drift_rows == 7
    assert result.drift_warning_or_worse == 2
    assert result.notifications_recorded == 5
    assert result.weekly_report_persisted is False  # Wednesday skip
    assert result.ok
    ma.assert_called_once_with(db, date(2026, 5, 27))
    dr.assert_called_once_with(db, date(2026, 5, 27))
    al.assert_called_once_with(db, date(2026, 5, 27))
    wk.assert_not_called()


def test_weekly_runs_on_monday():
    db = _fake_db()
    with patch("prometheus.meta.daily_analysis.run_daily_meta_analysis",
               return_value=_stub_analysis()), \
         patch("prometheus.meta.drift_monitor.run_daily_drift_check",
               return_value=_stub_drift()), \
         patch("prometheus.meta.notifications.evaluate_daily_alerts",
               return_value=_stub_alerts()), \
         patch("prometheus.meta.daily_analysis.run_weekly_report",
               return_value=_stub_weekly()) as wk:
        result = run_daily_autopilot(db, date(2026, 5, 25))  # Monday
    assert result.weekly_report_persisted is True
    wk.assert_called_once_with(db, date(2026, 5, 25))


def test_meta_analysis_failure_does_not_block_drift_or_alerts():
    db = _fake_db()
    with patch("prometheus.meta.daily_analysis.run_daily_meta_analysis",
               side_effect=RuntimeError("kaboom")), \
         patch("prometheus.meta.drift_monitor.run_daily_drift_check",
               return_value=_stub_drift(rows_count=2)) as dr, \
         patch("prometheus.meta.notifications.evaluate_daily_alerts",
               return_value=_stub_alerts(recorded=1)) as al:
        result = run_daily_autopilot(db, date(2026, 5, 27))

    assert result.meta_analysis_rows == 0
    assert result.drift_rows == 2
    assert result.notifications_recorded == 1
    assert any("meta_analysis" in e for e in result.errors)
    dr.assert_called_once()
    al.assert_called_once()


def test_drift_failure_does_not_block_alerts():
    db = _fake_db()
    with patch("prometheus.meta.daily_analysis.run_daily_meta_analysis",
               return_value=_stub_analysis()), \
         patch("prometheus.meta.drift_monitor.run_daily_drift_check",
               side_effect=ValueError("nope")), \
         patch("prometheus.meta.notifications.evaluate_daily_alerts",
               return_value=_stub_alerts(recorded=3)) as al:
        result = run_daily_autopilot(db, date(2026, 5, 27))

    assert any("drift_check" in e for e in result.errors)
    assert result.notifications_recorded == 3
    al.assert_called_once()


def test_partial_errors_set_any_errors_flag():
    db = _fake_db()
    with patch("prometheus.meta.daily_analysis.run_daily_meta_analysis",
               return_value=_stub_analysis(errored=True)), \
         patch("prometheus.meta.drift_monitor.run_daily_drift_check",
               return_value=_stub_drift()), \
         patch("prometheus.meta.notifications.evaluate_daily_alerts",
               return_value=_stub_alerts(errored=True)):
        result = run_daily_autopilot(db, date(2026, 5, 27))
    assert not result.ok
    # Both partial-error flags surfaced in errors list
    assert sum("partial" in e for e in result.errors) == 2


def test_drift_warning_count_propagates():
    db = _fake_db()
    with patch("prometheus.meta.daily_analysis.run_daily_meta_analysis",
               return_value=_stub_analysis()), \
         patch("prometheus.meta.drift_monitor.run_daily_drift_check",
               return_value=_stub_drift(rows_count=10, warn=4)), \
         patch("prometheus.meta.notifications.evaluate_daily_alerts",
               return_value=_stub_alerts()):
        result = run_daily_autopilot(db, date(2026, 5, 27))
    assert result.drift_warning_or_worse == 4
