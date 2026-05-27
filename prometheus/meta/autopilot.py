"""Daily autopilot orchestration.

Single entry point that runs the four daily meta steps in the right
order with full failure isolation:

  1. ``run_daily_meta_analysis``  — feedback insights, signal validations,
     diagnostics + proposals per strategy
  2. ``run_daily_drift_check``    — backtest-vs-live Sharpe/return/maxdd
     comparison (must run before alerts so the drift_alert rule sees
     today's rows)
  3. ``evaluate_daily_alerts``    — emit notifications for proposals,
     critical insights, signal degradation, diagnostic warnings, drift
  4. ``run_weekly_report``        — Monday-gated 5-day rollup

Each step is wrapped in try/except: any failure logs but does NOT block
later steps. This is intentional — partial output is more useful than
zero output during incidents.

Callers:
  * ``prometheus.orchestration.daily_orchestrator.DailyOrchestrator.run_pipeline``
    invokes this after OUTCOME_EVAL completes.
  * ``prometheus.orchestration.market_aware_daemon`` invokes this from
    the finalize job when US_EQ completes POST_CLOSE, so the autopilot
    fires automatically once per trading day without a separate cron.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class AutopilotResult:
    """Aggregated outcome of a single autopilot tick."""

    as_of_date: date
    meta_analysis_rows: int = 0
    drift_rows: int = 0
    drift_warning_or_worse: int = 0
    notifications_recorded: int = 0
    weekly_report_persisted: bool = False
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def run_daily_autopilot(
    db: DatabaseManager,
    as_of_date: date,
) -> AutopilotResult:
    """Run the four daily meta steps. Always returns; never raises."""
    result = AutopilotResult(as_of_date=as_of_date)

    # 1) Meta analysis (feedback, signal validations, diagnostics, proposals)
    try:
        from prometheus.meta.daily_analysis import run_daily_meta_analysis
        analysis = run_daily_meta_analysis(db, as_of_date)
        result.meta_analysis_rows = analysis.total_persisted
        if analysis.any_errors:
            result.errors.append("meta_analysis: partial (some steps errored)")
        logger.info(
            "autopilot[meta_analysis]: %d rows persisted across %d steps",
            analysis.total_persisted, len(analysis.steps),
        )
    except Exception as exc:
        result.errors.append(f"meta_analysis: {exc}")
        logger.exception("autopilot[meta_analysis] failed")

    # 2) Backtest-vs-live drift — must run before alerts so drift_alert
    # rule sees today's rows.
    try:
        from prometheus.meta.drift_monitor import run_daily_drift_check
        drift = run_daily_drift_check(db, as_of_date)
        result.drift_rows = len(drift.rows)
        result.drift_warning_or_worse = drift.warning_or_worse
        if drift.warning_or_worse:
            logger.warning(
                "autopilot[drift]: %d strategies in warning+ severity",
                drift.warning_or_worse,
            )
        else:
            logger.info("autopilot[drift]: %d rows, all within tolerance",
                        result.drift_rows)
    except Exception as exc:
        result.errors.append(f"drift_check: {exc}")
        logger.exception("autopilot[drift] failed")

    # 3) Alert rules — emit notifications into the inbox.
    try:
        from prometheus.meta.notifications import evaluate_daily_alerts
        alerts = evaluate_daily_alerts(db, as_of_date)
        result.notifications_recorded = alerts.total_recorded
        if alerts.any_errors:
            result.errors.append("alerts: partial (some rules errored)")
        logger.info(
            "autopilot[alerts]: %d notifications recorded across %d rules",
            alerts.total_recorded, len(alerts.evaluations),
        )
    except Exception as exc:
        result.errors.append(f"alerts: {exc}")
        logger.exception("autopilot[alerts] failed")

    # 4) Weekly report — Monday post-close rolls up the prior 5 days.
    if as_of_date.weekday() == 0:  # Monday
        try:
            from prometheus.meta.daily_analysis import run_weekly_report
            weekly = run_weekly_report(db, as_of_date)
            if weekly.error:
                result.errors.append(f"weekly_report: {weekly.error}")
                logger.warning("autopilot[weekly]: %s", weekly.error)
            else:
                result.weekly_report_persisted = bool(weekly.rows_persisted)
                logger.info(
                    "autopilot[weekly]: persisted for week ending %s",
                    as_of_date.isoformat(),
                )
        except Exception as exc:
            result.errors.append(f"weekly_report: {exc}")
            logger.exception("autopilot[weekly] failed")

    return result


def __getattr__(name: str) -> Any:
    """Lazy attribute access for forward compatibility."""
    raise AttributeError(name)
