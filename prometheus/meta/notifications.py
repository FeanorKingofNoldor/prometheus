"""Notifications rule engine for the autopilot feedback loop.

After the daily meta analysis runs, this module evaluates a small set
of rules against the new rows and records one notification per event
the human reviewer should know about. The frontend's notifications
inbox reads from the ``notifications`` table.

Idempotency: notifications are deduplicated by ``(as_of_date, kind,
source_id)`` so re-running the daily analysis (e.g. a catch-up) does
not multiply alerts. ``ON CONFLICT DO NOTHING`` handles that at the
SQL level.

Rules shipped in Phase B (extend as needed):

* ``proposal_pending`` — any new config proposal created today
  with confidence ≥ threshold.
* ``critical_insight`` — any meta_feedback_insights row with severity
  = "critical".
* ``signal_degradation`` — any signal validation whose verdict is
  bad (SIGNAL_INVERTED, HEDGE_INEFFECTIVE, SHARPE_NEGATIVE) for
  ≥ N of the last M days.
* ``diagnostic_warning`` — any diagnostic report with
  has_underperformers or has_high_risk = true.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from psycopg2.extras import Json

logger = get_logger(__name__)


# ── Constants ────────────────────────────────────────────────────────


KIND_PROPOSAL_PENDING = "proposal_pending"
KIND_CRITICAL_INSIGHT = "critical_insight"
KIND_SIGNAL_DEGRADATION = "signal_degradation"
KIND_DIAGNOSTIC_WARNING = "diagnostic_warning"
KIND_DRIFT_ALERT = "drift_alert"


# Verdicts that count as "bad" for the signal degradation rule.
_BAD_VERDICTS: frozenset[str] = frozenset({
    "SIGNAL_INVERTED",
    "HEDGE_INEFFECTIVE",
    "SHARPE_NEGATIVE",
})


# ── Result types ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class AlertEvaluation:
    """Result of one alert rule's evaluation."""

    kind: str
    recorded: int = 0
    error: str | None = None


@dataclass(frozen=True)
class AlertRunResult:
    as_of_date: date
    evaluations: list[AlertEvaluation] = field(default_factory=list)

    @property
    def total_recorded(self) -> int:
        return sum(e.recorded for e in self.evaluations)

    @property
    def any_errors(self) -> bool:
        return any(e.error for e in self.evaluations)


# ── Public API ──────────────────────────────────────────────────────


def record_notification(
    db: DatabaseManager,
    *,
    as_of_date: date,
    kind: str,
    severity: str,
    title: str,
    body: str | None = None,
    source_table: str | None = None,
    source_id: str | None = None,
    link_path: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> bool:
    """Insert one notification, idempotent on (as_of_date, kind, source_id).

    Returns ``True`` if a new row was inserted, ``False`` if it was a
    duplicate (DO NOTHING fired). Always safe to call repeatedly.
    """
    md = Json(dict(metadata)) if metadata else None
    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO notifications (
                    as_of_date, kind, severity, title, body,
                    source_table, source_id, link_path, metadata_json
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT DO NOTHING
                """,
                (
                    as_of_date, str(kind)[:48], str(severity)[:16],
                    str(title)[:200], body,
                    source_table, source_id, link_path, md,
                ),
            )
            return cur.rowcount > 0


def evaluate_daily_alerts(
    db: DatabaseManager,
    as_of_date: date,
    *,
    min_proposal_confidence: float = 0.4,
    signal_degradation_lookback_days: int = 10,
    signal_degradation_min_bad: int = 5,
) -> AlertRunResult:
    """Evaluate all daily alert rules. Each rule is failure-isolated."""
    result = AlertRunResult(as_of_date=as_of_date)

    result.evaluations.append(_safe_rule(
        _rule_proposal_pending, KIND_PROPOSAL_PENDING,
        db=db, as_of_date=as_of_date,
        min_confidence=min_proposal_confidence,
    ))
    result.evaluations.append(_safe_rule(
        _rule_critical_insight, KIND_CRITICAL_INSIGHT,
        db=db, as_of_date=as_of_date,
    ))
    result.evaluations.append(_safe_rule(
        _rule_signal_degradation, KIND_SIGNAL_DEGRADATION,
        db=db, as_of_date=as_of_date,
        lookback_days=signal_degradation_lookback_days,
        min_bad=signal_degradation_min_bad,
    ))
    result.evaluations.append(_safe_rule(
        _rule_diagnostic_warning, KIND_DIAGNOSTIC_WARNING,
        db=db, as_of_date=as_of_date,
    ))
    result.evaluations.append(_safe_rule(
        _rule_drift_alert, KIND_DRIFT_ALERT,
        db=db, as_of_date=as_of_date,
    ))

    logger.info(
        "evaluate_daily_alerts %s: %d notifications recorded (errors=%s)",
        as_of_date, result.total_recorded, result.any_errors,
    )
    return result


# ── Individual rules ────────────────────────────────────────────────


def _rule_proposal_pending(
    *, db: DatabaseManager, as_of_date: date, min_confidence: float,
) -> AlertEvaluation:
    """One notification per new proposal created today with sufficient
    confidence. Severity scales with confidence_score."""
    recorded = 0
    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT proposal_id, strategy_id, proposal_type,
                       target_component, confidence_score,
                       expected_sharpe_improvement, rationale
                FROM meta_config_proposals
                WHERE created_at::date = %s
                  AND confidence_score >= %s
                """,
                (as_of_date, float(min_confidence)),
            )
            rows = cur.fetchall()

    for row in rows:
        (proposal_id, strategy_id, proposal_type, target_component,
         confidence, sharpe_delta, rationale) = row
        severity = (
            "critical" if float(confidence or 0) >= 0.75
            else "warning" if float(confidence or 0) >= 0.5
            else "info"
        )
        title = (
            f"New proposal for {strategy_id}: "
            f"{proposal_type} on {target_component} "
            f"(confidence {float(confidence or 0):.0%}, "
            f"+{float(sharpe_delta or 0):.2f} Sharpe)"
        )
        if record_notification(
            db, as_of_date=as_of_date, kind=KIND_PROPOSAL_PENDING,
            severity=severity, title=title[:200],
            body=str(rationale or ""),
            source_table="meta_config_proposals",
            source_id=str(proposal_id),
            link_path=f"/insights/proposals/{proposal_id}",
            metadata={
                "strategy_id": strategy_id,
                "confidence": float(confidence or 0),
                "expected_sharpe_improvement": float(sharpe_delta or 0),
            },
        ):
            recorded += 1
    return AlertEvaluation(kind=KIND_PROPOSAL_PENDING, recorded=recorded)


def _rule_critical_insight(
    *, db: DatabaseManager, as_of_date: date,
) -> AlertEvaluation:
    """One notification per critical-severity feedback insight."""
    recorded = 0
    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT insight_id, category, message,
                       metric_name, metric_value, benchmark
                FROM meta_feedback_insights
                WHERE as_of_date = %s
                  AND severity = 'critical'
                """,
                (as_of_date,),
            )
            rows = cur.fetchall()

    for row in rows:
        (insight_id, category, message, metric_name, metric_value, benchmark) = row
        title = f"Critical: {category} — {metric_name} = {float(metric_value):.3f}"
        if record_notification(
            db, as_of_date=as_of_date, kind=KIND_CRITICAL_INSIGHT,
            severity="critical", title=title[:200],
            body=str(message),
            source_table="meta_feedback_insights",
            source_id=str(insight_id),
            link_path="/insights/diagnostics",
            metadata={
                "category": category,
                "metric_value": float(metric_value),
                "benchmark": float(benchmark),
            },
        ):
            recorded += 1
    return AlertEvaluation(kind=KIND_CRITICAL_INSIGHT, recorded=recorded)


def _rule_signal_degradation(
    *,
    db: DatabaseManager,
    as_of_date: date,
    lookback_days: int,
    min_bad: int,
) -> AlertEvaluation:
    """One notification per signal_name whose verdict has been bad for
    ≥ min_bad of the last lookback_days runs."""
    start = as_of_date - timedelta(days=lookback_days)
    recorded = 0
    bad_list = list(_BAD_VERDICTS)

    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT signal_name, COUNT(*) AS bad_count,
                       MAX(verdict) AS sample_verdict
                FROM meta_signal_validations
                WHERE as_of_date BETWEEN %s AND %s
                  AND verdict = ANY(%s)
                GROUP BY signal_name
                HAVING COUNT(*) >= %s
                """,
                (start, as_of_date, bad_list, int(min_bad)),
            )
            rows = cur.fetchall()

    for row in rows:
        (signal_name, bad_count, sample_verdict) = row
        title = (
            f"Signal degradation: {signal_name} verdict was bad on "
            f"{int(bad_count)} of last {lookback_days} days "
            f"(sample: {sample_verdict})"
        )
        if record_notification(
            db, as_of_date=as_of_date, kind=KIND_SIGNAL_DEGRADATION,
            severity="warning", title=title[:200],
            body=(
                f"The signal validation rule for {signal_name} has been "
                f"flagging issues persistently over the last "
                f"{lookback_days} trading days. Review the validations "
                f"page for the trend and consider tightening the "
                f"underlying engine's threshold."
            ),
            source_table="meta_signal_validations",
            source_id=str(signal_name),
            link_path="/insights/signal-validations",
            metadata={
                "signal_name": signal_name,
                "bad_days": int(bad_count),
                "lookback_days": int(lookback_days),
                "sample_verdict": sample_verdict,
            },
        ):
            recorded += 1
    return AlertEvaluation(kind=KIND_SIGNAL_DEGRADATION, recorded=recorded)


def _rule_diagnostic_warning(
    *, db: DatabaseManager, as_of_date: date,
) -> AlertEvaluation:
    """One notification per strategy with underperformers or high-risk
    configs in today's diagnostic report."""
    recorded = 0
    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT report_id, strategy_id,
                       has_underperformers, has_high_risk,
                       num_runs_analysed
                FROM meta_diagnostic_reports
                WHERE as_of_date = %s
                  AND (has_underperformers OR has_high_risk)
                """,
                (as_of_date,),
            )
            rows = cur.fetchall()

    for row in rows:
        (report_id, strategy_id, has_under, has_risk, n_runs) = row
        flags: list[str] = []
        if has_under:
            flags.append("underperforming configs")
        if has_risk:
            flags.append("high-risk configs")
        title = (
            f"Diagnostic warning for {strategy_id}: "
            f"{', '.join(flags)} (n={int(n_runs)} runs analysed)"
        )
        if record_notification(
            db, as_of_date=as_of_date, kind=KIND_DIAGNOSTIC_WARNING,
            severity="warning", title=title[:200],
            body=(
                f"The DiagnosticsEngine flagged {', '.join(flags)} for "
                f"strategy {strategy_id}. Open the diagnostics page for "
                f"the per-config breakdown."
            ),
            source_table="meta_diagnostic_reports",
            source_id=str(report_id),
            link_path=f"/insights/diagnostics/{strategy_id}",
            metadata={
                "strategy_id": strategy_id,
                "has_underperformers": bool(has_under),
                "has_high_risk": bool(has_risk),
                "num_runs_analysed": int(n_runs),
            },
        ):
            recorded += 1
    return AlertEvaluation(kind=KIND_DIAGNOSTIC_WARNING, recorded=recorded)


def _rule_drift_alert(
    *, db: DatabaseManager, as_of_date: date,
) -> AlertEvaluation:
    """One notification per drift row in today's run that's warning+."""
    recorded = 0
    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT drift_id, strategy_id, horizon_days,
                       sharpe_delta, live_sharpe, backtest_sharpe,
                       severity, notes
                FROM backtest_live_drift
                WHERE as_of_date = %s
                  AND severity IN ('warning', 'critical')
                """,
                (as_of_date,),
            )
            rows = cur.fetchall()

    for row in rows:
        (drift_id, strategy_id, horizon_days,
         sharpe_delta, live_sharpe, backtest_sharpe,
         severity, notes) = row
        title = (
            f"Backtest-vs-live drift: {strategy_id} "
            f"({int(horizon_days)}d horizon) sharpe Δ"
            f"{float(sharpe_delta or 0):+.2f} "
            f"(live={_fmt(live_sharpe)}, backtest={_fmt(backtest_sharpe)})"
        )
        if record_notification(
            db, as_of_date=as_of_date, kind=KIND_DRIFT_ALERT,
            severity=str(severity), title=title[:200],
            body=str(notes or ""),
            source_table="backtest_live_drift",
            source_id=str(drift_id),
            link_path=f"/insights/drift/{strategy_id}",
            metadata={
                "strategy_id": strategy_id,
                "horizon_days": int(horizon_days),
                "sharpe_delta": float(sharpe_delta or 0),
                "live_sharpe": float(live_sharpe) if live_sharpe is not None else None,
                "backtest_sharpe": (
                    float(backtest_sharpe) if backtest_sharpe is not None else None
                ),
            },
        ):
            recorded += 1
    return AlertEvaluation(kind=KIND_DRIFT_ALERT, recorded=recorded)


def _fmt(x: Any) -> str:
    if x is None:
        return "—"
    try:
        return f"{float(x):.2f}"
    except (TypeError, ValueError):
        return "?"


# ── Inbox queries (used by the API later) ───────────────────────────


def list_unread_notifications(
    db: DatabaseManager, *, limit: int = 100,
) -> list[dict[str, Any]]:
    """Newest unread + un-dismissed first. Used by the inbox API."""
    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT notification_id, created_at, as_of_date,
                       kind, severity, title, body,
                       source_table, source_id, link_path, metadata_json
                FROM notifications
                WHERE read_at IS NULL AND dismissed_at IS NULL
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (int(limit),),
            )
            cols = [
                "notification_id", "created_at", "as_of_date",
                "kind", "severity", "title", "body",
                "source_table", "source_id", "link_path", "metadata_json",
            ]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def mark_read(db: DatabaseManager, notification_id: int) -> bool:
    """Mark one notification as read. Idempotent."""
    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE notifications
                SET read_at = now()
                WHERE notification_id = %s AND read_at IS NULL
                """,
                (int(notification_id),),
            )
            return cur.rowcount > 0


def dismiss(db: DatabaseManager, notification_id: int) -> bool:
    """Mark one notification as dismissed. Idempotent."""
    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE notifications
                SET dismissed_at = now()
                WHERE notification_id = %s AND dismissed_at IS NULL
                """,
                (int(notification_id),),
            )
            return cur.rowcount > 0


# ── Helpers ─────────────────────────────────────────────────────────


def _safe_rule(fn: Any, kind: str, **kwargs: Any) -> AlertEvaluation:
    try:
        out = fn(**kwargs)
        if not isinstance(out, AlertEvaluation):
            return AlertEvaluation(kind=kind)
        return out
    except Exception as exc:
        logger.exception("alert rule failed: %s", fn.__name__)
        return AlertEvaluation(
            kind=kind,
            error=f"{type(exc).__name__}: {exc}",
        )


__all__ = [
    "AlertEvaluation",
    "AlertRunResult",
    "KIND_PROPOSAL_PENDING",
    "KIND_CRITICAL_INSIGHT",
    "KIND_SIGNAL_DEGRADATION",
    "KIND_DIAGNOSTIC_WARNING",
    "KIND_DRIFT_ALERT",
    "record_notification",
    "evaluate_daily_alerts",
    "list_unread_notifications",
    "mark_read",
    "dismiss",
]
