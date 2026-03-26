"""Prometheus -- Meta Learning Feedback Loop.

Analyzes decision outcomes to generate actionable insights:
1. Which engine decisions consistently underperform expectations?
2. Which regime conditions correlate with poor outcomes?
3. What parameter adjustments would improve results?

Produces a daily diagnostic report and (optionally) config proposals.
Called after outcome evaluation in the daily pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class FeedbackInsight:
    """A single insight from the feedback analysis."""
    category: str       # portfolio_quality, assessment_accuracy, risk_override_rate, regime_timing
    severity: str       # info, warning, critical
    message: str
    metric_name: str
    metric_value: float
    benchmark: float    # expected/historical value
    deviation: float    # how far from benchmark (signed)


@dataclass
class FeedbackReport:
    """Daily feedback report from the meta learning loop."""
    as_of_date: date
    insights: List[FeedbackInsight]
    portfolio_hit_rate: Optional[float] = None     # % of portfolio decisions with positive return
    assessment_accuracy: Optional[float] = None    # % of BUY signals that actually went up
    risk_override_pct: Optional[float] = None      # % of decisions overridden by risk constraints
    avg_decision_return: Optional[float] = None    # Mean return across evaluated decisions
    regime_at_decision: Optional[str] = None       # Most common regime during decisions


def compute_feedback_report(
    db_manager: DatabaseManager,
    as_of_date: date,
    lookback_days: int = 63,
) -> FeedbackReport:
    """Compute feedback report from recent decision outcomes.

    Analyzes the last `lookback_days` of decision outcomes to find
    systematic patterns — good or bad.
    """
    start = as_of_date - timedelta(days=lookback_days * 2)  # Extra buffer for weekends
    insights: List[FeedbackInsight] = []

    portfolio_hit_rate = None
    assessment_accuracy = None
    risk_override_pct = None
    avg_return = None

    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            # ── Portfolio decision outcomes ──────────────────────────
            cur.execute("""
                SELECT o.realized_return, o.realized_drawdown, o.horizon_days,
                       d.metadata
                FROM decision_outcomes o
                JOIN engine_decisions d ON o.decision_id = d.decision_id
                WHERE d.engine_name = 'PORTFOLIO'
                  AND d.as_of_date BETWEEN %s AND %s
                ORDER BY d.as_of_date DESC
            """, (start, as_of_date))
            port_outcomes = cur.fetchall()

            if port_outcomes:
                returns = [float(r[0]) for r in port_outcomes if r[0] is not None]
                drawdowns = [float(r[1]) for r in port_outcomes if r[1] is not None]

                if returns:
                    hit_rate = sum(1 for r in returns if r > 0) / len(returns)
                    portfolio_hit_rate = hit_rate
                    avg_return = sum(returns) / len(returns)

                    # Insight: portfolio hit rate
                    if hit_rate < 0.45:
                        insights.append(FeedbackInsight(
                            category="portfolio_quality",
                            severity="critical",
                            message=f"Portfolio hit rate {hit_rate:.0%} is below 45% over last {lookback_days} days",
                            metric_name="portfolio_hit_rate",
                            metric_value=hit_rate,
                            benchmark=0.55,
                            deviation=hit_rate - 0.55,
                        ))
                    elif hit_rate < 0.50:
                        insights.append(FeedbackInsight(
                            category="portfolio_quality",
                            severity="warning",
                            message=f"Portfolio hit rate {hit_rate:.0%} is below coin-flip threshold",
                            metric_name="portfolio_hit_rate",
                            metric_value=hit_rate,
                            benchmark=0.50,
                            deviation=hit_rate - 0.50,
                        ))

                    # Insight: average return
                    if avg_return < -0.02:
                        insights.append(FeedbackInsight(
                            category="portfolio_quality",
                            severity="warning",
                            message=f"Average portfolio decision return is {avg_return:.1%} — consistently losing",
                            metric_name="avg_decision_return",
                            metric_value=avg_return,
                            benchmark=0.0,
                            deviation=avg_return,
                        ))

                if drawdowns:
                    avg_dd = sum(drawdowns) / len(drawdowns)
                    if avg_dd < -0.10:
                        insights.append(FeedbackInsight(
                            category="portfolio_quality",
                            severity="warning",
                            message=f"Average decision drawdown {avg_dd:.1%} — risk too high",
                            metric_name="avg_decision_drawdown",
                            metric_value=avg_dd,
                            benchmark=-0.05,
                            deviation=avg_dd - (-0.05),
                        ))

            # ── Assessment decision accuracy ────────────────────────
            cur.execute("""
                SELECT d.metadata->>'mean_score' as mean_score,
                       o.realized_return, d.as_of_date
                FROM decision_outcomes o
                JOIN engine_decisions d ON o.decision_id = d.decision_id
                WHERE d.engine_name = 'ASSESSMENT'
                  AND d.as_of_date BETWEEN %s AND %s
                  AND o.realized_return IS NOT NULL
            """, (start, as_of_date))
            assess_outcomes = cur.fetchall()

            if assess_outcomes:
                # Check if high-score decisions outperform low-score
                high_score_rets = []
                low_score_rets = []
                for (mean_score_str, ret, _) in assess_outcomes:
                    try:
                        ms = float(mean_score_str) if mean_score_str else 0
                        r = float(ret)
                        if ms > 0.05:
                            high_score_rets.append(r)
                        elif ms < -0.05:
                            low_score_rets.append(r)
                    except (TypeError, ValueError):
                        continue

                if high_score_rets and low_score_rets:
                    high_avg = sum(high_score_rets) / len(high_score_rets)
                    low_avg = sum(low_score_rets) / len(low_score_rets)
                    spread = high_avg - low_avg

                    if spread < 0:
                        insights.append(FeedbackInsight(
                            category="assessment_accuracy",
                            severity="critical",
                            message=f"Assessment model inverted: high-score decisions return {high_avg:.1%} vs low-score {low_avg:.1%}",
                            metric_name="assessment_spread",
                            metric_value=spread,
                            benchmark=0.01,
                            deviation=spread - 0.01,
                        ))
                    elif spread < 0.005:
                        insights.append(FeedbackInsight(
                            category="assessment_accuracy",
                            severity="warning",
                            message=f"Assessment model has near-zero spread: {spread:.3%} between high/low scores",
                            metric_name="assessment_spread",
                            metric_value=spread,
                            benchmark=0.01,
                            deviation=spread - 0.01,
                        ))
                    else:
                        assessment_accuracy = spread
                        insights.append(FeedbackInsight(
                            category="assessment_accuracy",
                            severity="info",
                            message=f"Assessment model working: {spread:.3%} spread between high/low scores",
                            metric_name="assessment_spread",
                            metric_value=spread,
                            benchmark=0.01,
                            deviation=spread - 0.01,
                        ))

            # ── Turnover analysis ───────────────────────────────────
            cur.execute("""
                SELECT d.metadata->>'orders_planned' as planned,
                       d.metadata->>'orders_suppressed' as suppressed
                FROM engine_decisions d
                WHERE d.engine_name = 'EXECUTION'
                  AND d.as_of_date BETWEEN %s AND %s
            """, (start, as_of_date))
            exec_decisions = cur.fetchall()

            if exec_decisions:
                total_planned = 0
                total_suppressed = 0
                for (planned, suppressed) in exec_decisions:
                    try:
                        total_planned += int(planned or 0)
                        total_suppressed += int(suppressed or 0)
                    except (TypeError, ValueError):
                        continue

                if total_planned > 0:
                    suppress_rate = total_suppressed / (total_planned + total_suppressed)
                    risk_override_pct = suppress_rate
                    if suppress_rate > 0.5:
                        insights.append(FeedbackInsight(
                            category="risk_override_rate",
                            severity="warning",
                            message=f"{suppress_rate:.0%} of orders suppressed by turnover/risk filters — model generating too many trades",
                            metric_name="order_suppression_rate",
                            metric_value=suppress_rate,
                            benchmark=0.3,
                            deviation=suppress_rate - 0.3,
                        ))

    # Add summary insight if no issues found
    if not any(i.severity in ("warning", "critical") for i in insights):
        insights.append(FeedbackInsight(
            category="summary",
            severity="info",
            message="All meta feedback metrics within acceptable ranges",
            metric_name="overall_status",
            metric_value=1.0,
            benchmark=1.0,
            deviation=0.0,
        ))

    report = FeedbackReport(
        as_of_date=as_of_date,
        insights=insights,
        portfolio_hit_rate=portfolio_hit_rate,
        assessment_accuracy=assessment_accuracy,
        risk_override_pct=risk_override_pct,
        avg_decision_return=avg_return,
    )

    for insight in insights:
        log_fn = logger.warning if insight.severity in ("warning", "critical") else logger.info
        log_fn("Meta feedback [%s]: %s", insight.category, insight.message)

    return report
