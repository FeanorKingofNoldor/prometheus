"""Daily meta-analysis orchestrator.

Single entry point that the daily pipeline calls to run every piece
of meta machinery in sequence and persist the results to the new
analysis tables introduced in migration 0099.

The functions called here all existed already as orphaned modules
(``compute_feedback_report`` was logged but not persisted; the others
were on-demand-API-only). This module wires them into the daily DAG
so a human reviewer sees yesterday's diagnostics + insights +
proposals in the frontend each morning without manually running
anything.

Failure-isolated by design: a crash in one analysis module does not
prevent the others from running. The caller logs but never raises.
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from psycopg2.extras import Json

from prometheus.decisions.live_performance import LivePerformanceTracker
from prometheus.meta.diagnostics import DiagnosticsEngine
from prometheus.meta.feedback import compute_feedback_report
from prometheus.meta.proposal_generator import ProposalGenerator
from prometheus.meta.trade_monitor import compute_weekly_report, format_weekly_report

logger = get_logger(__name__)


# ── Result types ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class StepResult:
    """Per-step outcome of the daily analysis run."""

    name: str
    rows_persisted: int = 0
    skipped_reason: str | None = None
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.error is None and self.skipped_reason is None


@dataclass(frozen=True)
class DailyAnalysisResult:
    """Aggregated outcome across all steps."""

    as_of_date: date
    steps: list[StepResult] = field(default_factory=list)

    @property
    def total_persisted(self) -> int:
        return sum(s.rows_persisted for s in self.steps)

    @property
    def any_errors(self) -> bool:
        return any(s.error is not None for s in self.steps)


# ── Main entry ───────────────────────────────────────────────────────


def run_daily_meta_analysis(
    db: DatabaseManager,
    as_of_date: date,
    *,
    strategies: list[str] | None = None,
    lookback_days: int = 63,
    horizon_days: int = 21,
    portfolio_entity_id: str = "US_EQ",
    diagnostics_min_runs: int = 5,
) -> DailyAnalysisResult:
    """Run every meta-analysis module once + persist results.

    Designed to be called from the daily orchestrator after outcome
    evaluation completes. Each step is independent: a failure in one
    does not propagate.

    ``strategies`` defaults to whatever strategies have backtest_runs
    in the last 90 days (the diagnostics engine's natural scope).
    """
    result = DailyAnalysisResult(as_of_date=as_of_date)

    # Step 1 — feedback report
    result.steps.append(
        _safe(_run_feedback_step, db=db, as_of_date=as_of_date,
              lookback_days=lookback_days)
    )

    # Step 2 — signal validations
    result.steps.append(
        _safe(_run_signal_validations_step, db=db, as_of_date=as_of_date,
              lookback_days=lookback_days, horizon_days=horizon_days,
              portfolio_entity_id=portfolio_entity_id)
    )

    # Step 3 — diagnostics + proposals per strategy
    targets = strategies if strategies is not None else _discover_strategies(db)
    for strategy_id in targets:
        result.steps.append(
            _safe(
                _run_diagnostics_step, db=db, as_of_date=as_of_date,
                strategy_id=strategy_id, min_runs=diagnostics_min_runs,
            )
        )
        # Proposal generation depends on diagnostics having succeeded.
        last_step = result.steps[-1]
        if last_step.succeeded:
            result.steps.append(
                _safe(
                    _run_proposals_step, db=db, strategy_id=strategy_id,
                )
            )

    logger.info(
        "daily_meta_analysis %s: %d total rows persisted across %d steps "
        "(errors=%s)",
        as_of_date, result.total_persisted, len(result.steps),
        result.any_errors,
    )
    return result


# ── Individual steps ────────────────────────────────────────────────


def _run_feedback_step(
    *, db: DatabaseManager, as_of_date: date, lookback_days: int,
) -> StepResult:
    report = compute_feedback_report(db, as_of_date, lookback_days=lookback_days)
    if not report.insights:
        return StepResult(name="feedback_report",
                          skipped_reason="empty_insights")

    rows = _persist_feedback_insights(
        db, as_of_date=as_of_date,
        insights=report.insights, lookback_days=lookback_days,
    )
    return StepResult(name="feedback_report", rows_persisted=rows)


def _run_signal_validations_step(
    *,
    db: DatabaseManager,
    as_of_date: date,
    lookback_days: int,
    horizon_days: int,
    portfolio_entity_id: str,
) -> StepResult:
    tracker = LivePerformanceTracker(db_manager=db)

    validations: list[dict[str, Any]] = []

    # Fragility signal validation
    frag = tracker.validate_fragility_signal(
        as_of_date=as_of_date,
        lookback_days=lookback_days,
        horizon_days=horizon_days,
        entity_id=portfolio_entity_id,
    )
    validations.append({
        "signal_name": "fragility_vs_portfolio_return",
        "verdict": str(frag.get("verdict", "ERROR")),
        "metric_value": _safe_float(frag.get("spearman_rho")),
        "threshold": -0.2,
        "sample_size": int(frag.get("n", 0) or 0),
        "details": frag,
    })

    # Hedge effectiveness
    hedge = tracker.compute_hedge_effectiveness(
        as_of_date=as_of_date,
        lookback_days=lookback_days,
        horizon_days=1,
    )
    validations.append({
        "signal_name": "hedge_options_vs_portfolio_pnl",
        "verdict": str(hedge.get("verdict", "ERROR")),
        "metric_value": _safe_float(hedge.get("pearson_r")),
        "threshold": -0.2,
        "sample_size": int(hedge.get("n_dates", 0) or 0),
        "details": hedge,
    })

    # Rolling portfolio performance (the overall scorecard)
    rolling = tracker.compute_rolling_performance(
        as_of_date=as_of_date,
        lookback_days=lookback_days,
        horizon_days=horizon_days,
    )
    sharpe = _safe_float(rolling.get("sharpe"))
    if sharpe is not None:
        if sharpe > 0.5:
            verdict = "SHARPE_HEALTHY"
        elif sharpe > 0:
            verdict = "SHARPE_MARGINAL"
        else:
            verdict = "SHARPE_NEGATIVE"
    else:
        verdict = "INSUFFICIENT_DATA"
    validations.append({
        "signal_name": "rolling_portfolio_sharpe",
        "verdict": verdict,
        "metric_value": sharpe,
        "threshold": 0.5,
        "sample_size": int(rolling.get("n", 0) or 0),
        "details": rolling,
    })

    rows = _persist_signal_validations(
        db, as_of_date=as_of_date,
        validations=validations, lookback_days=lookback_days,
    )
    return StepResult(name="signal_validations", rows_persisted=rows)


def _run_diagnostics_step(
    *,
    db: DatabaseManager,
    as_of_date: date,
    strategy_id: str,
    min_runs: int,
) -> StepResult:
    engine = DiagnosticsEngine(db_manager=db)
    try:
        report = engine.analyze_strategy(strategy_id, min_sample_size=min_runs)
    except ValueError as exc:
        # Not enough backtest data — skip rather than error
        return StepResult(
            name=f"diagnostics[{strategy_id}]",
            skipped_reason=str(exc),
        )

    rows = _persist_diagnostic_report(
        db, as_of_date=as_of_date,
        strategy_id=strategy_id, report=report,
    )
    return StepResult(name=f"diagnostics[{strategy_id}]", rows_persisted=rows)


def _run_proposals_step(
    *, db: DatabaseManager, strategy_id: str,
) -> StepResult:
    engine = DiagnosticsEngine(db_manager=db)
    generator = ProposalGenerator(
        db_manager=db, diagnostics_engine=engine,
    )
    proposals = generator.generate_proposals(
        strategy_id, auto_save=True,
    )
    return StepResult(
        name=f"proposals[{strategy_id}]",
        rows_persisted=len(proposals),
    )


# ── Persistence ─────────────────────────────────────────────────────


def _persist_feedback_insights(
    db: DatabaseManager,
    *,
    as_of_date: date,
    insights: list[Any],
    lookback_days: int,
) -> int:
    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            for ins in insights:
                cur.execute(
                    """
                    INSERT INTO meta_feedback_insights (
                        as_of_date, category, severity, message,
                        metric_name, metric_value, benchmark, deviation,
                        lookback_days
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        as_of_date,
                        str(ins.category)[:64],
                        str(ins.severity)[:16],
                        str(ins.message)[:500],
                        str(ins.metric_name)[:64],
                        float(ins.metric_value),
                        float(ins.benchmark),
                        float(ins.deviation),
                        int(lookback_days),
                    ),
                )
    return len(insights)


def _persist_signal_validations(
    db: DatabaseManager,
    *,
    as_of_date: date,
    validations: list[dict[str, Any]],
    lookback_days: int,
) -> int:
    rows = 0
    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            for v in validations:
                cur.execute(
                    """
                    INSERT INTO meta_signal_validations (
                        as_of_date, signal_name, verdict,
                        metric_value, threshold, sample_size,
                        lookback_days, details_json
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s
                    )
                    """,
                    (
                        as_of_date,
                        str(v["signal_name"])[:64],
                        str(v["verdict"])[:32],
                        v.get("metric_value"),
                        v.get("threshold"),
                        v.get("sample_size"),
                        int(lookback_days),
                        Json(_safe_json(v.get("details") or {})),
                    ),
                )
                rows += 1
    return rows


def run_weekly_report(
    db: DatabaseManager,
    as_of_date: date,
    *,
    strategy_id: str | None = None,
) -> StepResult:
    """Compute + persist a weekly report for the trailing 5 trading days.

    Designed to be called on Monday post-close from the daily
    orchestrator. Idempotent on ``(week_start, strategy_id)`` —
    re-running the same Monday overwrites that week's row.
    """
    try:
        report = compute_weekly_report(db, as_of_date)
    except Exception as exc:
        logger.exception("compute_weekly_report failed")
        return StepResult(
            name="weekly_report",
            error=f"{type(exc).__name__}: {exc}",
        )

    try:
        markdown = format_weekly_report(report)
    except Exception:
        logger.debug("weekly report markdown render failed", exc_info=True)
        markdown = ""

    rows = _persist_weekly_report(
        db,
        report=report, strategy_id=strategy_id, markdown=markdown,
    )
    return StepResult(name="weekly_report", rows_persisted=rows)


def _persist_weekly_report(
    db: DatabaseManager,
    *,
    report: Any,
    strategy_id: str | None,
    markdown: str,
) -> int:
    """Upsert into ``weekly_reports`` keyed by (week_start, strategy_id).

    With strategy_id NULL the unique index does not enforce uniqueness
    (Postgres treats NULLs as distinct), so we DELETE-then-INSERT for
    the NULL case to keep "one row per week per strategy or overall"
    semantics intact.
    """
    report_dict = _safe_json(asdict(report))
    period_return = _safe_float(report.period_return_pct)
    period_sharpe = _safe_float(report.period_sharpe)
    max_dd = _safe_float(report.max_drawdown_period)
    n_trades = len(report.closed_trades)
    n_winners = sum(1 for t in report.closed_trades if (t.realized_pnl or 0) > 0)
    n_losers = sum(1 for t in report.closed_trades if (t.realized_pnl or 0) < 0)

    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            if strategy_id is None:
                cur.execute(
                    """
                    DELETE FROM weekly_reports
                    WHERE week_start = %s AND strategy_id IS NULL
                    """,
                    (report.period_start,),
                )
                cur.execute(
                    """
                    INSERT INTO weekly_reports (
                        week_start, week_end, strategy_id,
                        period_return, period_sharpe, period_max_drawdown,
                        n_trades, n_winners, n_losers,
                        report_json, markdown
                    ) VALUES (
                        %s, %s, NULL,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s
                    )
                    """,
                    (
                        report.period_start, report.period_end,
                        period_return, period_sharpe, max_dd,
                        n_trades, n_winners, n_losers,
                        Json(report_dict), markdown,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO weekly_reports (
                        week_start, week_end, strategy_id,
                        period_return, period_sharpe, period_max_drawdown,
                        n_trades, n_winners, n_losers,
                        report_json, markdown
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s
                    )
                    ON CONFLICT (week_start, strategy_id) DO UPDATE SET
                        week_end = EXCLUDED.week_end,
                        period_return = EXCLUDED.period_return,
                        period_sharpe = EXCLUDED.period_sharpe,
                        period_max_drawdown = EXCLUDED.period_max_drawdown,
                        n_trades = EXCLUDED.n_trades,
                        n_winners = EXCLUDED.n_winners,
                        n_losers = EXCLUDED.n_losers,
                        report_json = EXCLUDED.report_json,
                        markdown = EXCLUDED.markdown
                    """,
                    (
                        report.period_start, report.period_end, str(strategy_id)[:64],
                        period_return, period_sharpe, max_dd,
                        n_trades, n_winners, n_losers,
                        Json(report_dict), markdown,
                    ),
                )
    return 1


def _persist_diagnostic_report(
    db: DatabaseManager,
    *,
    as_of_date: date,
    strategy_id: str,
    report: Any,
) -> int:
    report_dict = _safe_json(asdict(report))
    n_underperforming = len(report.underperforming_configs)
    n_high_risk = len(report.high_risk_configs)
    n_runs = int(report.sample_metadata.get("total_runs", 0))

    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO meta_diagnostic_reports (
                    as_of_date, strategy_id,
                    has_underperformers, has_high_risk,
                    num_runs_analysed, report_json
                ) VALUES (
                    %s, %s,
                    %s, %s,
                    %s, %s
                )
                """,
                (
                    as_of_date, str(strategy_id)[:64],
                    n_underperforming > 0, n_high_risk > 0,
                    n_runs, Json(report_dict),
                ),
            )
    return 1


# ── Helpers ─────────────────────────────────────────────────────────


def _discover_strategies(db: DatabaseManager) -> list[str]:
    """Strategies with at least one backtest_runs row in the last 90 days."""
    try:
        with db.get_runtime_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT strategy_id
                    FROM backtest_runs
                    WHERE created_at >= now() - interval '90 days'
                      AND strategy_id IS NOT NULL
                    ORDER BY strategy_id
                    """,
                )
                return [str(r[0]) for r in cur.fetchall() if r[0]]
    except Exception as exc:
        logger.debug("_discover_strategies failed: %s", exc)
        return []


def _safe_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        f = float(x)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _safe_json(obj: Any) -> Any:
    """Round-trip through json.dumps(default=str) to scrub non-JSON
    values (datetimes, NaN/Inf).

    NaN and Inf get replaced with None *before* dumping — postgres
    JSONB doesn't accept them and ``allow_nan=False`` would raise. In
    practice these come from LivePerformanceTracker when the lookback
    window has insufficient data.
    """
    return json.loads(json.dumps(_scrub_nan(obj), default=str, allow_nan=False))


def _scrub_nan(obj: Any) -> Any:
    """Recursively replace NaN/Inf floats with None."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _scrub_nan(v) for k, v in obj.items()}
    if isinstance(obj, list | tuple):
        return [_scrub_nan(v) for v in obj]
    return obj


def _safe(fn: Any, **kwargs: Any) -> StepResult:
    """Run a step function; convert exceptions into StepResult.error."""
    try:
        result = fn(**kwargs)
        if not isinstance(result, StepResult):
            return StepResult(name=str(fn.__name__), rows_persisted=0)
        return result
    except Exception as exc:
        logger.exception("daily_meta_analysis step failed: %s", fn.__name__)
        return StepResult(
            name=str(fn.__name__),
            error=f"{type(exc).__name__}: {exc}",
        )


__all__ = [
    "StepResult",
    "DailyAnalysisResult",
    "run_daily_meta_analysis",
    "run_weekly_report",
]
