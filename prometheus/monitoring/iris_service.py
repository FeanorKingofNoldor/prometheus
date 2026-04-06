"""Prometheus v2 – Iris Service.

Assembles system context from the database and drives the LLM
conversation for the Iris chat endpoint.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from apathis.llm.gateway import get_llm

logger = get_logger(__name__)

# ── System prompt ────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are **Iris**, the meta-orchestrator of the Prometheus trading system.

## System Architecture
- **Alpha engine**: US_EQ_LONG_V12/K25 (lambda-driven, 25 names, 12-17% CAGR, 0.9 Sharpe)
- **Sector overlay**: SectorAllocator kills sick sectors (SHI<0.25), reduces weak (SHI<0.40), sizes SH.US hedge by fragility
- **Crisis alpha**: Offensive SPY puts when ≥5 sectors deteriorate (flash: instant, sustained: 3-day filter)
- **16 options strategies**: regime-adaptive (RISK_ON: income, CRISIS: hedges + crisis alpha)
- **Pipeline**: daily DAG (ingest → signals → universe → portfolio → execution → options)

## Your Capabilities
- Explain current state: regime, portfolio positions, sector health, fragility, pipeline status
- Analyse performance: assessment scorecard (hit rate, IC), live Sharpe, hedge effectiveness
- Identify risks: sector deterioration, regime shifts, position concentration
- Propose experiments: backtests, parameter changes, strategy adjustments
- Monitor signal quality: assessment IC, lambda accuracy, fragility validation

## Rules
- CANNOT execute changes — all actions require user approval via Control API
- Format proposals with: description, risk level (LOW/MODERATE/HIGH), parameters
- Ground answers in the system context below — if data is missing, say so
- Be concise and quantitative — cite numbers, not vague assessments
"""


# ── Context assembly ─────────────────────────────────────────────────


def _fetch_regime_context() -> str:
    """Latest regime state across regions."""
    db = get_db_manager()
    try:
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT region, regime_label, confidence, as_of_date
                FROM regimes
                ORDER BY as_of_date DESC, region
                LIMIT 10
                """
            )
            rows = cur.fetchall()
            cur.close()
        if not rows:
            return "Regime: no data available."
        lines = [f"- {r[0]}: {r[1]} (confidence={r[2]:.2f}, as_of={r[3]})" for r in rows]
        return "Current regimes:\n" + "\n".join(lines)
    except Exception as exc:
        logger.warning("[iris] Failed to fetch regime context: %s", exc)
        return "Regime: unavailable."


def _fetch_portfolio_context(portfolio_id: str = "IBKR_PAPER") -> str:
    """Current positions from the latest snapshot timestamp."""
    db = get_db_manager()
    try:
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            # Use latest timestamp only (not historical snapshots)
            cur.execute(
                "SELECT MAX(timestamp) FROM positions_snapshots WHERE portfolio_id = %s",
                (portfolio_id,),
            )
            snap_ts = (cur.fetchone() or (None,))[0]
            if snap_ts is None:
                cur.close()
                return f"Portfolio ({portfolio_id}): no positions."
            cur.execute(
                """
                SELECT instrument_id, quantity, market_value, unrealized_pnl
                FROM positions_snapshots
                WHERE portfolio_id = %s AND timestamp = %s
                ORDER BY ABS(market_value) DESC
                """,
                (portfolio_id, snap_ts),
            )
            rows = cur.fetchall()
            cur.close()
        if not rows:
            return f"Portfolio ({portfolio_id}): no positions."
        total_mv = sum(float(r[2] or 0) for r in rows)
        total_pnl = sum(float(r[3] or 0) for r in rows)
        lines = [f"- {r[0]}: qty={float(r[1]):.0f}, mv=${float(r[2]):,.0f}, pnl=${float(r[3]):,.0f}" for r in rows]
        header = f"Portfolio {portfolio_id}: {len(rows)} positions, total MV=${total_mv:,.0f}, unrealised PnL=${total_pnl:,.0f}"
        return header + "\n" + "\n".join(lines)
    except Exception as exc:
        logger.warning("[iris] Failed to fetch portfolio context: %s", exc)
        return f"Portfolio ({portfolio_id}): unavailable."


def _fetch_orders_context(portfolio_id: str = "IBKR_PAPER", limit: int = 10) -> str:
    """Recent orders (includes US_EQ_ALLOCATOR alias)."""
    db = get_db_manager()
    port_ids = [portfolio_id]
    if portfolio_id.startswith("IBKR_"):
        port_ids.append("US_EQ_ALLOCATOR")
    try:
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            placeholders = ",".join(["%s"] * len(port_ids))
            cur.execute(
                f"""
                SELECT instrument_id, side, quantity, status, timestamp
                FROM orders
                WHERE portfolio_id IN ({placeholders})
                ORDER BY timestamp DESC
                LIMIT %s
                """,
                (*port_ids, limit),
            )
            rows = cur.fetchall()
            cur.close()
        if not rows:
            return "Recent orders: none."
        lines = [f"- {r[0]} {r[1]} {r[2]} status={r[3]} @ {r[4]}" for r in rows]
        return "Recent orders:\n" + "\n".join(lines)
    except Exception as exc:
        logger.warning("[iris] Failed to fetch orders context: %s", exc)
        return "Recent orders: unavailable."


def _fetch_fragility_context() -> str:
    """Top fragility scores."""
    db = get_db_manager()
    try:
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT entity_id, fragility_score, as_of_date
                FROM fragility_measures
                ORDER BY as_of_date DESC, fragility_score DESC
                LIMIT 10
                """
            )
            rows = cur.fetchall()
            cur.close()
        if not rows:
            return "Fragility: no data."
        lines = [f"- {r[0]}: score={r[1]:.3f} (as_of={r[2]})" for r in rows]
        return "Top fragility:\n" + "\n".join(lines)
    except Exception as exc:
        logger.warning("[iris] Failed to fetch fragility context: %s", exc)
        return "Fragility: unavailable."


def _fetch_outcomes_context(lookback_days: int = 90) -> str:
    """Decision outcomes summary per engine+horizon for the last N days."""
    from datetime import date, timedelta

    db = get_db_manager()
    as_of = date.today()
    start = as_of - timedelta(days=lookback_days)
    try:
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT ed.engine_name,
                       dout.horizon_days,
                       COUNT(*) AS n,
                       ROUND(AVG(dout.realized_return)::numeric, 4) AS avg_ret,
                       ROUND(
                           SUM(CASE WHEN dout.realized_return > 0 THEN 1 ELSE 0 END)::numeric
                           / COUNT(*), 3
                       ) AS hit_rate,
                       ROUND(SUM(dout.realized_pnl)::numeric, 2) AS total_pnl
                FROM decision_outcomes dout
                JOIN engine_decisions ed ON dout.decision_id = ed.decision_id
                WHERE (ed.as_of_date + dout.horizon_days) >= %s
                  AND (ed.as_of_date + dout.horizon_days) <= %s
                GROUP BY ed.engine_name, dout.horizon_days
                ORDER BY ed.engine_name, dout.horizon_days
                """,
                (start, as_of),
            )
            rows = cur.fetchall()
            cur.close()
        if not rows:
            return f"Decision outcomes (last {lookback_days}d): no data."
        lines = [
            f"  {r[0]} @{r[1]}d: n={r[2]}, hit={float(r[4]):.0%}, avg_ret={float(r[3]):+.4f}, pnl={float(r[5]):+.2f}"
            for r in rows
        ]
        return f"Decision outcomes (last {lookback_days}d):\n" + "\n".join(lines)
    except Exception as exc:
        logger.warning("[iris] Failed to fetch outcomes context: %s", exc)
        return "Decision outcomes: unavailable."


def _fetch_live_performance_context() -> str:
    """Live Sharpe, regime breakdown, fragility validation, hedge effectiveness."""
    import math
    from datetime import date

    from apathis.core.database import get_db_manager as _get_db

    from prometheus.decisions.live_performance import LivePerformanceTracker

    db = _get_db()
    tracker = LivePerformanceTracker(db_manager=db)
    as_of = date.today()
    parts = []

    try:
        perf = tracker.compute_rolling_performance(as_of)
        if "error" not in perf:
            sharpe_str = f"{perf['sharpe']:.3f}" if not math.isnan(perf.get('sharpe', float('nan'))) else "n/a"
            wr_str = f"{perf['win_rate']:.0%}" if not math.isnan(perf.get('win_rate', float('nan'))) else "n/a"
            dd_str = f"{perf['max_drawdown']:.1%}" if not math.isnan(perf.get('max_drawdown', float('nan'))) else "n/a"
            parts.append(
                f"Live performance (PORTFOLIO @21d, 90d): n={perf['n']}"
                f" sharpe={sharpe_str} win={wr_str} max_dd={dd_str}"
                f" total_pnl={perf.get('total_pnl', 0):+.2f}"
            )
    except Exception as exc:
        parts.append(f"Live performance: unavailable ({exc})")

    try:
        regimes = tracker.compute_regime_breakdown(as_of)
        if regimes and "error" not in regimes[0]:
            regime_lines = [
                f"  {r['regime_label']}: n={r['n']}"
                f" sharpe={'%.3f' % r['sharpe'] if not math.isnan(r['sharpe']) else 'n/a'}"
                f" win={r['win_rate']:.0%}"
                for r in regimes
            ]
            parts.append("Regime breakdown (@21d):\n" + "\n".join(regime_lines))
    except Exception as exc:
        parts.append(f"Regime breakdown: unavailable ({exc})")

    try:
        frag = tracker.validate_fragility_signal(as_of)
        if "error" not in frag:
            rho_str = f"{frag['spearman_rho']:.3f}" if not math.isnan(frag.get('spearman_rho', float('nan'))) else "n/a"
            icon = "✓" if frag.get("verdict") == "SIGNAL_VALID" else "⚠"
            parts.append(
                f"Fragility signal: n={frag['n']} spearman_rho={rho_str}"
                f" verdict={frag.get('verdict', '?')} {icon}"
            )
    except Exception as exc:
        parts.append(f"Fragility signal: unavailable ({exc})")

    try:
        hedge = tracker.compute_hedge_effectiveness(as_of)
        if "error" not in hedge:
            r_str = f"{hedge['pearson_r']:.3f}" if not math.isnan(hedge.get('pearson_r', float('nan'))) else "n/a"
            icon = "✓" if hedge.get("verdict") == "HEDGE_EFFECTIVE" else "⚠"
            parts.append(
                f"Hedge effectiveness: n={hedge['n_dates']} pearson_r={r_str}"
                f" verdict={hedge.get('verdict', '?')} {icon}"
                f" opts_pnl={hedge.get('options_pnl_total', 0):+.2f}"
            )
    except Exception as exc:
        parts.append(f"Hedge effectiveness: unavailable ({exc})")

    return "\n".join(parts) if parts else "Live performance: no data."


def _fetch_intel_context() -> str:
    """Latest intel SITREP + flash alerts for Iris context."""
    try:
        from apathis.intel.store import get_briefs

        # Latest daily SITREP
        sitreps = get_briefs(brief_type="daily_sitrep", limit=1)
        sitrep_text = ""
        if sitreps:
            s = sitreps[0]
            sitrep_text = f"Latest SITREP ({s.get('created_at', '?')}):\n{s.get('summary', 'N/A')}"

        # Active flash alerts
        alerts = get_briefs(brief_type="flash_alert", limit=5)
        alert_text = ""
        if alerts:
            lines = [f"- [{a.get('severity', '?').upper()}] {a.get('title', '?')}" for a in alerts]
            alert_text = "Active flash alerts:\n" + "\n".join(lines)

        parts = [p for p in [sitrep_text, alert_text] if p]
        return "\n\n".join(parts) if parts else "Intel: no briefs generated yet."
    except Exception as exc:
        logger.warning("[iris] Failed to fetch intel context: %s", exc)
        return "Intel: unavailable."


def _fetch_sector_health_context() -> str:
    """Current sector health scores."""
    db = get_db_manager()
    try:
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT sector_name, score
                FROM sector_health_daily
                WHERE as_of_date = (SELECT MAX(as_of_date) FROM sector_health_daily)
                ORDER BY score ASC
            """)
            rows = cur.fetchall()
            cur.close()
        if not rows:
            return "Sector health: no data."
        sick = [r for r in rows if float(r[1]) < 0.25]
        weak = [r for r in rows if 0.25 <= float(r[1]) < 0.40]
        lines = [f"- {r[0]}: SHI={float(r[1]):.3f}{' ⚠ SICK' if float(r[1]) < 0.25 else ' weak' if float(r[1]) < 0.40 else ''}" for r in rows]
        header = f"Sector health ({len(rows)} sectors, {len(sick)} sick, {len(weak)} weak):"
        return header + "\n" + "\n".join(lines)
    except Exception as exc:
        logger.warning("[iris] Failed to fetch sector health: %s", exc)
        return "Sector health: unavailable."


def _fetch_scorecard_context() -> str:
    """Assessment prediction scorecard summary."""
    db = get_db_manager()
    try:
        from prometheus.decisions.scorecard import PredictionScorecard

        sc = PredictionScorecard(db_manager=db)
        report = sc.build_scorecard(horizon_days=21, max_decisions=100)
        if report.total_predictions == 0:
            return "Assessment scorecard: no predictions yet."
        return (
            f"Assessment scorecard (21d horizon, last 100 decisions):\n"
            f"  Hit rate: {report.hit_rate:.1%}  Spearman ρ: {report.spearman_rho:.3f}\n"
            f"  Total predictions: {report.total_predictions:,}\n"
            f"  Date range: {report.date_range[0]} → {report.date_range[1]}"
        )
    except Exception as exc:
        logger.warning("[iris] Failed to fetch scorecard: %s", exc)
        return "Assessment scorecard: unavailable."


def _fetch_pipeline_status_context() -> str:
    """Latest pipeline run status."""
    db = get_db_manager()
    try:
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT as_of_date, region, phase, error, updated_at
                FROM engine_runs
                ORDER BY as_of_date DESC, updated_at DESC
                LIMIT 3
            """)
            rows = cur.fetchall()
            cur.close()
        if not rows:
            return "Pipeline: no runs."
        lines = [f"- {r[0]} {r[1]}: {r[2]} (updated {str(r[4])[11:19]})" for r in rows]
        return "Pipeline runs:\n" + "\n".join(lines)
    except Exception as exc:
        logger.warning("[iris] Failed to fetch pipeline status: %s", exc)
        return "Pipeline: unavailable."


def build_system_context() -> str:
    """Assemble a combined context string from all sources."""
    sections = [
        _fetch_regime_context(),
        _fetch_portfolio_context(),
        _fetch_orders_context(),
        _fetch_fragility_context(),
        _fetch_sector_health_context(),
        _fetch_outcomes_context(),
        _fetch_live_performance_context(),
        _fetch_scorecard_context(),
        _fetch_pipeline_status_context(),
        _fetch_intel_context(),
    ]
    return "\n\n".join(sections)


# ── Chat orchestration ───────────────────────────────────────────────


# Tool-calling agent names for Iris.
_IRIS_TOOLS = [
    "get_current_date",
    "search_web",
    "query_fred_data",
    "get_nation_indicators",
    "search_wikipedia",
]


def iris_chat(
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """Run a Iris chat turn.

    Args:
        question: The user's question.
        history: Previous messages ``[{"role": "user"|"assistant", "content": "..."}]``.

    Returns:
        A dict with keys ``answer``, ``proposals``, ``sources`` matching
        the ``IrisResponse`` schema.
    """
    context_text = build_system_context()

    # Build message list
    messages: List[Dict[str, str]] = [
        {"role": "system", "content": SYSTEM_PROMPT + "\n\n---\n\n" + context_text},
    ]

    # Append conversation history
    if history:
        for msg in history:
            role = msg.get("role", "user")
            if role in ("user", "assistant"):
                messages.append({"role": role, "content": msg.get("content", "")})

    messages.append({"role": "user", "content": question})

    logger.info("[iris] Sending %d messages to LLM (with tools)", len(messages))

    # Try tool-agent first; fall back to plain LLM if it fails.
    try:
        from apathis.llm.agent import create_agent

        agent = create_agent(tool_names=_IRIS_TOOLS, max_rounds=3)
        answer = agent.run(messages, temperature=0.4, max_tokens=2048)
    except Exception:
        logger.warning("[iris] Tool-agent failed, falling back to plain LLM")
        llm = get_llm()
        answer = llm.complete(messages, temperature=0.4, max_tokens=2048)

    # Derive sources from which context sections had data
    sources: List[str] = []
    if "regimes" in context_text.lower() and "no data" not in context_text.lower().split("regime")[0]:
        sources.append("regimes")
    if "positions" in context_text.lower():
        sources.append("positions_snapshots")
    if "orders" in context_text.lower() and "none" not in context_text.lower().split("order")[-1][:20]:
        sources.append("orders")
    if "fragility" in context_text.lower() and "no data" not in context_text.lower().split("fragility")[0]:
        sources.append("fragility_measures")

    return {
        "answer": answer,
        "proposals": [],  # TODO: parse structured proposals from LLM output
        "sources": sources,
    }
