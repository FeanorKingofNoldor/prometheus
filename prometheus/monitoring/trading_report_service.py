"""Prometheus v2 – Trading Performance Report Service.

AI-powered trading & portfolio analysis agent. Assembles context from
regime state, positions, orders, risk metrics, fragility scores, and
engine decisions, then produces a structured performance report.

Report types:
- ``trading_daily``  — last 24 hours
- ``trading_weekly`` — last 7 days
- ``trading_custom`` — user-specified date range
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger

from prometheus.monitoring.report_service import _call_llm, _store_report

logger = get_logger(__name__)


# ── System prompt ───────────────────────────────────────────────────

TRADING_REPORT_PROMPT = """\
You are Prometheus Analytics, the trading performance analyst for
Prometheus v2 — a live quantitative trading system. You review portfolio
positions, P&L, regime state, fragility scores, risk actions, and engine
decisions to produce a structured performance report.

Below is operational and trading data collected from the Prometheus
system for the specified timeframe. Produce a performance report.

You MUST use this exact template. Fill every section or write "N/A".
Do NOT add sections not listed here. Output markdown.

If no meaningful activity occurred, keep the report concise.
When there is notable activity, be thorough with analysis.

---

# Trading Performance Report

## Executive Summary
(One paragraph: overall portfolio performance, key wins/losses, regime
context, and notable risk events for this period)

## Regime Analysis
- **Current Regime:** (regime label + confidence)
- **Transitions:** (list regime transitions, or "No transitions")
- **Implication:** (what the regime means for positioning)

## Portfolio Performance
- **NLV:** (net liquidation value)
- **Unrealised P&L:** (total across positions)
- **Top Movers:** (top 3 gainers and losers by unrealised P&L)

## Position Analysis
- **Count:** (number of positions)
- **Concentration:** (largest position as % of NLV)
- **Sector Exposure:** (breakdown by sector if available)

## Fragility Alerts
(Top 5 fragile entities held or relevant. Include score + class.
 Or "No fragility concerns.")

## Orders & Execution
- **Orders:** (count, breakdown by side BUY/SELL, status)
- **Notable:** (any failed or unusual orders)

## Risk Actions
- Actions taken: (list with instrument, action type, reason)
- Or "No risk actions in this period."

## Engine Activity
- Decisions: (count by engine_name)
- Notable: (any patterns or anomalies)

## Recommendations
- (bullet list of suggested portfolio actions, or
  "Portfolio operating within normal parameters.")

---
"""


# ── DB helper ───────────────────────────────────────────────────────

def _q(sql: str, params: tuple = (), *, db_type: str = "runtime") -> List[tuple]:
    """Run a query and return all rows."""
    db = get_db_manager()
    getter = db.get_runtime_connection if db_type == "runtime" else db.get_historical_connection
    with getter() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            return cur.fetchall()
        finally:
            cur.close()


# ── Context assembly ────────────────────────────────────────────────

def _build_trading_context(
    portfolio_id: str,
    since: date,
    until: date,
) -> str:
    """Assemble trading/portfolio context for the given date range."""
    sections: List[str] = []
    sections.append(f"TIMEFRAME: {since.isoformat()} to {until.isoformat()}")
    sections.append(f"PORTFOLIO: {portfolio_id}")

    # 1. Current regime state
    rows = _q("""
        SELECT region, regime_label, confidence, as_of_date
        FROM regimes
        ORDER BY as_of_date DESC, region
        LIMIT 10
    """)
    if rows:
        lines = [f"  {r[0]}: {r[1]} (confidence={r[2]:.2f}, as_of={r[3]})" for r in rows]
        sections.append("CURRENT REGIMES:\n" + "\n".join(lines))
    else:
        sections.append("CURRENT REGIMES: no data")

    # 2. Regime transitions in period
    rows = _q("""
        SELECT region, from_regime_label, to_regime_label, as_of_date, created_at
        FROM regime_transitions
        WHERE as_of_date >= %s AND as_of_date <= %s
        ORDER BY created_at DESC LIMIT 20
    """, (since, until))
    if rows:
        lines = [f"  {r[3]} {r[0]}: {r[1]} → {r[2]}" for r in rows]
        sections.append(f"REGIME TRANSITIONS: {len(rows)}\n" + "\n".join(lines))
    else:
        sections.append("REGIME TRANSITIONS: none")

    # 3. Positions snapshot (latest timestamp only — avoid mixing dates)
    rows = _q("""
        SELECT instrument_id, quantity, market_value, unrealized_pnl
        FROM positions_snapshots
        WHERE portfolio_id = %s
          AND timestamp = (
              SELECT MAX(timestamp) FROM positions_snapshots
              WHERE portfolio_id = %s
          )
        ORDER BY ABS(market_value) DESC
        LIMIT 30
    """, (portfolio_id, portfolio_id))
    if rows:
        total_mv = sum(r[2] or 0 for r in rows)
        total_pnl = sum(r[3] or 0 for r in rows)
        lines = [
            f"  {r[0]}: qty={r[1]}, mv=${r[2]:,.0f}, pnl=${r[3]:,.0f}"
            for r in rows
        ]
        header = f"POSITIONS: {len(rows)} positions, total MV=${total_mv:,.0f}, unrealised PnL=${total_pnl:,.0f}"
        sections.append(header + "\n" + "\n".join(lines))
    else:
        sections.append("POSITIONS: none")

    # 4. Risk metrics (latest portfolio_risk_reports)
    rows = _q("""
        SELECT as_of_date, risk_metrics, scenario_pnl
        FROM portfolio_risk_reports
        WHERE portfolio_id = %s
        ORDER BY as_of_date DESC
        LIMIT 1
    """, (portfolio_id,))
    if rows:
        r = rows[0]
        rm = r[1] if isinstance(r[1], dict) else {}
        vol = rm.get("expected_volatility", "?")
        dd = rm.get("max_drawdown", "?")
        var_keys = [k for k in rm if "scenario_var_95" in str(k)]
        var_val = rm.get(var_keys[0], "?") if var_keys else "?"
        sections.append(
            f"RISK METRICS (as_of={r[0]}):\n"
            f"  Expected Vol: {vol}\n"
            f"  VaR 95%: {var_val}\n"
            f"  Max Drawdown: {dd}"
        )
    else:
        sections.append("RISK METRICS: no data")

    # 5. Orders in period
    rows = _q("""
        SELECT instrument_id, side, quantity, status, order_type, timestamp
        FROM orders
        WHERE portfolio_id = %s
          AND timestamp::date >= %s AND timestamp::date <= %s
        ORDER BY timestamp DESC
        LIMIT 50
    """, (portfolio_id, since, until))
    if rows:
        buy_count = sum(1 for r in rows if r[1] and r[1].upper() == "BUY")
        sell_count = sum(1 for r in rows if r[1] and r[1].upper() == "SELL")
        lines = [
            f"  {r[5]}: {r[1]} {r[2]} {r[0]} ({r[4]}) status={r[3]}"
            for r in rows[:20]
        ]
        sections.append(
            f"ORDERS: {len(rows)} total (BUY={buy_count}, SELL={sell_count})\n"
            + "\n".join(lines)
        )
    else:
        sections.append("ORDERS: none in this period")

    # 6. Fragility — top 10
    rows = _q("""
        SELECT entity_id, fragility_score, as_of_date
        FROM fragility_measures
        ORDER BY as_of_date DESC, fragility_score DESC
        LIMIT 10
    """)
    if rows:
        lines = [f"  {r[0]}: score={r[1]:.3f}, as_of={r[2]}" for r in rows]
        sections.append("TOP FRAGILITY:\n" + "\n".join(lines))
    else:
        sections.append("FRAGILITY: no data")

    # 7. Engine decisions in period
    rows = _q("""
        SELECT engine_name, COUNT(*)
        FROM engine_decisions
        WHERE as_of_date >= %s AND as_of_date <= %s
        GROUP BY engine_name ORDER BY COUNT(*) DESC
    """, (since, until))
    if rows:
        lines = [f"  {r[0]}: {r[1]} decisions" for r in rows]
        total = sum(r[1] for r in rows)
        sections.append(f"ENGINE DECISIONS: {total} total\n" + "\n".join(lines))
    else:
        sections.append("ENGINE DECISIONS: none")

    # 8. Risk actions in period
    rows = _q("""
        SELECT action_type, instrument_id, strategy_id, details_json, created_at
        FROM risk_actions
        WHERE created_at::date >= %s AND created_at::date <= %s
        ORDER BY created_at DESC LIMIT 30
    """, (since, until))
    if rows:
        lines = []
        for r in rows[:15]:
            details = r[3] if isinstance(r[3], dict) else {}
            reason = details.get("reason", "")
            lines.append(f"  {r[4]}: {r[0]} on {r[1]} (strategy={r[2]}) {reason}")
        sections.append(f"RISK ACTIONS: {len(rows)}\n" + "\n".join(lines))
    else:
        sections.append("RISK ACTIONS: none")

    return "\n\n".join(sections)


# ── Public API ──────────────────────────────────────────────────────

def generate_trading_report(
    report_type: str = "trading_daily",
    portfolio_id: str = "IBKR_PAPER",
    start_date: date | None = None,
    end_date: date | None = None,
) -> Dict[str, Any]:
    """Generate and store a trading performance report.

    Args:
        report_type: ``trading_daily``, ``trading_weekly``, or ``trading_custom``.
        portfolio_id: Portfolio to analyse.
        start_date: Start of analysis period (required for trading_custom).
        end_date: End of analysis period (defaults to today).

    Returns the stored report dict.
    """
    today = date.today()
    end = end_date or today

    if report_type == "trading_daily":
        since = today - timedelta(days=1)
        label = f"Daily Trading Report — {today.isoformat()}"
    elif report_type == "trading_weekly":
        since = today - timedelta(days=7)
        label = f"Weekly Trading Report — w/e {today.isoformat()}"
    elif report_type == "trading_custom":
        since = start_date or (today - timedelta(days=1))
        label = f"Trading Report — {since.isoformat()} to {end.isoformat()}"
    else:
        raise ValueError(f"Unknown report_type: {report_type}")

    logger.info(
        "[trading-reports] Generating %s for %s (%s to %s)",
        report_type, portfolio_id, since, end,
    )
    context = _build_trading_context(portfolio_id, since, end)
    content = _call_llm(TRADING_REPORT_PROMPT, context, max_tokens=2048)

    # Extract summary from the report
    summary = "Trading performance report generated"
    for line in content.split("\n"):
        if "Executive Summary" in line:
            continue
        if line.strip() and not line.startswith("#") and summary == "Trading performance report generated":
            summary = line.strip()[:200]
            break

    return _store_report(
        report_type,
        label,
        content,
        summary=summary,
        as_of=today,
        metadata={
            "portfolio_id": portfolio_id,
            "since": since.isoformat(),
            "until": end.isoformat(),
        },
    )
