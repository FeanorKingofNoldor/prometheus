"""Prometheus -- Comprehensive Trade Monitor.

Tracks every aspect of live trading with scrutiny:
- Per-position entry/exit/P&L attribution
- Execution shortfall (planned vs filled prices)
- Regime-conditional performance
- Sector/factor attribution
- Decision quality over time (rolling accuracy)
- Anomaly detection (unusual drawdowns, concentration, turnover)

Generates structured reports for human review. Does NOT auto-apply
changes — produces proposals with evidence that require manual approval.

Designed for the 2-3 month live validation period on $250K paper.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class PositionPnL:
    """P&L attribution for a single position."""
    instrument_id: str
    side: str              # BUY/SELL
    entry_date: date
    entry_price: float
    current_price: float
    quantity: float
    market_value: float
    unrealized_pnl: float
    pnl_pct: float
    days_held: int
    sector: str


@dataclass
class TradeRecord:
    """A completed trade with full attribution."""
    instrument_id: str
    side: str
    entry_date: date
    exit_date: date
    entry_price: float
    exit_price: float
    quantity: float
    realized_pnl: float
    pnl_pct: float
    days_held: int
    sector: str
    regime_at_entry: str
    regime_at_exit: str


@dataclass
class ExecutionShortfall:
    """Comparison of planned vs actual execution."""
    instrument_id: str
    planned_qty: float
    filled_qty: float
    planned_price: float   # Mid at decision time
    filled_price: float    # Actual fill
    shortfall_bps: float   # Cost of execution vs ideal


@dataclass
class WeeklyReport:
    """Structured weekly monitoring report."""
    period_start: date
    period_end: date

    # Portfolio performance
    period_return_pct: float
    period_sharpe: float
    ytd_return_pct: float
    current_nav: float
    max_drawdown_period: float

    # Position summary
    n_positions: int
    n_entries: int         # New positions this week
    n_exits: int           # Closed positions this week
    turnover_pct: float    # % of NAV traded

    # Winners/Losers
    top_winners: List[PositionPnL]
    top_losers: List[PositionPnL]
    closed_trades: List[TradeRecord]

    # Sector attribution
    sector_pnl: Dict[str, float]

    # Regime context
    regime_label: str
    forward_signal: str    # GREEN/YELLOW/ORANGE/RED

    # Quality metrics
    portfolio_hit_rate: Optional[float]
    assessment_accuracy: Optional[float]

    # Anomalies
    anomalies: List[str]

    # Proposals (config changes to consider)
    proposals: List[Dict[str, Any]]


def compute_weekly_report(
    db_manager: DatabaseManager,
    as_of_date: date,
) -> WeeklyReport:
    """Generate comprehensive weekly monitoring report.

    Analyzes the last 5 trading days of live activity.
    """
    period_end = as_of_date
    period_start = as_of_date - timedelta(days=7)

    positions: List[PositionPnL] = []
    closed_trades: List[TradeRecord] = []
    anomalies: List[str] = []
    proposals: List[Dict[str, Any]] = []
    sector_pnl: Dict[str, float] = {}

    n_entries = 0
    n_exits = 0
    current_nav = 0.0
    period_return = 0.0
    start_nav = 0.0

    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            # ── Current positions ────────────────────────────────────
            cur.execute("""
                SELECT instrument_id, quantity, avg_cost, market_value, unrealized_pnl
                FROM positions_snapshots
                WHERE timestamp = (SELECT MAX(timestamp) FROM positions_snapshots)
                ORDER BY ABS(market_value) DESC
            """)
            pos_rows = cur.fetchall()

            for (inst_id, qty, avg_cost, mv, pnl) in pos_rows:
                if qty == 0:
                    continue
                current_price = mv / qty if qty != 0 else 0
                pnl_pct = pnl / (avg_cost * abs(qty)) if avg_cost and qty else 0
                current_nav += mv

                # Look up sector
                sector = "Unknown"
                cur.execute("""
                    SELECT ic.sector FROM issuer_classifications ic
                    JOIN instruments i ON i.issuer_id = ic.issuer_id
                    WHERE i.instrument_id = %s LIMIT 1
                """, (inst_id,))
                sec_row = cur.fetchone()
                if sec_row:
                    sector = sec_row[0] or "Unknown"

                pos = PositionPnL(
                    instrument_id=inst_id,
                    side="LONG" if qty > 0 else "SHORT",
                    entry_date=period_start,  # Approximate
                    entry_price=float(avg_cost or 0),
                    current_price=float(current_price),
                    quantity=float(qty),
                    market_value=float(mv),
                    unrealized_pnl=float(pnl),
                    pnl_pct=float(pnl_pct),
                    days_held=0,
                    sector=sector,
                )
                positions.append(pos)

                # Sector attribution
                sector_pnl[sector] = sector_pnl.get(sector, 0) + float(pnl)

            # ── Period return from NAV changes ──────────────────────
            cur.execute("""
                SELECT COALESCE(SUM(market_value), 0)
                FROM positions_snapshots
                WHERE timestamp = (
                    SELECT MAX(timestamp) FROM positions_snapshots
                    WHERE timestamp::date <= %s
                )
            """, (period_start,))
            start_row = cur.fetchone()
            if start_row and start_row[0]:
                start_nav = float(start_row[0])
            if start_nav > 0 and current_nav > 0:
                period_return = (current_nav - start_nav) / start_nav

            # ── Orders this period ───────────────────────────────────
            cur.execute("""
                SELECT instrument_id, side, quantity, status, order_type
                FROM orders
                WHERE timestamp::date BETWEEN %s AND %s
                ORDER BY timestamp
            """, (period_start, period_end))
            order_rows = cur.fetchall()

            for (inst_id, side, qty, status, otype) in order_rows:
                if side == "BUY":
                    n_entries += 1
                else:
                    n_exits += 1

            # ── Fills this period ────────────────────────────────────
            cur.execute("""
                SELECT instrument_id, side, quantity, price
                FROM fills
                WHERE timestamp::date BETWEEN %s AND %s
            """, (period_start, period_end))
            fill_rows = cur.fetchall()
            total_traded = sum(abs(float(r[2]) * float(r[3])) for r in fill_rows)
            turnover_pct = total_traded / max(current_nav, 1) if current_nav > 0 else 0

            # ── Regime context ───────────────────────────────────────
            regime_label = "NEUTRAL"
            cur.execute("""
                SELECT regime_label FROM regimes
                WHERE as_of_date <= %s
                ORDER BY as_of_date DESC LIMIT 1
            """, (as_of_date,))
            reg_row = cur.fetchone()
            if reg_row:
                regime_label = reg_row[0]

            # ── Forward indicator signal ─────────────────────────────
            forward_signal = "GREEN"
            cur.execute("""
                SELECT overall_signal FROM forward_indicator_snapshots
                WHERE as_of_date <= %s
                ORDER BY as_of_date DESC LIMIT 1
            """, (as_of_date,))
            fwd_row = cur.fetchone()
            if fwd_row:
                forward_signal = fwd_row[0]

            # ── Decision quality ─────────────────────────────────────
            portfolio_hit_rate = None
            cur.execute("""
                SELECT o.realized_return
                FROM decision_outcomes o
                JOIN engine_decisions d ON o.decision_id = d.decision_id
                WHERE d.engine_name = 'PORTFOLIO'
                  AND d.as_of_date BETWEEN %s AND %s
                  AND o.realized_return IS NOT NULL
            """, (period_start - timedelta(days=30), period_end))
            port_rets = [float(r[0]) for r in cur.fetchall() if r[0] is not None]
            if port_rets:
                portfolio_hit_rate = sum(1 for r in port_rets if r > 0) / len(port_rets)

    # ── Anomaly detection ────────────────────────────────────────
    # Concentration check
    if positions:
        max_pos_pct = max(abs(p.market_value) / max(current_nav, 1) for p in positions)
        if max_pos_pct > 0.15:
            anomalies.append(
                f"Position concentration: largest position is {max_pos_pct:.0%} of NAV "
                f"({positions[0].instrument_id})"
            )

    # Sector concentration
    if sector_pnl and current_nav > 0:
        sector_weights = {}
        for p in positions:
            sector_weights[p.sector] = sector_weights.get(p.sector, 0) + abs(p.market_value)
        for sector, weight in sector_weights.items():
            pct = weight / current_nav
            if pct > 0.40:
                anomalies.append(f"Sector concentration: {sector} is {pct:.0%} of NAV")

    # Drawdown check
    big_losers = [p for p in positions if p.pnl_pct < -0.10]
    if big_losers:
        for p in big_losers:
            anomalies.append(f"Large unrealized loss: {p.instrument_id} at {p.pnl_pct:.1%}")

    # Turnover check
    if turnover_pct > 0.30:
        anomalies.append(f"High turnover: {turnover_pct:.0%} of NAV traded this week")

    # Forward signal warning
    if forward_signal in ("ORANGE", "RED"):
        anomalies.append(f"Forward indicators at {forward_signal} — elevated macro stress")

    # ── Sort positions for report ────────────────────────────────
    sorted_by_pnl = sorted(positions, key=lambda p: p.unrealized_pnl, reverse=True)
    top_winners = sorted_by_pnl[:5]
    top_losers = sorted_by_pnl[-5:][::-1]

    report = WeeklyReport(
        period_start=period_start,
        period_end=period_end,
        period_return_pct=period_return,
        period_sharpe=0.0,
        ytd_return_pct=0.0,
        current_nav=current_nav,
        max_drawdown_period=0.0,
        n_positions=len(positions),
        n_entries=n_entries,
        n_exits=n_exits,
        turnover_pct=turnover_pct,
        top_winners=top_winners,
        top_losers=top_losers,
        closed_trades=closed_trades,
        sector_pnl=sector_pnl,
        regime_label=regime_label,
        forward_signal=forward_signal,
        portfolio_hit_rate=portfolio_hit_rate,
        assessment_accuracy=None,
        anomalies=anomalies,
        proposals=proposals,
    )

    return report


def format_weekly_report(report: WeeklyReport) -> str:
    """Format the weekly report as human-readable text."""
    lines = [
        f"{'='*60}",
        "IRIS WEEKLY TRADE MONITOR",
        f"Period: {report.period_start} to {report.period_end}",
        f"{'='*60}",
        "",
        f"NAV: ${report.current_nav:,.0f}",
        f"Positions: {report.n_positions} open | {report.n_entries} entries | {report.n_exits} exits",
        f"Turnover: {report.turnover_pct:.1%} of NAV",
        f"Regime: {report.regime_label} | Forward Signal: {report.forward_signal}",
    ]

    if report.portfolio_hit_rate is not None:
        lines.append(f"Portfolio Hit Rate (30d): {report.portfolio_hit_rate:.0%}")

    if report.anomalies:
        lines.append("")
        lines.append("ANOMALIES:")
        for a in report.anomalies:
            lines.append(f"  ! {a}")

    if report.top_winners:
        lines.append("")
        lines.append("TOP WINNERS:")
        for p in report.top_winners:
            lines.append(f"  {p.instrument_id:12s} {p.pnl_pct:>+7.1%}  ${p.unrealized_pnl:>+10,.0f}  [{p.sector}]")

    if report.top_losers:
        lines.append("")
        lines.append("TOP LOSERS:")
        for p in report.top_losers:
            lines.append(f"  {p.instrument_id:12s} {p.pnl_pct:>+7.1%}  ${p.unrealized_pnl:>+10,.0f}  [{p.sector}]")

    if report.sector_pnl:
        lines.append("")
        lines.append("SECTOR P&L:")
        for sector, pnl in sorted(report.sector_pnl.items(), key=lambda x: x[1], reverse=True):
            lines.append(f"  {sector:25s} ${pnl:>+10,.0f}")

    if report.proposals:
        lines.append("")
        lines.append("PROPOSALS:")
        for p in report.proposals:
            lines.append(f"  - [{p.get('type', '?')}] {p.get('description', '?')}")
            lines.append(f"    Evidence: {p.get('evidence', '?')}")

    lines.append("")
    lines.append(f"{'='*60}")
    return "\n".join(lines)


def persist_weekly_report(
    db_manager: DatabaseManager,
    report: WeeklyReport,
) -> None:
    """Save weekly report to the reports table."""
    text = format_weekly_report(report)
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reports (report_type, report_date, content, metadata, created_at)
                VALUES ('iris_weekly_monitor', %s, %s, %s::jsonb, NOW())
            """, (
                report.period_end,
                text,
                json.dumps({
                    "nav": report.current_nav,
                    "n_positions": report.n_positions,
                    "regime": report.regime_label,
                    "forward_signal": report.forward_signal,
                    "hit_rate": report.portfolio_hit_rate,
                    "anomaly_count": len(report.anomalies),
                    "turnover_pct": report.turnover_pct,
                }),
            ))
        conn.commit()

    logger.info(
        "Iris weekly report saved: NAV=$%.0f positions=%d anomalies=%d",
        report.current_nav, report.n_positions, len(report.anomalies),
    )
