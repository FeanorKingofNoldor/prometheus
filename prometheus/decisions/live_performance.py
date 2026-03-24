"""Prometheus v2 – Live Performance Tracker.

Closes the intelligence gaps that DiagnosticsEngine (backtest-only) cannot:

1. compute_rolling_performance  — rolling Sharpe / win-rate / drawdown from
   live decision_outcomes for the PORTFOLIO engine.
2. compute_regime_breakdown     — same metrics split by US regime label.
3. validate_fragility_signal    — Spearman ρ between fragility_score and
   realized_return (expected: ρ < 0).
4. compute_hedge_effectiveness  — Pearson ρ between OPTIONS and PORTFOLIO
   P&L (expected: ρ < 0).

All SQL uses ``dout`` as the alias for ``decision_outcomes`` (``do`` is a
reserved word in PostgreSQL).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List

from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger

logger = get_logger(__name__)


# ── Spearman helper (no scipy) ────────────────────────────────────────


def _rank(lst: List[float]) -> List[float]:
    """Return ranks (1-based) for a list of floats."""
    n = len(lst)
    sorted_pairs = sorted(enumerate(lst), key=lambda t: t[1])
    ranks = [0.0] * n
    for r, (i, _) in enumerate(sorted_pairs):
        ranks[i] = float(r + 1)
    return ranks


def _spearman_rho(x: List[float], y: List[float]) -> float:
    """Spearman rank correlation without scipy.

    Returns float('nan') if n < 3.
    """
    n = len(x)
    if n < 3:
        return float("nan")
    rx = _rank(x)
    ry = _rank(y)
    d2 = sum((rx[i] - ry[i]) ** 2 for i in range(n))
    denom = n * (n**2 - 1)
    if denom == 0:
        return float("nan")
    return 1.0 - 6.0 * d2 / denom


# ── Pearson helper ────────────────────────────────────────────────────


def _pearson_r(x: List[float], y: List[float]) -> float:
    """Pearson correlation. Returns float('nan') if n < 3."""
    n = len(x)
    if n < 3:
        return float("nan")
    mx = sum(x) / n
    my = sum(y) / n
    num = sum((x[i] - mx) * (y[i] - my) for i in range(n))
    sx = math.sqrt(sum((v - mx) ** 2 for v in x))
    sy = math.sqrt(sum((v - my) ** 2 for v in y))
    if sx == 0 or sy == 0:
        return float("nan")
    return num / (sx * sy)


# ── Main tracker ──────────────────────────────────────────────────────


@dataclass
class LivePerformanceTracker:
    """Compute live performance metrics from decision_outcomes.

    Args:
        db_manager: DatabaseManager instance (runtime DB).
    """

    db_manager: DatabaseManager

    # ------------------------------------------------------------------
    # 1. Rolling portfolio performance
    # ------------------------------------------------------------------

    def compute_rolling_performance(
        self,
        as_of_date: date,
        lookback_days: int = 90,
        horizon_days: int = 21,
    ) -> Dict[str, Any]:
        """Rolling Sharpe / win-rate / drawdown for the PORTFOLIO engine.

        Uses annualised Sharpe = mean(r) / std(r) * sqrt(252 / horizon_days).

        Returns a dict with keys:
            n, sharpe, win_rate, max_drawdown, avg_return, total_pnl,
            by_strategy (per-engine_name breakdown for all engines).
        """
        start = as_of_date - timedelta(days=lookback_days)
        try:
            with self.db_manager.get_runtime_connection() as conn:
                cur = conn.cursor()
                # PORTFOLIO engine, specified horizon
                cur.execute(
                    """
                    SELECT dout.realized_return, dout.realized_pnl
                    FROM decision_outcomes dout
                    JOIN engine_decisions ed ON dout.decision_id = ed.decision_id
                    WHERE ed.engine_name = 'PORTFOLIO'
                      AND dout.horizon_days = %s
                      AND (ed.as_of_date + dout.horizon_days) >= %s
                      AND (ed.as_of_date + dout.horizon_days) <= %s
                    ORDER BY ed.as_of_date
                    """,
                    (horizon_days, start, as_of_date),
                )
                rows = cur.fetchall()

                # All engines breakdown (ignore horizon filter here)
                cur.execute(
                    """
                    SELECT ed.engine_name,
                           COUNT(*) AS n,
                           AVG(dout.realized_return) AS avg_ret,
                           SUM(dout.realized_pnl) AS total_pnl,
                           SUM(CASE WHEN dout.realized_return > 0 THEN 1 ELSE 0 END)
                               AS wins
                    FROM decision_outcomes dout
                    JOIN engine_decisions ed ON dout.decision_id = ed.decision_id
                    WHERE (ed.as_of_date + dout.horizon_days) >= %s
                      AND (ed.as_of_date + dout.horizon_days) <= %s
                    GROUP BY ed.engine_name
                    ORDER BY ed.engine_name
                    """,
                    (start, as_of_date),
                )
                by_strategy_rows = cur.fetchall()
                cur.close()

            returns = [float(r[0]) for r in rows]
            pnls = [float(r[1]) for r in rows]

            n = len(returns)
            if n < 2:
                sharpe = float("nan")
                win_rate = float("nan")
                max_dd = float("nan")
                avg_ret = float("nan")
            else:
                mean_r = sum(returns) / n
                std_r = math.sqrt(sum((r - mean_r) ** 2 for r in returns) / (n - 1))
                sharpe = (mean_r / std_r * math.sqrt(252 / horizon_days)) if std_r > 0 else float("nan")
                win_rate = sum(1 for r in returns if r > 0) / n
                avg_ret = mean_r

                # Max drawdown from cumulative P&L
                cum = 0.0
                peak = 0.0
                max_dd = 0.0
                for p in pnls:
                    cum += p
                    if cum > peak:
                        peak = cum
                    dd = (peak - cum) / abs(peak) if peak != 0 else 0.0
                    if dd > max_dd:
                        max_dd = dd

            by_strategy = [
                {
                    "engine": r[0],
                    "n": r[1],
                    "avg_return": float(r[2]) if r[2] is not None else None,
                    "total_pnl": float(r[3]) if r[3] is not None else None,
                    "win_rate": float(r[4]) / r[1] if r[1] else None,
                }
                for r in by_strategy_rows
            ]

            return {
                "n": n,
                "sharpe": sharpe,
                "win_rate": win_rate,
                "max_drawdown": max_dd,
                "avg_return": avg_ret if n >= 2 else float("nan"),
                "total_pnl": sum(pnls) if pnls else 0.0,
                "by_strategy": by_strategy,
                "horizon_days": horizon_days,
                "lookback_days": lookback_days,
            }
        except Exception as exc:
            logger.warning("[live_perf] compute_rolling_performance failed: %s", exc)
            return {"error": str(exc)}

    # ------------------------------------------------------------------
    # 2. Regime-conditioned breakdown
    # ------------------------------------------------------------------

    def compute_regime_breakdown(
        self,
        as_of_date: date,
        lookback_days: int = 90,
        horizon_days: int = 21,
    ) -> List[Dict[str, Any]]:
        """Per-regime performance for the PORTFOLIO engine.

        Joins decision_outcomes → engine_decisions → regimes (US region).

        Returns list of dicts: {regime_label, n, avg_return, win_rate, sharpe}.
        """
        start = as_of_date - timedelta(days=lookback_days)
        try:
            with self.db_manager.get_runtime_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT r.regime_label,
                           dout.realized_return
                    FROM decision_outcomes dout
                    JOIN engine_decisions ed ON dout.decision_id = ed.decision_id
                    JOIN regimes r
                      ON r.as_of_date = ed.as_of_date
                     AND r.region = 'US'
                    WHERE ed.engine_name = 'PORTFOLIO'
                      AND dout.horizon_days = %s
                      AND (ed.as_of_date + dout.horizon_days) >= %s
                      AND (ed.as_of_date + dout.horizon_days) <= %s
                    ORDER BY r.regime_label
                    """,
                    (horizon_days, start, as_of_date),
                )
                rows = cur.fetchall()
                cur.close()

            # Group by regime
            buckets: Dict[str, List[float]] = {}
            for regime_label, ret in rows:
                buckets.setdefault(regime_label, []).append(float(ret))

            results = []
            for label, returns in sorted(buckets.items()):
                n = len(returns)
                if n < 2:
                    sharpe = float("nan")
                else:
                    mean_r = sum(returns) / n
                    std_r = math.sqrt(sum((r - mean_r) ** 2 for r in returns) / (n - 1))
                    sharpe = (mean_r / std_r * math.sqrt(252 / horizon_days)) if std_r > 0 else float("nan")
                results.append(
                    {
                        "regime_label": label,
                        "n": n,
                        "avg_return": sum(returns) / n,
                        "win_rate": sum(1 for r in returns if r > 0) / n,
                        "sharpe": sharpe,
                    }
                )
            return results
        except Exception as exc:
            logger.warning("[live_perf] compute_regime_breakdown failed: %s", exc)
            return [{"error": str(exc)}]

    # ------------------------------------------------------------------
    # 3. Fragility signal validation
    # ------------------------------------------------------------------

    def validate_fragility_signal(
        self,
        as_of_date: date,
        lookback_days: int = 90,
        horizon_days: int = 21,
        entity_id: str = "US_EQ",
    ) -> Dict[str, Any]:
        """Spearman ρ between fragility_score and realized_return.

        Groups by decision date, takes avg fragility and avg return per day.
        Expected: ρ < 0 (high fragility → worse outcomes).

        Returns: {n, spearman_rho, verdict, entity_id}.
        """
        start = as_of_date - timedelta(days=lookback_days)
        try:
            with self.db_manager.get_runtime_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT ed.as_of_date,
                           AVG(fm.fragility_score)   AS avg_fragility,
                           AVG(dout.realized_return) AS avg_return
                    FROM decision_outcomes dout
                    JOIN engine_decisions ed ON dout.decision_id = ed.decision_id
                    JOIN fragility_measures fm
                      ON fm.as_of_date = ed.as_of_date
                     AND fm.entity_id = %s
                    WHERE ed.engine_name = 'PORTFOLIO'
                      AND dout.horizon_days = %s
                      AND (ed.as_of_date + dout.horizon_days) >= %s
                      AND (ed.as_of_date + dout.horizon_days) <= %s
                    GROUP BY ed.as_of_date
                    ORDER BY ed.as_of_date
                    """,
                    (entity_id, horizon_days, start, as_of_date),
                )
                rows = cur.fetchall()
                cur.close()

            n = len(rows)
            if n < 3:
                return {
                    "n": n,
                    "spearman_rho": float("nan"),
                    "verdict": "INSUFFICIENT_DATA",
                    "entity_id": entity_id,
                }

            fragility = [float(r[1]) for r in rows]
            returns = [float(r[2]) for r in rows]
            rho = _spearman_rho(fragility, returns)

            if math.isnan(rho):
                verdict = "INSUFFICIENT_DATA"
            elif rho < -0.2:
                verdict = "SIGNAL_VALID"   # high fragility → bad outcomes ✓
            elif rho > 0.1:
                verdict = "SIGNAL_INVERTED"  # fragility predicts *better* returns ⚠
            else:
                verdict = "SIGNAL_WEAK"

            return {
                "n": n,
                "spearman_rho": rho,
                "verdict": verdict,
                "entity_id": entity_id,
            }
        except Exception as exc:
            logger.warning("[live_perf] validate_fragility_signal failed: %s", exc)
            return {"error": str(exc)}

    # ------------------------------------------------------------------
    # 4. Options hedge effectiveness
    # ------------------------------------------------------------------

    def compute_hedge_effectiveness(
        self,
        as_of_date: date,
        lookback_days: int = 90,
        horizon_days: int = 1,
    ) -> Dict[str, Any]:
        """Pearson ρ between OPTIONS and PORTFOLIO P&L per outcome date.

        Expected: ρ < 0 (options spike when portfolio hurts).

        Returns: {n_dates, pearson_r, verdict, options_pnl_total, portfolio_pnl_total}.
        """
        start = as_of_date - timedelta(days=lookback_days)
        try:
            with self.db_manager.get_runtime_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT (ed.as_of_date + dout.horizon_days) AS outcome_date,
                           ed.engine_name,
                           SUM(dout.realized_pnl) AS total_pnl
                    FROM decision_outcomes dout
                    JOIN engine_decisions ed ON dout.decision_id = ed.decision_id
                    WHERE ed.engine_name IN ('PORTFOLIO', 'OPTIONS')
                      AND (ed.as_of_date + dout.horizon_days) >= %s
                      AND (ed.as_of_date + dout.horizon_days) <= %s
                    GROUP BY (ed.as_of_date + dout.horizon_days), ed.engine_name
                    ORDER BY outcome_date
                    """,
                    (start, as_of_date),
                )
                rows = cur.fetchall()
                cur.close()

            # Pivot: {date: {engine: pnl}}
            pivot: Dict[date, Dict[str, float]] = {}
            for outcome_date, engine_name, total_pnl in rows:
                pivot.setdefault(outcome_date, {})[engine_name] = float(total_pnl)

            paired = [
                (v["PORTFOLIO"], v["OPTIONS"])
                for v in pivot.values()
                if "PORTFOLIO" in v and "OPTIONS" in v
            ]

            n = len(paired)
            if n < 3:
                return {
                    "n_dates": n,
                    "pearson_r": float("nan"),
                    "verdict": "INSUFFICIENT_DATA",
                    "options_pnl_total": sum(v.get("OPTIONS", 0) for v in pivot.values()),
                    "portfolio_pnl_total": sum(v.get("PORTFOLIO", 0) for v in pivot.values()),
                }

            port_pnl = [p[0] for p in paired]
            opt_pnl = [p[1] for p in paired]
            r = _pearson_r(port_pnl, opt_pnl)

            if math.isnan(r):
                verdict = "INSUFFICIENT_DATA"
            elif r < -0.2:
                verdict = "HEDGE_EFFECTIVE"   # options spike when portfolio drops ✓
            elif r > 0.1:
                verdict = "HEDGE_INEFFECTIVE"  # both move together ⚠
            else:
                verdict = "HEDGE_NEUTRAL"

            return {
                "n_dates": n,
                "pearson_r": r,
                "verdict": verdict,
                "options_pnl_total": sum(opt_pnl),
                "portfolio_pnl_total": sum(port_pnl),
            }
        except Exception as exc:
            logger.warning("[live_perf] compute_hedge_effectiveness failed: %s", exc)
            return {"error": str(exc)}
