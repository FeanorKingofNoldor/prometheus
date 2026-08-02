"""Backtest-vs-live performance drift comparator.

Per strategy + horizon, computes how far the live realized
performance has drifted from what the most recent completed backtest
predicted, then persists one row per (as_of_date, strategy_id,
horizon_days). The notifications rule engine reads from here to fire
``drift_alert`` notifications on warning/critical severities.

Semantics:

* **Live Sharpe** is the rolling annualised Sharpe over the lookback
  window (default 90 days) using outcomes at ``horizon_days`` from
  ``LivePerformanceTracker.compute_rolling_performance``.
* **Backtest Sharpe** is the ``annualised_sharpe`` recorded in the
  most recent ``backtest_runs.metrics_json`` for that strategy.
* **Delta** = live − backtest. Negative deltas mean live underperforms
  the backtest; positive means live is doing better than the model
  predicted (which is also worth a look — usually means alpha decay
  in the *opposite* direction, or a stale backtest config).

Severity buckets on absolute Sharpe delta:

* |Δ| < 0.20 → ``info`` (no drift)
* 0.20 ≤ |Δ| < 0.50 → ``warning``
* |Δ| ≥ 0.50 → ``critical``

If either side has fewer than ``min_live_outcomes`` (default 30)
samples, severity is forced to ``info`` and notes record the reason.
The alert rule fires on warning + critical.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from psycopg2.extras import Json

from prometheus.decisions.live_performance import LivePerformanceTracker

logger = get_logger(__name__)


SEVERITY_INFO = "info"
SEVERITY_WARNING = "warning"
SEVERITY_CRITICAL = "critical"

# backtest_runs is polluted with hundreds of throwaway grid-search /
# synthetic strategies (BT_*, CPP_*, LAMBDA_FACT_*, PERF_TEST*). Diffing
# live performance against those is meaningless, so discovery is gated by
# an explicit allowlist of strategies that actually trade live.
DEFAULT_STRATEGY_ALLOWLIST: tuple[str, ...] = (
    "US_CORE_LONG_EQ",
    # Regional core long-equity strategies (multi-market rollout).
    "UK_CORE_LONG_EQ",
    "EU_CORE_LONG_EQ",
    "HK_CORE_LONG_EQ",
    "KR_CORE_LONG_EQ",
    "AU_CORE_LONG_EQ",
)
EXCLUDED_STRATEGY_PREFIXES: tuple[str, ...] = (
    "BT_", "CPP_", "LAMBDA_FACT_", "PERF_TEST",
)


@dataclass(frozen=True)
class DriftRow:
    """One drift snapshot — what gets persisted."""

    as_of_date: date
    strategy_id: str
    horizon_days: int
    n_live_outcomes: int
    backtest_run_id: str | None
    live_sharpe: float | None
    backtest_sharpe: float | None
    sharpe_delta: float | None
    live_return: float | None
    backtest_return: float | None
    return_delta: float | None
    live_max_drawdown: float | None
    backtest_max_drawdown: float | None
    max_drawdown_delta: float | None
    severity: str
    notes: str | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DriftRunResult:
    as_of_date: date
    rows: list[DriftRow] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def total_strategies(self) -> int:
        return len({r.strategy_id for r in self.rows})

    @property
    def warning_or_worse(self) -> int:
        return sum(
            1 for r in self.rows
            if r.severity in (SEVERITY_WARNING, SEVERITY_CRITICAL)
        )


# ── Public API ──────────────────────────────────────────────────────


def run_daily_drift_check(
    db: DatabaseManager,
    as_of_date: date,
    *,
    strategies: Iterable[str] | None = None,
    horizons: Iterable[int] = (21,),
    lookback_days: int = 90,
    min_live_outcomes: int = 30,
    strategy_allowlist: Iterable[str] | None = None,
) -> DriftRunResult:
    """Compute + persist drift for every (strategy, horizon) combo.

    ``strategies`` (explicit) wins outright. Otherwise discovery is
    restricted to ``strategy_allowlist`` (default:
    ``DEFAULT_STRATEGY_ALLOWLIST``) so grid-search artifacts in
    backtest_runs never generate drift rows. Failure on one
    (strategy, horizon) does not affect others.
    """
    result = DriftRunResult(as_of_date=as_of_date)
    tracker = LivePerformanceTracker(db_manager=db)

    allowlist = (
        tuple(strategy_allowlist) if strategy_allowlist is not None
        else DEFAULT_STRATEGY_ALLOWLIST
    )
    target_strategies = (
        list(strategies) if strategies is not None
        else _discover_strategies(db, allowlist=allowlist)
    )

    for strategy_id in target_strategies:
        backtest_row = _latest_backtest_metrics(db, strategy_id)
        for horizon in horizons:
            try:
                row = _compute_drift(
                    db=db, tracker=tracker,
                    as_of_date=as_of_date,
                    strategy_id=strategy_id,
                    horizon_days=horizon,
                    backtest=backtest_row,
                    lookback_days=lookback_days,
                    min_live_outcomes=min_live_outcomes,
                )
                _persist(db, row)
                result.rows.append(row)
            except Exception as exc:
                logger.exception(
                    "drift_check failed for strategy=%s horizon=%d",
                    strategy_id, horizon,
                )
                result.errors.append(
                    f"{strategy_id}/{horizon}d: {type(exc).__name__}: {exc}"
                )

    logger.info(
        "run_daily_drift_check %s: %d rows across %d strategies, "
        "%d warning+ (errors=%d)",
        as_of_date, len(result.rows), result.total_strategies,
        result.warning_or_worse, len(result.errors),
    )
    return result


# ── Internals ───────────────────────────────────────────────────────


def _compute_drift(
    *,
    db: DatabaseManager,
    tracker: LivePerformanceTracker,
    as_of_date: date,
    strategy_id: str,
    horizon_days: int,
    backtest: dict[str, Any] | None,
    lookback_days: int,
    min_live_outcomes: int,
) -> DriftRow:
    """Compute one (strategy, horizon) drift row.

    The live side is filtered to *this* strategy's decisions — without the
    strategy_id filter every strategy would be diffed against the same
    global live Sharpe, which is exactly the bug this parameter fixes.
    """
    live = tracker.compute_rolling_performance(
        as_of_date=as_of_date,
        lookback_days=lookback_days,
        horizon_days=horizon_days,
        strategy_id=strategy_id,
    )

    live_n = int(live.get("n", 0) or 0)
    live_sharpe = _safe_float(live.get("sharpe"))
    live_return = _safe_float(live.get("avg_return"))
    live_max_dd = _safe_float(live.get("max_drawdown"))

    backtest_run_id = None
    backtest_sharpe: float | None = None
    backtest_return: float | None = None
    backtest_max_dd: float | None = None
    if backtest is not None:
        backtest_run_id = str(backtest.get("run_id") or "") or None
        metrics = backtest.get("metrics") or {}
        backtest_sharpe = _safe_float(metrics.get("annualised_sharpe"))
        backtest_return = _safe_float(metrics.get("cumulative_return"))
        backtest_max_dd = _safe_float(metrics.get("max_drawdown"))
        # Some backtests report max_drawdown as a negative number; the
        # live side is positive. Normalize to positive for both.
        if backtest_max_dd is not None and backtest_max_dd < 0:
            backtest_max_dd = abs(backtest_max_dd)

    sharpe_delta = _delta(live_sharpe, backtest_sharpe)
    return_delta = _delta(live_return, backtest_return)
    max_dd_delta = _delta(live_max_dd, backtest_max_dd)

    severity, notes = _classify(
        live_n=live_n,
        min_live_outcomes=min_live_outcomes,
        backtest_present=backtest_sharpe is not None,
        sharpe_delta=sharpe_delta,
    )

    metadata = {
        "lookback_days": lookback_days,
        "live_total_pnl": _safe_float(live.get("total_pnl")),
        "live_win_rate": _safe_float(live.get("win_rate")),
    }

    return DriftRow(
        as_of_date=as_of_date,
        strategy_id=strategy_id,
        horizon_days=horizon_days,
        n_live_outcomes=live_n,
        backtest_run_id=backtest_run_id,
        live_sharpe=live_sharpe,
        backtest_sharpe=backtest_sharpe,
        sharpe_delta=sharpe_delta,
        live_return=live_return,
        backtest_return=backtest_return,
        return_delta=return_delta,
        live_max_drawdown=live_max_dd,
        backtest_max_drawdown=backtest_max_dd,
        max_drawdown_delta=max_dd_delta,
        severity=severity,
        notes=notes,
        metadata=_scrub_nan(metadata),
    )


def _classify(
    *,
    live_n: int,
    min_live_outcomes: int,
    backtest_present: bool,
    sharpe_delta: float | None,
) -> tuple[str, str | None]:
    """Pick a severity bucket + an explanatory note."""
    if live_n < min_live_outcomes:
        return SEVERITY_INFO, (
            f"live sample size {live_n} below {min_live_outcomes} — "
            "drift assessment deferred"
        )
    if not backtest_present:
        return SEVERITY_INFO, "no recent backtest_runs row for this strategy"
    if sharpe_delta is None:
        return SEVERITY_INFO, "sharpe_delta could not be computed"

    abs_delta = abs(sharpe_delta)
    if abs_delta >= 0.5:
        return SEVERITY_CRITICAL, (
            f"|sharpe_delta|={abs_delta:.2f} ≥ 0.50 — investigate data "
            "quality, slippage, or alpha decay"
        )
    if abs_delta >= 0.2:
        return SEVERITY_WARNING, (
            f"|sharpe_delta|={abs_delta:.2f} ≥ 0.20 — mild drift, monitor"
        )
    return SEVERITY_INFO, f"|sharpe_delta|={abs_delta:.2f} — no drift"


def _persist(db: DatabaseManager, row: DriftRow) -> None:
    """UPSERT one row keyed on (as_of_date, strategy_id, horizon_days)."""
    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO backtest_live_drift (
                    as_of_date, strategy_id, horizon_days,
                    n_live_outcomes, backtest_run_id,
                    live_sharpe, backtest_sharpe, sharpe_delta,
                    live_return, backtest_return, return_delta,
                    live_max_drawdown, backtest_max_drawdown, max_drawdown_delta,
                    severity, notes, metadata_json
                ) VALUES (
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (as_of_date, strategy_id, horizon_days) DO UPDATE SET
                    n_live_outcomes = EXCLUDED.n_live_outcomes,
                    backtest_run_id = EXCLUDED.backtest_run_id,
                    live_sharpe = EXCLUDED.live_sharpe,
                    backtest_sharpe = EXCLUDED.backtest_sharpe,
                    sharpe_delta = EXCLUDED.sharpe_delta,
                    live_return = EXCLUDED.live_return,
                    backtest_return = EXCLUDED.backtest_return,
                    return_delta = EXCLUDED.return_delta,
                    live_max_drawdown = EXCLUDED.live_max_drawdown,
                    backtest_max_drawdown = EXCLUDED.backtest_max_drawdown,
                    max_drawdown_delta = EXCLUDED.max_drawdown_delta,
                    severity = EXCLUDED.severity,
                    notes = EXCLUDED.notes,
                    metadata_json = EXCLUDED.metadata_json
                """,
                (
                    row.as_of_date, str(row.strategy_id)[:64], int(row.horizon_days),
                    int(row.n_live_outcomes), row.backtest_run_id,
                    row.live_sharpe, row.backtest_sharpe, row.sharpe_delta,
                    row.live_return, row.backtest_return, row.return_delta,
                    row.live_max_drawdown, row.backtest_max_drawdown,
                    row.max_drawdown_delta,
                    row.severity, row.notes,
                    Json(row.metadata) if row.metadata else None,
                ),
            )
        conn.commit()


def _latest_backtest_metrics(
    db: DatabaseManager, strategy_id: str,
) -> dict[str, Any] | None:
    """Pull the most recent backtest_runs row for a strategy."""
    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_id, metrics_json
                FROM backtest_runs
                WHERE strategy_id = %s
                  AND metrics_json IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (str(strategy_id),),
            )
            row = cur.fetchone()
            if row is None:
                return None
            run_id, metrics_json = row
            return {"run_id": run_id, "metrics": metrics_json or {}}


def _discover_strategies(
    db: DatabaseManager,
    *,
    allowlist: tuple[str, ...] = DEFAULT_STRATEGY_ALLOWLIST,
) -> list[str]:
    """Strategies with a recent backtest row, filtered to live strategies.

    Only strategies in ``allowlist`` (and never those with an excluded
    grid-search/synthetic prefix) are returned — the drift comparison is
    only meaningful for strategies that actually generate live decisions.
    """
    try:
        with db.get_runtime_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT strategy_id
                    FROM backtest_runs
                    WHERE strategy_id IS NOT NULL
                      AND metrics_json IS NOT NULL
                      AND created_at >= now() - interval '180 days'
                    ORDER BY strategy_id
                    """,
                )
                discovered = [str(r[0]) for r in cur.fetchall() if r[0]]
        allowed = set(allowlist)
        return [
            s for s in discovered
            if s in allowed
            and not s.startswith(EXCLUDED_STRATEGY_PREFIXES)
        ]
    except Exception as exc:
        logger.debug("_discover_strategies failed: %s", exc)
        return []


# ── Helpers ─────────────────────────────────────────────────────────


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


def _delta(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return a - b


def _scrub_nan(obj: Any) -> Any:
    if isinstance(obj, float):
        return None if math.isnan(obj) or math.isinf(obj) else obj
    if isinstance(obj, dict):
        return {k: _scrub_nan(v) for k, v in obj.items()}
    if isinstance(obj, list | tuple):
        return [_scrub_nan(v) for v in obj]
    return obj


__all__ = [
    "DriftRow",
    "DriftRunResult",
    "DEFAULT_STRATEGY_ALLOWLIST",
    "EXCLUDED_STRATEGY_PREFIXES",
    "SEVERITY_INFO",
    "SEVERITY_WARNING",
    "SEVERITY_CRITICAL",
    "run_daily_drift_check",
]


# Silence unused import warnings for json — used implicitly via Json adapter
_ = json
