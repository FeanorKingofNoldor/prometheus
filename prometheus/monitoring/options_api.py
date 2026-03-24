"""Options backtest results API.

Serves options overlay backtest results and Monte Carlo campaign data
from JSON files under ``results/options_backtest/``.  No database required.

Wire into the main app with::

    from prometheus.monitoring.options_api import router as options_router
    app.include_router(options_router)
"""

from __future__ import annotations

import json
from pathlib import Path
from statistics import mean, median
from typing import Any

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/options", tags=["options"])

# Resolve results root relative to the repo root (two levels up from this file).
_REPO_ROOT = Path(__file__).resolve().parents[2]
_RESULTS_DIR = _REPO_ROOT / "results" / "options_backtest"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_json(path: Path) -> dict[str, Any]:
    with open(path) as f:
        return json.load(f)


def _quantile(vals: list[float], q: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    idx = q * (len(s) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(s) - 1)
    frac = idx - lo
    return s[lo] * (1 - frac) + s[hi] * frac


def _is_campaign_dir(p: Path) -> bool:
    """A campaign directory has a summary.csv and individual JSON files."""
    return p.is_dir() and (p / "summary.csv").exists()


def _summarise_result(path: Path) -> dict[str, Any]:
    """Read a single result JSON and return a compact summary.

    Returns a dict whose shape matches the frontend Options.tsx expectations:
    - ``summary`` — KPI fields (cagr, sharpe, max_drawdown, total_options_pnl)
    - ``strategies`` — list[{strategy, median_pnl, mean_pnl, win_rate}]
    - ``guardrails`` — {total_triggers}
    - plus flat fields for list views and campaign aggregation.
    """
    data = _read_json(path)
    raw_summary = data.get("summary", data)  # nested or flat
    strategy_metrics = data.get("strategy_metrics", {})
    strat_pnl = {
        k: v.get("cumulative_pnl", 0.0)
        for k, v in strategy_metrics.items()
    }

    cagr = raw_summary.get("cagr", 0.0)
    sharpe = raw_summary.get("sharpe", 0.0)
    max_drawdown = raw_summary.get("max_drawdown", 0.0)
    options_total_pnl = raw_summary.get("options_total_pnl", 0.0)
    guardrail_halts = raw_summary.get("guardrail_halt_triggers", 0)
    guardrail_force_closes = raw_summary.get("guardrail_force_close_triggers", 0)

    # Build strategies array for the frontend table/chart
    strategies_list: list[dict[str, Any]] = []
    for name, metrics in strategy_metrics.items():
        pnl = metrics.get("cumulative_pnl", 0.0)
        trades = metrics.get("trade_count", 0)
        strategies_list.append({
            "strategy": name,
            "median_pnl": pnl,
            "mean_pnl": pnl,
            "win_rate": 1.0 if pnl > 0 else 0.0,
            "trade_count": trades,
            "realized_pnl": metrics.get("realized_pnl", pnl),
            "unrealized_pnl": metrics.get("unrealized_pnl", 0.0),
            "year_pnl": metrics.get("year_pnl", {}),
        })

    return {
        "result_id": path.stem,
        "file": path.name,
        # Nested objects the frontend destructures
        "summary": {
            "cagr": cagr,
            "sharpe": sharpe,
            "max_drawdown": max_drawdown,
            "total_options_pnl": options_total_pnl,
            "annualised_vol": raw_summary.get("annualised_vol", 0.0),
            "final_nav": raw_summary.get("final_nav", 0.0),
        },
        "strategies": strategies_list,
        "guardrails": {
            "total_triggers": guardrail_halts + guardrail_force_closes,
            "halt_count": guardrail_halts,
            "force_close_count": guardrail_force_closes,
        },
        # Flat fields kept for campaign aggregation
        "cagr": cagr,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
        "options_total_pnl": options_total_pnl,
        "guardrail_halts": guardrail_halts,
        "guardrail_force_closes": guardrail_force_closes,
        "strategy_pnl": strat_pnl,
        "start_date": raw_summary.get("start_date", ""),
        "end_date": raw_summary.get("end_date", ""),
        "years": raw_summary.get("years", 0.0),
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/results")
async def list_results() -> list[dict[str, Any]]:
    """List available single-run result files (non-campaign JSONs)."""
    if not _RESULTS_DIR.exists():
        return []
    out: list[dict[str, Any]] = []
    for f in sorted(_RESULTS_DIR.glob("*.json")):
        if f.name == "equity_nav_series.json":
            continue
        try:
            out.append(_summarise_result(f))
        except Exception:
            continue
    return out


@router.get("/results/{result_id}")
async def get_result(result_id: str) -> dict[str, Any]:
    """Get detailed result for a single run."""
    path = _RESULTS_DIR / f"{result_id}.json"
    if not path.exists():
        # Try with .json suffix already included
        path = _RESULTS_DIR / result_id
    if not path.exists():
        raise HTTPException(404, f"Result not found: {result_id}")
    return _summarise_result(path)


@router.get("/results/{result_id}/attribution")
async def get_result_attribution(result_id: str) -> dict[str, Any]:
    """Full strategy-level P&L attribution for a single run.

    Returns realized_pnl, unrealized_pnl, trade_count, and per-year
    breakdown for each strategy.
    """
    path = _RESULTS_DIR / f"{result_id}.json"
    if not path.exists():
        path = _RESULTS_DIR / result_id
    if not path.exists():
        raise HTTPException(404, f"Result not found: {result_id}")

    data = _read_json(path)
    strategy_metrics = data.get("strategy_metrics", {})

    attribution: list[dict[str, Any]] = []
    for name, metrics in sorted(
        strategy_metrics.items(), key=lambda x: x[1].get("cumulative_pnl", 0.0)
    ):
        attribution.append({
            "strategy": name,
            "cumulative_pnl": metrics.get("cumulative_pnl", 0.0),
            "realized_pnl": metrics.get("realized_pnl", metrics.get("cumulative_pnl", 0.0)),
            "unrealized_pnl": metrics.get("unrealized_pnl", 0.0),
            "trade_count": metrics.get("trade_count", 0),
            "year_pnl": metrics.get("year_pnl", {}),
        })

    raw_summary = data.get("summary", data)
    return {
        "result_id": result_id,
        "start_date": raw_summary.get("start_date", ""),
        "end_date": raw_summary.get("end_date", ""),
        "options_total_pnl": raw_summary.get("options_total_pnl", 0.0),
        "strategies": attribution,
    }


@router.get("/campaigns")
async def list_campaigns() -> list[dict[str, Any]]:
    """List available Monte Carlo campaign directories."""
    if not _RESULTS_DIR.exists():
        return []
    out: list[dict[str, Any]] = []
    for d in sorted(_RESULTS_DIR.iterdir()):
        if not _is_campaign_dir(d):
            continue
        n_realities = len(list(d.glob("*.json")))
        out.append({
            "campaign_id": d.name,
            "n_realities": n_realities,
            "has_summary": (d / "summary.csv").exists(),
        })
    return out


@router.get("/campaigns/{campaign_id}/summary")
async def get_campaign_summary(campaign_id: str) -> dict[str, Any]:
    """Aggregate statistics for a Monte Carlo campaign."""
    campaign_dir = _RESULTS_DIR / campaign_id
    if not _is_campaign_dir(campaign_dir):
        raise HTTPException(404, f"Campaign not found: {campaign_id}")

    jsons = sorted(campaign_dir.glob("*.json"))
    if not jsons:
        raise HTTPException(404, "No reality results found")

    cagrs: list[float] = []
    sharpes: list[float] = []
    max_dds: list[float] = []
    opts_pnls: list[float] = []
    halts: list[int] = []
    force_closes: list[int] = []
    strat_pnls: dict[str, list[float]] = {}

    for jp in jsons:
        try:
            s = _summarise_result(jp)
        except Exception:
            continue
        cagrs.append(s["cagr"])
        sharpes.append(s["sharpe"])
        max_dds.append(s["max_drawdown"])
        opts_pnls.append(s["options_total_pnl"])
        halts.append(s["guardrail_halts"])
        force_closes.append(s["guardrail_force_closes"])
        for strat, pnl in s["strategy_pnl"].items():
            strat_pnls.setdefault(strat, []).append(pnl)

    n = len(cagrs)
    if n == 0:
        raise HTTPException(404, "No valid results")

    def _agg(vals: list[float]) -> dict[str, float]:
        return {
            "mean": mean(vals),
            "median": median(vals),
            "p5": _quantile(vals, 0.05),
            "p25": _quantile(vals, 0.25),
            "p75": _quantile(vals, 0.75),
            "p95": _quantile(vals, 0.95),
            "min": min(vals),
            "max": max(vals),
        }

    strat_summary = {}
    for strat, pnls in strat_pnls.items():
        strat_summary[strat] = {
            "median_pnl": median(pnls),
            "mean_pnl": mean(pnls),
            "win_rate": sum(1 for p in pnls if p > 0) / len(pnls),
        }

    # Build metrics array in the shape the frontend expects:
    # [{metric, mean, median, p5, p95, worst}]
    cagr_agg = _agg(cagrs)
    sharpe_agg = _agg(sharpes)
    max_dd_agg = _agg(max_dds)
    opts_agg = _agg(opts_pnls)

    metrics: list[dict[str, Any]] = [
        {"metric": "CAGR", "mean": cagr_agg["mean"], "median": cagr_agg["median"], "p5": cagr_agg["p5"], "p95": cagr_agg["p95"], "worst": cagr_agg["min"]},
        {"metric": "Sharpe", "mean": sharpe_agg["mean"], "median": sharpe_agg["median"], "p5": sharpe_agg["p5"], "p95": sharpe_agg["p95"], "worst": sharpe_agg["min"]},
        {"metric": "Max Drawdown", "mean": max_dd_agg["mean"], "median": max_dd_agg["median"], "p5": max_dd_agg["p5"], "p95": max_dd_agg["p95"], "worst": max_dd_agg["min"]},
        {"metric": "Options PnL", "mean": opts_agg["mean"], "median": opts_agg["median"], "p5": opts_agg["p5"], "p95": opts_agg["p95"], "worst": opts_agg["min"]},
    ]

    # Build strategies array for the table
    strategies_list: list[dict[str, Any]] = [
        {"strategy": name, **vals}
        for name, vals in strat_summary.items()
    ]

    return {
        "campaign_id": campaign_id,
        "n_realities": n,
        "metrics": metrics,
        "strategies": strategies_list,
        "cagr": cagr_agg,
        "sharpe": sharpe_agg,
        "max_drawdown": max_dd_agg,
        "options_pnl": opts_agg,
        "options_pnl_positive_rate": sum(1 for p in opts_pnls if p > 0) / n,
        "guardrail_halts_mean": mean(halts) if halts else 0,
        "guardrail_force_closes_mean": mean(force_closes) if force_closes else 0,
        "strategy_summary": strat_summary,
    }


@router.get("/campaigns/{campaign_id}/distribution")
async def get_campaign_distribution(campaign_id: str) -> list[dict[str, Any]]:
    """Per-reality stats for histogram / distribution rendering."""
    campaign_dir = _RESULTS_DIR / campaign_id
    if not _is_campaign_dir(campaign_dir):
        raise HTTPException(404, f"Campaign not found: {campaign_id}")

    out: list[dict[str, Any]] = []
    for jp in sorted(campaign_dir.glob("*.json")):
        try:
            s = _summarise_result(jp)
            out.append({
                "reality_id": s["result_id"],
                "cagr": s["cagr"],
                "sharpe": s["sharpe"],
                "max_drawdown": s["max_drawdown"],
                "options_pnl": s["options_total_pnl"],
                "guardrail_halts": s["guardrail_halts"],
            })
        except Exception:
            continue
    return out
