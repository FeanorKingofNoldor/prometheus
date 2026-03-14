"""Grid search for C++ equity backtester tuning.

Locks in the winning config (BLENDED_UNIVERSE_AND_SIZE with conviction + SA)
and varies only the knobs that matter:
  - score_concentration_power: how aggressively to weight top names
  - portfolio_max_names: concentration vs diversification
  - min_rebalance_pct: turnover filter to save slippage
  - lambda_blend_weights: h5/h63 signal mix

Usage::

    PYTHONPATH=cpp/build ./venv/bin/python \
        -m prometheus.scripts.run.run_equity_grid_search
"""

from __future__ import annotations

import csv
import itertools
import json
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger

logger = get_logger(__name__)


def _load_real_instrument_ids(market_id: str) -> List[str]:
    db = get_db_manager()
    sql = """
        SELECT instrument_id
        FROM instruments
        WHERE market_id = %s
          AND instrument_id NOT LIKE 'SYNTH_%%'
        ORDER BY instrument_id
    """
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (market_id,))
            rows = cur.fetchall()
        finally:
            cur.close()
    return [r[0] for r in rows]


def run_single_config(
    prom2,
    instrument_ids: List[str],
    lambda_csv: str,
    label: str,
    score_concentration_power: float = 1.0,
    portfolio_max_names: int = 50,
    min_rebalance_pct: float = 0.0,
    blend_weights: List[float] = None,
    persist: bool = False,
) -> Dict[str, Any]:
    """Run one C++ backtest config and return the best sleeve result."""

    if blend_weights is None:
        blend_weights = [0.45, 0.55]

    cfg = {
        "market_id": "US_EQ",
        "start": "1997-01-02",
        "end": "2026-03-02",
        "instrument_ids": instrument_ids,
        "lambda_scores_csv": lambda_csv,
        "horizons": [5, 63],
        "lambda_weight": 10.0,
        "initial_cash": 1_000_000.0,
        "apply_risk": True,
        "apply_fragility_overlay": False,
        "slippage_bps": 5.0,
        "num_threads": 32,
        "verbose": False,
        "persist_to_db": persist,
        "persist_execution_to_db": False,
        "persist_meta_to_db": False,
        "conviction_enabled": True,
        "sector_allocator_enabled": True,
        "portfolio_max_names": portfolio_max_names,
        "run_blended_sleeves": True,
        "lambda_blend_weights": blend_weights,
        # Only run the winning mode
        "modes": ["universe_and_size"],
        # Tuning knobs
        "score_concentration_power": score_concentration_power,
        "min_rebalance_pct": min_rebalance_pct,
    }

    t0 = time.time()
    results = prom2.run_lambda_factorial_backtests(cfg)
    elapsed = time.time() - t0

    # Find the BLENDED_UNIVERSE_AND_SIZE sleeve
    best = None
    for r in results:
        sid = r.get("sleeve_id", "")
        if "BLENDED" in sid and "UNIVERSE_AND_SIZE" in sid:
            best = r
            break

    if best is None and results:
        # Fallback: pick best by cumulative return
        best = max(results, key=lambda r: r.get("metrics", r).get("cumulative_return", 0))

    if best is None:
        return {"label": label, "error": "no results", "elapsed": elapsed}

    m = best.get("metrics", best)
    n_days = m.get("n_trading_days", 0)
    years = n_days / 252.0 if n_days > 0 else 1.0
    cum_ret = m.get("cumulative_return", 0.0)
    cagr = (1.0 + cum_ret) ** (1.0 / years) - 1.0 if years > 0 and cum_ret > -1 else 0.0

    return {
        "label": label,
        "sleeve_id": best.get("sleeve_id", "?"),
        "run_id": best.get("run_id", "?"),
        "cagr": cagr,
        "sharpe": m.get("annualised_sharpe", 0.0),
        "max_drawdown": m.get("max_drawdown", 0.0),
        "ann_vol": m.get("annualised_vol", 0.0),
        "n_days": n_days,
        "elapsed": elapsed,
        "score_concentration_power": score_concentration_power,
        "portfolio_max_names": portfolio_max_names,
        "min_rebalance_pct": min_rebalance_pct,
        "blend_weights": blend_weights,
    }


def main() -> None:
    import prom2_cpp as prom2

    lambda_csv = "data/cache_ic_v2/lambda_scores/lambda_cluster_scores_smoothed_US_EQ_1997-01-02_2026-03-02_h5-63_8col.csv"
    instrument_ids = _load_real_instrument_ids("US_EQ")
    logger.info("Loaded %d instruments", len(instrument_ids))

    # Define grid
    grid = []

    # Vary score_concentration_power
    for scp in [1.0, 1.25, 1.5, 2.0, 2.5]:
        grid.append({"score_concentration_power": scp, "portfolio_max_names": 50,
                      "min_rebalance_pct": 0.0, "blend_weights": [0.45, 0.55],
                      "label": f"scp={scp}"})

    # Vary portfolio_max_names (with best default scp=1.0)
    for mn in [25, 30, 40, 60, 75, 100]:
        grid.append({"score_concentration_power": 1.0, "portfolio_max_names": mn,
                      "min_rebalance_pct": 0.0, "blend_weights": [0.45, 0.55],
                      "label": f"maxn={mn}"})

    # Vary min_rebalance_pct
    for mrp in [0.02, 0.05, 0.10]:
        grid.append({"score_concentration_power": 1.0, "portfolio_max_names": 50,
                      "min_rebalance_pct": mrp, "blend_weights": [0.45, 0.55],
                      "label": f"mreb={mrp}"})

    # Vary blend_weights
    for bw in [[0.3, 0.7], [0.5, 0.5], [0.6, 0.4], [0.2, 0.8], [0.7, 0.3]]:
        grid.append({"score_concentration_power": 1.0, "portfolio_max_names": 50,
                      "min_rebalance_pct": 0.0, "blend_weights": bw,
                      "label": f"bw={bw}"})

    print(f"\n{'='*80}")
    print(f"  Equity Engine Grid Search: {len(grid)} configs")
    print(f"  Locked: BLENDED_UNIVERSE_AND_SIZE + conviction + SA")
    print(f"  Instruments: {len(instrument_ids)}")
    print(f"{'='*80}\n")

    results = []
    for i, params in enumerate(grid):
        label = params.pop("label")
        print(f"  [{i+1}/{len(grid)}] {label} ...", end=" ", flush=True)
        r = run_single_config(prom2, instrument_ids, lambda_csv, label, **params)
        results.append(r)
        if "error" in r:
            print(f"ERROR: {r['error']}")
        else:
            print(f"CAGR={r['cagr']*100:.2f}%  Sharpe={r['sharpe']:.3f}  "
                  f"MaxDD={r['max_drawdown']*100:+.1f}%  ({r['elapsed']:.1f}s)")

    # Sort by CAGR
    results_sorted = sorted(results, key=lambda r: r.get("cagr", 0), reverse=True)

    print(f"\n{'='*80}")
    print(f"  RESULTS (sorted by CAGR)")
    print(f"{'='*80}")
    print(f"{'Rank':<5s} {'Label':<22s} {'CAGR':>8s} {'Sharpe':>8s} {'MaxDD':>8s} {'Vol':>8s}")
    print("-" * 65)
    for i, r in enumerate(results_sorted):
        if "error" in r:
            print(f"{i+1:<5d} {r['label']:<22s} ERROR")
            continue
        marker = " <-- BEST" if i == 0 else ""
        print(f"{i+1:<5d} {r['label']:<22s} {r['cagr']*100:>7.2f}% {r['sharpe']:>8.3f} "
              f"{r['max_drawdown']*100:>+7.1f}% {r['ann_vol']*100:>7.1f}%{marker}")

    # Save results
    out_path = Path("results/equity_grid_search/grid_results.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results_sorted, indent=2, default=str))
    print(f"\nResults saved to {out_path}")

    # Also save as CSV for easy viewing
    csv_path = out_path.with_suffix(".csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "label", "cagr", "sharpe", "max_drawdown", "ann_vol",
            "score_concentration_power", "portfolio_max_names",
            "min_rebalance_pct", "blend_weights", "elapsed",
        ])
        writer.writeheader()
        for r in results_sorted:
            if "error" not in r:
                writer.writerow({k: r.get(k, "") for k in writer.fieldnames})
    print(f"CSV saved to {csv_path}")

    # Report best config
    best = results_sorted[0]
    print(f"\n{'='*80}")
    print(f"  BEST CONFIG: {best['label']}")
    print(f"  CAGR: {best['cagr']*100:.2f}%  Sharpe: {best['sharpe']:.3f}  MaxDD: {best['max_drawdown']*100:.1f}%")
    print(f"  score_concentration_power: {best.get('score_concentration_power')}")
    print(f"  portfolio_max_names: {best.get('portfolio_max_names')}")
    print(f"  min_rebalance_pct: {best.get('min_rebalance_pct')}")
    print(f"  blend_weights: {best.get('blend_weights')}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
