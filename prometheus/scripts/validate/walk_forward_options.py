"""Walk-forward validation for v12 options parameters.

Tests whether the v12 options tuning (vix_tail_nav_pct=0.005,
iron_butterfly_max_vix=22) is overfit by running a rolling
train/test split on the options overlay.

The equity NAV is fixed (lambda factorial, full period).  Only the
options parameters are re-optimised on each training window, then
evaluated out-of-sample on the test window.

Usage::

    ./venv/bin/python -m prometheus.scripts.validate.walk_forward_options \
        --equity-nav results/options_backtest/equity_nav_series_v12_fresh.json

"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from apathis.core.logging import get_logger

logger = get_logger(__name__)


# ── v12 tuned params & defaults ──────────────────────────────────────

V12_OVERRIDES = {
    "vix_tail_nav_pct": 0.005,
    "iron_butterfly_max_vix": 22.0,
}

OPTIONS_DEFAULTS = {
    "nav_pct_scale": 1.0,
    "derivatives_budget_pct": 0.15,
    "iron_butterfly_max_vix": 18.0,
    "iron_condor_max_vix": 20.0,
    "momentum_call_min_momentum": 0.02,
    "vix_tail_nav_pct": 0.03,
}

# Parameters to sweep (same grid as the original search)
SWEEP_GRID = {
    "vix_tail_nav_pct": [0.005, 0.01, 0.02, 0.03, 0.04, 0.05],
    "iron_butterfly_max_vix": [14, 16, 18, 20, 22, 25],
    "iron_condor_max_vix": [16, 18, 20, 22, 25, 30],
    "nav_pct_scale": [0.50, 0.75, 1.0, 1.25, 1.50],
}


# ── Walk-forward folds ───────────────────────────────────────────────

@dataclass
class Fold:
    name: str
    train_start: date
    train_end: date
    test_start: date
    test_end: date


FOLDS = [
    Fold("Fold1", date(1997, 1, 2), date(2011, 12, 31), date(2012, 1, 2), date(2016, 12, 31)),
    Fold("Fold2", date(1997, 1, 2), date(2016, 12, 31), date(2017, 1, 2), date(2021, 12, 31)),
    Fold("Fold3", date(1997, 1, 2), date(2021, 12, 31), date(2022, 1, 2), date(2026, 3, 2)),
]


# ── Single run helper ────────────────────────────────────────────────

def _run_single(
    cfg: Dict[str, Any],
    equity_nav_path: str,
    start: date,
    end: date,
) -> Dict[str, float]:
    """Run one options backtest config on a date window.  Returns summary dict."""
    from prometheus.backtest.options_backtest import (
        OptionsBacktestConfig,
        OptionsBacktestEngine,
    )
    from apathis.core.database import get_db_manager
    from apathis.data.reader import DataReader
    from prometheus.scripts.grid_search.param_grid_search import _build_strategy_overrides

    bt_cfg = OptionsBacktestConfig(
        start_date=start,
        end_date=end,
        initial_nav=1_000_000.0,
        derivatives_budget_pct=cfg.get("derivatives_budget_pct", 0.15),
        equity_backtest_path=equity_nav_path,
        log_every_n_days=9999,  # silent
    )

    db = get_db_manager()
    reader = DataReader(db_manager=db)
    overrides = _build_strategy_overrides(cfg)

    engine = OptionsBacktestEngine(bt_cfg, data_reader=reader, strategy_overrides=overrides)
    result = engine.run()
    return result.summary


def _sweep_best(
    equity_nav_path: str,
    start: date,
    end: date,
) -> Tuple[Dict[str, Any], Dict[str, float]]:
    """Run a one-at-a-time sweep and return (best_cfg, best_summary)."""
    best_sharpe = -999.0
    best_cfg: Dict[str, Any] = {}
    best_summary: Dict[str, float] = {}

    for param, values in SWEEP_GRID.items():
        for val in values:
            cfg = dict(OPTIONS_DEFAULTS)
            cfg[param] = val
            try:
                summary = _run_single(cfg, equity_nav_path, start, end)
                sharpe = summary.get("sharpe", 0.0)
                if sharpe > best_sharpe:
                    best_sharpe = sharpe
                    best_cfg = dict(cfg)
                    best_summary = summary
            except Exception as exc:
                logger.debug("Sweep error %s=%s: %s", param, val, exc)

    return best_cfg, best_summary


# ── Main ─────────────────────────────────────────────────────────────

def run_walk_forward(equity_nav_path: str) -> List[Dict[str, Any]]:
    """Run the full walk-forward validation."""
    results = []

    for fold in FOLDS:
        print(f"\n{'='*80}")
        print(f"  {fold.name}: Train {fold.train_start}→{fold.train_end}  |  "
              f"Test {fold.test_start}→{fold.test_end}")
        print(f"{'='*80}")

        # 1. Sweep on training window
        t0 = time.time()
        best_cfg, best_train_summary = _sweep_best(
            equity_nav_path, fold.train_start, fold.train_end,
        )
        sweep_time = time.time() - t0
        best_diff = {k: v for k, v in best_cfg.items()
                     if v != OPTIONS_DEFAULTS.get(k)}
        print(f"\n  Training sweep ({sweep_time:.0f}s): "
              f"best Sharpe={best_train_summary.get('sharpe', 0):.3f}  "
              f"params={best_diff}")

        # 2. Evaluate on test window: (a) in-sample best, (b) v12, (c) defaults
        configs_to_test = {
            "fold_best": best_cfg,
            "v12": {**OPTIONS_DEFAULTS, **V12_OVERRIDES},
            "defaults": dict(OPTIONS_DEFAULTS),
        }

        fold_result = {"fold": fold.name, "train": str(fold.train_start) + "→" + str(fold.train_end),
                       "test": str(fold.test_start) + "→" + str(fold.test_end)}

        print(f"\n  {'Label':<12s} {'CAGR':>8s} {'Sharpe':>8s} {'MaxDD':>8s} {'OptPnL':>12s}")
        print(f"  {'-'*52}")

        for label, cfg in configs_to_test.items():
            try:
                summary = _run_single(cfg, equity_nav_path, fold.test_start, fold.test_end)
                cagr = summary.get("cagr", 0)
                sharpe = summary.get("sharpe", 0)
                maxdd = summary.get("max_drawdown", 0)
                opt_pnl = summary.get("options_total_pnl", 0)
                print(f"  {label:<12s} {cagr:>7.2%} {sharpe:>8.3f} {maxdd:>7.2%} ${opt_pnl:>10,.0f}")
                fold_result[label] = {
                    "cagr": round(cagr, 4),
                    "sharpe": round(sharpe, 3),
                    "max_drawdown": round(maxdd, 4),
                    "options_pnl": round(opt_pnl, 2),
                }
                if label == "fold_best":
                    fold_result["fold_best_params"] = best_diff
            except Exception as exc:
                print(f"  {label:<12s} ERROR: {exc}")
                fold_result[label] = {"error": str(exc)}

        results.append(fold_result)

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n{'='*80}")
    print("  WALK-FORWARD SUMMARY (out-of-sample test periods)")
    print(f"{'='*80}")
    print(f"  {'Fold':<8s} {'v12 Sharpe':>12s} {'Default Sharpe':>16s} {'Best Sharpe':>14s} {'Best Params'}")
    print(f"  {'-'*80}")
    for r in results:
        v12_s = r.get("v12", {}).get("sharpe", "N/A")
        def_s = r.get("defaults", {}).get("sharpe", "N/A")
        best_s = r.get("fold_best", {}).get("sharpe", "N/A")
        params = r.get("fold_best_params", {})

        v12_str = f"{v12_s:.3f}" if isinstance(v12_s, float) else str(v12_s)
        def_str = f"{def_s:.3f}" if isinstance(def_s, float) else str(def_s)
        best_str = f"{best_s:.3f}" if isinstance(best_s, float) else str(best_s)

        print(f"  {r['fold']:<8s} {v12_str:>12s} {def_str:>16s} {best_str:>14s}  {params}")

    # Save
    out_path = "results/validation/walk_forward_options.json"
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text(json.dumps(results, indent=2))
    print(f"\nResults saved to {out_path}")

    return results


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Walk-forward validation for v12 options params")
    parser.add_argument(
        "--equity-nav", type=str,
        default="results/options_backtest/equity_nav_series_v12_fresh.json",
        help="Path to equity NAV JSON",
    )
    args = parser.parse_args()

    run_walk_forward(args.equity_nav)


if __name__ == "__main__":
    main()
