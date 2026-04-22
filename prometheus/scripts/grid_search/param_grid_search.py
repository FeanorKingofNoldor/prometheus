"""Prometheus v2 – Comprehensive Parameter Grid Search Engine.

Generates parameter combinations and batch-runs them across:
  - Phase 1 (equity): C++ lambda factorial backtester
  - Phase 2 (options): Python synthetic options backtester

Usage::

    # Equity one-at-a-time sweep (~30 runs, ~30 min):
    PYTHONPATH=cpp/build ./venv/bin/python \\
        -m prometheus.scripts.grid_search.param_grid_search equity --mode sweep

    # Equity random sample (200 configs from full grid):
    PYTHONPATH=cpp/build ./venv/bin/python \\
        -m prometheus.scripts.grid_search.param_grid_search equity --mode random --n-samples 200

    # Options sweep on best equity NAV:
    ./venv/bin/python \\
        -m prometheus.scripts.grid_search.param_grid_search options --mode sweep \\
        --equity-nav results/options_backtest/equity_nav_series.json

    # Full pipeline (equity sweep → export best NAV → options sweep):
    PYTHONPATH=cpp/build ./venv/bin/python \\
        -m prometheus.scripts.grid_search.param_grid_search full --mode sweep
"""

from __future__ import annotations

import argparse
import itertools
import json
import random
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from apathis.core.logging import get_logger

logger = get_logger(__name__)


# ── Defaults (current best config) ───────────────────────────────────

EQUITY_DEFAULTS = {
    "score_concentration_power": 2.5,
    "portfolio_max_names": 25,
    "min_rebalance_pct": 0.10,
    "blend_weights": [0.45, 0.55],
    "sector_tilt_strength": 1.5,
    "sector_max_tilt": 0.40,
    "include_delisted_instruments": False,
}

OPTIONS_DEFAULTS = {
    "nav_pct_scale": 1.0,          # Global multiplier on all strategy nav_pct
    "derivatives_budget_pct": 0.15,
    "iron_butterfly_max_vix": 18.0,
    "iron_condor_max_vix": 20.0,
    "momentum_call_min_momentum": 0.02,
    "vix_tail_nav_pct": 0.03,
}


# ── Equity Grid Definition ───────────────────────────────────────────

EQUITY_GRID = {
    "score_concentration_power": [1.0, 1.5, 2.0, 2.5, 3.0, 3.5],
    "portfolio_max_names": [10, 15, 20, 25, 30, 40, 50],
    "min_rebalance_pct": [0.0, 0.02, 0.05, 0.10, 0.15, 0.20],
    "blend_weights": [
        [0.20, 0.80], [0.30, 0.70], [0.40, 0.60],
        [0.45, 0.55], [0.50, 0.50], [0.60, 0.40],
    ],
    "sector_tilt_strength": [0.5, 1.0, 1.5, 2.0, 2.5, 3.0],
    "sector_max_tilt": [0.15, 0.20, 0.30, 0.40, 0.50, 0.60],
    "include_delisted_instruments": [True, False],
}


# ── Options Grid Definition ──────────────────────────────────────────

OPTIONS_GRID = {
    "nav_pct_scale": [0.25, 0.50, 0.75, 1.0, 1.25, 1.50, 2.0],
    "derivatives_budget_pct": [0.05, 0.10, 0.15, 0.20, 0.25, 0.30],
    "iron_butterfly_max_vix": [14, 16, 18, 20, 22, 25],
    "iron_condor_max_vix": [16, 18, 20, 22, 25, 30],
    "momentum_call_min_momentum": [0.00, 0.01, 0.02, 0.03, 0.05],
    "vix_tail_nav_pct": [0.005, 0.01, 0.02, 0.03, 0.04, 0.05],
}


# ── Grid generation helpers ──────────────────────────────────────────

def _generate_sweep_configs(
    grid: Dict[str, list],
    defaults: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """One-at-a-time sweep: vary each parameter while holding others at defaults."""
    configs = []
    for param, values in grid.items():
        for val in values:
            cfg = dict(defaults)
            cfg[param] = val
            cfg["_sweep_param"] = param
            cfg["_sweep_value"] = str(val)
            configs.append(cfg)
    return configs


def _generate_random_configs(
    grid: Dict[str, list],
    n_samples: int,
    seed: int = 42,
) -> List[Dict[str, Any]]:
    """Random sample from the full cartesian grid."""
    rng = random.Random(seed)
    configs = []
    for _ in range(n_samples):
        cfg = {}
        for param, values in grid.items():
            cfg[param] = rng.choice(values)
        configs.append(cfg)
    return configs


def _generate_full_configs(grid: Dict[str, list]) -> List[Dict[str, Any]]:
    """Full cartesian product."""
    keys = list(grid.keys())
    values = list(grid.values())
    configs = []
    for combo in itertools.product(*values):
        configs.append(dict(zip(keys, combo)))
    return configs


# ── Equity Grid Search ───────────────────────────────────────────────

LAMBDA_CSV = (
    "data/cache_ic_v2/lambda_scores/"
    "lambda_cluster_scores_smoothed_US_EQ_1997-01-02_2026-03-02_h5-63_8col.csv"
)


@dataclass
class EquityResult:
    config: Dict[str, Any]
    sleeve_id: str = ""
    cagr: float = 0.0
    sharpe: float = 0.0
    max_drawdown: float = 0.0
    annualised_vol: float = 0.0
    n_trading_days: int = 0
    elapsed_seconds: float = 0.0
    error: str = ""


def _load_instruments(market_id: str = "US_EQ") -> List[str]:
    """Load real instrument IDs from database."""
    from apathis.core.database import get_db_manager
    db = get_db_manager()
    sql = """
        SELECT instrument_id FROM instruments
        WHERE market_id = %s AND instrument_id NOT LIKE 'SYNTH_%%'
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


def run_equity_config(
    cfg: Dict[str, Any],
    instrument_ids: List[str],
    lambda_csv: str,
    cpp_threads: int = 32,
) -> EquityResult:
    """Run a single equity backtest config via prom2_cpp."""
    try:
        import prom2_cpp as prom2
    except ImportError:
        return EquityResult(config=cfg, error="prom2_cpp not available")

    blend_weights = cfg.get("blend_weights", EQUITY_DEFAULTS["blend_weights"])

    cpp_cfg = {
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
        "num_threads": cpp_threads,
        "verbose": False,
        "persist_to_db": False,
        "conviction_enabled": True,
        "sector_allocator_enabled": True,
        "portfolio_max_names": cfg.get("portfolio_max_names", 25),
        "run_blended_sleeves": True,
        "lambda_blend_weights": blend_weights,
        "modes": ["universe_and_size"],
        "score_concentration_power": cfg.get("score_concentration_power", 2.5),
        "min_rebalance_pct": cfg.get("min_rebalance_pct", 0.10),
        "sector_tilt_strength": cfg.get("sector_tilt_strength", 1.5),
        "sector_max_tilt": cfg.get("sector_max_tilt", 0.40),
        "include_delisted_instruments": cfg.get("include_delisted_instruments", False),
    }

    t0 = time.time()
    try:
        results = prom2.run_lambda_factorial_backtests(cpp_cfg)
    except Exception as exc:
        return EquityResult(config=cfg, error=str(exc), elapsed_seconds=time.time() - t0)
    elapsed = time.time() - t0

    # Find the best blended sleeve
    best = None
    for r in results:
        sid = r.get("sleeve_id", "")
        if "BLENDED" not in sid.upper():
            continue
        m = r.get("metrics", r)
        cum_ret = m.get("cumulative_return", 0.0)
        n_days = m.get("n_trading_days", 0)
        years = n_days / 252.0 if n_days > 0 else 1.0
        cagr = (1.0 + cum_ret) ** (1.0 / years) - 1.0 if years > 0 and cum_ret > -1 else 0.0
        sharpe = m.get("annualised_sharpe", 0.0)
        max_dd = m.get("max_drawdown", 0.0)
        ann_vol = m.get("annualised_vol", 0.0)

        if best is None or cagr > best.cagr:
            best = EquityResult(
                config=cfg,
                sleeve_id=sid,
                cagr=cagr,
                sharpe=sharpe,
                max_drawdown=max_dd,
                annualised_vol=ann_vol,
                n_trading_days=n_days,
                elapsed_seconds=elapsed,
            )

    if best is None:
        # Fallback: pick best overall
        for r in results:
            m = r.get("metrics", r)
            cum_ret = m.get("cumulative_return", 0.0)
            n_days = m.get("n_trading_days", 0)
            years = n_days / 252.0 if n_days > 0 else 1.0
            cagr = (1.0 + cum_ret) ** (1.0 / years) - 1.0 if years > 0 and cum_ret > -1 else 0.0
            if best is None or cagr > best.cagr:
                best = EquityResult(
                    config=cfg,
                    sleeve_id=r.get("sleeve_id", "?"),
                    cagr=cagr,
                    sharpe=m.get("annualised_sharpe", 0.0),
                    max_drawdown=m.get("max_drawdown", 0.0),
                    annualised_vol=m.get("annualised_vol", 0.0),
                    n_trading_days=n_days,
                    elapsed_seconds=elapsed,
                )

    return best or EquityResult(config=cfg, error="No results returned", elapsed_seconds=elapsed)


def run_equity_grid(
    configs: List[Dict[str, Any]],
    lambda_csv: str = LAMBDA_CSV,
    cpp_threads: int = 32,
    output_path: Optional[str] = None,
) -> List[EquityResult]:
    """Run all equity grid configs and return sorted results."""
    instrument_ids = _load_instruments()
    logger.info("Loaded %d instruments. Running %d equity configs...", len(instrument_ids), len(configs))

    results: List[EquityResult] = []
    for i, cfg in enumerate(configs):
        sweep_info = cfg.get("_sweep_param", "")
        sweep_val = cfg.get("_sweep_value", "")
        label = f"{sweep_info}={sweep_val}" if sweep_info else f"config_{i}"

        logger.info("[%d/%d] Running %s ...", i + 1, len(configs), label)
        result = run_equity_config(cfg, instrument_ids, lambda_csv, cpp_threads)
        results.append(result)

        if result.error:
            logger.warning("  -> ERROR: %s", result.error)
        else:
            logger.info(
                "  -> CAGR=%.2f%% Sharpe=%.3f MaxDD=%.2f%% (%.1fs)",
                result.cagr * 100, result.sharpe, result.max_drawdown * 100,
                result.elapsed_seconds,
            )

    # Sort by CAGR descending
    results.sort(key=lambda r: r.cagr, reverse=True)

    # Print leaderboard
    print(f"\n{'='*90}")
    print(f"  EQUITY GRID SEARCH RESULTS  ({len(results)} configs)")
    print(f"{'='*90}")
    print(f"{'#':>3s}  {'CAGR':>8s} {'Sharpe':>8s} {'MaxDD':>8s} {'Vol':>8s}  Config")
    print("-" * 90)
    for i, r in enumerate(results[:30]):
        # Compact config display
        diff = {k: v for k, v in r.config.items() if not k.startswith("_") and v != EQUITY_DEFAULTS.get(k)}
        diff_str = ", ".join(f"{k}={v}" for k, v in diff.items())
        if not diff_str:
            diff_str = "(defaults)"
        print(f"{i+1:>3d}  {r.cagr:>7.2%} {r.sharpe:>8.3f} {r.max_drawdown:>7.2%} {r.annualised_vol:>7.2%}  {diff_str}")
    print()

    # Save results
    if output_path is None:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_path = f"results/grid_search/equity_grid_{ts}.json"

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "n_configs": len(results),
        "best": {
            "cagr": round(results[0].cagr, 6) if results else 0,
            "sharpe": round(results[0].sharpe, 4) if results else 0,
            "max_drawdown": round(results[0].max_drawdown, 6) if results else 0,
            "config": results[0].config if results else {},
        },
        "results": [
            {
                "rank": i + 1,
                "cagr": round(r.cagr, 6),
                "sharpe": round(r.sharpe, 4),
                "max_drawdown": round(r.max_drawdown, 6),
                "annualised_vol": round(r.annualised_vol, 6),
                "n_trading_days": r.n_trading_days,
                "elapsed_seconds": round(r.elapsed_seconds, 1),
                "sleeve_id": r.sleeve_id,
                "config": {k: v for k, v in r.config.items() if not k.startswith("_")},
                "error": r.error or None,
            }
            for i, r in enumerate(results)
        ],
    }
    out.write_text(json.dumps(payload, indent=2, default=str))
    print(f"Results saved to {output_path}")
    return results


# ── Options Grid Search ──────────────────────────────────────────────

@dataclass
class OptionsResult:
    config: Dict[str, Any]
    cagr: float = 0.0
    sharpe: float = 0.0
    max_drawdown: float = 0.0
    options_pnl: float = 0.0
    equity_cagr: float = 0.0
    elapsed_seconds: float = 0.0
    strategy_metrics: Dict[str, Any] = field(default_factory=dict)
    error: str = ""


def _build_strategy_overrides(cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Convert a flat grid-search config into per-strategy config overrides.

    Returns a dict of {ConfigClassName: {field: value}} suitable for
    OptionsBacktestEngine(strategy_overrides=...).
    """
    overrides: Dict[str, Dict[str, Any]] = {}
    scale = cfg.get("nav_pct_scale", 1.0)

    # Direct field overrides
    if "vix_tail_nav_pct" in cfg:
        overrides.setdefault("VixTailHedgeConfig", {})["nav_pct"] = cfg["vix_tail_nav_pct"]
    if "iron_butterfly_max_vix" in cfg:
        overrides.setdefault("IronButterflyConfig", {})["max_vix"] = cfg["iron_butterfly_max_vix"]
    if "iron_condor_max_vix" in cfg:
        overrides.setdefault("IronCondorConfig", {})["max_vix"] = cfg["iron_condor_max_vix"]
    if "momentum_call_min_momentum" in cfg:
        overrides.setdefault("MomentumCallConfig", {})["min_momentum_63d"] = cfg["momentum_call_min_momentum"]

    # Global nav_pct scaling — multiply each strategy's base nav_pct
    if scale != 1.0:
        nav_pct_map = [
            ("VixTailHedgeConfig", "nav_pct", 0.03),
            ("IronButterflyConfig", "nav_pct", 0.03),
            ("IronCondorConfig", "nav_pct", 0.04),
            ("BullCallSpreadConfig", "max_risk_per_trade_pct", 0.03),
            ("WheelConfig", "max_nav_pct_per_position", 0.06),
            ("MomentumCallConfig", "nav_pct", 0.03),
        ]
        for cls_name, attr, base_val in nav_pct_map:
            d = overrides.setdefault(cls_name, {})
            # If the field was already set by a direct override, scale that;
            # otherwise scale the base value.
            d[attr] = round(d.get(attr, base_val) * scale, 6)

    return overrides


def run_options_config(
    cfg: Dict[str, Any],
    equity_nav_path: str,
) -> OptionsResult:
    """Run a single options backtest config."""
    from apathis.core.database import get_db_manager
    from apathis.data.reader import DataReader

    from prometheus.backtest.options_backtest import (
        OptionsBacktestConfig,
        OptionsBacktestEngine,
    )

    deriv_budget = cfg.get("derivatives_budget_pct", 0.15)

    bt_cfg = OptionsBacktestConfig(
        start_date=date(1997, 1, 2),
        end_date=date(2026, 3, 2),
        initial_nav=1_000_000.0,
        derivatives_budget_pct=deriv_budget,
        equity_backtest_path=equity_nav_path,
        slippage_pct=0.25,
        max_position_count=100,
        log_every_n_days=252,  # Quiet — yearly logging only
    )

    # Connect to DB for market data
    try:
        db = get_db_manager()
        data_reader = DataReader(db_manager=db)
    except Exception as exc:
        return OptionsResult(config=cfg, error=f"DB connection failed: {exc}")

    # Build per-strategy config overrides from flat grid cfg
    strategy_overrides = _build_strategy_overrides(cfg)

    t0 = time.time()
    try:
        engine = OptionsBacktestEngine(
            bt_cfg,
            data_reader=data_reader,
            strategy_overrides=strategy_overrides,
        )
        result = engine.run()
    except Exception as exc:
        return OptionsResult(config=cfg, error=str(exc), elapsed_seconds=time.time() - t0)

    elapsed = time.time() - t0
    s = result.summary

    return OptionsResult(
        config=cfg,
        cagr=s.get("cagr", 0.0),
        sharpe=s.get("sharpe", 0.0),
        max_drawdown=s.get("max_drawdown", 0.0),
        options_pnl=s.get("options_total_pnl", 0.0),
        equity_cagr=s.get("equity_only_cagr", 0.0),
        elapsed_seconds=elapsed,
        strategy_metrics=result.strategy_metrics,
    )


def run_options_grid(
    configs: List[Dict[str, Any]],
    equity_nav_path: str,
    output_path: Optional[str] = None,
) -> List[OptionsResult]:
    """Run all options grid configs and return sorted results."""
    logger.info("Running %d options configs on equity NAV: %s", len(configs), equity_nav_path)

    results: List[OptionsResult] = []
    for i, cfg in enumerate(configs):
        sweep_info = cfg.get("_sweep_param", "")
        sweep_val = cfg.get("_sweep_value", "")
        label = f"{sweep_info}={sweep_val}" if sweep_info else f"config_{i}"

        logger.info("[%d/%d] Running options %s ...", i + 1, len(configs), label)
        result = run_options_config(cfg, equity_nav_path)
        results.append(result)

        if result.error:
            logger.warning("  -> ERROR: %s", result.error)
        else:
            logger.info(
                "  -> CAGR=%.2f%% Sharpe=%.3f MaxDD=%.2f%% OptPnL=$%.0fM (%.1fs)",
                result.cagr * 100, result.sharpe, result.max_drawdown * 100,
                result.options_pnl / 1e6, result.elapsed_seconds,
            )

    # Sort by Sharpe descending (risk-adjusted is more meaningful for options)
    results.sort(key=lambda r: r.sharpe, reverse=True)

    # Print leaderboard
    print(f"\n{'='*100}")
    print(f"  OPTIONS GRID SEARCH RESULTS  ({len(results)} configs)")
    print(f"{'='*100}")
    print(f"{'#':>3s}  {'CAGR':>8s} {'Sharpe':>8s} {'MaxDD':>8s} {'OptPnL':>10s}  Config")
    print("-" * 100)
    for i, r in enumerate(results[:30]):
        diff = {k: v for k, v in r.config.items() if not k.startswith("_") and v != OPTIONS_DEFAULTS.get(k)}
        diff_str = ", ".join(f"{k}={v}" for k, v in diff.items())
        if not diff_str:
            diff_str = "(defaults)"
        print(f"{i+1:>3d}  {r.cagr:>7.2%} {r.sharpe:>8.3f} {r.max_drawdown:>7.2%} ${r.options_pnl/1e6:>8.1f}M  {diff_str}")
    print()

    # Save results
    if output_path is None:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_path = f"results/grid_search/options_grid_{ts}.json"

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "equity_nav_path": equity_nav_path,
        "n_configs": len(results),
        "best": {
            "cagr": round(results[0].cagr, 6) if results else 0,
            "sharpe": round(results[0].sharpe, 4) if results else 0,
            "max_drawdown": round(results[0].max_drawdown, 6) if results else 0,
            "options_pnl": round(results[0].options_pnl, 2) if results else 0,
            "config": results[0].config if results else {},
        },
        "results": [
            {
                "rank": i + 1,
                "cagr": round(r.cagr, 6),
                "sharpe": round(r.sharpe, 4),
                "max_drawdown": round(r.max_drawdown, 6),
                "options_pnl": round(r.options_pnl, 2),
                "equity_cagr": round(r.equity_cagr, 6),
                "elapsed_seconds": round(r.elapsed_seconds, 1),
                "config": {k: v for k, v in r.config.items() if not k.startswith("_")},
                "strategy_metrics": r.strategy_metrics,
                "error": r.error or None,
            }
            for i, r in enumerate(results)
        ],
    }
    out.write_text(json.dumps(payload, indent=2, default=str))
    print(f"Results saved to {output_path}")
    return results


# ── NAV Export ────────────────────────────────────────────────────────

def export_best_equity_nav(
    best_config: Dict[str, Any],
    output_path: str = "results/options_backtest/equity_nav_series.json",
    cpp_threads: int = 32,
) -> str:
    """Re-run best equity config with DB persistence and export NAV series."""
    import prom2_cpp as prom2

    instrument_ids = _load_instruments()
    blend_weights = best_config.get("blend_weights", EQUITY_DEFAULTS["blend_weights"])

    cpp_cfg = {
        "market_id": "US_EQ",
        "start": "1997-01-02",
        "end": "2026-03-02",
        "instrument_ids": instrument_ids,
        "lambda_scores_csv": LAMBDA_CSV,
        "horizons": [5, 63],
        "lambda_weight": 10.0,
        "initial_cash": 1_000_000.0,
        "apply_risk": True,
        "apply_fragility_overlay": False,
        "slippage_bps": 5.0,
        "num_threads": cpp_threads,
        "verbose": False,
        "persist_to_db": True,  # Persist to extract NAV
        "conviction_enabled": True,
        "sector_allocator_enabled": True,
        "portfolio_max_names": best_config.get("portfolio_max_names", 25),
        "run_blended_sleeves": True,
        "lambda_blend_weights": blend_weights,
        "modes": ["universe_and_size"],
        "score_concentration_power": best_config.get("score_concentration_power", 2.5),
        "min_rebalance_pct": best_config.get("min_rebalance_pct", 0.10),
        "sector_tilt_strength": best_config.get("sector_tilt_strength", 1.5),
        "sector_max_tilt": best_config.get("sector_max_tilt", 0.40),
        "include_delisted_instruments": best_config.get("include_delisted_instruments", False),
    }

    logger.info("Re-running best equity config with persistence for NAV export...")
    results = prom2.run_lambda_factorial_backtests(cpp_cfg)

    # Find the blended sleeve and extract its run_id
    best_run_id = None
    for r in results:
        sid = r.get("sleeve_id", "")
        if "BLENDED" in sid.upper():
            best_run_id = r.get("run_id", "")
            break

    if not best_run_id:
        # Fallback: use first result
        best_run_id = results[0].get("run_id", "") if results else ""

    if not best_run_id:
        logger.error("No run_id found in results — cannot export NAV")
        return ""

    # Extract daily NAV from database
    from apathis.core.database import get_db_manager
    db = get_db_manager()

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """SELECT date, equity_curve_value
                   FROM backtest_daily_equity
                   WHERE run_id = %s
                   ORDER BY date""",
                (best_run_id,),
            )
            rows = cur.fetchall()
        finally:
            cur.close()

    daily_nav = {str(row[0]): float(row[1]) for row in rows}
    logger.info("Extracted %d NAV points from run %s", len(daily_nav), best_run_id)

    # Write
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "engine": "lambda_factorial",
        "daily_nav": daily_nav,
        "source_run_id": best_run_id,
        "config": {k: v for k, v in best_config.items() if not k.startswith("_")},
    }
    out.write_text(json.dumps(payload, indent=2))
    logger.info("NAV series written to %s (engine=lambda_factorial)", output_path)
    return output_path


# ── CLI ──────────────────────────────────────────────────────────────

def _parse_date(value: str) -> date:
    year, month, day = map(int, value.split("-"))
    return date(year, month, day)


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Prometheus v2 Parameter Grid Search Engine",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ── equity subcommand ──
    eq_parser = sub.add_parser("equity", help="Run equity (C++) parameter grid search")
    eq_parser.add_argument("--mode", choices=["sweep", "random", "full"], default="sweep",
                           help="Grid generation mode")
    eq_parser.add_argument("--n-samples", type=int, default=200,
                           help="Number of random samples (for --mode random)")
    eq_parser.add_argument("--lambda-csv", type=str, default=LAMBDA_CSV)
    eq_parser.add_argument("--cpp-threads", type=int, default=32)
    eq_parser.add_argument("--output", type=str, default=None)
    eq_parser.add_argument("--seed", type=int, default=42)

    # ── options subcommand ──
    opt_parser = sub.add_parser("options", help="Run options (Python) parameter grid search")
    opt_parser.add_argument("--mode", choices=["sweep", "random", "full"], default="sweep")
    opt_parser.add_argument("--n-samples", type=int, default=50)
    opt_parser.add_argument("--equity-nav", type=str,
                            default="results/options_backtest/equity_nav_series.json",
                            help="Path to equity NAV series JSON")
    opt_parser.add_argument("--output", type=str, default=None)
    opt_parser.add_argument("--seed", type=int, default=42)

    # ── full subcommand ──
    full_parser = sub.add_parser("full", help="Run equity grid → export NAV → options grid")
    full_parser.add_argument("--mode", choices=["sweep", "random"], default="sweep")
    full_parser.add_argument("--n-equity-samples", type=int, default=200)
    full_parser.add_argument("--n-options-samples", type=int, default=50)
    full_parser.add_argument("--lambda-csv", type=str, default=LAMBDA_CSV)
    full_parser.add_argument("--cpp-threads", type=int, default=32)
    full_parser.add_argument("--seed", type=int, default=42)

    args = parser.parse_args(argv)

    if args.command == "equity":
        if args.mode == "sweep":
            configs = _generate_sweep_configs(EQUITY_GRID, EQUITY_DEFAULTS)
        elif args.mode == "random":
            configs = _generate_random_configs(EQUITY_GRID, args.n_samples, args.seed)
        else:
            configs = _generate_full_configs(EQUITY_GRID)

        print(f"\nEquity grid: {len(configs)} configs ({args.mode} mode)")
        run_equity_grid(configs, args.lambda_csv, args.cpp_threads, args.output)

    elif args.command == "options":
        if args.mode == "sweep":
            configs = _generate_sweep_configs(OPTIONS_GRID, OPTIONS_DEFAULTS)
        elif args.mode == "random":
            configs = _generate_random_configs(OPTIONS_GRID, args.n_samples, args.seed)
        else:
            configs = _generate_full_configs(OPTIONS_GRID)

        print(f"\nOptions grid: {len(configs)} configs ({args.mode} mode)")
        run_options_grid(configs, args.equity_nav, args.output)

    elif args.command == "full":
        # Phase 1: Equity grid
        if args.mode == "sweep":
            eq_configs = _generate_sweep_configs(EQUITY_GRID, EQUITY_DEFAULTS)
        else:
            eq_configs = _generate_random_configs(EQUITY_GRID, args.n_equity_samples, args.seed)

        print(f"\n{'='*80}")
        print(f"  PHASE 1: Equity Grid ({len(eq_configs)} configs)")
        print(f"{'='*80}")
        eq_results = run_equity_grid(eq_configs, args.lambda_csv, args.cpp_threads)

        if not eq_results or eq_results[0].error:
            print("ERROR: No valid equity results. Aborting.")
            return

        # Phase 2: Export best NAV
        print(f"\n{'='*80}")
        print(f"  PHASE 2: Exporting best equity NAV (CAGR={eq_results[0].cagr:.2%})")
        print(f"{'='*80}")
        nav_path = export_best_equity_nav(eq_results[0].config, cpp_threads=args.cpp_threads)
        if not nav_path:
            print("ERROR: NAV export failed. Aborting.")
            return

        # Phase 3: Options grid
        if args.mode == "sweep":
            opt_configs = _generate_sweep_configs(OPTIONS_GRID, OPTIONS_DEFAULTS)
        else:
            opt_configs = _generate_random_configs(OPTIONS_GRID, args.n_options_samples, args.seed)

        print(f"\n{'='*80}")
        print(f"  PHASE 3: Options Grid ({len(opt_configs)} configs)")
        print(f"{'='*80}")
        run_options_grid(opt_configs, nav_path)


if __name__ == "__main__":
    main()
