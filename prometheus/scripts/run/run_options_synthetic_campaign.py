"""Run the options backtest across 100 synthetic market realities.

Monte Carlo validation of the v8 options strategy configuration.
For each synthetic reality, derives VIX from SPY realized vol, builds
an equity NAV series, and runs the full options overlay.

Usage::

    ./venv/bin/python -m prometheus.scripts.run.run_options_synthetic_campaign \
        --manifest results/synthetic_campaigns/OOS_100x25yr/manifest.json \
        --equity-detail results/synthetic_campaigns/OOS_100x25yr/runs/baseline_only/detail.csv \
        --workers 8

    # Single-reality test:
    ./venv/bin/python -m prometheus.scripts.run.run_options_synthetic_campaign \
        --manifest results/synthetic_campaigns/OOS_100x25yr/manifest.json \
        --equity-detail results/synthetic_campaigns/OOS_100x25yr/runs/baseline_only/detail.csv \
        --limit 1
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
from apathis.core.logging import get_logger

logger = get_logger(__name__)

# Sector ETFs to load per reality
SECTOR_ETFS = ["XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLY", "XLU", "XLB"]

OUT_DIR = Path("results/options_backtest/synthetic_campaign")


# ── Data loading ─────────────────────────────────────────────────────

def _load_synthetic_prices(
    db_conn_params: Dict[str, Any],
    reality_id: str,
    symbols: List[str],
) -> Dict[str, Dict[date, float]]:
    """Load close prices for synthetic instruments from the historical DB."""
    import psycopg2

    prefix = f"SYNTH_{reality_id[:8]}"
    instrument_ids = [f"{prefix}_{sym}.US" for sym in symbols]

    conn = psycopg2.connect(**db_conn_params)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT instrument_id, trade_date, close
            FROM prices_daily
            WHERE instrument_id = ANY(%s)
            ORDER BY instrument_id, trade_date
            """,
            (instrument_ids,),
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    price_map: Dict[str, Dict[date, float]] = {}
    for iid, td, close in rows:
        # Extract symbol: SYNTH_xxx_SPY.US → SPY
        sym = iid.replace(f"{prefix}_", "").replace(".US", "")
        if sym not in price_map:
            price_map[sym] = {}
        price_map[sym][td] = float(close)

    return price_map


def _derive_synthetic_vix(
    spy_prices: Dict[date, float],
    window: int = 21,
    iv_hv_premium: float = 1.2,
) -> Dict[date, float]:
    """Derive VIX-like index from SPY realized vol."""
    sorted_dates = sorted(spy_prices.keys())
    closes = [spy_prices[d] for d in sorted_dates]

    vix_cache: Dict[date, float] = {}
    for i in range(window, len(closes)):
        rets = []
        for j in range(i - window + 1, i + 1):
            if closes[j - 1] > 0:
                rets.append(np.log(closes[j] / closes[j - 1]))
        if len(rets) >= 15:
            rv = float(np.std(rets)) * np.sqrt(252)
            synth_vix = rv * 100.0 * iv_hv_premium
            synth_vix = max(synth_vix, 10.0)
            synth_vix = min(synth_vix, 80.0)
            vix_cache[sorted_dates[i]] = synth_vix

    return vix_cache


def _build_equity_nav(
    spy_prices: Dict[date, float],
    equity_cagr: float,
    initial_nav: float = 1_000_000.0,
) -> Dict[str, float]:
    """Build daily equity NAV from synthetic SPY returns, scaled to match
    the C++ backtest's equity CAGR for this reality.

    The shape follows SPY (preserving drawdowns), but the total return
    is scaled to match the actual equity-only CAGR.
    """
    sorted_dates = sorted(spy_prices.keys())
    if len(sorted_dates) < 2:
        return {}

    # Compute raw SPY cumulative returns
    spy_navs = [initial_nav]
    for i in range(1, len(sorted_dates)):
        prev_price = spy_prices[sorted_dates[i - 1]]
        curr_price = spy_prices[sorted_dates[i]]
        if prev_price > 0:
            daily_ret = curr_price / prev_price - 1.0
        else:
            daily_ret = 0.0
        spy_navs.append(spy_navs[-1] * (1.0 + daily_ret))

    # Compute SPY CAGR
    years = len(sorted_dates) / 252.0
    spy_total_return = spy_navs[-1] / spy_navs[0] - 1.0
    if spy_total_return > -1.0 and years > 0:
        spy_cagr = (1.0 + spy_total_return) ** (1.0 / years) - 1.0
    else:
        spy_cagr = 0.0

    # Scale factor: shift daily returns so total CAGR matches equity backtest
    # daily_scale = (1 + equity_cagr) / (1 + spy_cagr) per year → per day
    if spy_cagr > -0.99 and equity_cagr > -0.99:
        daily_scale = ((1.0 + equity_cagr) / max(1.0 + spy_cagr, 0.01)) ** (1.0 / 252.0)
    else:
        daily_scale = 1.0

    # Rebuild NAV with scaled returns
    nav_series: Dict[str, float] = {}
    nav = initial_nav
    nav_series[sorted_dates[0].isoformat()] = nav
    for i in range(1, len(sorted_dates)):
        prev_price = spy_prices[sorted_dates[i - 1]]
        curr_price = spy_prices[sorted_dates[i]]
        if prev_price > 0:
            daily_ret = (curr_price / prev_price - 1.0) * daily_scale
        else:
            daily_ret = 0.0
        nav *= (1.0 + daily_ret)
        nav_series[sorted_dates[i].isoformat()] = nav

    return nav_series


# ── Single-reality backtest ──────────────────────────────────────────

def _run_single_reality(
    reality_id: str,
    start_date_str: str,
    end_date_str: str,
    equity_cagr: float,
    db_conn_params: Dict[str, Any],
    output_dir: str,
    derivatives_budget: float = 0.15,
    strategy_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Run the options backtest on one synthetic reality.

    Called in a worker process. Returns summary dict.
    """
    from prometheus.backtest.options_backtest import (
        OptionsBacktestConfig,
        OptionsBacktestEngine,
    )

    t0 = time.time()

    # Parse dates
    start = date.fromisoformat(start_date_str)
    end = date.fromisoformat(end_date_str)

    # 1. Load synthetic prices
    symbols = ["SPY"] + SECTOR_ETFS
    price_map = _load_synthetic_prices(db_conn_params, reality_id, symbols)

    spy_prices = price_map.get("SPY", {})
    if len(spy_prices) < 252:
        return {
            "reality_id": reality_id,
            "error": f"Insufficient SPY data: {len(spy_prices)} points",
        }

    # 2. Derive synthetic VIX
    vix_cache = _derive_synthetic_vix(spy_prices)

    # 3. Build equity NAV
    equity_nav = _build_equity_nav(spy_prices, equity_cagr)

    # 4. Compute realized vols for SPY + ETFs
    vol_caches: Dict[str, Dict[date, float]] = {}
    for sym, cache in price_map.items():
        sorted_d = sorted(cache.keys())
        prices = [cache[d] for d in sorted_d]
        vc: Dict[date, float] = {}
        for i in range(21, len(prices)):
            rets = []
            for j in range(i - 20, i + 1):
                if prices[j - 1] > 0:
                    rets.append(np.log(prices[j] / prices[j - 1]))
            if len(rets) >= 15:
                vc[sorted_d[i]] = float(np.std(rets)) * np.sqrt(252)
        vol_caches[sym] = vc

    # 5. Configure engine (no DB, no persistence)
    config = OptionsBacktestConfig(
        start_date=start,
        end_date=end,
        initial_nav=1_000_000.0,
        derivatives_budget_pct=derivatives_budget,
        equity_backtest_path=None,  # We inject directly
        log_every_n_days=9999,  # Suppress logging
    )

    engine = OptionsBacktestEngine(
        config, data_reader=None, writer=None,
        strategy_overrides=strategy_overrides,
    )

    # 6. Inject caches directly (bypass _preload_data)
    engine._equity_nav = equity_nav
    engine._vix_cache = vix_cache
    engine._price_cache = price_map  # SPY + sector ETFs
    engine._vol_cache = vol_caches

    # 7. Run
    try:
        result = engine.run()
    except Exception as exc:
        return {
            "reality_id": reality_id,
            "error": str(exc),
        }

    elapsed = time.time() - t0

    # 8. Collect metrics
    summary = result.summary
    strategy_pnl = {
        k: v.get("cumulative_pnl", 0.0)
        for k, v in result.strategy_metrics.items()
    }

    out = {
        "reality_id": reality_id,
        "elapsed_s": round(elapsed, 1),
        "equity_cagr": equity_cagr,
        "cagr": summary.get("cagr", 0.0),
        "sharpe": summary.get("sharpe", 0.0),
        "max_drawdown": summary.get("max_drawdown", 0.0),
        "annualised_vol": summary.get("annualised_vol", 0.0),
        "final_nav": summary.get("final_nav", 0.0),
        "options_pnl": summary.get("options_total_pnl", 0.0),
        "equity_only_cagr": summary.get("equity_only_cagr", 0.0),
        "n_trading_days": summary.get("n_trading_days", 0),
        **{f"pnl_{k}": v for k, v in strategy_pnl.items()},
    }

    # Optionally save per-reality JSON
    out_path = Path(output_dir) / f"{reality_id}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({"summary": summary, "strategy_metrics": result.strategy_metrics}, indent=2))

    return out


# ── Main ─────────────────────────────────────────────────────────────

def main(argv=None):
    parser = argparse.ArgumentParser(description="Options backtest on synthetic realities")
    parser.add_argument("--manifest", required=True, help="Path to manifest.json")
    parser.add_argument("--equity-detail", required=True, help="Path to detail.csv with equity CAGR per reality")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers")
    parser.add_argument("--limit", type=int, default=0, help="Max realities to run (0=all)")
    parser.add_argument("--derivatives-budget", type=float, default=0.15, help="Derivatives budget fraction")
    parser.add_argument("--output-dir", type=str, default=str(OUT_DIR), help="Output directory")
    parser.add_argument("--strategy-overrides", type=str, default=None,
                        help="JSON file with strategy config overrides (e.g. v12 config)")
    args = parser.parse_args(argv)

    # Load strategy overrides if provided
    strategy_overrides: Optional[Dict[str, Dict[str, Any]]] = None
    if args.strategy_overrides:
        strategy_overrides = json.loads(Path(args.strategy_overrides).read_text())
        logger.info("Loaded strategy overrides: %s", list(strategy_overrides.keys()))

    # Load manifest
    manifest = json.loads(Path(args.manifest).read_text())
    realities = manifest["realities"]
    logger.info("Loaded %d realities from manifest", len(realities))

    # Load equity CAGR per reality
    equity_cagrs: Dict[str, float] = {}
    with open(args.equity_detail) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rid = row["reality_id"]
            if rid not in equity_cagrs:  # Take first sleeve only
                equity_cagrs[rid] = float(row["cagr"])
    logger.info("Loaded equity CAGR for %d realities", len(equity_cagrs))

    # DB connection params from pydantic config
    from apathis.core.config import get_config
    cfg = get_config()
    db_cfg = cfg.historical_db
    db_conn_params = {
        "host": db_cfg.host,
        "port": db_cfg.port,
        "dbname": db_cfg.name,
        "user": db_cfg.user,
        "password": db_cfg.password,
    }

    # Build work items
    work: List[Dict[str, Any]] = []
    for r in realities:
        rid = r["reality_id"]
        cagr = equity_cagrs.get(rid, 0.0)
        work.append({
            "reality_id": rid,
            "start_date_str": r["start_date"],
            "end_date_str": r["end_date"],
            "equity_cagr": cagr,
            "db_conn_params": db_conn_params,
            "output_dir": args.output_dir,
            "derivatives_budget": args.derivatives_budget,
            "strategy_overrides": strategy_overrides,
        })

    if args.limit > 0:
        work = work[:args.limit]

    logger.info("Running %d realities with %d workers", len(work), args.workers)
    print(f"\n{'='*70}")
    print(f"  Options Synthetic Campaign: {len(work)} realities × ~25yr each")
    print(f"  Workers: {args.workers}")
    print(f"{'='*70}\n")

    t0 = time.time()
    results: List[Dict[str, Any]] = []

    if args.workers <= 1:
        # Serial for debugging
        for i, w in enumerate(work):
            print(f"  [{i+1}/{len(work)}] Reality {w['reality_id'][:8]}...", end=" ", flush=True)
            res = _run_single_reality(**w)
            results.append(res)
            if "error" in res:
                print(f"ERROR: {res['error']}")
            else:
                print(f"CAGR={res['cagr']*100:.1f}% Sharpe={res['sharpe']:.2f} OptPnL=${res['options_pnl']:,.0f} ({res['elapsed_s']:.0f}s)")
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(_run_single_reality, **w): w["reality_id"]
                for w in work
            }
            done = 0
            for future in as_completed(futures):
                done += 1
                rid = futures[future]
                try:
                    res = future.result()
                    results.append(res)
                    if "error" in res:
                        print(f"  [{done}/{len(work)}] {rid[:8]} ERROR: {res['error']}")
                    else:
                        print(
                            f"  [{done}/{len(work)}] {rid[:8]} "
                            f"CAGR={res['cagr']*100:.1f}% "
                            f"Sharpe={res['sharpe']:.2f} "
                            f"OptPnL=${res['options_pnl']:,.0f} "
                            f"({res['elapsed_s']:.0f}s)"
                        )
                except Exception as exc:
                    print(f"  [{done}/{len(work)}] {rid[:8]} EXCEPTION: {exc}")
                    results.append({"reality_id": rid, "error": str(exc)})

    elapsed = time.time() - t0

    # Filter successful results
    ok = [r for r in results if "error" not in r]
    errors = [r for r in results if "error" in r]

    # Write aggregate CSV
    out_csv = Path(args.output_dir) / "summary.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    if ok:
        fieldnames = list(ok[0].keys())
        with open(out_csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(ok)

    # Print aggregate stats
    print(f"\n{'='*70}")
    print(f"  Campaign Complete: {len(ok)} succeeded, {len(errors)} failed ({elapsed:.0f}s)")
    print(f"{'='*70}")

    if ok:
        cagrs = [r["cagr"] for r in ok]
        sharpes = [r["sharpe"] for r in ok]
        drawdowns = [r["max_drawdown"] for r in ok]
        opt_pnls = [r["options_pnl"] for r in ok]

        def _pct(values, p):
            s = sorted(values)
            idx = int(len(s) * p / 100.0)
            return s[min(idx, len(s) - 1)]

        print("\n  Combined CAGR (equity + options):")
        print(f"    Mean:   {np.mean(cagrs)*100:>7.2f}%")
        print(f"    Median: {np.median(cagrs)*100:>7.2f}%")
        print(f"    Std:    {np.std(cagrs)*100:>7.2f}%")
        print(f"    P5:     {_pct(cagrs, 5)*100:>7.2f}%")
        print(f"    P95:    {_pct(cagrs, 95)*100:>7.2f}%")

        print("\n  Sharpe Ratio:")
        print(f"    Mean:   {np.mean(sharpes):>7.3f}")
        print(f"    Median: {np.median(sharpes):>7.3f}")
        print(f"    Std:    {np.std(sharpes):>7.3f}")
        print(f"    P5:     {_pct(sharpes, 5):>7.3f}")
        print(f"    P95:    {_pct(sharpes, 95):>7.3f}")

        print("\n  Max Drawdown:")
        print(f"    Mean:   {np.mean(drawdowns)*100:>7.2f}%")
        print(f"    Median: {np.median(drawdowns)*100:>7.2f}%")
        print(f"    Worst:  {min(drawdowns)*100:>7.2f}%")

        print("\n  Options P&L:")
        print(f"    Mean:   ${np.mean(opt_pnls):>12,.0f}")
        print(f"    Median: ${np.median(opt_pnls):>12,.0f}")
        print(f"    Std:    ${np.std(opt_pnls):>12,.0f}")
        n_positive = sum(1 for p in opt_pnls if p > 0)
        print(f"    Positive: {n_positive}/{len(opt_pnls)} ({n_positive/len(opt_pnls)*100:.0f}%)")

        # Per-strategy breakdown
        strat_keys = [k for k in ok[0] if k.startswith("pnl_")]
        if strat_keys:
            print("\n  Per-Strategy Options P&L (median across realities):")
            for sk in sorted(strat_keys, key=lambda k: np.median([r.get(k, 0) for r in ok]), reverse=True):
                vals = [r.get(sk, 0) for r in ok]
                name = sk.replace("pnl_", "")
                n_pos = sum(1 for v in vals if v > 0)
                print(f"    {name:<20s} median=${np.median(vals):>10,.0f}  mean=${np.mean(vals):>10,.0f}  win={n_pos}/{len(vals)}")

    print(f"\n  Results saved to: {out_csv}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
