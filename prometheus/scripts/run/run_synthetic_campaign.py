"""Run a synthetic-reality campaign for out-of-sample backtester validation.

This script generates N synthetic market realities via block bootstrap,
writes each to the database, runs the C++ lambda factorial backtester on
each, aggregates results, and cleans up.

Modes:
  (default)        Generate realities, backtest, clean up.
  --generate-only  Generate + persist realities and write manifest.json.
  --backtest-only  Load manifest.json and run backtests (no generation).

Typical usage:

  # Full pipeline (generate + backtest + cleanup):
  PYTHONPATH=cpp/build python -m prometheus.scripts.run.run_synthetic_campaign \
    --name OOS_100x25yr --num-realities 100 --horizon-days 6300 \
    --base-start 1997-01-02 --base-end 2024-12-31 --seed 42 \
    --lambda-csv data/cache_ic_v1/lambda_scores/...csv \
    --cpp-threads 16 --conviction --sector-allocator

  # Generate only (persist to DB):
  PYTHONPATH=cpp/build python -m prometheus.scripts.run.run_synthetic_campaign \
    --name OOS_100x25yr --generate-only --num-realities 100 --horizon-days 6300 \
    --base-start 1997-01-02 --base-end 2024-12-31 --seed 42 \
    --lambda-csv data/cache_ic_v1/lambda_scores/...csv --gen-workers 0

  # Backtest only (against persisted realities):
  PYTHONPATH=cpp/build python -m prometheus.scripts.run.run_synthetic_campaign \
    --name OOS_100x25yr --backtest-only --run-tag zero_cost \
    --lambda-csv data/cache_ic_v1/lambda_scores/...csv \
    --cpp-threads 16 --slippage-bps 0
"""

from __future__ import annotations

import argparse
import json
import multiprocessing
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd
from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger
from apatheon.data.reader import DataReader

from prometheus.synthetic import (
    RealityConfig,
    ScenarioStorage,
    SyntheticScenarioEngine,
)

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    year, month, day = map(int, value.split("-"))
    return date(year, month, day)


@dataclass
class SleeveMetrics:
    sleeve_id: str
    cumulative_return: float
    max_drawdown: float
    annualised_vol: float
    annualised_sharpe: float
    n_trading_days: int


@dataclass
class RealityResult:
    reality_id: str
    elapsed_s: float
    sleeves: List[SleeveMetrics] = field(default_factory=list)


def _run_cpp_backtest(
    *,
    instrument_ids: List[str],
    market_id: str,
    start: str,
    end: str,
    lambda_csv_path: Optional[str],
    horizons: List[int],
    lambda_weight: float,
    cpp_threads: int,
    conviction_enabled: bool,
    sector_allocator_enabled: bool,
    slippage_bps: float = 5.0,
    portfolio_max_names: int = 0,
    run_blended_sleeves: bool = False,
    lambda_blend_weights: Optional[List[float]] = None,
    modes: Optional[List[str]] = None,
    sector_tilt_strength: Optional[float] = None,
    sector_max_tilt: Optional[float] = None,
    sector_shi_ema_span: Optional[int] = None,
    sector_hard_kill_on: Optional[float] = None,
    sector_hard_kill_off: Optional[float] = None,
    sector_max_concentration: Optional[float] = None,
    execution_price: Optional[str] = None,
    price_basis: Optional[str] = None,
    verbose: bool = False,
) -> List[SleeveMetrics]:
    """Invoke the C++ backtester on a set of instrument_ids already in the DB."""

    try:
        import prom2_cpp as prom2
    except ImportError:
        logger.error(
            "Cannot import prom2_cpp; ensure PYTHONPATH includes cpp/build. "
            "Run: PYTHONPATH=cpp/build python -m ..."
        )
        raise

    cfg: Dict[str, Any] = {
        "market_id": market_id,
        "start": start,
        "end": end,
        "instrument_ids": instrument_ids,
        "lambda_scores_csv": lambda_csv_path or "",
        "horizons": horizons,
        "lambda_weight": lambda_weight,
        "initial_cash": 1_000_000.0,
        "apply_risk": True,
        "apply_fragility_overlay": False,
        "slippage_bps": slippage_bps,
        "num_threads": cpp_threads,
        "verbose": verbose,
        "persist_to_db": False,
        "persist_execution_to_db": False,
        "persist_meta_to_db": False,
        "conviction_enabled": conviction_enabled,
        "sector_allocator_enabled": sector_allocator_enabled,
        "portfolio_max_names": portfolio_max_names,
        "run_blended_sleeves": run_blended_sleeves,
    }
    if lambda_blend_weights:
        cfg["lambda_blend_weights"] = lambda_blend_weights
    if modes:
        cfg["modes"] = modes
    # Sector allocator v2 params (only set if explicitly provided).
    if sector_tilt_strength is not None:
        cfg["sector_tilt_strength"] = sector_tilt_strength
    if sector_max_tilt is not None:
        cfg["sector_max_tilt"] = sector_max_tilt
    if sector_shi_ema_span is not None:
        cfg["sector_shi_ema_span"] = sector_shi_ema_span
    if sector_hard_kill_on is not None:
        cfg["sector_hard_kill_on"] = sector_hard_kill_on
    if sector_hard_kill_off is not None:
        cfg["sector_hard_kill_off"] = sector_hard_kill_off
    if sector_max_concentration is not None:
        cfg["sector_max_concentration"] = sector_max_concentration
    # Execution model overrides (omit to use C++ defaults: open + adjusted).
    if execution_price is not None:
        cfg["execution_price"] = execution_price
    if price_basis is not None:
        cfg["price_basis"] = price_basis

    results = prom2.run_lambda_factorial_backtests(cfg)

    sleeves: List[SleeveMetrics] = []
    for r in results:
        m = r.get("metrics", r)  # metrics may be nested or flat
        sleeves.append(SleeveMetrics(
            sleeve_id=r["sleeve_id"],
            cumulative_return=m["cumulative_return"],
            max_drawdown=m["max_drawdown"],
            annualised_vol=m["annualised_vol"],
            annualised_sharpe=m["annualised_sharpe"],
            n_trading_days=m["n_trading_days"],
        ))

    return sleeves


def _reality_to_manifest_entry(reality, lambda_csv_path: Optional[str]) -> Dict[str, Any]:
    """Extract the fields needed for the manifest from a SyntheticReality."""

    if reality.trade_dates is not None:
        dates = sorted(reality.trade_dates)
    else:
        dates = sorted(reality.prices_df["trade_date"].unique())

    start_idx = min(252, len(dates) - 10)
    start_date = dates[start_idx]
    end_date = dates[-1]

    synth_instrument_ids = [
        iid for iid in reality.instrument_ids
        if iid not in reality.sector_etf_ids
    ]

    return {
        "reality_id": reality.reality_id,
        "instrument_ids": synth_instrument_ids,
        "market_id": reality.config.markets[0] if reality.config.markets else "US_EQ",
        "start_date": str(start_date),
        "end_date": str(end_date),
        "lambda_csv_path": lambda_csv_path,
        "n_trade_dates": len(dates),
    }


def _bt_worker(args_tuple: tuple) -> tuple:
    """Run a single backtest in a spawned worker process."""
    entry, bt_kwargs, lambda_csv_override = args_tuple
    lcsv = lambda_csv_override or entry.get("lambda_csv_path")
    sleeves = _run_cpp_backtest(
        instrument_ids=entry["instrument_ids"],
        market_id=entry["market_id"],
        start=entry["start_date"],
        end=entry["end_date"],
        lambda_csv_path=lcsv,
        **bt_kwargs,
    )
    return entry["reality_id"], sleeves


def _aggregate_results(
    all_results: List[RealityResult],
    results_dir: Path,
) -> None:
    """Compute and write summary + detail CSVs."""

    print("\n" + "=" * 60)
    print("AGGREGATE RESULTS")
    print("=" * 60)

    sleeve_stats: Dict[str, List[Dict[str, float]]] = {}
    for rr in all_results:
        for s in rr.sleeves:
            if s.sleeve_id not in sleeve_stats:
                sleeve_stats[s.sleeve_id] = []
            cagr = (1 + s.cumulative_return) ** (252.0 / max(s.n_trading_days, 1)) - 1
            sleeve_stats[s.sleeve_id].append({
                "cagr": cagr,
                "sharpe": s.annualised_sharpe,
                "max_dd": s.max_drawdown,
                "vol": s.annualised_vol,
                "cum_ret": s.cumulative_return,
            })

    summary_rows: List[Dict[str, Any]] = []
    for sleeve_id in sorted(sleeve_stats.keys()):
        entries = sleeve_stats[sleeve_id]
        n = len(entries)
        cagrs = [e["cagr"] for e in entries]
        sharpes = [e["sharpe"] for e in entries]
        max_dds = [e["max_dd"] for e in entries]

        row = {
            "sleeve_id": sleeve_id,
            "n_realities": n,
            "cagr_mean": np.mean(cagrs),
            "cagr_std": np.std(cagrs),
            "cagr_median": np.median(cagrs),
            "sharpe_mean": np.mean(sharpes),
            "sharpe_std": np.std(sharpes),
            "sharpe_median": np.median(sharpes),
            "max_dd_mean": np.mean(max_dds),
            "max_dd_std": np.std(max_dds),
        }
        summary_rows.append(row)

        print(f"\n{sleeve_id} (n={n}):")
        print(f"  CAGR:   {row['cagr_mean']:+.2%} ± {row['cagr_std']:.2%}  (median {row['cagr_median']:+.2%})")
        print(f"  Sharpe: {row['sharpe_mean']:.3f} ± {row['sharpe_std']:.3f}  (median {row['sharpe_median']:.3f})")
        print(f"  MaxDD:  {row['max_dd_mean']:+.2%} ± {row['max_dd_std']:.2%}")

    summary_csv = results_dir / "summary.csv"
    if summary_rows:
        pd.DataFrame(summary_rows).to_csv(summary_csv, index=False)
        print(f"\nSummary written to {summary_csv}")

    detail_rows: List[Dict[str, Any]] = []
    for rr in all_results:
        for s in rr.sleeves:
            cagr = (1 + s.cumulative_return) ** (252.0 / max(s.n_trading_days, 1)) - 1
            detail_rows.append({
                "reality_id": rr.reality_id,
                "sleeve_id": s.sleeve_id,
                "cagr": cagr,
                "sharpe": s.annualised_sharpe,
                "max_dd": s.max_drawdown,
                "vol": s.annualised_vol,
                "cum_ret": s.cumulative_return,
                "n_days": s.n_trading_days,
            })

    detail_csv = results_dir / "detail.csv"
    if detail_rows:
        pd.DataFrame(detail_rows).to_csv(detail_csv, index=False)
        print(f"Detail written to {detail_csv}")


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic realities and run factorial backtests"
    )

    # Mode flags.
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--generate-only", action="store_true",
                            help="Generate + persist realities, write manifest, skip backtesting")
    mode_group.add_argument("--backtest-only", action="store_true",
                            help="Load manifest and run backtests against persisted realities")

    parser.add_argument("--name", type=str, default="SYNTH_CAMPAIGN", help="Campaign name")
    parser.add_argument("--run-tag", type=str, default=None,
                        help="Sub-directory tag for backtest-only results (default: timestamp)")

    # Generation args (ignored in --backtest-only mode).
    parser.add_argument("--num-realities", type=int, default=10)
    parser.add_argument("--horizon-days", type=int, default=1260, help="Trading days per reality (~5 years)")
    parser.add_argument("--block-length", type=int, default=20)
    parser.add_argument("--base-start", type=_parse_date, default=None)
    parser.add_argument("--base-end", type=_parse_date, default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--lambda-mode", type=str, default="passthrough",
                        choices=["passthrough", "noisy", "shuffle", "none"])
    parser.add_argument("--lambda-noise-std", type=float, default=0.5)
    parser.add_argument("--gen-workers", type=int, default=0,
                        help="Parallel workers for reality generation (0=all cores)")

    # Backtest args.
    parser.add_argument("--bt-workers", type=int, default=1,
                        help="Parallel backtest workers (default: 1 = serial)")
    parser.add_argument("--lambda-csv", type=str, default=None)
    parser.add_argument("--horizons", type=int, nargs="+", default=[5, 21, 63])
    parser.add_argument("--lambda-weight", type=float, default=10.0)
    parser.add_argument("--cpp-threads", type=int, default=8)
    parser.add_argument("--conviction", action="store_true")
    parser.add_argument("--sector-allocator", action="store_true")
    parser.add_argument("--slippage-bps", type=float, default=5.0,
                        help="Slippage in basis points (default: 5.0)")
    parser.add_argument("--portfolio-max-names", type=int, default=0,
                        help="Max portfolio names per sleeve (0=unlimited)")
    parser.add_argument("--blended", action="store_true",
                        help="Enable horizon-blended sleeves")
    parser.add_argument("--blend-weights", type=float, nargs="+", default=None,
                        help="Blend weights per horizon (e.g. 0.4 0.1 0.5)")
    parser.add_argument("--modes", type=str, nargs="+", default=None,
                        help="Sleeve modes to run (e.g. baseline universe_only)")

    # Sector allocator v2 tuning.
    parser.add_argument("--sector-tilt-strength", type=float, default=None,
                        help="SA v2 tilt sensitivity (default: 1.5)")
    parser.add_argument("--sector-max-tilt", type=float, default=None,
                        help="SA v2 max absolute tilt (default: 0.40)")
    parser.add_argument("--sector-shi-ema-span", type=int, default=None,
                        help="SA v2 SHI EMA smoothing span (default: 21)")
    parser.add_argument("--sector-hard-kill-on", type=float, default=None,
                        help="SA v2 hard-kill threshold (default: 0.10)")
    parser.add_argument("--sector-hard-kill-off", type=float, default=None,
                        help="SA v2 hard-kill recovery threshold (default: 0.20)")
    parser.add_argument("--sector-max-concentration", type=float, default=None,
                        help="SA v2 per-sector concentration cap (default: 0.30)")

    # Execution model (omit to use C++ defaults: open execution, adjusted prices).
    parser.add_argument("--execution-price", type=str, choices=["open", "close"], default=None,
                        help="Override execution fill price (default: C++ default = 'open')")
    parser.add_argument("--price-basis", type=str, choices=["raw", "adjusted"], default=None,
                        help="Override price basis (default: C++ default = 'adjusted')")

    # Output / misc.
    parser.add_argument("--output-dir", type=str, default="results/synthetic_campaigns")
    parser.add_argument("--keep-data", action="store_true",
                        help="Do not clean up synthetic DB data after run")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args(argv)

    # Validation.
    if not args.backtest_only and (args.base_start is None or args.base_end is None):
        parser.error("--base-start and --base-end are required unless --backtest-only")

    output_dir = Path(args.output_dir) / args.name
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "manifest.json"

    t0 = time.time()

    # ── BACKTEST-ONLY MODE ─────────────────────────────────────────────
    if args.backtest_only:
        if not manifest_path.exists():
            print(f"ERROR: manifest not found at {manifest_path}")
            print("Run --generate-only first to create it.")
            sys.exit(1)

        with open(manifest_path) as f:
            manifest = json.load(f)

        entries = manifest["realities"]
        print(f"Loaded manifest: {len(entries)} realities")

        # Results sub-directory.
        run_tag = args.run_tag or time.strftime("%Y%m%d_%H%M%S")
        results_dir = output_dir / "runs" / run_tag
        results_dir.mkdir(parents=True, exist_ok=True)

        # Save run config for reproducibility.
        run_config = {
            "run_tag": run_tag,
            "horizons": args.horizons,
            "lambda_weight": args.lambda_weight,
            "lambda_csv": args.lambda_csv,
            "slippage_bps": args.slippage_bps,
            "conviction": args.conviction,
            "sector_allocator": args.sector_allocator,
            "portfolio_max_names": args.portfolio_max_names,
            "blended": args.blended,
            "blend_weights": args.blend_weights,
            "modes": args.modes,
            "execution_price": args.execution_price,
            "price_basis": args.price_basis,
        }
        with open(results_dir / "run_config.json", "w") as f:
            json.dump(run_config, f, indent=2)

        all_results: List[RealityResult] = []
        n_workers = max(1, args.bt_workers)
        cpp_threads_per = max(1, args.cpp_threads // n_workers)

        # Common kwargs for every backtest.
        bt_kwargs = dict(
            horizons=args.horizons,
            lambda_weight=args.lambda_weight,
            cpp_threads=cpp_threads_per,
            conviction_enabled=args.conviction,
            sector_allocator_enabled=args.sector_allocator,
            slippage_bps=args.slippage_bps,
            portfolio_max_names=args.portfolio_max_names,
            run_blended_sleeves=args.blended,
            lambda_blend_weights=args.blend_weights,
            modes=args.modes,
            sector_tilt_strength=args.sector_tilt_strength,
            sector_max_tilt=args.sector_max_tilt,
            sector_shi_ema_span=args.sector_shi_ema_span,
            sector_hard_kill_on=args.sector_hard_kill_on,
            sector_hard_kill_off=args.sector_hard_kill_off,
            sector_max_concentration=args.sector_max_concentration,
            execution_price=args.execution_price,
            price_basis=args.price_basis,
            verbose=False,  # suppress per-worker verbose in parallel
        )

        if n_workers > 1:
            print(f"Running {len(entries)} backtests with {n_workers} parallel workers "
                  f"({cpp_threads_per} C++ threads each)")

            done = 0
            ctx = multiprocessing.get_context("spawn")
            work_items = [(e, bt_kwargs, args.lambda_csv) for e in entries]
            with ProcessPoolExecutor(max_workers=n_workers, mp_context=ctx) as pool:
                futs = {pool.submit(_bt_worker, item): item[0]["reality_id"] for item in work_items}
                for fut in as_completed(futs):
                    done += 1
                    rid = futs[fut]
                    try:
                        rid_out, sleeves = fut.result()
                    except Exception:
                        logger.exception("Backtest failed for reality %s", rid)
                        sleeves = []
                        rid_out = rid
                    all_results.append(RealityResult(
                        reality_id=rid_out, elapsed_s=0.0, sleeves=sleeves,
                    ))
                    if done % 10 == 0 or done == len(entries):
                        elapsed = time.time() - t0
                        print(f"  [{done}/{len(entries)}] completed  ({elapsed:.0f}s elapsed)")
        else:
            for i, entry in enumerate(entries):
                rid = entry["reality_id"]
                lambda_csv = args.lambda_csv or entry.get("lambda_csv_path")

                t_bt = time.time()
                try:
                    sleeves = _run_cpp_backtest(
                        instrument_ids=entry["instrument_ids"],
                        market_id=entry["market_id"],
                        start=entry["start_date"],
                        end=entry["end_date"],
                        lambda_csv_path=lambda_csv,
                        **bt_kwargs,
                    )
                except Exception:
                    logger.exception("Backtest failed for reality %s", rid)
                    sleeves = []

                bt_time = time.time() - t_bt
                if (i + 1) % 10 == 0 or i == 0:
                    print(f"  [{i+1}/{len(entries)}] {rid[:12]}: {len(sleeves)} sleeves in {bt_time:.1f}s")

                all_results.append(RealityResult(
                    reality_id=rid, elapsed_s=bt_time, sleeves=sleeves,
                ))

        _aggregate_results(all_results, results_dir)
        total_time = time.time() - t0
        print(f"\nTotal backtest-only time: {total_time:.1f}s")
        return

    # ── GENERATION (full or generate-only) ─────────────────────────────
    db_manager = get_db_manager()
    data_reader = DataReader(db_manager=db_manager)
    engine = SyntheticScenarioEngine(db_manager=db_manager, data_reader=data_reader)
    storage = ScenarioStorage(db_manager=db_manager)

    config = RealityConfig(
        name=args.name,
        category="BLOCK_BOOTSTRAP",
        horizon_days=args.horizon_days,
        num_realities=args.num_realities,
        block_length=args.block_length,
        markets=["US_EQ"],
        base_date_start=args.base_start,
        base_date_end=args.base_end,
        seed=args.seed,
        include_fragility=True,
        lambda_mode=args.lambda_mode,
        lambda_noise_std=args.lambda_noise_std,
        lambda_csv_path=args.lambda_csv,
    )

    print(f"Generating {args.num_realities} synthetic realities...")
    realities = engine.generate_realities(config, max_workers=args.gen_workers)
    gen_time = time.time() - t0
    print(f"  Generated {len(realities)} realities in {gen_time:.1f}s")

    # Try to import prom2 for C++ DB writer.
    try:
        import prom2_cpp as prom2_mod
        has_cpp = True
    except ImportError:
        prom2_mod = None
        has_cpp = False

    manifest_entries: List[Dict[str, Any]] = []
    all_results: List[RealityResult] = []

    for i, reality in enumerate(realities):
        print(f"\n── Reality {i+1}/{len(realities)}: {reality.reality_id[:12]} ──")

        # Write prices + instruments + fragility to DB.
        t_write = time.time()

        if reality.cpp_arrays is not None and has_cpp:
            prefix = f"SYNTH_{reality.reality_id[:8]}"
            prom2_mod.write_reality_to_db({
                "close": reality.cpp_arrays["close"],
                "open": reality.cpp_arrays["open"],
                "high": reality.cpp_arrays["high"],
                "low": reality.cpp_arrays["low"],
                "volume": reality.cpp_arrays["volume"],
                "fragility": reality.cpp_arrays["fragility"],
                "prefix": prefix,
                "market_id": reality.config.markets[0] if reality.config.markets else "US_EQ",
                "reality_id": reality.reality_id,
                "real_instrument_ids": reality.panel_ids,
                "sectors": reality.panel_sectors,
                "trade_dates": reality.trade_dates_int,
            })
        else:
            storage.write_reality(reality)

        # Write lambda CSV.
        lambda_csv_path = None
        if reality.lambda_df is not None and not reality.lambda_df.empty:
            lambda_csv_path = str(storage.write_reality_lambda_csv(
                reality, output_dir / "lambda",
            ))
        elif args.lambda_csv and args.lambda_mode == "none":
            lambda_csv_path = args.lambda_csv

        write_time = time.time() - t_write
        print(f"  Written to DB in {write_time:.1f}s")

        # Build manifest entry.
        manifest_entries.append(_reality_to_manifest_entry(reality, lambda_csv_path))

        if args.generate_only:
            continue

        # Run C++ backtest.
        entry = manifest_entries[-1]
        t_bt = time.time()
        try:
            sleeves = _run_cpp_backtest(
                instrument_ids=entry["instrument_ids"],
                market_id=entry["market_id"],
                start=entry["start_date"],
                end=entry["end_date"],
                lambda_csv_path=lambda_csv_path,
                horizons=args.horizons,
                lambda_weight=args.lambda_weight,
                cpp_threads=args.cpp_threads,
                conviction_enabled=args.conviction,
                sector_allocator_enabled=args.sector_allocator,
                slippage_bps=args.slippage_bps,
                portfolio_max_names=args.portfolio_max_names,
                run_blended_sleeves=args.blended,
                lambda_blend_weights=args.blend_weights,
                modes=args.modes,
                sector_tilt_strength=args.sector_tilt_strength,
                sector_max_tilt=args.sector_max_tilt,
                sector_shi_ema_span=args.sector_shi_ema_span,
                sector_hard_kill_on=args.sector_hard_kill_on,
                sector_hard_kill_off=args.sector_hard_kill_off,
                sector_max_concentration=args.sector_max_concentration,
                execution_price=args.execution_price,
                price_basis=args.price_basis,
                verbose=args.verbose,
            )
        except Exception:
            logger.exception("Backtest failed for reality %s", reality.reality_id)
            sleeves = []

        bt_time = time.time() - t_bt
        print(f"  Backtest: {len(sleeves)} sleeves in {bt_time:.1f}s")

        for s in sleeves:
            cagr = (1 + s.cumulative_return) ** (252.0 / max(s.n_trading_days, 1)) - 1
            print(f"    {s.sleeve_id}: CAGR={cagr:+.1%} Sharpe={s.annualised_sharpe:.3f} MaxDD={s.max_drawdown:+.1%}")

        all_results.append(RealityResult(
            reality_id=reality.reality_id,
            elapsed_s=bt_time,
            sleeves=sleeves,
        ))

        # Cleanup synthetic data unless --keep-data or --generate-only.
        if not args.keep_data:
            storage.cleanup_reality(reality.reality_id)

    # ── Write manifest ─────────────────────────────────────────────────
    manifest = {"realities": manifest_entries}
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"\nManifest written to {manifest_path} ({len(manifest_entries)} realities)")

    if args.generate_only:
        total_time = time.time() - t0
        print(f"\nGenerate-only complete in {total_time:.1f}s")
        return

    # ── Aggregate results ──────────────────────────────────────────────
    _aggregate_results(all_results, output_dir)
    total_time = time.time() - t0
    print(f"\nTotal campaign time: {total_time:.1f}s")


if __name__ == "__main__":
    main()
