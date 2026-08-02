"""Run the C++ lambda factorial backtester on real market data.

This is a standalone driver that calls prom2_cpp.run_lambda_factorial_backtests()
directly on the real (non-synthetic) instruments stored in the database,
matching the settings used by the best synthetic campaign configuration
(sa_v2_tilt: conviction + sector_allocator + blended).

Usage:
    PYTHONPATH=cpp/build ./venv/bin/python \
        -m prometheus.scripts.run.run_realistic_cpp_backtest \
        --start 1997-01-02 --end 2026-03-02 \
        --horizons 5 63 \
        --lambda-csv data/cache_ic_v1/lambda_scores/lambda_cluster_scores_smoothed_US_EQ_2015-01-02_2024-12-31_h5-21-63_d4e97aa3071e.csv \
        --cpp-threads 32 --conviction --sector-allocator --blended \
        --blend-weights 0.45 0.55 --portfolio-max-names 50 \
        --persist
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    year, month, day = map(int, value.split("-"))
    return date(year, month, day)


def _load_real_instrument_ids(market_id: str) -> List[str]:
    """Return all non-synthetic instrument IDs for a market."""

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


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run C++ lambda factorial backtest on real market data"
    )

    parser.add_argument("--market-id", type=str, default="US_EQ")
    parser.add_argument("--start", type=_parse_date, required=True)
    parser.add_argument("--end", type=_parse_date, required=True)

    parser.add_argument("--horizons", type=int, nargs="+", default=[5, 63])
    parser.add_argument("--lambda-csv", type=str, required=True)
    parser.add_argument("--lambda-weight", type=float, default=10.0)

    # Lambda-as-risk experiment: when set, lambda is used as a 1/(1+lambda)
    # risk/vol-target denominator (size DOWN in high-dispersion clusters) with
    # selection left on the base score, instead of additive positive alpha.
    parser.add_argument("--lambda-as-risk", action="store_true",
                        help="Use lambda as a 1/(1+lambda) risk denominator (size DOWN "
                             "in high-dispersion clusters) instead of additive alpha. "
                             "Selection stays on the base score.")
    parser.add_argument("--lambda-risk-scale", type=float, default=1.0,
                        help="Strength of the 1/(1+scale*lambda) risk denominator "
                             "(only used with --lambda-as-risk)")

    parser.add_argument("--portfolio-max-names", type=int, default=50)
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--initial-cash", type=float, default=1_000_000.0)

    # Fill convention. "open" = honest next-bar fill: a position decided on
    # close[t] is filled at the NEXT trading day's open (open[t+1]) and earns
    # open[t+1]->close[t+1] onward. "close" = legacy same-bar fill at close[t]
    # (kept only for parity/debug; it has look-ahead and overstates returns).
    parser.add_argument("--execution-price", type=str, default="open",
                        choices=["open", "close"],
                        help="Fill convention: 'open' (next-bar, honest) or 'close' (same-bar, look-ahead)")

    parser.add_argument("--conviction", action="store_true")
    parser.add_argument("--sector-allocator", action="store_true")
    parser.add_argument("--blended", action="store_true")
    parser.add_argument("--blend-weights", type=float, nargs="+", default=None)
    parser.add_argument("--modes", type=str, nargs="+", default=None,
                        help="Sleeve modes (e.g. baseline universe_only sizing_only universe_and_size)")

    parser.add_argument("--score-concentration-power", type=float, default=1.0,
                        help="Score^power before normalising weights (1.0=linear, 2.0=quadratic)")
    parser.add_argument("--min-rebalance-pct", type=float, default=0.0,
                        help="Minimum rebalance threshold (0.02=2%% turnover filter)")

    parser.add_argument("--cpp-threads", type=int, default=32)
    parser.add_argument("--persist", action="store_true")
    parser.add_argument("--persist-execution", action="store_true")
    parser.add_argument("--persist-meta", action="store_true")
    parser.add_argument("--verbose", action="store_true")

    parser.add_argument("--output", type=str, default=None,
                        help="Optional output JSON path for summary results (sleeve metrics only)")
    parser.add_argument("--nav-output", type=str, default=None,
                        help="Optional output JSON path for the per-day equity NAV series "
                             "in the {engine, daily_nav} shape consumed by "
                             "run_options_backtest --equity-backtest. Forces --persist so the "
                             "daily NAV can be read back from backtest_daily_equity.")

    args = parser.parse_args(argv)

    try:
        import prom2_cpp as prom2
    except ImportError:
        raise SystemExit(
            "prom2_cpp not available. Build it and run with PYTHONPATH=cpp/build"
        )

    # Load real instrument IDs.
    instrument_ids = _load_real_instrument_ids(args.market_id)
    logger.info("Loaded %d real instruments for %s", len(instrument_ids), args.market_id)

    horizons = sorted(set(int(h) for h in args.horizons))

    # Exporting a per-day NAV series requires the daily equity curve to be
    # persisted so it can be read back from backtest_daily_equity.
    persist_to_db = args.persist or bool(args.nav_output)
    if args.nav_output and not args.persist:
        logger.info("--nav-output set: enabling --persist so daily NAV can be exported")

    cfg: Dict[str, Any] = {
        "market_id": args.market_id,
        "start": args.start.isoformat(),
        "end": args.end.isoformat(),
        "instrument_ids": instrument_ids,
        "lambda_scores_csv": str(args.lambda_csv),
        "horizons": horizons,
        "lambda_weight": args.lambda_weight,
        "lambda_as_risk": args.lambda_as_risk,
        "lambda_risk_scale": args.lambda_risk_scale,
        "initial_cash": args.initial_cash,
        "execution_price": args.execution_price,
        "apply_risk": True,
        "apply_fragility_overlay": False,
        "slippage_bps": args.slippage_bps,
        "num_threads": args.cpp_threads,
        "verbose": args.verbose,
        "persist_to_db": persist_to_db,
        "persist_execution_to_db": args.persist_execution,
        "persist_meta_to_db": args.persist_meta,
        "conviction_enabled": args.conviction,
        "sector_allocator_enabled": args.sector_allocator,
        "portfolio_max_names": args.portfolio_max_names,
        "run_blended_sleeves": args.blended,
    }
    if args.blend_weights:
        cfg["lambda_blend_weights"] = args.blend_weights
    if args.modes:
        cfg["modes"] = args.modes
    if args.score_concentration_power != 1.0:
        cfg["score_concentration_power"] = args.score_concentration_power
    if args.min_rebalance_pct > 0.0:
        cfg["min_rebalance_pct"] = args.min_rebalance_pct

    logger.info(
        "Running C++ backtest: %s %s→%s horizons=%s conviction=%s SA=%s blended=%s max_names=%d threads=%d",
        args.market_id,
        args.start,
        args.end,
        horizons,
        args.conviction,
        args.sector_allocator,
        args.blended,
        args.portfolio_max_names,
        args.cpp_threads,
    )

    t0 = time.time()
    results = prom2.run_lambda_factorial_backtests(cfg)
    elapsed = time.time() - t0

    # Sort and display.
    results_sorted = sorted(
        results,
        key=lambda r: (int(r.get("horizon", 0)), str(r.get("mode", "")), str(r.get("sleeve_id", ""))),
    )

    print(f"\n{'='*80}")
    print(f"  Realistic C++ Backtest Results  ({elapsed:.1f}s)")
    print(f"  {args.market_id} {args.start} → {args.end}  horizons={horizons}")
    print(f"  conviction={args.conviction}  sector_allocator={args.sector_allocator}  blended={args.blended}")
    print(f"  instruments={len(instrument_ids)}  portfolio_max_names={args.portfolio_max_names}")
    print(f"{'='*80}\n")

    print(f"{'sleeve_id':<50s} {'CAGR':>8s} {'Sharpe':>8s} {'MaxDD':>8s} {'Vol':>8s} {'Days':>6s}")
    print("-" * 90)

    output_rows = []
    for r in results_sorted:
        m = r.get("metrics", r)
        sid = r.get("sleeve_id", "?")
        horizon = r.get("horizon", "?")
        mode = r.get("mode", "?")
        cum_ret = m.get("cumulative_return", 0.0)
        max_dd = m.get("max_drawdown", 0.0)
        ann_vol = m.get("annualised_vol", 0.0)
        ann_sharpe = m.get("annualised_sharpe", 0.0)
        n_days = m.get("n_trading_days", 0)

        # Approximate CAGR from cumulative return and days.
        years = n_days / 252.0 if n_days > 0 else 1.0
        cagr = (1.0 + cum_ret) ** (1.0 / years) - 1.0 if years > 0 and cum_ret > -1.0 else 0.0

        print(f"{sid:<50s} {cagr:>7.2%} {ann_sharpe:>8.3f} {max_dd:>7.2%} {ann_vol:>7.2%} {n_days:>6d}")

        output_rows.append({
            "sleeve_id": sid,
            "horizon": horizon,
            "mode": mode,
            "cagr": round(cagr, 6),
            "annualised_sharpe": round(ann_sharpe, 4),
            "max_drawdown": round(max_dd, 6),
            "annualised_vol": round(ann_vol, 6),
            "cumulative_return": round(cum_ret, 6),
            "n_trading_days": n_days,
        })

    print(f"\n{len(results_sorted)} sleeves completed in {elapsed:.1f}s")

    # Write output JSON if requested.
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "config": {
                "market_id": args.market_id,
                "start": args.start.isoformat(),
                "end": args.end.isoformat(),
                "horizons": horizons,
                "lambda_csv": str(args.lambda_csv),
                "lambda_weight": args.lambda_weight,
                "conviction": args.conviction,
                "sector_allocator": args.sector_allocator,
                "blended": args.blended,
                "blend_weights": args.blend_weights,
                "portfolio_max_names": args.portfolio_max_names,
                "slippage_bps": args.slippage_bps,
                "n_instruments": len(instrument_ids),
            },
            "elapsed_seconds": round(elapsed, 1),
            "results": output_rows,
        }
        out_path.write_text(json.dumps(payload, indent=2))
        print(f"Results written to {out_path}")

    # Export per-day NAV series for the options overlay if requested.
    if args.nav_output:
        _export_daily_nav(results_sorted, args)


def _export_daily_nav(results_sorted: List[Dict[str, Any]], args: Any) -> None:
    """Read the persisted daily equity curve and write the NAV-series JSON.

    Picks the primary sleeve's ``run_id`` (preferring a BLENDED sleeve, else
    the highest-Sharpe sleeve with a run_id) and reads ``backtest_daily_equity``
    for it, writing the ``{engine: "lambda_factorial", daily_nav: {...}}`` shape
    that ``run_options_backtest --equity-backtest`` consumes.
    """
    # Choose the run_id to export.
    candidates = [r for r in results_sorted if r.get("run_id")]
    if not candidates:
        logger.error(
            "No run_id in results — daily NAV cannot be exported. "
            "Ensure the C++ build persisted backtest_daily_equity (persist_to_db=True).",
        )
        return

    # Prefer a BLENDED sleeve (production config); otherwise prefer the
    # canonical BASELINE sleeve (the honest equity-only curve); else fall back
    # to the highest-Sharpe sleeve with a run_id.
    blended = [r for r in candidates if "BLENDED" in str(r.get("sleeve_id", "")).upper()]
    baseline = [r for r in candidates if "BASELINE" in str(r.get("sleeve_id", "")).upper()]
    pool = blended or baseline or candidates
    chosen = max(
        pool,
        key=lambda r: r.get("metrics", r).get("annualised_sharpe", 0.0),
    )
    run_id = chosen.get("run_id")
    logger.info(
        "Exporting daily NAV for sleeve=%s horizon=%s mode=%s run_id=%s",
        chosen.get("sleeve_id"), chosen.get("horizon"), chosen.get("mode"), run_id,
    )

    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """SELECT date, equity_curve_value
                   FROM backtest_daily_equity
                   WHERE run_id = %s
                   ORDER BY date""",
                (run_id,),
            )
            rows = cur.fetchall()
        finally:
            cur.close()

    if not rows:
        logger.error("No backtest_daily_equity rows for run_id=%s — NAV export empty", run_id)
        return

    daily_nav = {str(row[0]): float(row[1]) for row in rows}
    out = Path(args.nav_output)
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "engine": "lambda_factorial",
        "source_run_id": run_id,
        "source_sleeve_id": chosen.get("sleeve_id"),
        "execution_price": args.execution_price,
        "start": args.start.isoformat(),
        "end": args.end.isoformat(),
        "daily_nav": daily_nav,
    }
    out.write_text(json.dumps(payload, indent=2))
    print(
        f"Daily NAV series written to {out} "
        f"({len(daily_nav)} points, run_id={run_id}, execution_price={args.execution_price})"
    )


if __name__ == "__main__":
    main()
