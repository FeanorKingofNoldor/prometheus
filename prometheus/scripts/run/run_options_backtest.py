"""Run the synthetic options backtester.

Overlays all 15 options strategies on top of a pre-computed equity
backtest, using synthetic option chains, VIX-derived IV surfaces,
and Black-Scholes pricing.

Usage::

    # With equity backtest overlay:
    ./venv/bin/python -m prometheus.scripts.run.run_options_backtest \
        --start 1997-01-02 --end 2026-03-02 \
        --equity-backtest results/realistic_backtest/best_run.json \
        --derivatives-budget 0.15 \
        --output results/options_backtest/full_run.json

    # Standalone (flat equity NAV):
    ./venv/bin/python -m prometheus.scripts.run.run_options_backtest \
        --start 2020-01-02 --end 2026-03-02 \
        --output results/options_backtest/standalone.json
"""

from __future__ import annotations

import argparse
import time
from datetime import date
from typing import Optional, Sequence

from apathis.core.logging import get_logger

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    year, month, day = map(int, value.split("-"))
    return date(year, month, day)


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run synthetic options backtester over historical data",
    )

    parser.add_argument("--start", type=_parse_date, default="1997-01-02",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=_parse_date, default="2026-03-02",
                        help="End date (YYYY-MM-DD)")
    parser.add_argument("--initial-nav", type=float, default=1_000_000.0,
                        help="Initial portfolio NAV")
    parser.add_argument("--derivatives-budget", type=float, default=0.15,
                        help="Derivatives capital as fraction of NAV (0.15 = 15%%)")
    parser.add_argument("--equity-backtest", type=str, default=None,
                        help="Path to equity backtest results JSON")
    parser.add_argument("--slippage", type=float, default=0.25,
                        help="Slippage as fraction of half-spread (0.25 = 25%%)")
    parser.add_argument("--max-positions", type=int, default=100,
                        help="Maximum simultaneous option positions")
    parser.add_argument("--log-frequency", type=int, default=63,
                        help="Log every N trading days")
    parser.add_argument("--output", type=str, default=None,
                        help="Output JSON path")
    parser.add_argument("--use-db", action="store_true",
                        help="Load VIX/prices from database (requires DB connection)")
    parser.add_argument("--equity-universe", type=str, default=None,
                        help="Comma-separated instrument IDs to load for the options universe "
                             "(e.g. AAPL.US,MSFT.US,GOOGL.US). Requires --use-db.")
    parser.add_argument("--load-equity-universe", action="store_true",
                        help="Auto-load the full US_EQ equity universe from the runtime DB. "
                             "Gives short_put meaningful single-stock underlyings. Requires --use-db.")
    parser.add_argument("--persist", action="store_true",
                        help="Persist trades, daily positions, and summary to runtime DB")
    parser.add_argument("--lambda-csv", type=str, default=None,
                        help="Enable real λ-factorial scores for short_put / bull_call_spread "
                             "stock selection (requires --use-db). When set, per-instrument "
                             "scores are loaded from the instrument_scores DB table "
                             "(strategy US_CORE_LONG_EQ, horizon 21). The path argument is "
                             "used for logging/documentation only (not read as a CSV).")

    args = parser.parse_args(argv)

    from prometheus.backtest.options_backtest import (
        OptionsBacktestConfig,
        OptionsBacktestEngine,
    )

    equity_universe_ids = []
    if args.equity_universe:
        equity_universe_ids = [
            x.strip() for x in args.equity_universe.split(",") if x.strip()
        ]

    config = OptionsBacktestConfig(
        start_date=args.start,
        end_date=args.end,
        initial_nav=args.initial_nav,
        derivatives_budget_pct=args.derivatives_budget,
        equity_backtest_path=args.equity_backtest,
        slippage_pct=args.slippage,
        max_position_count=args.max_positions,
        log_every_n_days=args.log_frequency,
        equity_universe_ids=equity_universe_ids,
        load_equity_universe_from_db=args.load_equity_universe,
        lambda_csv_path=args.lambda_csv,
    )

    # Optionally connect to database for historical data
    data_reader = None
    if args.use_db:
        try:
            from apathis.core.database import get_db_manager
            from apathis.data.reader import DataReader
            db = get_db_manager()
            data_reader = DataReader(db_manager=db)
            logger.info("Connected to database for historical data")
        except Exception as exc:
            logger.warning("Could not connect to database: %s — using fallbacks", exc)

    # Set up persistence writer if requested
    writer = None
    if args.persist:
        try:
            from apathis.core.database import get_db_manager

            from prometheus.backtest.backtest_options_writer import (
                BacktestOptionsWriter,
                generate_run_id,
            )
            db = get_db_manager()
            run_id = generate_run_id()
            writer = BacktestOptionsWriter(db_manager=db, run_id=run_id)
            logger.info("Persistence enabled: run_id=%s", run_id)
        except Exception as exc:
            logger.warning("Could not set up persistence: %s — running without", exc)

    # Run backtest
    print(f"\n{'='*70}")
    print("  Synthetic Options Backtest")
    print(f"  {args.start} → {args.end}")
    print(f"  Initial NAV: ${args.initial_nav:,.0f}")
    print(f"  Derivatives Budget: {args.derivatives_budget*100:.0f}% of NAV")
    if args.equity_backtest:
        print(f"  Equity Backtest: {args.equity_backtest}")
    print(f"  Slippage: {args.slippage*100:.0f}% of half-spread")
    if equity_universe_ids:
        print(f"  Equity Universe: {len(equity_universe_ids)} explicit instruments")
    if args.load_equity_universe:
        print("  Equity Universe: auto-loading US_EQ from DB (gives short_put real underlyings)")
    if args.lambda_csv:
        print(f"  λ-factorial scores: {args.lambda_csv}")
    if writer:
        print(f"  Persisting to DB: run_id={writer.run_id}")
    print(f"{'='*70}\n")

    t0 = time.time()
    engine = OptionsBacktestEngine(config, data_reader=data_reader, writer=writer)
    result = engine.run()
    elapsed = time.time() - t0

    # Display results
    summary = result.summary
    print(f"\n{'='*70}")
    print(f"  Results  ({elapsed:.1f}s)")
    print(f"{'='*70}")
    print(f"  Period:          {summary.get('start_date')} → {summary.get('end_date')}")
    print(f"  Trading Days:    {summary.get('n_trading_days', 0):,}")
    print(f"  Years:           {summary.get('years', 0):.1f}")
    print()
    print("  Combined (Equity + Options):")
    print(f"    CAGR:          {summary.get('cagr', 0)*100:>7.2f}%")
    print(f"    Sharpe:        {summary.get('sharpe', 0):>7.3f}")
    print(f"    Max Drawdown:  {summary.get('max_drawdown', 0)*100:>7.2f}%")
    print(f"    Ann. Vol:      {summary.get('annualised_vol', 0)*100:>7.2f}%")
    print(f"    Final NAV:     ${summary.get('final_nav', 0):>12,.0f}")
    print()
    print("  Equity Only:")
    print(f"    CAGR:          {summary.get('equity_only_cagr', 0)*100:>7.2f}%")
    print()
    print("  Options Overlay:")
    print(f"    Total P&L:     ${summary.get('options_total_pnl', 0):>12,.0f}")
    print(f"    P&L % of NAV:  {summary.get('options_pnl_pct', 0)*100:>7.2f}%")
    print(f"{'='*70}\n")

    # Save results
    if args.output:
        result.to_json(args.output)
        print(f"Results saved to {args.output}")
    else:
        # Default output path
        out_path = f"results/options_backtest/options_{args.start}_{args.end}.json"
        result.to_json(out_path)
        print(f"Results saved to {out_path}")


if __name__ == "__main__":
    main()
