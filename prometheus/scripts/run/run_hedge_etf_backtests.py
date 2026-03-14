"""Run fast C++ backtests for Hedge ETF sleeves.

This script runs the C++ hedge ETF backtest engine over the sleeves
defined in the book registry (configs/meta/books.yaml) for a given
HEDGE_ETF book.

Typical usage (C++ backend):

  PYTHONPATH=cpp/build ./venv/bin/python -m prometheus.scripts.run.run_hedge_etf_backtests \
    --book-id US_EQ_HEDGE_ETF \
    --start 2015-01-01 --end 2024-12-31 \
    --cpp-threads 16

Notes:
- Default execution model matches the C++ lambda runner:
  - execute at open[t+1]
  - adjusted price basis
  - example costs: 5 bps slippage + 0.005/share commission with $1 min and 1% max
- Results can optionally be persisted to runtime DB.
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Optional, Sequence

from prometheus.books.registry import BookKind, HedgeEtfSleeveSpec, load_book_registry
from apathis.core.logging import get_logger


logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Run C++ hedge ETF sleeve backtests")

    parser.add_argument(
        "--book-id",
        type=str,
        default="US_EQ_HEDGE_ETF",
        help="Book id from configs/meta/books.yaml (default: US_EQ_HEDGE_ETF)",
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--sleeve-id",
        type=str,
        default=None,
        help="Optional sleeve_id filter (run only this sleeve)",
    )

    # C++ controls.
    parser.add_argument("--cpp-threads", type=int, default=0, help="Threads for C++ backend (0=auto).")
    parser.add_argument("--cpp-verbose", action="store_true", help="Enable progress output from C++ runner.")

    parser.add_argument("--cpp-persist", action="store_true", help="Persist to runtime DB (backtest_runs + backtest_daily_equity).")
    parser.add_argument("--cpp-persist-execution", action="store_true", help="Also persist execution artifacts (orders/fills/snapshots).")
    parser.add_argument("--cpp-persist-meta", action="store_true", help="Also persist engine_decisions + decision_outcomes.")

    parser.add_argument(
        "--cpp-execution-price",
        type=str,
        default="open",
        choices=["open", "close"],
        help="Execution price for rebalances (open or close). Default: open.",
    )
    parser.add_argument(
        "--cpp-price-basis",
        type=str,
        default="adjusted",
        choices=["raw", "adjusted"],
        help="Price basis (raw or adjusted). Default: adjusted.",
    )
    parser.add_argument("--cpp-slippage-bps", type=float, default=5.0, help="Slippage in bps (default: 5).")

    parser.add_argument("--cpp-commission-per-share", type=float, default=0.005)
    parser.add_argument("--cpp-commission-min-per-order", type=float, default=1.0)
    parser.add_argument("--cpp-commission-max-pct-trade-value", type=float, default=0.01)
    parser.add_argument("--cpp-finra-taf-per-share", type=float, default=0.0)
    parser.add_argument("--cpp-finra-taf-max-per-order", type=float, default=0.0)
    parser.add_argument("--cpp-sec-fee-rate", type=float, default=0.0)

    args = parser.parse_args(argv)

    try:
        import prom2_cpp  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise SystemExit(
            "prom2_cpp not available. Build it (./cpp/scripts/build.sh) and run with PYTHONPATH=cpp/build"
        ) from exc

    registry = load_book_registry()
    book = registry.get(str(args.book_id))
    if book is None:
        raise SystemExit(f"Unknown book_id={args.book_id!r}; check configs/meta/books.yaml")
    if book.kind != BookKind.HEDGE_ETF:
        raise SystemExit(f"book_id={args.book_id!r} is kind={book.kind}, expected HEDGE_ETF")

    sleeves = []
    for sid, spec in book.sleeves.items():
        if not isinstance(spec, HedgeEtfSleeveSpec):
            continue
        if args.sleeve_id is not None and sid != args.sleeve_id:
            continue

        sleeves.append(
            {
                "sleeve_id": str(spec.sleeve_id),
                "instrument_ids": list(spec.instrument_ids),
                "sizing_mode": str(spec.sizing_mode),
                "max_hedge_allocation": float(spec.max_hedge_allocation),
                "fragility_threshold": float(spec.fragility_threshold),
                "rebalance_frequency": str(spec.rebalance_frequency),
            }
        )

    if not sleeves:
        raise SystemExit("No hedge ETF sleeves selected")

    cfg = {
        "market_id": str(book.market_id),
        "regime_region": str(book.region),
        "base_prefix": str(args.book_id),
        "start": args.start.isoformat(),
        "end": args.end.isoformat(),
        "sleeves": sleeves,
        "num_threads": int(args.cpp_threads),
        "verbose": bool(args.cpp_verbose),
        "persist_to_db": bool(args.cpp_persist),
        "persist_execution_to_db": bool(args.cpp_persist_execution),
        "persist_meta_to_db": bool(args.cpp_persist_meta),
        # Execution/realism knobs.
        "execution_price": str(args.cpp_execution_price),
        "mark_price": "close",
        "price_basis": str(args.cpp_price_basis),
        "slippage_bps": float(args.cpp_slippage_bps),
        "commission_per_share": float(args.cpp_commission_per_share),
        "commission_min_per_order": float(args.cpp_commission_min_per_order),
        "commission_max_pct_trade_value": float(args.cpp_commission_max_pct_trade_value),
        "finra_taf_per_share": float(args.cpp_finra_taf_per_share),
        "finra_taf_max_per_order": float(args.cpp_finra_taf_max_per_order),
        "sec_fee_rate": float(args.cpp_sec_fee_rate),
    }

    logger.info(
        "Running hedge ETF C++ backtests book_id=%s market_id=%s start=%s end=%s sleeves=%d",
        args.book_id,
        book.market_id,
        args.start,
        args.end,
        len(sleeves),
    )

    results = prom2_cpp.run_hedge_etf_backtests(cfg)
    results_sorted = sorted(results, key=lambda r: str(r.get("sleeve_id", "")))
    for r in results_sorted:
        sleeve_id = r.get("sleeve_id")
        run_id = r.get("run_id")
        metrics = r.get("metrics", {})
        if args.cpp_persist or args.cpp_persist_execution or args.cpp_persist_meta:
            print(sleeve_id, run_id, metrics)
        else:
            print(sleeve_id, metrics)


if __name__ == "__main__":  # pragma: no cover
    main()
