"""Run the signal research harness on a real signal and register the verdict.

Proves the harness end-to-end. Default signal: 6-1 cross-sectional price
momentum (Bucket 1, replayable) over a liquid US-equity universe from
``prometheus_historical.prices_daily``.

Usage::

    python -m prometheus.scripts.run.run_signal_harness \
        --start 2022-01-01 --end 2025-12-31 --lookback 126 --skip 21

The script computes the signal point-in-time, builds a forward-return + forward
-vol panel (no look-ahead), evaluates IC / deciles / turnover / what-it-predicts,
prints the report, and upserts the verdict into the ``signal_registry`` table.
"""

from __future__ import annotations

import argparse
from datetime import date

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger

from prometheus.research.signal_harness import (
    evaluate_signal,
    forward_return_panel,
    momentum_signal,
)
from prometheus.research.signal_registry import list_signals, update_from_report

logger = get_logger(__name__)


def _parse_date(s: str) -> date:
    y, m, d = map(int, s.split("-"))
    return date(y, m, d)


def _liquid_universe(db_manager, start: date, end: date, top_n: int) -> list[str]:
    """Pick the top_n most-data-complete US equities over the window."""
    sql = """
        SELECT instrument_id, COUNT(*) c
        FROM prices_daily
        WHERE trade_date BETWEEN %s AND %s
          AND instrument_id LIKE '%%.US'
          AND COALESCE(adjusted_close, close) > 0
        GROUP BY instrument_id
        ORDER BY c DESC
        LIMIT %s
    """
    with db_manager.get_historical_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (start, end, top_n))
            return [r[0] for r in cur.fetchall()]


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", type=_parse_date, default=date(2022, 1, 1))
    ap.add_argument("--end", type=_parse_date, default=date(2025, 12, 31))
    ap.add_argument("--lookback", type=int, default=126)
    ap.add_argument("--skip", type=int, default=21)
    ap.add_argument("--universe-size", type=int, default=400)
    ap.add_argument("--sample-every", type=int, default=5)
    ap.add_argument("--no-register", action="store_true")
    args = ap.parse_args(argv)

    db = get_db_manager()
    universe = _liquid_universe(db, args.start, args.end, args.universe_size)
    logger.info("Universe: %d instruments", len(universe))

    name = f"momentum_{args.lookback}_{args.skip}"
    scores = momentum_signal(
        db, args.start, args.end,
        lookback=args.lookback, skip=args.skip,
        universe=universe, sample_every=args.sample_every,
    )
    logger.info("Signal: %d obs over %d dates", len(scores), scores["as_of_date"].nunique())

    panel = forward_return_panel(db, scores, horizons=(1, 5, 21, 63), universe=universe)
    report = evaluate_signal(
        scores, panel, name=name, horizons=(1, 5, 21, 63), headline_horizon=21,
    )

    print("\n" + "=" * 78)
    print(report.headline())
    print("=" * 78 + "\n")

    if not args.no_register:
        update_from_report(
            db, report,
            description=f"{args.lookback}-day cross-sectional momentum skipping last {args.skip}d "
                        f"(classic momentum gap), liquid US equities",
            bucket=1,
            integrity_note="Pure past-price computation; score uses only data up to as_of-skip. "
                           "Forward returns strictly after as_of. No look-ahead.",
        )
        print("Registered. Current registry:")
        for rec in list_signals(db):
            print(f"  [{rec.bucket}] {rec.name:24} verdict={rec.verdict:7} "
                  f"predicts={rec.what_predicts:16} IC@{rec.headline_horizon}d={rec.headline_ic:+.4f} "
                  f"turnover={rec.turnover:.3f}")


if __name__ == "__main__":
    main()
