"""Replay regime classifications through the updated RegimeEngine.

This script re-runs regime classification for every calendar day in a
date range using the current RegimeEngine logic (5-day hold period,
CRISIS override, trading-day guard). It replaces existing regime data
so that backtest results reflect the improved transition logic.

Usage:
    python -m prometheus.scripts.backfill.backfill_regime_replay \
        --start 2015-01-02 --end 2025-12-08 \
        --region US --min-hold-days 5

The replay is sequential (order matters for hold-period state) but fast
since it's just CSV lookups + DB upserts (~2,700 days).
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from datetime import date, timedelta
from typing import Optional, Sequence

from apatheon.core.config import get_config
from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from apatheon.core.time import TradingCalendar, TradingCalendarConfig
from apatheon.regime.engine import RegimeEngine
from apatheon.regime.model_proxy import MarketProxyRegimeModel
from apatheon.regime.storage import RegimeStorage

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date {value!r}, expected YYYY-MM-DD"
        ) from exc


def _clear_existing_data(
    db_manager: DatabaseManager,
    region: str,
    start_date: date,
    end_date: date,
) -> tuple[int, int]:
    """Delete existing regimes and transitions for region in date range.

    Returns (regimes_deleted, transitions_deleted).
    """
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                DELETE FROM regime_transitions
                WHERE region = %s
                  AND as_of_date BETWEEN %s AND %s
                """,
                (region, start_date, end_date),
            )
            tx_deleted = cursor.rowcount

            cursor.execute(
                """
                DELETE FROM regimes
                WHERE region = %s
                  AND as_of_date BETWEEN %s AND %s
                """,
                (region, start_date, end_date),
            )
            reg_deleted = cursor.rowcount

            conn.commit()
        finally:
            cursor.close()

    return reg_deleted, tx_deleted


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Replay regime classifications through updated RegimeEngine"
    )
    parser.add_argument(
        "--start", type=_parse_date, required=True, help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end", type=_parse_date, required=True, help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--region", type=str, default="US", help="Region code (default: US)"
    )
    parser.add_argument(
        "--min-hold-days",
        type=int,
        default=5,
        help="Minimum trading days between transitions (default: 5)",
    )
    parser.add_argument(
        "--hazard-profile",
        type=str,
        default=None,
        help="Hazard overlay profile name (default: auto)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Classify but don't persist (print labels only)",
    )
    parser.add_argument(
        "--skip-clear",
        action="store_true",
        help="Don't delete existing data before replay",
    )

    args = parser.parse_args(argv)

    if args.end < args.start:
        parser.error("--end must be >= --start")

    config = get_config()
    db_manager = DatabaseManager(config)

    # -- Clear existing data --
    if not args.dry_run and not args.skip_clear:
        reg_del, tx_del = _clear_existing_data(
            db_manager, args.region, args.start, args.end
        )
        print(
            f"Cleared existing data: {reg_del} regime records, "
            f"{tx_del} transitions deleted"
        )

    # -- Build engine --
    calendar = TradingCalendar(TradingCalendarConfig(market="US_EQ"))
    model = MarketProxyRegimeModel(
        db_manager=db_manager,
        profile_name=args.hazard_profile,
    )
    storage = RegimeStorage(db_manager=db_manager)
    engine = RegimeEngine(
        model=model,
        storage=storage,
        min_hold_days=args.min_hold_days,
        calendar=calendar,
    )

    # -- Replay day by day --
    total_days = (args.end - args.start).days + 1
    label_counts: Counter[str] = Counter()
    trading_days_processed = 0
    skipped_days = 0
    t0 = time.perf_counter()

    current = args.start
    step = 0
    while current <= args.end:
        step += 1

        if args.dry_run:
            # In dry-run mode, just classify without persisting
            if calendar.is_trading_day(current):
                state = model.classify(current, args.region)
                label_counts[state.regime_label.value] += 1
                trading_days_processed += 1
            else:
                skipped_days += 1
        else:
            # Full engine run: trading-day guard + hold period + persist
            state = engine.get_regime(current, args.region)
            label_counts[state.regime_label.value] += 1
            if calendar.is_trading_day(current):
                trading_days_processed += 1
            else:
                skipped_days += 1

        # Progress every 500 days
        if step % 500 == 0:
            elapsed = time.perf_counter() - t0
            pct = step * 100.0 / total_days
            eta = (elapsed / step) * (total_days - step) if step > 0 else 0
            sys.stderr.write(
                f"\r  [{step}/{total_days}] {pct:.1f}% "
                f"elapsed={elapsed:.0f}s eta={eta:.0f}s"
                "\033[K"
            )
            sys.stderr.flush()

        current += timedelta(days=1)

    elapsed = time.perf_counter() - t0
    sys.stderr.write("\n")

    # -- Summary --
    print(f"\n{'='*60}")
    print("Regime Replay Complete")
    print(f"{'='*60}")
    print(f"Region:              {args.region}")
    print(f"Date range:          {args.start} -> {args.end}")
    print(f"Total calendar days: {total_days}")
    print(f"Trading days:        {trading_days_processed}")
    print(f"Non-trading days:    {skipped_days}")
    print(f"Min hold days:       {args.min_hold_days}")
    print(f"Elapsed:             {elapsed:.1f}s")
    print(f"Mode:                {'DRY RUN' if args.dry_run else 'PERSISTED'}")
    print()
    print("Label distribution:")
    for label in sorted(label_counts.keys()):
        count = label_counts[label]
        pct = count * 100.0 / sum(label_counts.values()) if label_counts else 0
        print(f"  {label:<12} {count:>6}  ({pct:.1f}%)")

    if not args.dry_run:
        # Query actual DB state
        with db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM regimes
                    WHERE region = %s
                      AND as_of_date BETWEEN %s AND %s
                    """,
                    (args.region, args.start, args.end),
                )
                db_regimes = cursor.fetchone()[0]

                cursor.execute(
                    """
                    SELECT COUNT(*) FROM regime_transitions
                    WHERE region = %s
                      AND as_of_date BETWEEN %s AND %s
                    """,
                    (args.region, args.start, args.end),
                )
                db_transitions = cursor.fetchone()[0]
            finally:
                cursor.close()

        print()
        print("DB state after replay:")
        print(f"  Regime records:  {db_regimes}")
        print(f"  Transitions:     {db_transitions}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
