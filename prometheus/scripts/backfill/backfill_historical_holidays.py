"""Backfill historical market holidays for backtesting.

This script uses pandas_market_calendars to populate accurate historical
holiday data (1990-2025) into the market_holidays table.

This is needed for backtesting because EODHD only provides future holidays.

Usage:
    # Backfill US equities
    python -m prometheus.scripts.backfill_historical_holidays --market-id US_EQ

    # Backfill with custom date range
    python -m prometheus.scripts.backfill_historical_holidays \\
        --market-id US_EQ \\
        --start-year 2000 \\
        --end-year 2024

    # Backfill all markets
    python -m prometheus.scripts.backfill_historical_holidays --all-markets
"""

from __future__ import annotations

import argparse
import sys
from typing import Sequence

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger
from apatheon.data_ingestion.historical_holidays import (
    HAS_PANDAS_MARKET_CALENDARS,
    MARKET_TO_CALENDAR,
    backfill_historical_holidays,
)

logger = get_logger(__name__)


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entrypoint for backfilling historical holidays."""
    parser = argparse.ArgumentParser(
        description="Backfill historical market holidays for backtesting",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--market-id",
        type=str,
        help=f"Market ID to backfill (choices: {', '.join(MARKET_TO_CALENDAR.keys())})",
    )
    parser.add_argument(
        "--all-markets",
        action="store_true",
        help="Backfill all supported markets",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=1990,
        help="Start year (default: 1990)",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=2025,
        help="End year (default: 2025)",
    )

    args = parser.parse_args(argv)

    # Check if pandas_market_calendars is available
    if not HAS_PANDAS_MARKET_CALENDARS:
        print(
            "ERROR: pandas_market_calendars is required but not installed.\n"
            "Install with: pip install pandas_market_calendars",
            file=sys.stderr,
        )
        sys.exit(1)

    # Determine markets to backfill
    if args.all_markets:
        markets = list(MARKET_TO_CALENDAR.keys())
    elif args.market_id:
        if args.market_id not in MARKET_TO_CALENDAR:
            print(
                f"ERROR: Unsupported market_id: {args.market_id}\n"
                f"Supported markets: {', '.join(MARKET_TO_CALENDAR.keys())}",
                file=sys.stderr,
            )
            sys.exit(1)
        markets = [args.market_id]
    else:
        parser.error("Must specify either --market-id or --all-markets")

    # Backfill
    db_manager = get_db_manager()
    total_count = 0

    for market_id in markets:
        logger.info(
            "Backfilling historical holidays for %s (%d-%d)",
            market_id,
            args.start_year,
            args.end_year,
        )

        try:
            count = backfill_historical_holidays(
                db_manager,
                market_id,
                start_year=args.start_year,
                end_year=args.end_year,
            )
            total_count += count
            print(f"✅ {market_id}: Backfilled {count} holidays")
        except Exception as exc:
            logger.exception("Failed to backfill %s: %s", market_id, exc)
            print(f"❌ {market_id}: FAILED - {exc}", file=sys.stderr)
            continue

    print(f"\n🎉 Total: Backfilled {total_count} holidays across {len(markets)} market(s)")
    print(f"\nHistorical holidays are now available for {args.start_year}-{args.end_year}")
    print("Your TradingCalendar will automatically use these for backtesting.")


if __name__ == "__main__":
    main()
