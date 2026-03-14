"""
Prometheus v2: Backfill market holidays from EODHD

This script fetches market holiday calendars from EODHD and populates the
market_holidays table in the historical DB. Intended for one-time or
periodic updates to ensure TradingCalendar has accurate holiday data.

Key responsibilities:
- Accept market_id as CLI argument
- Fetch holidays from EODHD via load_and_cache_market_holidays
- Report number of holidays cached

External dependencies:
- argparse: CLI argument parsing
- prometheus.data_ingestion.market_calendar: Calendar loader

Database tables accessed:
- historical_db.market_holidays: Write

Thread safety: Thread-safe (stateless script)

Author: Prometheus Team
Created: 2025-12-01
Last Modified: 2025-12-01
Status: Development
Version: v0.1.0
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from apathis.data_ingestion.market_calendar import load_and_cache_market_holidays

logger = get_logger(__name__)


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entrypoint for backfilling market holidays."""
    parser = argparse.ArgumentParser(
        description="Backfill market holidays from EODHD into market_holidays table",
    )
    parser.add_argument(
        "--market-id",
        type=str,
        required=True,
        help="Market ID to backfill, e.g. US_EQ",
    )
    args = parser.parse_args(argv)

    db_manager = get_db_manager()
    market_id = args.market_id

    logger.info("Backfilling market holidays for market_id=%s", market_id)

    try:
        count = load_and_cache_market_holidays(db_manager, market_id)
        logger.info("Successfully cached %d holidays for %s", count, market_id)
        print(f"Cached {count} holidays for market {market_id}")
    except Exception as exc:
        logger.exception("Failed to backfill market holidays: %s", exc)
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
