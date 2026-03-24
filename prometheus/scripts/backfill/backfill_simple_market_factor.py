"""CLI to backfill a simple market factor from returns_daily.

This is a thin wrapper around ``prometheus.data_ingestion.factors_simple``.

Example usage:

    poetry run prometheus backfill-simple-market-factor \
        --market-id US_EQ \
        --factor-id MKT_US_EQ \
        --start 1997-01-02 \
        --end 2025-11-21
"""

from __future__ import annotations

import argparse
from datetime import datetime

from apathis.core.logging import get_logger, setup_logging
from apathis.data_ingestion.factors_simple import (
    FactorBackfillConfig,
    backfill_simple_market_factor,
)

logger = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill simple market factor from returns_daily")
    parser.add_argument("--market-id", required=True, help="Market id, e.g. US_EQ")
    parser.add_argument("--factor-id", required=True, help="Factor id to write, e.g. MKT_US_EQ")
    parser.add_argument("--start", required=True, help="Inclusive start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="Inclusive end date YYYY-MM-DD")
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = _parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()

    config = FactorBackfillConfig(
      market_id=args.market_id,
      factor_id=args.factor_id,
      start_date=start_date,
      end_date=end_date,
    )

    logger.info(
        "Starting simple market factor backfill for market_id=%s factor_id=%s %s→%s",
        config.market_id,
        config.factor_id,
        config.start_date,
        config.end_date,
    )

    num_factors, num_exposures = backfill_simple_market_factor(config=config)

    logger.info(
        "Backfill complete: %d factor rows, %d exposure rows written",
        num_factors,
        num_exposures,
    )


if __name__ == "__main__":
    main()
