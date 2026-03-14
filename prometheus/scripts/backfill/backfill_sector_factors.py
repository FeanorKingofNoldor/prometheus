"""CLI to backfill sector factors from returns_daily.

This is a thin wrapper around ``prometheus.data_ingestion.factors_sector``.

Example usage:

    python -m prometheus.scripts.backfill_sector_factors \
        --market-id US_EQ \
        --factor-prefix SECTOR \
        --start 1997-01-31 \
        --end   2025-11-21
"""

from __future__ import annotations

import argparse
from datetime import datetime

from apathis.core.logging import setup_logging, get_logger
from apathis.data_ingestion.factors_sector import (
    SectorFactorBackfillConfig,
    backfill_sector_factors,
)


logger = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill sector factors from returns_daily")
    parser.add_argument("--market-id", required=True, help="Market id, e.g. US_EQ")
    parser.add_argument(
        "--factor-prefix",
        default="SECTOR",
        help="Prefix used when constructing factor_ids (default: SECTOR)",
    )
    parser.add_argument("--start", required=True, help="Inclusive start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="Inclusive end date YYYY-MM-DD")
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = _parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()

    config = SectorFactorBackfillConfig(
        market_id=args.market_id,
        factor_prefix=args.factor_prefix,
        start_date=start_date,
        end_date=end_date,
    )

    logger.info(
        "Starting sector factor backfill for market_id=%s prefix=%s %s→%s",
        config.market_id,
        config.factor_prefix,
        config.start_date,
        config.end_date,
    )

    num_factors, num_exposures = backfill_sector_factors(config=config)

    logger.info(
        "Sector factor backfill complete: %d factor rows, %d exposure rows written",
        num_factors,
        num_exposures,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
