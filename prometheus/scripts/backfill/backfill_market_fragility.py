"""Backfill market-level fragility scores for US_EQ.

This script computes historical fragility scores for the US equity market
using the MarketFragilityModel and stores them in the fragility_measures
table with entity_type='MARKET', entity_id='US_EQ'.

Usage:
    python -m prometheus.scripts.backfill.backfill_market_fragility \\
        --start-date 2000-01-01 \\
        --end-date 2024-12-31

    # Dry run (no DB writes)
    python -m prometheus.scripts.backfill.backfill_market_fragility \\
        --start-date 2020-01-01 \\
        --end-date 2020-03-31 \\
        --dry-run
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from typing import Sequence

from apatheon.core.config import get_config
from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from apatheon.fragility.engine import FragilityAlphaEngine
from apatheon.fragility.model_basic import BasicFragilityAlphaModel
from apatheon.fragility.model_market import MarketFragilityModel
from apatheon.fragility.storage import FragilityStorage
from apatheon.stability.storage import StabilityStorage

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    """Parse YYYY-MM-DD date string."""
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def backfill_market_fragility(
    db_manager: DatabaseManager,
    start_date: date,
    end_date: date,
    market_id: str = "US_EQ",
    dry_run: bool = False,
) -> None:
    """Backfill market fragility scores for a date range.

    Args:
        db_manager: DatabaseManager instance
        start_date: First date to compute (inclusive)
        end_date: Last date to compute (inclusive)
        market_id: Market identifier (default 'US_EQ')
        dry_run: If True, compute scores but don't save to DB
    """
    logger.info(
        "Backfilling market fragility: market=%s start=%s end=%s dry_run=%s",
        market_id,
        start_date,
        end_date,
        dry_run,
    )

    # Initialize models and storage
    fragility_storage = FragilityStorage(db_manager=db_manager)
    market_model = MarketFragilityModel(db_manager=db_manager)

    # Note: We still need to pass an instrument model for the engine,
    # but we won't use it for market scoring
    stability_storage = StabilityStorage(db_manager=db_manager)
    instrument_model = BasicFragilityAlphaModel(
        db_manager=db_manager,
        stability_storage=stability_storage,
        scenario_set_id=None,
    )

    engine = FragilityAlphaEngine(
        model=instrument_model,
        storage=fragility_storage,
        market_model=market_model,
    )

    # Iterate through date range
    current = start_date
    success_count = 0
    fail_count = 0
    skip_count = 0

    while current <= end_date:
        # Check if already exists (skip if not dry-run)
        if not dry_run:
            existing = fragility_storage.get_latest_measure(
                "MARKET",
                market_id,
                as_of_date=current,
            )
            if existing and existing.as_of_date == current:
                logger.debug("Skipping %s (already exists)", current)
                skip_count += 1
                current += timedelta(days=1)
                continue

        try:
            if dry_run:
                # Compute but don't save
                measure = market_model.score_entity(current, "MARKET", market_id)
                logger.info(
                    "[DRY RUN] Market fragility: date=%s score=%.4f class=%s",
                    current,
                    measure.fragility_score,
                    measure.class_label.value,
                )
            else:
                # Compute and save
                measure = engine.score_and_save(current, "MARKET", market_id)
                logger.info(
                    "Saved market fragility: date=%s score=%.4f class=%s",
                    current,
                    measure.fragility_score,
                    measure.class_label.value,
                )
            success_count += 1

        except ValueError as exc:
            # Expected for dates with insufficient data
            logger.warning("Insufficient data for %s: %s", current, exc)
            fail_count += 1

        except Exception:
            logger.exception("Unexpected error computing fragility for %s", current)
            fail_count += 1

        current += timedelta(days=1)

    logger.info(
        "Backfill complete: success=%d fail=%d skip=%d total_days=%d",
        success_count,
        fail_count,
        skip_count,
        (end_date - start_date).days + 1,
    )


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Backfill market-level fragility scores for US_EQ"
    )

    parser.add_argument(
        "--start-date",
        type=_parse_date,
        required=True,
        help="Start date (YYYY-MM-DD, inclusive)",
    )
    parser.add_argument(
        "--end-date",
        type=_parse_date,
        required=True,
        help="End date (YYYY-MM-DD, inclusive)",
    )
    parser.add_argument(
        "--market-id",
        type=str,
        default="US_EQ",
        help="Market identifier (default: US_EQ)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute scores but don't save to DB",
    )

    args = parser.parse_args(argv)

    if args.end_date < args.start_date:
        parser.error("--end-date must be >= --start-date")

    config = get_config()
    db_manager = DatabaseManager(config)

    backfill_market_fragility(
        db_manager=db_manager,
        start_date=args.start_date,
        end_date=args.end_date,
        market_id=args.market_id,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
