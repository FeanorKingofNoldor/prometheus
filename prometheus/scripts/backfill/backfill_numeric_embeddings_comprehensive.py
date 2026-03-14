"""Comprehensive backfill for numeric window embeddings across all models.

This script backfills numeric embeddings for all five core numeric models
across multiple temporal snapshots to provide complete temporal coverage for
the engines. It processes all instruments that have sufficient price history.

Models backfilled:
- num-regime-core-v1
- num-stab-core-v1
- num-profile-core-v1
- num-scenario-core-v1
- num-portfolio-core-v1

Examples
--------

    # Backfill all models for latest available date
    python -m prometheus.scripts.backfill_numeric_embeddings_comprehensive

    # Backfill for specific as-of date
    python -m prometheus.scripts.backfill_numeric_embeddings_comprehensive \
        --as-of 2025-11-21

    # Backfill with temporal coverage (6 monthly snapshots)
    python -m prometheus.scripts.backfill_numeric_embeddings_comprehensive \
        --temporal-snapshots 6

    # Dry run to see what would be processed
    python -m prometheus.scripts.backfill_numeric_embeddings_comprehensive \
        --dry-run
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from typing import List, Optional, Sequence

from apathis.core.database import DatabaseManager, get_db_manager
from apathis.core.logging import get_logger
from apathis.core.time import TradingCalendar, TradingCalendarConfig, US_EQ
from apathis.data.reader import DataReader
from apathis.encoders import (
    NumericWindowSpec,
    NumericWindowBuilder,
    NumericEmbeddingStore,
    NumericWindowEncoder,
)
from apathis.encoders.models_simple_numeric import PadToDimNumericEmbeddingModel

logger = get_logger(__name__)

# Core numeric embedding models
CORE_MODELS = [
    "num-regime-core-v1",
    "num-stab-core-v1",
    "num-profile-core-v1",
    "num-scenario-core-v1",
    "num-portfolio-core-v1",
]


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _get_latest_price_date(db_manager: DatabaseManager) -> date:
    """Get the latest trade_date from prices_daily."""
    with db_manager.get_historical_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT MAX(trade_date) FROM prices_daily")
        result = cur.fetchone()
        cur.close()
        if not result or result[0] is None:
            raise RuntimeError("No prices found in database")
        return result[0]


def _load_all_instruments_with_prices(db_manager: DatabaseManager) -> List[str]:
    """Return all instrument_ids that have price data.
    
    This does NOT filter by status=ACTIVE since most instruments don't have
    that status set yet. We rely on price existence as the filter.
    """
    sql = """
        SELECT DISTINCT instrument_id
        FROM prices_daily
        ORDER BY instrument_id
    """
    
    with db_manager.get_historical_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        finally:
            cur.close()
    
    return [r[0] for r in rows]


def _check_existing_embeddings(
    db_manager: DatabaseManager,
    model_id: str,
    as_of_date: date,
) -> set[str]:
    """Return set of instrument_ids that already have embeddings for this model/date."""
    sql = """
        SELECT DISTINCT entity_id
        FROM numeric_window_embeddings
        WHERE model_id = %s
          AND as_of_date = %s
          AND entity_type = 'INSTRUMENT'
    """
    
    with db_manager.get_historical_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (model_id, as_of_date))
            rows = cur.fetchall()
        finally:
            cur.close()
    
    return {r[0] for r in rows}


def _build_encoder(
    db_manager: DatabaseManager,
    window_days: int,
    model_id: str,
    market: str,
) -> NumericWindowEncoder:
    """Construct a NumericWindowEncoder for the given model_id."""
    reader = DataReader(db_manager=db_manager)
    calendar = TradingCalendar(TradingCalendarConfig(market=market))
    builder = NumericWindowBuilder(reader, calendar)
    store = NumericEmbeddingStore(db_manager=db_manager)
    
    # All core models use PadToDim with 384 dimensions
    model = PadToDimNumericEmbeddingModel(target_dim=384)
    
    return NumericWindowEncoder(builder=builder, model=model, store=store, model_id=model_id)


def _generate_monthly_dates(latest_date: date, num_snapshots: int) -> List[date]:
    """Generate end-of-month dates going backward from latest_date."""
    dates = [latest_date]
    current = latest_date
    
    for _ in range(num_snapshots - 1):
        # Move to first of current month, then back one day to get end of previous month
        first_of_month = current.replace(day=1)
        current = first_of_month - timedelta(days=1)
        dates.append(current)
    
    return dates


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Comprehensive backfill of numeric embeddings for all core models",
    )
    
    parser.add_argument(
        "--as-of",
        dest="as_of",
        type=_parse_date,
        default=None,
        help="Specific as-of date (YYYY-MM-DD). If not provided, uses latest price date.",
    )
    parser.add_argument(
        "--temporal-snapshots",
        type=int,
        default=1,
        help="Number of monthly snapshots to backfill (default: 1, only the as-of date)",
    )
    parser.add_argument(
        "--start-date",
        type=_parse_date,
        default=None,
        help="Start date for daily backfill (YYYY-MM-DD). Use with --end-date for full date range.",
    )
    parser.add_argument(
        "--end-date",
        type=_parse_date,
        default=None,
        help="End date for daily backfill (YYYY-MM-DD). Use with --start-date for full date range.",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=63,
        help="Number of trading days in the lookback window (default: 63)",
    )
    parser.add_argument(
        "--market",
        type=str,
        default=US_EQ,
        help="Trading calendar market code (default: US_EQ)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip instruments that already have embeddings (default: false, will overwrite)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without actually creating embeddings",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of instruments to process (for testing)",
    )
    
    args = parser.parse_args(argv)
    
    db_manager = get_db_manager()
    
    # Determine target dates
    if args.start_date and args.end_date:
        # Daily backfill mode: get all trading days in range
        logger.info("Daily backfill mode: %s to %s", args.start_date, args.end_date)
        calendar = TradingCalendar(TradingCalendarConfig(market=args.market))
        # TradingCalendar exposes `trading_days_between`, not `get_trading_days_between`.
        target_dates = list(calendar.trading_days_between(args.start_date, args.end_date))
    elif args.as_of:
        # Monthly snapshot mode from specific date
        latest_date = args.as_of
        target_dates = _generate_monthly_dates(latest_date, args.temporal_snapshots)
    else:
        # Monthly snapshot mode from latest price date
        latest_date = _get_latest_price_date(db_manager)
        logger.info("Using latest price date: %s", latest_date)
        target_dates = _generate_monthly_dates(latest_date, args.temporal_snapshots)
    
    # Load all instruments with prices
    all_instruments = _load_all_instruments_with_prices(db_manager)
    
    if args.limit:
        all_instruments = all_instruments[:args.limit]
    
    logger.info(
        "Backfill plan: %d models × %d dates × %d instruments = %d total embeddings",
        len(CORE_MODELS),
        len(target_dates),
        len(all_instruments),
        len(CORE_MODELS) * len(target_dates) * len(all_instruments),
    )
    logger.info("Models: %s", ", ".join(CORE_MODELS))
    logger.info("Dates: %s", ", ".join(str(d) for d in target_dates))
    
    if args.dry_run:
        logger.info("DRY RUN - no embeddings will be created")
        return
    
    # Process each model × date combination
    total_success = 0
    total_failures = 0
    total_skipped = 0
    
    for model_id in CORE_MODELS:
        logger.info("=" * 80)
        logger.info("Processing model: %s", model_id)

        # Track instruments that clearly have no sufficient history in the
        # requested date range so we don't keep retrying them every day.
        no_history_instruments: set[str] = set()

        encoder = _build_encoder(
            db_manager=db_manager,
            window_days=args.window_days,
            model_id=model_id,
            market=args.market,
        )

        for as_of_date in target_dates:
            logger.info("  Processing as-of date: %s", as_of_date)

            # Check existing embeddings for this model/date if requested
            existing = set()
            if args.skip_existing:
                existing = _check_existing_embeddings(db_manager, model_id, as_of_date)
                if existing:
                    logger.info("    Found %d existing embeddings (will skip)", len(existing))

            date_success = 0
            date_failures = 0
            date_skipped = 0

            for instrument_id in all_instruments:
                # Skip instruments we know we should not process for this
                # date either because an embedding already exists or
                # because we have previously determined that there is not
                # enough price history for this model/range.
                if instrument_id in existing or instrument_id in no_history_instruments:
                    date_skipped += 1
                    continue

                spec = NumericWindowSpec(
                    entity_type="INSTRUMENT",
                    entity_id=instrument_id,
                    window_days=args.window_days,
                )

                try:
                    _ = encoder.embed_and_store(spec, as_of_date)
                    date_success += 1
                except Exception as exc:  # pragma: no cover - defensive
                    message = str(exc)
                    # If the failure is due to lack of price history for
                    # this instrument around ``as_of_date``, log it once
                    # and skip this instrument for the remainder of the
                    # run instead of emitting the same warning on every
                    # subsequent date.
                    if "Insufficient price rows" in message:
                        if instrument_id not in no_history_instruments:
                            no_history_instruments.add(instrument_id)
                            logger.warning(
                                "    No sufficient history for %s at %s: %s (will skip for remaining dates)",
                                instrument_id,
                                as_of_date,
                                message,
                            )
                        date_skipped += 1
                    else:
                        date_failures += 1
                        if date_failures <= 5:  # Only log first 5 unexpected failures per date
                            logger.warning(
                                "    Failed to embed %s at %s: %s",
                                instrument_id,
                                as_of_date,
                                message,
                            )

            logger.info(
                "    Date complete: success=%d failures=%d skipped=%d",
                date_success,
                date_failures,
                date_skipped,
            )

            total_success += date_success
            total_failures += date_failures
            total_skipped += date_skipped
    
    logger.info("=" * 80)
    logger.info(
        "Backfill COMPLETE: total_success=%d total_failures=%d total_skipped=%d",
        total_success,
        total_failures,
        total_skipped,
    )


if __name__ == "__main__":
    main()
