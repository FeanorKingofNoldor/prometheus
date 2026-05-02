"""
Prometheus v2: Backfill missing returns and volatility data

This script identifies instruments that have price data but missing derived
returns/volatility, and backfills them to ensure complete coverage for all
engines that depend on these features.

Key responsibilities:
- Find instruments with prices but missing returns/volatility
- Backfill returns_daily and volatility_daily tables
- Report progress and statistics

External dependencies:
- argparse: CLI argument parsing

Database tables accessed:
- historical_db.prices_daily: Read
- historical_db.returns_daily: Write
- historical_db.volatility_daily: Write

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
from typing import Optional, Sequence

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger
from apatheon.data_ingestion.derived.returns_volatility import (
    compute_returns_and_volatility_for_instrument,
)

logger = get_logger(__name__)


def find_instruments_with_missing_returns(db_manager: object) -> list[str]:
    """Find instrument IDs that have prices but incomplete returns coverage.

    Returns:
        List of instrument_ids that need returns/volatility backfill.
    """
    sql = r"""
        SELECT DISTINCT p.instrument_id
        FROM prices_daily p
        WHERE p.instrument_id NOT LIKE 'SYNTH\_%'
          AND NOT EXISTS (
              SELECT 1 FROM returns_daily r
              WHERE r.instrument_id = p.instrument_id
                AND r.trade_date = p.trade_date
          )
        ORDER BY p.instrument_id
    """

    with db_manager.get_historical_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        finally:
            cur.close()

    return [r[0] for r in rows]


def main(argv: Optional[Sequence[str]] = None) -> None:
    """CLI entrypoint for backfilling missing returns/volatility."""
    parser = argparse.ArgumentParser(
        description="Backfill missing returns and volatility for instruments with price data",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be backfilled without actually doing it",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of instruments to process (for testing)",
    )
    args = parser.parse_args(argv)

    db_manager = get_db_manager()

    logger.info("Finding instruments with missing returns/volatility...")
    instruments = find_instruments_with_missing_returns(db_manager)

    if not instruments:
        logger.info("No instruments found with missing returns/volatility")
        print("✅ All instruments have complete returns/volatility coverage!")
        return

    logger.info("Found %d instruments with missing returns/volatility", len(instruments))
    print(f"\n📊 Found {len(instruments)} instruments needing backfill")

    if args.limit:
        instruments = instruments[: args.limit]
        print(f"   (Limited to first {len(instruments)} instruments for this run)")

    if args.dry_run:
        print("\n🔍 DRY RUN - Would backfill:")
        for inst in instruments[:10]:  # Show first 10
            print(f"   - {inst}")
        if len(instruments) > 10:
            print(f"   ... and {len(instruments) - 10} more")
        return

    # Backfill each instrument
    success = 0
    failures = 0
    total_returns_rows = 0
    total_vol_rows = 0

    print("\n⚙️  Starting backfill...")
    for i, instrument_id in enumerate(instruments, 1):
        try:
            result = compute_returns_and_volatility_for_instrument(
                instrument_id, db_manager=db_manager
            )
            success += 1
            total_returns_rows += result.returns_rows
            total_vol_rows += result.volatility_rows

            if i % 10 == 0:
                print(
                    f"   Progress: {i}/{len(instruments)} instruments "
                    f"({success} success, {failures} failures)"
                )

            logger.info(
                "Backfilled %s: %d returns, %d volatility rows",
                instrument_id,
                result.returns_rows,
                result.volatility_rows,
            )

        except Exception as exc:
            failures += 1
            logger.exception("Failed to backfill %s: %s", instrument_id, exc)
            print(f"   ⚠️  Failed: {instrument_id} - {exc}", file=sys.stderr)

    # Report results
    print("\n✅ Backfill complete!")
    print(f"   Instruments processed: {success + failures}")
    print(f"   Success: {success}")
    print(f"   Failures: {failures}")
    print(f"   Total returns rows added: {total_returns_rows:,}")
    print(f"   Total volatility rows added: {total_vol_rows:,}")

    if failures > 0:
        print(f"\n⚠️  {failures} instruments failed - check logs for details")
        sys.exit(1)


if __name__ == "__main__":
    main()
