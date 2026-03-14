"""Compute derived returns and volatility for instruments.

This script drives the helpers in
:mod:`prometheus.data_ingestion.derived.returns_volatility` to populate
``returns_daily`` and ``volatility_daily`` from existing ``prices_daily``
rows.

You can run it for a single instrument, all instruments in a given
market, or all instruments in the database.

Examples
--------

    # Compute for a single instrument
    python -m prometheus.scripts.compute_returns_volatility \
        --instrument-id AAPL

    # Compute for all US_EQ equities
    python -m prometheus.scripts.compute_returns_volatility \
        --market-id US_EQ

    # Compute for all instruments
    python -m prometheus.scripts.compute_returns_volatility --all
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import List, Optional

from apathis.core.database import DatabaseManager, get_db_manager
from apathis.core.logging import get_logger
from apathis.data_ingestion.derived.returns_volatility import (
    DerivedStatsResult,
    compute_returns_and_volatility_for_instruments,
)


logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _get_instruments_by_market(db_manager: DatabaseManager, market_id: str) -> List[str]:
    sql = """
        SELECT instrument_id
        FROM instruments
        WHERE market_id = %s
          AND status = 'ACTIVE'
        ORDER BY instrument_id
    """
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (market_id,))
            rows = cursor.fetchall()
        finally:
            cursor.close()
    return [row[0] for row in rows]


def _get_all_instruments(db_manager: DatabaseManager) -> List[str]:
    sql = """
        SELECT instrument_id
        FROM instruments
        WHERE status = 'ACTIVE'
        ORDER BY instrument_id
    """
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
        finally:
            cursor.close()
    return [row[0] for row in rows]


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Compute returns_daily and volatility_daily")

    parser.add_argument(
        "--instrument-id",
        type=str,
        help="Compute for a single instrument_id",
    )
    parser.add_argument(
        "--market-id",
        type=str,
        help="Compute for all instruments in a given market_id (e.g. US_EQ)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Compute for all active instruments",
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        type=_parse_date,
        help="Optional start date (inclusive, YYYY-MM-DD)",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        type=_parse_date,
        help="Optional end date (inclusive, YYYY-MM-DD)",
    )

    args = parser.parse_args(argv)

    if not (args.instrument_id or args.market_id or args.all):
        parser.error("Must specify --instrument-id, --market-id, or --all")

    db_manager = get_db_manager()

    instrument_ids: List[str]
    if args.instrument_id:
        instrument_ids = [args.instrument_id]
    elif args.market_id:
        instrument_ids = _get_instruments_by_market(db_manager, args.market_id)
    else:
        instrument_ids = _get_all_instruments(db_manager)

    if not instrument_ids:
        logger.info("No instruments found for the requested scope")
        return

    logger.info("Computing derived data for %d instruments", len(instrument_ids))

    results: List[DerivedStatsResult] = compute_returns_and_volatility_for_instruments(
        instrument_ids,
        start_date=args.from_date,
        end_date=args.to_date,
        db_manager=db_manager,
    )

    total_returns = sum(r.returns_rows for r in results)
    total_vol = sum(r.volatility_rows for r in results)
    logger.info(
        "Derived data complete: %d instruments, %d returns rows, %d volatility rows",
        len(results),
        total_returns,
        total_vol,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
