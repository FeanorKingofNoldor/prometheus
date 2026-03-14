"""Prometheus v2 – Backfill daily returns and volatility.

This script is a small CLI wrapper around
:func:`prometheus.data_ingestion.derived.returns_volatility.compute_returns_and_volatility_for_instruments`.

It discovers instruments from the historical ``prices_daily`` table and
computes:

- 1/5/21-day simple returns into ``returns_daily``.
- 21/63-day realised volatility into ``volatility_daily``.

Usage examples
--------------

Backfill all instruments that have prices::

    python -m prometheus.scripts.backfill_returns_and_volatility

Backfill a subset of instruments::

    python -m prometheus.scripts.backfill_returns_and_volatility \
        --instrument-id AAPL.US --instrument-id MSFT.US

Backfill an explicit date window::

    python -m prometheus.scripts.backfill_returns_and_volatility \
        --start 2010-01-01 --end 2024-12-31
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import List, Optional, Sequence

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
        msg = f"Invalid date {value!r}, expected YYYY-MM-DD"
        raise argparse.ArgumentTypeError(msg) from exc


def _discover_instruments_from_prices(db_manager: DatabaseManager) -> List[str]:
    """Return distinct instrument_ids that have price history.

    This uses the historical ``prices_daily`` table as the source of
    truth rather than runtime instruments, so it also covers any
    temporary/test instruments that happen to have prices.
    """

    sql = """
        SELECT DISTINCT instrument_id
        FROM prices_daily
        WHERE instrument_id NOT LIKE 'SYNTH\_%'
        ORDER BY instrument_id
    """

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return [r[0] for r in rows]


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Backfill daily returns and volatility from prices_daily",
    )

    parser.add_argument(
        "--instrument-id",
        dest="instrument_ids",
        action="append",
        help=(
            "Instrument_id to process; can be specified multiple times. "
            "If omitted, all instruments present in prices_daily are used."
        ),
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Optional start date (inclusive, YYYY-MM-DD). Defaults to first price date.",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        help="Optional end date (inclusive, YYYY-MM-DD). Defaults to last price date.",
    )

    args = parser.parse_args(argv)

    db_manager = get_db_manager()

    if args.instrument_ids:
        instrument_ids: List[str] = list(dict.fromkeys(args.instrument_ids))
    else:
        instrument_ids = _discover_instruments_from_prices(db_manager)

    if not instrument_ids:
        logger.info("No instruments found for returns/volatility backfill; exiting")
        return

    logger.info(
        "Backfilling returns/volatility for %d instruments%s",
        len(instrument_ids),
        " (explicit list)" if args.instrument_ids else " (discovered from prices_daily)",
    )

    results: List[DerivedStatsResult] = compute_returns_and_volatility_for_instruments(
        instrument_ids,
        start_date=args.start,
        end_date=args.end,
        db_manager=db_manager,
    )

    total_returns = sum(r.returns_rows for r in results)
    total_vol = sum(r.volatility_rows for r in results)

    logger.info(
        "Backfill complete: %d instruments, %d returns rows, %d volatility rows",
        len(results),
        total_returns,
        total_vol,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
