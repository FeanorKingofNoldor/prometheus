"""CLI to ingest S&P 500 instruments and issuers from EODHD.

This script:

1. Fetches S&P 500 constituents (current + historical) from the EODHD
   fundamentals API for `GSPC.INDX`.
2. Upserts corresponding rows into `markets`, `issuers`, and
   `instruments` in the runtime database, assigning them to the
   `US_EQ` market.

It does **not** fetch price history; use the existing
`backfill_eodhd_us_eq` script to backfill `prices_daily` once
instruments exist.

Examples
--------

    # Basic run: ingest all known S&P 500 tickers
    python -m prometheus.scripts.ingest_eodhd_sp500_instruments
"""

from __future__ import annotations

import argparse
from typing import Optional

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger
from apatheon.data_ingestion.eodhd_client import EodhdClient
from apatheon.data_ingestion.eodhd_sp500_instruments import (
    fetch_sp500_constituents,
    upsert_sp500_instruments,
)

logger = get_logger(__name__)


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest S&P 500 instruments from EODHD")
    _ = parser.parse_args(argv)  # no options yet; placeholder for future filters

    db_manager = get_db_manager()
    client = EodhdClient()

    constituents = fetch_sp500_constituents(client)
    if not constituents:
        logger.warning("No S&P 500 constituents returned from EODHD; aborting")
        return

    issuers_written, instruments_written = upsert_sp500_instruments(
        constituents,
        db_manager=db_manager,
    )

    logger.info(
        "S&P 500 ingestion complete: %d issuers, %d instruments",
        issuers_written,
        instruments_written,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
