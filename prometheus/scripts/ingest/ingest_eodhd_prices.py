"""Prometheus v2 – EODHD price ingestion CLI.

This script provides a small command-line interface for fetching end-of-
-day prices from EODHD for one or more instruments and writing them into
``prices_daily`` in the historical database via :class:`DataWriter`.

It is intended for ad-hoc backfilling and experimentation. Larger scale
backfills can build on the same ingestion helpers.

Usage examples::

    # Ingest AAPL for calendar year 2024 into a custom TEST instrument
    python -m prometheus.scripts.ingest_eodhd_prices \
        --instrument-id TEST_AAPL \
        --symbol AAPL.US \
        --from 2024-01-01 --to 2024-12-31

    # Ingest multiple instruments in one call
    python -m prometheus.scripts.ingest_eodhd_prices \
        --mapping TEST_AAPL=AAPL.US --mapping TEST_MSFT=MSFT.US \
        --from 2024-01-01 --to 2024-12-31
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Dict, Optional

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from apathis.data.writer import DataWriter
from apathis.data_ingestion.eodhd_client import EodhdClient
from apathis.data_ingestion.eodhd_prices import (
    ingest_eodhd_prices_for_instrument,
    ingest_eodhd_prices_for_instruments,
)

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _parse_mapping(values: list[str] | None) -> Dict[str, str]:
    """Parse --mapping arguments of the form INSTRUMENT_ID=EODHD_SYMBOL."""

    result: Dict[str, str] = {}
    if not values:
        return result

    for raw in values:
        if "=" not in raw:
            msg = f"Invalid --mapping value {raw!r}, expected INSTRUMENT_ID=EODHD_SYMBOL"
            raise argparse.ArgumentTypeError(msg)
        instrument_id, symbol = raw.split("=", 1)
        instrument_id = instrument_id.strip()
        symbol = symbol.strip()
        if not instrument_id or not symbol:
            msg = f"Invalid --mapping value {raw!r}, empty instrument_id or symbol"
            raise argparse.ArgumentTypeError(msg)
        result[instrument_id] = symbol

    return result


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Prometheus v2 EODHD price ingestion")

    parser.add_argument(
        "--instrument-id",
        type=str,
        help="Single instrument_id to ingest into (used with --symbol)",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        help="EODHD symbol for --instrument-id (e.g. AAPL.US)",
    )
    parser.add_argument(
        "--mapping",
        action="append",
        help="Mapping INSTRUMENT_ID=EODHD_SYMBOL; can be provided multiple times",
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        type=_parse_date,
        required=True,
        help="Start date (inclusive, YYYY-MM-DD)",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        type=_parse_date,
        required=True,
        help="End date (inclusive, YYYY-MM-DD)",
    )
    parser.add_argument(
        "--currency",
        type=str,
        default="USD",
        help="Currency code to use for written prices (default: USD)",
    )

    args = parser.parse_args(argv)

    # Build mapping from CLI options
    mapping: Dict[str, str] = _parse_mapping(args.mapping)

    if args.instrument_id and args.symbol:
        mapping[args.instrument_id] = args.symbol

    if not mapping:
        parser.error("No instruments specified; use --instrument-id/--symbol or --mapping")

    # Set up client and writer
    db_manager = get_db_manager()
    writer = DataWriter(db_manager=db_manager)
    client = EodhdClient()

    if len(mapping) == 1 and args.instrument_id and args.symbol:
        instrument_id = args.instrument_id
        symbol = args.symbol
        result = ingest_eodhd_prices_for_instrument(
            instrument_id=instrument_id,
            eodhd_symbol=symbol,
            start_date=args.from_date,
            end_date=args.to_date,
            currency=args.currency,
            client=client,
            writer=writer,
        )
        logger.info(
            "Ingested %d bars for %s (%s)",
            result.bars_written,
            result.instrument_id,
            result.eodhd_symbol,
        )
        return

    # Multi-instrument path
    results = ingest_eodhd_prices_for_instruments(
        mapping=mapping,
        start_date=args.from_date,
        end_date=args.to_date,
        default_currency=args.currency,
        currency_by_instrument=None,
        client=client,
        writer=writer,
    )

    total_bars = sum(r.bars_written for r in results)
    logger.info("Ingested %d bars across %d instruments", total_bars, len(results))


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
