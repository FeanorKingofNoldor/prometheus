"""Prometheus v2 – Ingest sector ETF instruments and prices.

This script ensures all 11 GICS sector ETFs (plus HYG, QQQ) exist in
the instruments table and have historical prices in prices_daily.

These are needed for the Sector Health Index (SHI) computation.

Usage
-----
  python -m prometheus.scripts.ingest.ingest_sector_etfs --from 1998-01-01 --to 2025-12-31
  python -m prometheus.scripts.ingest.ingest_sector_etfs --instruments-only  # just register, no prices
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Dict, Optional, Sequence

from psycopg2.extras import Json

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from apathis.data.writer import DataWriter
from apathis.data_ingestion.eodhd_client import EodhdClient
from apathis.data_ingestion.eodhd_prices import ingest_eodhd_prices_for_instruments

logger = get_logger(__name__)

# Sector ETF mapping: instrument_id -> EODHD symbol
# Maps GICS sectors to their SPDR/equivalent ETFs.
SECTOR_ETFS: Dict[str, str] = {
    "XLK.US": "XLK.US",     # Technology
    "XLF.US": "XLF.US",     # Financial Services
    "XLV.US": "XLV.US",     # Healthcare
    "XLI.US": "XLI.US",     # Industrials
    "XLY.US": "XLY.US",     # Consumer Cyclical (Discretionary)
    "XLP.US": "XLP.US",     # Consumer Defensive (Staples)
    "XLE.US": "XLE.US",     # Energy
    "XLU.US": "XLU.US",     # Utilities
    "XLRE.US": "XLRE.US",   # Real Estate (launched 2015-10)
    "XLC.US": "XLC.US",     # Communication Services (launched 2018-06)
    "XLB.US": "XLB.US",     # Basic Materials
}

# Additional ETFs for MHI / market data signals.
ADDITIONAL_ETFS: Dict[str, str] = {
    "HYG.US": "HYG.US",     # iShares High Yield Corporate Bond (credit proxy)
    "QQQ.US": "QQQ.US",     # Nasdaq 100
    "SH.US": "SH.US",       # ProShares Short S&P 500 (inverse)
    "SDS.US": "SDS.US",     # ProShares UltraShort S&P 500 (2x inverse)
    "VIXY.US": "VIXY.US",   # ProShares VIX Short-Term Futures ETF (volatility proxy)
}

# Sector name mapping for metadata.
SECTOR_ETF_NAMES: Dict[str, str] = {
    "XLK.US": "Technology",
    "XLF.US": "Financial Services",
    "XLV.US": "Healthcare",
    "XLI.US": "Industrials",
    "XLY.US": "Consumer Cyclical",
    "XLP.US": "Consumer Defensive",
    "XLE.US": "Energy",
    "XLU.US": "Utilities",
    "XLRE.US": "Real Estate",
    "XLC.US": "Communication Services",
    "XLB.US": "Basic Materials",
    "HYG.US": "High Yield Credit",
    "QQQ.US": "Nasdaq 100",
    "SH.US": "Short S&P 500 (Inverse)",
    "SDS.US": "UltraShort S&P 500 (2x Inverse)",
    "VIXY.US": "VIX Short-Term Futures",
}


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except Exception as exc:
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _upsert_instruments(instrument_ids: Sequence[str], *, market_id: str = "US_EQ", dry_run: bool = False) -> int:
    """Register sector ETF instruments in the runtime instruments table."""

    sql = """
        INSERT INTO instruments (
            instrument_id, issuer_id, market_id, asset_class,
            symbol, exchange, currency, status, metadata
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (instrument_id) DO UPDATE SET
            market_id = EXCLUDED.market_id,
            asset_class = EXCLUDED.asset_class,
            symbol = EXCLUDED.symbol,
            exchange = EXCLUDED.exchange,
            currency = EXCLUDED.currency,
            status = EXCLUDED.status,
            metadata = EXCLUDED.metadata
    """

    if dry_run:
        for iid in instrument_ids:
            logger.info("DRY RUN: would upsert %s", iid)
        return len(instrument_ids)

    db = get_db_manager()
    count = 0

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            for iid in instrument_ids:
                symbol = iid.split(".", 1)[0].strip()
                sector_name = SECTOR_ETF_NAMES.get(iid, "")
                meta = {
                    "source": "sector_etf_ingest",
                    "sector_name": sector_name,
                    "eodhd_symbol": iid,
                }
                cur.execute(sql, (
                    iid,
                    None,       # issuer_id (ETFs have no issuer)
                    market_id,
                    "ETF",
                    symbol,
                    "US",
                    "USD",
                    "ACTIVE",
                    Json(meta),
                ))
                count += 1
            conn.commit()
        finally:
            cur.close()

    return count


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest sector ETF instruments and prices")

    parser.add_argument(
        "--from", dest="from_date", type=_parse_date, default=date(1998, 1, 1),
        help="Start date for price ingestion (default: 1998-01-01)",
    )
    parser.add_argument(
        "--to", dest="to_date", type=_parse_date, default=date(2025, 12, 31),
        help="End date for price ingestion (default: 2025-12-31)",
    )
    parser.add_argument(
        "--instruments-only", action="store_true",
        help="Only register instruments, do not fetch prices",
    )
    parser.add_argument(
        "--skip-existing", action="store_true",
        help="Skip price ingestion for instruments that already have data",
    )
    parser.add_argument(
        "--include-additional", action="store_true", default=True,
        help="Include HYG and QQQ alongside sector ETFs (default: True)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB")

    args = parser.parse_args(argv)

    # Build full mapping.
    mapping: Dict[str, str] = dict(SECTOR_ETFS)
    if args.include_additional:
        mapping.update(ADDITIONAL_ETFS)

    all_ids = sorted(mapping.keys())

    # Step 1: Register instruments.
    logger.info("Registering %d ETF instruments...", len(all_ids))
    n_upserted = _upsert_instruments(all_ids, dry_run=args.dry_run)
    logger.info("Upserted %d instruments", n_upserted)

    if args.instruments_only:
        return

    # Step 2: Optionally skip instruments with existing price data.
    ingest_mapping = dict(mapping)

    if args.skip_existing:
        db = get_db_manager()
        with db.get_historical_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT DISTINCT instrument_id FROM prices_daily WHERE instrument_id = ANY(%s)",
                (all_ids,),
            )
            existing = {row[0] for row in cur.fetchall()}
            cur.close()

        for iid in existing:
            logger.info("Skipping %s (already has price data)", iid)
            ingest_mapping.pop(iid, None)

    if not ingest_mapping:
        logger.info("All instruments already have price data, nothing to ingest")
        return

    if args.dry_run:
        logger.info("DRY RUN: would ingest prices for %s", sorted(ingest_mapping.keys()))
        return

    # Step 3: Ingest prices via EODHD.
    logger.info(
        "Ingesting prices for %d instruments from %s to %s...",
        len(ingest_mapping), args.from_date, args.to_date,
    )

    db_manager = get_db_manager()
    writer = DataWriter(db_manager=db_manager)
    client = EodhdClient()

    results = ingest_eodhd_prices_for_instruments(
        mapping=ingest_mapping,
        start_date=args.from_date,
        end_date=args.to_date,
        default_currency="USD",
        currency_by_instrument=None,
        client=client,
        writer=writer,
    )

    total_bars = sum(r.bars_written for r in results)
    logger.info(
        "Price ingestion complete: %d bars across %d instruments",
        total_bars, len(results),
    )
    for r in results:
        logger.info("  %s: %d bars (%s)", r.instrument_id, r.bars_written, r.eodhd_symbol)


if __name__ == "__main__":  # pragma: no cover
    main()
