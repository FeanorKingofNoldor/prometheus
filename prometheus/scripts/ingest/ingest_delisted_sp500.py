"""Ingest delisted S&P 500 members from EODHD to eliminate survivorship bias.

This script:
1. Fetches the full delisted US ticker list from EODHD's exchange-symbol-list API.
2. Matches against a curated list of known S&P 500 historical members.
3. Creates instrument + issuer rows in the runtime DB.
4. Backfills EOD prices from EODHD into prices_daily.

This is critical for backtest integrity: without delisted companies
(Lehman, Countrywide, Bear Stearns, etc.), any equal-weight portfolio
backtest suffers from survivorship bias.

Usage:
    python -m prometheus.scripts.ingest.ingest_delisted_sp500
    python -m prometheus.scripts.ingest.ingest_delisted_sp500 --dry-run
"""

from __future__ import annotations

import argparse
import time
from datetime import date
from typing import Dict, List, Optional, Tuple

import requests
from psycopg2.extras import Json

from apathis.core.database import get_db_manager, DatabaseManager
from apathis.core.logging import get_logger
from apathis.data.writer import DataWriter
from apathis.data_ingestion.eodhd_client import EodhdClient
from apathis.data_ingestion.eodhd_prices import ingest_eodhd_prices_for_instrument

logger = get_logger(__name__)


# ── Known S&P 500 historical members not tracked by EODHD's GSPC.INDX ──
# These were identified by cross-referencing the EODHD delisted exchange
# list against known index changes.  We store (code, name, sector) so we
# can create proper issuer records.

CURATED_DELISTED: List[Tuple[str, str, str]] = [
    # GFC bankruptcies / forced mergers (2007-2009)
    ("LEH",  "Lehman Brothers Holdings Inc",       "Financial Services"),
    ("MER",  "Merrill Lynch & Co Inc",             "Financial Services"),
    ("CFC",  "Countrywide Financial Corp",         "Financial Services"),
    ("FNM",  "Fannie Mae",                         "Financial Services"),
    ("ABK",  "Ambac Financial Group Inc",          "Financial Services"),
    ("NCC",  "National City Corp",                 "Financial Services"),
    ("SOV",  "Sovereign Bancorp Inc",              "Financial Services"),
    # Major acquisitions (2008-2015)
    ("BNI",  "Burlington Northern Santa Fe Corp",  "Industrials"),
    ("WYE",  "Wyeth",                              "Healthcare"),
    ("SGP",  "Schering-Plough Corp",               "Healthcare"),
    ("ROH",  "Rohm and Haas Co",                   "Basic Materials"),
    ("CEPH", "Cephalon Inc",                       "Healthcare"),
    ("GENZ", "Genzyme Corp",                       "Healthcare"),
    ("ACS",  "Affiliated Computer Services Inc",   "Technology"),
    ("MWW",  "Monster Worldwide Inc",              "Industrials"),
    ("TYC",  "Tyco International plc",             "Industrials"),
    ("XTO",  "XTO Energy Inc",                     "Energy"),
    ("CTX",  "Centex Corp",                        "Consumer Cyclical"),
    ("EDS",  "Electronic Data Systems Corp",       "Technology"),
    ("FRE",  "Freddie Mac",                        "Financial Services"),
]


def fetch_delisted_ticker_set(client: EodhdClient) -> Dict[str, dict]:
    """Fetch delisted US common stocks from EODHD exchange-symbol-list."""
    url = f"{client._base_url}/exchange-symbol-list/US"
    params = {"api_token": client._api_token, "fmt": "json", "delisted": "1"}
    logger.info("Fetching delisted US ticker list from EODHD...")
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return {
        d["Code"]: d
        for d in data
        if d.get("Type") == "Common Stock"
    }


def create_delisted_instrument(
    code: str,
    name: str,
    sector: str,
    db: DatabaseManager,
) -> None:
    """Create issuer + instrument rows for a delisted S&P 500 member."""
    issuer_id = code
    instrument_id = f"{code}.US"

    issuer_sql = """
        INSERT INTO issuers (issuer_id, issuer_type, name, country, sector, metadata)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (issuer_id) DO UPDATE SET
            name = EXCLUDED.name,
            sector = COALESCE(EXCLUDED.sector, issuers.sector),
            metadata = EXCLUDED.metadata
    """
    instrument_sql = """
        INSERT INTO instruments (
            instrument_id, issuer_id, market_id, asset_class,
            symbol, exchange, currency, status, metadata
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (instrument_id) DO UPDATE SET
            status = EXCLUDED.status,
            metadata = EXCLUDED.metadata
    """

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(issuer_sql, (
                issuer_id, "COMPANY", name, "US", sector,
                Json({"source": "eodhd", "sp500": True, "is_delisted": 1}),
            ))
            cur.execute(instrument_sql, (
                instrument_id, issuer_id, "US_EQ", "EQUITY",
                code, "US", "USD", "DELISTED",
                Json({"source": "eodhd", "index": "SP500", "is_delisted": True}),
            ))
            conn.commit()
        finally:
            cur.close()


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Ingest delisted S&P 500 members from EODHD",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be ingested without writing")
    parser.add_argument("--from-date", type=str, default="1997-01-01",
                        help="Start date for price backfill (YYYY-MM-DD)")
    parser.add_argument("--to-date", type=str, default="2025-12-31",
                        help="End date for price backfill (YYYY-MM-DD)")
    args = parser.parse_args(argv)

    start_date = date.fromisoformat(args.from_date)
    end_date = date.fromisoformat(args.to_date)

    db = get_db_manager()
    client = EodhdClient()
    writer = DataWriter(db_manager=db)

    # Check which curated tickers exist in EODHD and are missing from our DB.
    delisted_tickers = fetch_delisted_ticker_set(client)

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT instrument_id FROM instruments")
        existing = {r[0] for r in cur.fetchall()}
        cur.close()

    to_ingest: List[Tuple[str, str, str]] = []
    for code, name, sector in CURATED_DELISTED:
        instrument_id = f"{code}.US"
        if instrument_id in existing:
            logger.info("SKIP %s — already in DB", instrument_id)
            continue
        if code not in delisted_tickers:
            logger.warning("SKIP %s — not found in EODHD delisted list", code)
            continue
        to_ingest.append((code, name, sector))

    logger.info("Will ingest %d delisted S&P 500 members", len(to_ingest))

    if args.dry_run:
        for code, name, _ in to_ingest:
            print(f"  [DRY RUN] {code}.US: {name}")
        return

    total_bars = 0
    for i, (code, name, sector) in enumerate(to_ingest):
        instrument_id = f"{code}.US"
        eodhd_symbol = f"{code}.US"

        logger.info("[%d/%d] Creating instrument %s (%s)...",
                    i + 1, len(to_ingest), instrument_id, name)
        create_delisted_instrument(code, name, sector, db)

        logger.info("[%d/%d] Fetching prices for %s...",
                    i + 1, len(to_ingest), eodhd_symbol)
        try:
            result = ingest_eodhd_prices_for_instrument(
                instrument_id=instrument_id,
                eodhd_symbol=eodhd_symbol,
                start_date=start_date,
                end_date=end_date,
                currency="USD",
                client=client,
                writer=writer,
            )
            logger.info("[%d/%d] %s: %d bars written",
                        i + 1, len(to_ingest), instrument_id, result.bars_written)
            total_bars += result.bars_written
        except Exception as exc:
            logger.error("[%d/%d] %s: FAILED — %s",
                         i + 1, len(to_ingest), instrument_id, exc)

        # Rate limiting: EODHD allows 100K calls/day, but be polite.
        time.sleep(0.3)

    logger.info("Done. Ingested %d total bars for %d delisted instruments.",
                total_bars, len(to_ingest))


if __name__ == "__main__":
    main()
