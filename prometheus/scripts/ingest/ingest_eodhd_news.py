"""CLI to ingest news and headlines from EODHD.

This script drives :mod:`prometheus.data_ingestion.news_eodhd` to fetch
news articles for the US equity market (or another market_id) and write
them into:

- ``news_articles``
- ``news_links``

Example (US_EQ from 1997 to present)::

    python -m prometheus.scripts.ingest_eodhd_news \
        --market-id US_EQ \
        --start 1997-01-01 \
        --end 2025-11-21
"""

from __future__ import annotations

import argparse
from datetime import datetime
from typing import Optional

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from apathis.data_ingestion.eodhd_client import EodhdClient
from apathis.data_ingestion.news_eodhd import (
    NewsIngestionConfig,
    ingest_eodhd_news_for_market,
)

logger = get_logger(__name__)


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest EODHD news into news_articles/news_links")
    parser.add_argument("--market-id", default="US_EQ", help="Market id to ingest for (default: US_EQ)")
    parser.add_argument("--start", required=True, help="Inclusive start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="Inclusive end date YYYY-MM-DD")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = _parse_args(argv)

    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()

    db_manager = get_db_manager()
    client = EodhdClient()

    config = NewsIngestionConfig(
        start_date=start_date,
        end_date=end_date,
        market_id=args.market_id,
    )

    logger.info(
        "Starting EODHD news ingestion for market_id=%s %s→%s",
        config.market_id,
        config.start_date,
        config.end_date,
    )

    num_articles, num_links = ingest_eodhd_news_for_market(
        config,
        db_manager=db_manager,
        client=client,
    )

    logger.info(
        "EODHD news ingestion CLI finished: %d news_articles rows, %d news_links rows",
        num_articles,
        num_links,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
