"""CLI to ingest fundamentals from EODHD into the historical database.

This script drives the ``eodhd_fundamentals`` ingestion module to fetch
financial statements and basic ratios from EODHD and persist them into:

- ``financial_statements``
- ``fundamental_ratios``

You can run it either for specific issuers (by ``issuer_id``) or for all
S&P 500 issuers previously ingested via
``ingest_eodhd_sp500_instruments``.

Examples
--------

    # Ingest fundamentals for a couple of issuers
    python -m prometheus.scripts.ingest_eodhd_fundamentals \
        --issuer-id AAPL --issuer-id MSFT

    # Ingest fundamentals for all SP500 issuers in the runtime DB
    python -m prometheus.scripts.ingest_eodhd_fundamentals --sp500
"""

from __future__ import annotations

import argparse
from typing import List, Optional

from apatheon.core.database import DatabaseManager, get_db_manager
from apatheon.core.logging import get_logger
from apatheon.data_ingestion.eodhd_client import EodhdClient
from apatheon.data_ingestion.eodhd_fundamentals import (
    ingest_fundamentals_for_issuers,
)

logger = get_logger(__name__)


def _load_sp500_issuers(db: DatabaseManager) -> List[str]:
    """Return all issuer_ids flagged as S&P 500 in the runtime DB.

    We rely on metadata written by ``upsert_sp500_instruments`` which tags
    issuers with ``metadata->>'sp500' = 'true'``.
    """

    sql = """
        SELECT issuer_id
        FROM issuers
        WHERE metadata->>'sp500' = 'true'
        ORDER BY issuer_id
    """

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        finally:
            cur.close()

    return [r[0] for r in rows]


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Ingest fundamentals from EODHD into financial_statements/fundamental_ratios",
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--issuer-id",
        dest="issuer_ids",
        action="append",
        help="Issuer ID to ingest fundamentals for (can be specified multiple times)",
    )
    group.add_argument(
        "--sp500",
        action="store_true",
        help="Ingest fundamentals for all S&P 500 issuers (from runtime DB)",
    )

    args = parser.parse_args(argv)

    db_manager = get_db_manager()
    client = EodhdClient()

    if args.sp500:
        issuer_ids = _load_sp500_issuers(db_manager)
        if not issuer_ids:
            logger.warning("No SP500 issuers found in runtime DB; did you run ingest_eodhd_sp500_instruments?")
            return
        logger.info("Loaded %d SP500 issuers for fundamentals ingestion", len(issuer_ids))
    else:
        issuer_ids = list(dict.fromkeys(args.issuer_ids or []))  # dedupe while preserving order
        if not issuer_ids:
            logger.warning("No issuer_ids provided; nothing to do")
            return
        logger.info("Ingesting fundamentals for %d explicit issuer_ids", len(issuer_ids))

    statements_written, ratios_written = ingest_fundamentals_for_issuers(
        issuer_ids,
        db_manager=db_manager,
        client=client,
    )

    logger.info(
        "Fundamentals ingestion CLI finished: %d financial_statements rows, %d fundamental_ratios rows",
        statements_written,
        ratios_written,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
