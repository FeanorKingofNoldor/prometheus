"""Prometheus v2 – Show instrument identifier coverage.

This script reports, for a given as_of_date and market set:
- how many instruments have an as-of `instrument_identifiers` entry for a given
  identifier_type,
- how many are missing.

It is intended as a Layer 0 validation tool while tightening instrument identity
policies (ticker changes, identifier reuse, vendor mappings).
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Optional, Sequence

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from apathis.core.time import US_EQ

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except Exception as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Report instrument identifier coverage for instruments")

    parser.add_argument("--as-of", type=_parse_date, required=True, help="As-of date (YYYY-MM-DD)")
    parser.add_argument(
        "--market-id",
        dest="market_ids",
        action="append",
        default=None,
        help=f"Market ID to include (can specify multiple times; default: {US_EQ})",
    )
    parser.add_argument(
        "--identifier-type",
        type=str,
        default="SYMBOL",
        help="Identifier type to check (default: SYMBOL)",
    )
    parser.add_argument("--asset-class", type=str, default="EQUITY")
    parser.add_argument("--status", type=str, default="ACTIVE")

    args = parser.parse_args(argv)

    as_of: date = args.as_of
    market_ids = args.market_ids if args.market_ids else [US_EQ]
    identifier_type = str(args.identifier_type)
    asset_class = str(args.asset_class)
    status = str(args.status)

    db = get_db_manager()

    sql_summary = """
        WITH base AS (
            SELECT
                i.instrument_id,
                NULLIF(NULLIF(ii.identifier_value, ''), 'UNKNOWN') AS identifier_value
            FROM instruments AS i
            LEFT JOIN LATERAL (
                SELECT ii.identifier_value
                FROM instrument_identifiers AS ii
                WHERE ii.instrument_id = i.instrument_id
                  AND ii.identifier_type = %s
                  AND ii.effective_start <= %s
                  AND (ii.effective_end IS NULL OR %s < ii.effective_end)
                ORDER BY ii.effective_start DESC
                LIMIT 1
            ) AS ii ON TRUE
            WHERE i.market_id = ANY(%s)
              AND i.asset_class = %s
              AND i.status = %s
        )
        SELECT
            COUNT(*) AS total_instruments,
            SUM(CASE WHEN identifier_value IS NOT NULL THEN 1 ELSE 0 END) AS with_identifier,
            SUM(CASE WHEN identifier_value IS NULL THEN 1 ELSE 0 END) AS missing_identifier
        FROM base
    """

    # Overlap sanity: should be empty if trigger/constraints are working.
    sql_overlap = """
        SELECT 1
        FROM instrument_identifiers AS a
        JOIN instrument_identifiers AS b
          ON a.instrument_id = b.instrument_id
         AND a.identifier_type = b.identifier_type
         AND a.instrument_identifier_id < b.instrument_identifier_id
         AND daterange(a.effective_start, COALESCE(a.effective_end, 'infinity'::date), '[)')
             && daterange(b.effective_start, COALESCE(b.effective_end, 'infinity'::date), '[)')
        WHERE a.identifier_type = %s
        LIMIT 1
    """

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_summary, (identifier_type, as_of, as_of, list(market_ids), asset_class, status))
            total_instruments, with_identifier, missing_identifier = cur.fetchone()

            cur.execute(sql_overlap, (identifier_type,))
            overlap_exists = cur.fetchone() is not None
        finally:
            cur.close()

    total_instruments = int(total_instruments or 0)
    with_identifier = int(with_identifier or 0)
    missing_identifier = int(missing_identifier or 0)

    report = {
        "as_of_date": as_of.isoformat(),
        "market_ids": list(market_ids),
        "identifier_type": identifier_type,
        "asset_class": asset_class,
        "status": status,
        "total_instruments": total_instruments,
        "with_identifier": with_identifier,
        "missing_identifier": missing_identifier,
        "missing_identifier_frac": (missing_identifier / total_instruments) if total_instruments else None,
        "overlap_check_passed": not overlap_exists,
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
