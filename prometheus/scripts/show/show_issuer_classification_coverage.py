"""Prometheus v2 – Show issuer classification coverage.

This script reports, for a given as_of_date and market set:
- how many instruments have an as-of issuer_classifications entry,
- how many fall back to issuers.sector,
- how many remain UNKNOWN/missing.

It is intended as a Layer 0 validation tool while tightening data
contracts around time-versioned issuer classifications.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Optional, Sequence

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger
from apatheon.core.time import US_EQ
from apatheon.data.classifications import DEFAULT_CLASSIFICATION_TAXONOMY

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except Exception as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Report issuer classification coverage for instruments")

    parser.add_argument("--as-of", type=_parse_date, required=True, help="As-of date (YYYY-MM-DD)")
    parser.add_argument(
        "--market-id",
        dest="market_ids",
        action="append",
        default=None,
        help=f"Market ID to include (can specify multiple times; default: {US_EQ})",
    )
    parser.add_argument(
        "--taxonomy",
        type=str,
        default=DEFAULT_CLASSIFICATION_TAXONOMY,
        help=f"Classification taxonomy to use (default: {DEFAULT_CLASSIFICATION_TAXONOMY})",
    )
    parser.add_argument("--asset-class", type=str, default="EQUITY")
    parser.add_argument("--status", type=str, default="ACTIVE")
    parser.add_argument(
        "--sp500-members-asof",
        action="store_true",
        help=(
            "Restrict to issuers tagged sp500=true whose (start_date,end_date) window contains --as-of. "
            "This is useful when instruments.status is not time-versioned."
        ),
    )

    args = parser.parse_args(argv)

    as_of: date = args.as_of
    market_ids = args.market_ids if args.market_ids else [US_EQ]
    taxonomy = str(args.taxonomy)
    asset_class = str(args.asset_class)
    status = str(args.status)

    db = get_db_manager()

    sp500_filter_sql = ""
    if args.sp500_members_asof:
        # Issuers metadata is written by EODHD SP500 ingestion.
        sp500_filter_sql = """
          AND u.metadata->>'sp500' = 'true'
          AND (NULLIF(u.metadata->>'start_date', '')::date IS NULL OR NULLIF(u.metadata->>'start_date', '')::date <= %s)
          AND (NULLIF(u.metadata->>'end_date', '')::date IS NULL OR %s <= NULLIF(u.metadata->>'end_date', '')::date)
        """

    # Coverage / source breakdown.
    sql_summary = f"""
        WITH base AS (
            SELECT
                i.instrument_id,
                i.issuer_id,
                i.market_id,
                NULLIF(NULLIF(ic.sector, ''), 'UNKNOWN') AS sector_class,
                NULLIF(NULLIF(u.sector, ''), 'UNKNOWN') AS sector_issuer
            FROM instruments AS i
            LEFT JOIN LATERAL (
                SELECT ic.sector
                FROM issuer_classifications AS ic
                WHERE ic.issuer_id = i.issuer_id
                  AND ic.taxonomy = %s
                  AND ic.effective_start <= %s
                  AND (ic.effective_end IS NULL OR %s < ic.effective_end)
                ORDER BY ic.effective_start DESC
                LIMIT 1
            ) AS ic ON TRUE
            LEFT JOIN issuers AS u
              ON u.issuer_id = i.issuer_id
            WHERE i.market_id = ANY(%s)
              AND i.asset_class = %s
              AND i.status = %s
              {sp500_filter_sql}
        )
        SELECT
            COUNT(*) AS total_instruments,
            SUM(CASE WHEN sector_class IS NOT NULL THEN 1 ELSE 0 END) AS with_classification,
            SUM(CASE WHEN sector_class IS NULL AND sector_issuer IS NOT NULL THEN 1 ELSE 0 END) AS with_issuer_fallback,
            SUM(CASE WHEN COALESCE(sector_class, sector_issuer) IS NULL THEN 1 ELSE 0 END) AS missing_sector
        FROM base
    """

    sql_breakdown = f"""
        WITH base AS (
            SELECT
                i.instrument_id,
                i.issuer_id,
                NULLIF(NULLIF(ic.sector, ''), 'UNKNOWN') AS sector_class,
                NULLIF(NULLIF(u.sector, ''), 'UNKNOWN') AS sector_issuer
            FROM instruments AS i
            LEFT JOIN LATERAL (
                SELECT ic.sector
                FROM issuer_classifications AS ic
                WHERE ic.issuer_id = i.issuer_id
                  AND ic.taxonomy = %s
                  AND ic.effective_start <= %s
                  AND (ic.effective_end IS NULL OR %s < ic.effective_end)
                ORDER BY ic.effective_start DESC
                LIMIT 1
            ) AS ic ON TRUE
            LEFT JOIN issuers AS u
              ON u.issuer_id = i.issuer_id
            WHERE i.market_id = ANY(%s)
              AND i.asset_class = %s
              AND i.status = %s
              {sp500_filter_sql}
        )
        SELECT
            CASE
                WHEN sector_class IS NOT NULL THEN 'issuer_classifications'
                WHEN sector_issuer IS NOT NULL THEN 'issuers'
                ELSE 'UNKNOWN'
            END AS sector_source,
            COUNT(*) AS n
        FROM base
        GROUP BY 1
        ORDER BY n DESC
    """

    # Overlap sanity: should be empty if trigger/constraints are working.
    sql_overlap = """
        SELECT 1
        FROM issuer_classifications AS a
        JOIN issuer_classifications AS b
          ON a.issuer_id = b.issuer_id
         AND a.taxonomy = b.taxonomy
         AND a.classification_id < b.classification_id
         AND daterange(a.effective_start, COALESCE(a.effective_end, 'infinity'::date), '[)')
             && daterange(b.effective_start, COALESCE(b.effective_end, 'infinity'::date), '[)')
        WHERE a.taxonomy = %s
        LIMIT 1
    """

    summary_params: list[object] = [taxonomy, as_of, as_of, list(market_ids), asset_class, status]
    breakdown_params: list[object] = [taxonomy, as_of, as_of, list(market_ids), asset_class, status]

    if args.sp500_members_asof:
        # Extra params for membership window check.
        summary_params.extend([as_of, as_of])
        breakdown_params.extend([as_of, as_of])

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_summary, tuple(summary_params))
            total_instruments, with_class, with_fallback, missing = cur.fetchone()

            cur.execute(sql_breakdown, tuple(breakdown_params))
            breakdown_rows = cur.fetchall()

            cur.execute(sql_overlap, (taxonomy,))
            overlap_exists = cur.fetchone() is not None
        finally:
            cur.close()

    total_instruments = int(total_instruments or 0)
    with_class = int(with_class or 0)
    with_fallback = int(with_fallback or 0)
    missing = int(missing or 0)

    report = {
        "as_of_date": as_of.isoformat(),
        "market_ids": list(market_ids),
        "taxonomy": taxonomy,
        "asset_class": asset_class,
        "status": status,
        "sp500_members_asof": bool(args.sp500_members_asof),
        "total_instruments": total_instruments,
        "with_classification": with_class,
        "with_issuer_fallback": with_fallback,
        "missing_sector": missing,
        "missing_sector_frac": (missing / total_instruments) if total_instruments else None,
        "sector_source_breakdown": {str(src): int(n) for src, n in breakdown_rows},
        "overlap_check_passed": not overlap_exists,
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
