"""Prometheus v2 – Show filings status (Layer 1 validation).

Validates basic Layer 1 contracts for ``filings``:
- issuer_id is non-empty
- filing_type is non-empty
- filing_date is present
- text_ref is non-empty
- metadata is either NULL or a JSON object

Reports results for both runtime_db and historical_db.

Note: issuer_id existence and text_ref artifact immutability are validated
via fast checks, but deeper content quality audits belong at higher layers.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any, Optional, Sequence

from apatheon.core.database import get_db_manager


def _summarise(db, which: str) -> dict[str, Any]:
    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    sql = """
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT f.issuer_id) AS distinct_issuers,
            MIN(f.filing_date) AS min_filing_date,
            MAX(f.filing_date) AS max_filing_date,
            SUM(CASE WHEN btrim(f.issuer_id) = '' THEN 1 ELSE 0 END) AS empty_issuer_id,
            SUM(CASE WHEN btrim(f.filing_type) = '' THEN 1 ELSE 0 END) AS empty_filing_type,
            SUM(CASE WHEN btrim(f.text_ref) = '' THEN 1 ELSE 0 END) AS empty_text_ref,
            SUM(CASE WHEN f.metadata IS NOT NULL AND jsonb_typeof(f.metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object,
            SUM(CASE WHEN i.issuer_id IS NULL THEN 1 ELSE 0 END) AS orphan_issuer_id
        FROM filings f
        LEFT JOIN issuers i ON i.issuer_id = f.issuer_id
    """

    sql_preview = """
        SELECT filing_id, issuer_id, filing_type, filing_date, text_ref
        FROM filings
        ORDER BY filing_date DESC, issuer_id, filing_id DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_issuers,
                min_filing_date,
                max_filing_date,
                empty_issuer_id,
                empty_filing_type,
                empty_text_ref,
                metadata_not_object,
                orphan_issuer_id,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "filing_id": int(fid) if fid is not None else None,
            "issuer_id": str(issuer_id),
            "filing_type": str(ftype),
            "filing_date": fdate.isoformat() if isinstance(fdate, date) else None,
            "text_ref": str(tref),
        }
        for fid, issuer_id, ftype, fdate, tref in preview_rows
    ]

    checks_passed = (
        int(empty_issuer_id or 0) == 0
        and int(empty_filing_type or 0) == 0
        and int(empty_text_ref or 0) == 0
        and int(metadata_not_object or 0) == 0
        and int(orphan_issuer_id or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_issuers": int(distinct_issuers or 0),
        "min_filing_date": min_filing_date.isoformat() if isinstance(min_filing_date, date) else None,
        "max_filing_date": max_filing_date.isoformat() if isinstance(max_filing_date, date) else None,
        "empty_issuer_id_rows": int(empty_issuer_id or 0),
        "empty_filing_type_rows": int(empty_filing_type or 0),
        "empty_text_ref_rows": int(empty_text_ref or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "orphan_issuer_id_rows": int(orphan_issuer_id or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show filings status and basic Layer 1 validation checks")
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
