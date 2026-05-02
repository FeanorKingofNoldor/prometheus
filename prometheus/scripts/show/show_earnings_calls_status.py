"""Prometheus v2 – Show earnings_calls status (Layer 1 validation).

Validates basic Layer 1 contracts for ``earnings_calls``:
- issuer_id is non-empty
- call_date is present
- transcript_ref is non-empty
- metadata is either NULL or a JSON object

Reports results for both runtime_db and historical_db.

Note: transcript parsing/embedding quality is a higher-level audit.
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
            COUNT(DISTINCT ec.issuer_id) AS distinct_issuers,
            MIN(ec.call_date) AS min_call_date,
            MAX(ec.call_date) AS max_call_date,
            SUM(CASE WHEN btrim(ec.issuer_id) = '' THEN 1 ELSE 0 END) AS empty_issuer_id,
            SUM(CASE WHEN btrim(ec.transcript_ref) = '' THEN 1 ELSE 0 END) AS empty_transcript_ref,
            SUM(CASE WHEN ec.metadata IS NOT NULL AND jsonb_typeof(ec.metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object,
            SUM(CASE WHEN i.issuer_id IS NULL THEN 1 ELSE 0 END) AS orphan_issuer_id
        FROM earnings_calls ec
        LEFT JOIN issuers i ON i.issuer_id = ec.issuer_id
    """

    sql_preview = """
        SELECT call_id, issuer_id, call_date, transcript_ref
        FROM earnings_calls
        ORDER BY call_date DESC, issuer_id, call_id DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_issuers,
                min_call_date,
                max_call_date,
                empty_issuer_id,
                empty_transcript_ref,
                metadata_not_object,
                orphan_issuer_id,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "call_id": int(cid) if cid is not None else None,
            "issuer_id": str(issuer_id),
            "call_date": cdate.isoformat() if isinstance(cdate, date) else None,
            "transcript_ref": str(tref),
        }
        for cid, issuer_id, cdate, tref in preview_rows
    ]

    checks_passed = (
        int(empty_issuer_id or 0) == 0
        and int(empty_transcript_ref or 0) == 0
        and int(metadata_not_object or 0) == 0
        and int(orphan_issuer_id or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_issuers": int(distinct_issuers or 0),
        "min_call_date": min_call_date.isoformat() if isinstance(min_call_date, date) else None,
        "max_call_date": max_call_date.isoformat() if isinstance(max_call_date, date) else None,
        "empty_issuer_id_rows": int(empty_issuer_id or 0),
        "empty_transcript_ref_rows": int(empty_transcript_ref or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "orphan_issuer_id_rows": int(orphan_issuer_id or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show earnings_calls status and basic Layer 1 validation checks"
    )
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
