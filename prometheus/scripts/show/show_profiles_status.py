"""Prometheus v2 – Show profiles status (Layer 2 validation).

Validates basic Layer 2 contracts for ``profiles``:
- issuer_id is non-empty
- issuer_id exists in issuers (checked via LEFT JOIN)
- structured is a JSON object
- risk_flags is a JSON object
- embedding_vector_ref is either NULL or non-empty

Reports results for both runtime_db and historical_db.

Note: profile completeness/coverage is a higher-level audit.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager


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
            COUNT(DISTINCT p.issuer_id) AS distinct_issuers,
            MIN(p.as_of_date) AS min_as_of_date,
            MAX(p.as_of_date) AS max_as_of_date,
            SUM(CASE WHEN btrim(p.issuer_id) = '' THEN 1 ELSE 0 END) AS empty_issuer_id,
            SUM(CASE WHEN p.embedding_vector_ref IS NOT NULL AND btrim(p.embedding_vector_ref) = '' THEN 1 ELSE 0 END) AS empty_embedding_ref,
            SUM(CASE WHEN jsonb_typeof(p.structured) <> 'object' THEN 1 ELSE 0 END) AS structured_not_object,
            SUM(CASE WHEN jsonb_typeof(p.risk_flags) <> 'object' THEN 1 ELSE 0 END) AS risk_flags_not_object,
            SUM(CASE WHEN i.issuer_id IS NULL THEN 1 ELSE 0 END) AS orphan_issuer_id
        FROM profiles p
        LEFT JOIN issuers i ON i.issuer_id = p.issuer_id
    """

    sql_preview = """
        SELECT profile_id, issuer_id, as_of_date, embedding_vector_ref
        FROM profiles
        ORDER BY as_of_date DESC, issuer_id, profile_id DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_issuers,
                min_as_of_date,
                max_as_of_date,
                empty_issuer_id,
                empty_embedding_ref,
                structured_not_object,
                risk_flags_not_object,
                orphan_issuer_id,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "profile_id": int(pid) if pid is not None else None,
            "issuer_id": str(issuer_id),
            "as_of_date": as_of.isoformat() if isinstance(as_of, date) else None,
            "embedding_vector_ref": str(ref) if ref is not None else None,
        }
        for pid, issuer_id, as_of, ref in preview_rows
    ]

    checks_passed = (
        int(empty_issuer_id or 0) == 0
        and int(empty_embedding_ref or 0) == 0
        and int(structured_not_object or 0) == 0
        and int(risk_flags_not_object or 0) == 0
        and int(orphan_issuer_id or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_issuers": int(distinct_issuers or 0),
        "min_as_of_date": min_as_of_date.isoformat() if isinstance(min_as_of_date, date) else None,
        "max_as_of_date": max_as_of_date.isoformat() if isinstance(max_as_of_date, date) else None,
        "empty_issuer_id_rows": int(empty_issuer_id or 0),
        "empty_embedding_vector_ref_rows": int(empty_embedding_ref or 0),
        "structured_not_object_rows": int(structured_not_object or 0),
        "risk_flags_not_object_rows": int(risk_flags_not_object or 0),
        "orphan_issuer_id_rows": int(orphan_issuer_id or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show profiles status and basic Layer 2 validation checks")
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
