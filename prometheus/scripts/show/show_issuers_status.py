"""Prometheus v2 – Show issuers status (Layer 0 validation).

Validates core Layer 0 contracts for the ``issuers`` table:
- issuer_id is non-empty (PK ensures uniqueness but not non-empty)
- issuer_type is non-empty and consistently formatted
- name is non-empty

Reports results for both runtime_db and historical_db.
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager


def _summarise(db, which: str) -> dict[str, Any]:
    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    sql_overview = """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN btrim(issuer_id) = '' THEN 1 ELSE 0 END) AS empty_issuer_id,
            SUM(CASE WHEN issuer_type IS NULL OR btrim(issuer_type) = '' THEN 1 ELSE 0 END) AS empty_issuer_type,
            SUM(CASE WHEN name IS NULL OR btrim(name) = '' THEN 1 ELSE 0 END) AS empty_name,
            SUM(CASE WHEN issuer_type IS NOT NULL AND issuer_type !~ '^[A-Z_]+$' THEN 1 ELSE 0 END) AS bad_issuer_type_format
        FROM issuers
    """

    sql_type_breakdown = """
        SELECT issuer_type, COUNT(*)
        FROM issuers
        GROUP BY issuer_type
        ORDER BY COUNT(*) DESC, issuer_type
        LIMIT 50
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_overview)
            (
                total,
                empty_issuer_id,
                empty_issuer_type,
                empty_name,
                bad_issuer_type_format,
            ) = cur.fetchone()

            cur.execute(sql_type_breakdown)
            breakdown_rows = cur.fetchall()
        finally:
            cur.close()

    breakdown = {str(t): int(n) for (t, n) in breakdown_rows}

    checks_passed = (
        int(empty_issuer_id or 0) == 0
        and int(empty_issuer_type or 0) == 0
        and int(empty_name or 0) == 0
        and int(bad_issuer_type_format or 0) == 0
    )

    return {
        "total_issuers": int(total or 0),
        "empty_issuer_id_rows": int(empty_issuer_id or 0),
        "empty_issuer_type_rows": int(empty_issuer_type or 0),
        "empty_name_rows": int(empty_name or 0),
        "bad_issuer_type_format_rows": int(bad_issuer_type_format or 0),
        "issuer_type_breakdown": breakdown,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show issuers status and Layer 0 validation checks")
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
