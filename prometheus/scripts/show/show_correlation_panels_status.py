"""Prometheus v2 – Show correlation_panels status (Layer 1 validation).

Validates basic Layer 1 contracts for ``correlation_panels``:
- panel_id is non-empty
- start_date <= end_date
- universe_spec is a JSON object
- matrix_ref is non-empty (should point to an immutable artifact)

Reports results for both runtime_db and historical_db.

Note: matrix shape correctness and artifact immutability are higher-level
concerns; this validator focuses on schema-level sanity.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
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
            MIN(start_date) AS min_start_date,
            MAX(end_date) AS max_end_date,
            SUM(CASE WHEN btrim(panel_id) = '' THEN 1 ELSE 0 END) AS empty_panel_id,
            SUM(CASE WHEN start_date > end_date THEN 1 ELSE 0 END) AS bad_date_range,
            SUM(CASE WHEN btrim(matrix_ref) = '' THEN 1 ELSE 0 END) AS empty_matrix_ref,
            SUM(CASE WHEN universe_spec IS NOT NULL AND jsonb_typeof(universe_spec) <> 'object' THEN 1 ELSE 0 END) AS universe_spec_not_object
        FROM correlation_panels
    """

    sql_preview = """
        SELECT panel_id, start_date, end_date, matrix_ref, created_at
        FROM correlation_panels
        ORDER BY created_at DESC, panel_id
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                min_start_date,
                max_end_date,
                empty_panel_id,
                bad_date_range,
                empty_matrix_ref,
                universe_spec_not_object,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "panel_id": str(panel_id),
            "start_date": sd.isoformat() if isinstance(sd, date) else None,
            "end_date": ed.isoformat() if isinstance(ed, date) else None,
            "matrix_ref": str(matrix_ref),
            "created_at": ca.isoformat() if isinstance(ca, datetime) else None,
        }
        for panel_id, sd, ed, matrix_ref, ca in preview_rows
    ]

    checks_passed = (
        int(empty_panel_id or 0) == 0
        and int(bad_date_range or 0) == 0
        and int(empty_matrix_ref or 0) == 0
        and int(universe_spec_not_object or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "min_start_date": min_start_date.isoformat() if isinstance(min_start_date, date) else None,
        "max_end_date": max_end_date.isoformat() if isinstance(max_end_date, date) else None,
        "empty_panel_id_rows": int(empty_panel_id or 0),
        "bad_date_range_rows": int(bad_date_range or 0),
        "empty_matrix_ref_rows": int(empty_matrix_ref or 0),
        "universe_spec_not_object_rows": int(universe_spec_not_object or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show correlation_panels status and basic Layer 1 validation checks",
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
