"""Prometheus v2 – Show regime_transitions status (Layer 2 validation).

Validates basic Layer 2 contracts for ``regime_transitions``:
- transition_id, region, from_regime_label, to_regime_label are non-empty
- from/to labels are in a controlled set
- from_regime_label != to_regime_label
- metadata is either NULL or a JSON object

Reports results for both runtime_db and historical_db.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager


_ALLOWED_LABELS = ("CRISIS", "RISK_OFF", "CARRY", "NEUTRAL")


def _summarise(db, which: str) -> dict[str, Any]:
    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    sql = f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT region) AS distinct_regions,
            MIN(as_of_date) AS min_as_of_date,
            MAX(as_of_date) AS max_as_of_date,
            MIN(created_at) AS min_created_at,
            MAX(created_at) AS max_created_at,
            SUM(CASE WHEN btrim(transition_id) = '' THEN 1 ELSE 0 END) AS empty_transition_id,
            SUM(CASE WHEN btrim(region) = '' THEN 1 ELSE 0 END) AS empty_region,
            SUM(CASE WHEN btrim(from_regime_label) = '' THEN 1 ELSE 0 END) AS empty_from_label,
            SUM(CASE WHEN btrim(to_regime_label) = '' THEN 1 ELSE 0 END) AS empty_to_label,
            SUM(CASE WHEN from_regime_label NOT IN {tuple(_ALLOWED_LABELS)!r} THEN 1 ELSE 0 END) AS bad_from_label,
            SUM(CASE WHEN to_regime_label NOT IN {tuple(_ALLOWED_LABELS)!r} THEN 1 ELSE 0 END) AS bad_to_label,
            SUM(CASE WHEN from_regime_label = to_regime_label THEN 1 ELSE 0 END) AS self_transition_rows,
            SUM(CASE WHEN metadata IS NOT NULL AND jsonb_typeof(metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object
        FROM regime_transitions
    """

    sql_preview = """
        SELECT transition_id, region, as_of_date, from_regime_label, to_regime_label
        FROM regime_transitions
        ORDER BY as_of_date DESC, transition_id
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_regions,
                min_as_of_date,
                max_as_of_date,
                min_created_at,
                max_created_at,
                empty_transition_id,
                empty_region,
                empty_from_label,
                empty_to_label,
                bad_from_label,
                bad_to_label,
                self_transition_rows,
                metadata_not_object,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = []
    for tid, region, as_of, frm, to in preview_rows:
        preview.append(
            {
                "transition_id": str(tid),
                "region": str(region),
                "as_of_date": as_of.isoformat() if isinstance(as_of, date) else None,
                "from_regime_label": str(frm),
                "to_regime_label": str(to),
            }
        )

    checks_passed = (
        int(empty_transition_id or 0) == 0
        and int(empty_region or 0) == 0
        and int(empty_from_label or 0) == 0
        and int(empty_to_label or 0) == 0
        and int(bad_from_label or 0) == 0
        and int(bad_to_label or 0) == 0
        and int(self_transition_rows or 0) == 0
        and int(metadata_not_object or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_regions": int(distinct_regions or 0),
        "min_as_of_date": min_as_of_date.isoformat() if isinstance(min_as_of_date, date) else None,
        "max_as_of_date": max_as_of_date.isoformat() if isinstance(max_as_of_date, date) else None,
        "min_created_at": min_created_at.isoformat() if isinstance(min_created_at, datetime) else None,
        "max_created_at": max_created_at.isoformat() if isinstance(max_created_at, datetime) else None,
        "empty_transition_id_rows": int(empty_transition_id or 0),
        "empty_region_rows": int(empty_region or 0),
        "empty_from_regime_label_rows": int(empty_from_label or 0),
        "empty_to_regime_label_rows": int(empty_to_label or 0),
        "bad_from_regime_label_rows": int(bad_from_label or 0),
        "bad_to_regime_label_rows": int(bad_to_label or 0),
        "self_transition_rows": int(self_transition_rows or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show regime_transitions status and basic Layer 2 validation checks"
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
