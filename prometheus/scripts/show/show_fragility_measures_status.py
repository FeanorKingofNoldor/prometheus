"""Prometheus v2 – Show fragility_measures status (Layer 2 validation).

Validates basic Layer 2 contracts for ``fragility_measures``:
- fragility_id, entity_type, entity_id are non-empty
- fragility_score is finite and within [0, 1]
- scenario_losses is either NULL or a JSON object
- metadata is either NULL or a JSON object

Reports results for both runtime_db and historical_db.

Note: scenario-loss semantics and component calibration are higher-level audits.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from typing import Any, Optional, Sequence

from apatheon.core.database import get_db_manager

_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


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
            MIN(as_of_date) AS min_as_of_date,
            MAX(as_of_date) AS max_as_of_date,
            MIN(created_at) AS min_created_at,
            MAX(created_at) AS max_created_at,
            SUM(CASE WHEN btrim(fragility_id) = '' THEN 1 ELSE 0 END) AS empty_fragility_id,
            SUM(CASE WHEN btrim(entity_type) = '' THEN 1 ELSE 0 END) AS empty_entity_type,
            SUM(CASE WHEN btrim(entity_id) = '' THEN 1 ELSE 0 END) AS empty_entity_id,
            SUM(CASE WHEN fragility_score IN {_NONFINITE} THEN 1 ELSE 0 END) AS fragility_score_nonfinite,
            SUM(CASE WHEN fragility_score < 0.0 OR fragility_score > 1.0 THEN 1 ELSE 0 END) AS fragility_score_out_of_range,
            SUM(CASE WHEN scenario_losses IS NOT NULL AND jsonb_typeof(scenario_losses) <> 'object' THEN 1 ELSE 0 END) AS scenario_losses_not_object,
            SUM(CASE WHEN metadata IS NOT NULL AND jsonb_typeof(metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object
        FROM fragility_measures
    """

    sql_preview = """
        SELECT fragility_id, entity_type, entity_id, as_of_date, fragility_score
        FROM fragility_measures
        ORDER BY as_of_date DESC, fragility_id
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                min_as_of_date,
                max_as_of_date,
                min_created_at,
                max_created_at,
                empty_fragility_id,
                empty_entity_type,
                empty_entity_id,
                fragility_score_nonfinite,
                fragility_score_out_of_range,
                scenario_losses_not_object,
                metadata_not_object,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = []
    for fid, etype, eid, as_of, score in preview_rows:
        preview.append(
            {
                "fragility_id": str(fid),
                "entity_type": str(etype),
                "entity_id": str(eid),
                "as_of_date": as_of.isoformat() if isinstance(as_of, date) else None,
                "fragility_score": float(score) if score is not None else None,
            }
        )

    checks_passed = (
        int(empty_fragility_id or 0) == 0
        and int(empty_entity_type or 0) == 0
        and int(empty_entity_id or 0) == 0
        and int(fragility_score_nonfinite or 0) == 0
        and int(fragility_score_out_of_range or 0) == 0
        and int(scenario_losses_not_object or 0) == 0
        and int(metadata_not_object or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "min_as_of_date": min_as_of_date.isoformat() if isinstance(min_as_of_date, date) else None,
        "max_as_of_date": max_as_of_date.isoformat() if isinstance(max_as_of_date, date) else None,
        "min_created_at": min_created_at.isoformat() if isinstance(min_created_at, datetime) else None,
        "max_created_at": max_created_at.isoformat() if isinstance(max_created_at, datetime) else None,
        "empty_fragility_id_rows": int(empty_fragility_id or 0),
        "empty_entity_type_rows": int(empty_entity_type or 0),
        "empty_entity_id_rows": int(empty_entity_id or 0),
        "fragility_score_nonfinite_rows": int(fragility_score_nonfinite or 0),
        "fragility_score_out_of_range_rows": int(fragility_score_out_of_range or 0),
        "scenario_losses_not_object_rows": int(scenario_losses_not_object or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show fragility_measures status and basic Layer 2 validation checks"
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
