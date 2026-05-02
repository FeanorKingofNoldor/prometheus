"""Prometheus v2 – Show soft_target_classes status (Layer 2 validation).

Validates basic Layer 2 contracts for ``soft_target_classes``:
- soft_target_id, entity_type, entity_id are non-empty
- soft_target_class is non-empty and in a controlled set
- score/component fields are finite and within [0, 100]
- metadata is either NULL or a JSON object

Reports results for both runtime_db and historical_db.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from typing import Any, Optional, Sequence

from apatheon.core.database import get_db_manager

_ALLOWED_CLASSES = ("STABLE", "WATCH", "FRAGILE", "TARGETABLE", "BREAKER")
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
            SUM(CASE WHEN btrim(soft_target_id) = '' THEN 1 ELSE 0 END) AS empty_soft_target_id,
            SUM(CASE WHEN btrim(entity_type) = '' THEN 1 ELSE 0 END) AS empty_entity_type,
            SUM(CASE WHEN btrim(entity_id) = '' THEN 1 ELSE 0 END) AS empty_entity_id,
            SUM(CASE WHEN btrim(soft_target_class) = '' THEN 1 ELSE 0 END) AS empty_soft_target_class,
            SUM(CASE WHEN soft_target_class NOT IN {tuple(_ALLOWED_CLASSES)!r} THEN 1 ELSE 0 END) AS bad_soft_target_class,
            SUM(CASE WHEN soft_target_score IN {_NONFINITE} THEN 1 ELSE 0 END) AS soft_target_score_nonfinite,
            SUM(CASE WHEN soft_target_score < 0.0 OR soft_target_score > 100.0 THEN 1 ELSE 0 END) AS soft_target_score_out_of_range,
            SUM(CASE WHEN instability IN {_NONFINITE} THEN 1 ELSE 0 END) AS instability_nonfinite,
            SUM(CASE WHEN instability < 0.0 OR instability > 100.0 THEN 1 ELSE 0 END) AS instability_out_of_range,
            SUM(CASE WHEN high_fragility IN {_NONFINITE} THEN 1 ELSE 0 END) AS high_fragility_nonfinite,
            SUM(CASE WHEN high_fragility < 0.0 OR high_fragility > 100.0 THEN 1 ELSE 0 END) AS high_fragility_out_of_range,
            SUM(CASE WHEN complacent_pricing IN {_NONFINITE} THEN 1 ELSE 0 END) AS complacent_pricing_nonfinite,
            SUM(CASE WHEN complacent_pricing < 0.0 OR complacent_pricing > 100.0 THEN 1 ELSE 0 END) AS complacent_pricing_out_of_range,
            SUM(CASE WHEN metadata IS NOT NULL AND jsonb_typeof(metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object
        FROM soft_target_classes
    """

    sql_preview = """
        SELECT soft_target_id, entity_type, entity_id, as_of_date, soft_target_class, soft_target_score
        FROM soft_target_classes
        ORDER BY as_of_date DESC, soft_target_id
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
                empty_soft_target_id,
                empty_entity_type,
                empty_entity_id,
                empty_soft_target_class,
                bad_soft_target_class,
                soft_target_score_nonfinite,
                soft_target_score_out_of_range,
                instability_nonfinite,
                instability_out_of_range,
                high_fragility_nonfinite,
                high_fragility_out_of_range,
                complacent_pricing_nonfinite,
                complacent_pricing_out_of_range,
                metadata_not_object,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = []
    for sid, etype, eid, as_of, cls, score in preview_rows:
        preview.append(
            {
                "soft_target_id": str(sid),
                "entity_type": str(etype),
                "entity_id": str(eid),
                "as_of_date": as_of.isoformat() if isinstance(as_of, date) else None,
                "soft_target_class": str(cls),
                "soft_target_score": float(score) if score is not None else None,
            }
        )

    checks_passed = (
        int(empty_soft_target_id or 0) == 0
        and int(empty_entity_type or 0) == 0
        and int(empty_entity_id or 0) == 0
        and int(empty_soft_target_class or 0) == 0
        and int(bad_soft_target_class or 0) == 0
        and int(soft_target_score_nonfinite or 0) == 0
        and int(soft_target_score_out_of_range or 0) == 0
        and int(instability_nonfinite or 0) == 0
        and int(instability_out_of_range or 0) == 0
        and int(high_fragility_nonfinite or 0) == 0
        and int(high_fragility_out_of_range or 0) == 0
        and int(complacent_pricing_nonfinite or 0) == 0
        and int(complacent_pricing_out_of_range or 0) == 0
        and int(metadata_not_object or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "min_as_of_date": min_as_of_date.isoformat() if isinstance(min_as_of_date, date) else None,
        "max_as_of_date": max_as_of_date.isoformat() if isinstance(max_as_of_date, date) else None,
        "min_created_at": min_created_at.isoformat() if isinstance(min_created_at, datetime) else None,
        "max_created_at": max_created_at.isoformat() if isinstance(max_created_at, datetime) else None,
        "empty_soft_target_id_rows": int(empty_soft_target_id or 0),
        "empty_entity_type_rows": int(empty_entity_type or 0),
        "empty_entity_id_rows": int(empty_entity_id or 0),
        "empty_soft_target_class_rows": int(empty_soft_target_class or 0),
        "bad_soft_target_class_rows": int(bad_soft_target_class or 0),
        "soft_target_score_nonfinite_rows": int(soft_target_score_nonfinite or 0),
        "soft_target_score_out_of_range_rows": int(soft_target_score_out_of_range or 0),
        "instability_nonfinite_rows": int(instability_nonfinite or 0),
        "instability_out_of_range_rows": int(instability_out_of_range or 0),
        "high_fragility_nonfinite_rows": int(high_fragility_nonfinite or 0),
        "high_fragility_out_of_range_rows": int(high_fragility_out_of_range or 0),
        "complacent_pricing_nonfinite_rows": int(complacent_pricing_nonfinite or 0),
        "complacent_pricing_out_of_range_rows": int(complacent_pricing_out_of_range or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show soft_target_classes status and basic Layer 2 validation checks"
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
