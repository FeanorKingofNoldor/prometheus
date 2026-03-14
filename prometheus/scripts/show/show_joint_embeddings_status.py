"""Prometheus v2 – Show joint_embeddings status (Layer 2 validation).

Validates basic Layer 2 contracts for ``joint_embeddings``:
- joint_type, model_id are non-empty
- entity_scope is a JSON object
- vector_ref is either NULL or non-empty
- at least one of (vector, vector_ref) is present
- vector bytes are non-empty when present
- no duplicate keys on (joint_type, as_of_date, model_id, entity_scope)

Reports results for both runtime_db and historical_db.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
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
        WITH dups AS (
            SELECT joint_type, as_of_date, model_id, entity_scope, COUNT(*) AS cnt
            FROM joint_embeddings
            GROUP BY 1,2,3,4
            HAVING COUNT(*) > 1
        )
        SELECT
            COUNT(*) AS total,
            MIN(as_of_date) AS min_as_of_date,
            MAX(as_of_date) AS max_as_of_date,
            MIN(created_at) AS min_created_at,
            MAX(created_at) AS max_created_at,
            SUM(CASE WHEN btrim(joint_type) = '' THEN 1 ELSE 0 END) AS empty_joint_type,
            SUM(CASE WHEN btrim(model_id) = '' THEN 1 ELSE 0 END) AS empty_model_id,
            SUM(CASE WHEN jsonb_typeof(entity_scope) <> 'object' THEN 1 ELSE 0 END) AS entity_scope_not_object,
            SUM(CASE WHEN vector_ref IS NOT NULL AND btrim(vector_ref) = '' THEN 1 ELSE 0 END) AS empty_vector_ref,
            SUM(CASE WHEN vector IS NULL AND vector_ref IS NULL THEN 1 ELSE 0 END) AS missing_vector_and_ref,
            SUM(CASE WHEN vector IS NOT NULL AND octet_length(vector) = 0 THEN 1 ELSE 0 END) AS empty_vector_bytes,
            (SELECT COALESCE(SUM(cnt - 1), 0) FROM dups) AS duplicate_key_rows
        FROM joint_embeddings
    """

    sql_preview = """
        SELECT joint_id, joint_type, as_of_date, model_id, vector_ref, created_at
        FROM joint_embeddings
        ORDER BY created_at DESC, joint_id DESC
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
                empty_joint_type,
                empty_model_id,
                entity_scope_not_object,
                empty_vector_ref,
                missing_vector_and_ref,
                empty_vector_bytes,
                duplicate_key_rows,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = []
    for jid, jtype, as_of, mid, vref, created_at in preview_rows:
        preview.append(
            {
                "joint_id": int(jid) if jid is not None else None,
                "joint_type": str(jtype),
                "as_of_date": as_of.isoformat() if isinstance(as_of, date) else None,
                "model_id": str(mid),
                "vector_ref": str(vref) if vref is not None else None,
                "created_at": created_at.isoformat() if isinstance(created_at, datetime) else None,
            }
        )

    checks_passed = (
        int(empty_joint_type or 0) == 0
        and int(empty_model_id or 0) == 0
        and int(entity_scope_not_object or 0) == 0
        and int(empty_vector_ref or 0) == 0
        and int(missing_vector_and_ref or 0) == 0
        and int(empty_vector_bytes or 0) == 0
        and int(duplicate_key_rows or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "min_as_of_date": min_as_of_date.isoformat() if isinstance(min_as_of_date, date) else None,
        "max_as_of_date": max_as_of_date.isoformat() if isinstance(max_as_of_date, date) else None,
        "min_created_at": min_created_at.isoformat() if isinstance(min_created_at, datetime) else None,
        "max_created_at": max_created_at.isoformat() if isinstance(max_created_at, datetime) else None,
        "empty_joint_type_rows": int(empty_joint_type or 0),
        "empty_model_id_rows": int(empty_model_id or 0),
        "entity_scope_not_object_rows": int(entity_scope_not_object or 0),
        "empty_vector_ref_rows": int(empty_vector_ref or 0),
        "missing_vector_and_ref_rows": int(missing_vector_and_ref or 0),
        "empty_vector_bytes_rows": int(empty_vector_bytes or 0),
        "duplicate_key_rows": int(duplicate_key_rows or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show joint_embeddings status and basic Layer 2 validation checks"
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
