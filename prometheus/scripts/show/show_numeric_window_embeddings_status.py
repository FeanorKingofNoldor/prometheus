"""Prometheus v2 – Show numeric_window_embeddings status (Layer 2 validation).

Validates basic Layer 2 contracts for ``numeric_window_embeddings``:
- entity_type, entity_id, model_id are non-empty
- window_spec is a JSON object
- vector_ref is either NULL or non-empty
- at least one of (vector, vector_ref) is present
- vector bytes are non-empty when present
- no duplicate keys on (entity_type, entity_id, as_of_date, model_id, window_spec)

Reports results for both runtime_db and historical_db.
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
        WITH dups AS (
            SELECT entity_type, entity_id, as_of_date, model_id, window_spec, COUNT(*) AS cnt
            FROM numeric_window_embeddings
            GROUP BY 1,2,3,4,5
            HAVING COUNT(*) > 1
        )
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT (entity_type || ':' || entity_id)) AS distinct_entities,
            MIN(as_of_date) AS min_as_of_date,
            MAX(as_of_date) AS max_as_of_date,
            MIN(created_at) AS min_created_at,
            MAX(created_at) AS max_created_at,
            SUM(CASE WHEN btrim(entity_type) = '' THEN 1 ELSE 0 END) AS empty_entity_type,
            SUM(CASE WHEN btrim(entity_id) = '' THEN 1 ELSE 0 END) AS empty_entity_id,
            SUM(CASE WHEN btrim(model_id) = '' THEN 1 ELSE 0 END) AS empty_model_id,
            SUM(CASE WHEN jsonb_typeof(window_spec) <> 'object' THEN 1 ELSE 0 END) AS window_spec_not_object,
            SUM(CASE WHEN vector_ref IS NOT NULL AND btrim(vector_ref) = '' THEN 1 ELSE 0 END) AS empty_vector_ref,
            SUM(CASE WHEN vector IS NULL AND vector_ref IS NULL THEN 1 ELSE 0 END) AS missing_vector_and_ref,
            SUM(CASE WHEN vector IS NOT NULL AND octet_length(vector) = 0 THEN 1 ELSE 0 END) AS empty_vector_bytes,
            (SELECT COALESCE(SUM(cnt - 1), 0) FROM dups) AS duplicate_key_rows
        FROM numeric_window_embeddings
    """

    sql_preview = """
        SELECT embedding_id, entity_type, entity_id, as_of_date, model_id, vector_ref, created_at
        FROM numeric_window_embeddings
        ORDER BY created_at DESC, embedding_id DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_entities,
                min_as_of_date,
                max_as_of_date,
                min_created_at,
                max_created_at,
                empty_entity_type,
                empty_entity_id,
                empty_model_id,
                window_spec_not_object,
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
    for eid, etype, entity_id, as_of, mid, vref, created_at in preview_rows:
        preview.append(
            {
                "embedding_id": int(eid) if eid is not None else None,
                "entity_type": str(etype),
                "entity_id": str(entity_id),
                "as_of_date": as_of.isoformat() if isinstance(as_of, date) else None,
                "model_id": str(mid),
                "vector_ref": str(vref) if vref is not None else None,
                "created_at": created_at.isoformat() if isinstance(created_at, datetime) else None,
            }
        )

    checks_passed = (
        int(empty_entity_type or 0) == 0
        and int(empty_entity_id or 0) == 0
        and int(empty_model_id or 0) == 0
        and int(window_spec_not_object or 0) == 0
        and int(empty_vector_ref or 0) == 0
        and int(missing_vector_and_ref or 0) == 0
        and int(empty_vector_bytes or 0) == 0
        and int(duplicate_key_rows or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_entities": int(distinct_entities or 0),
        "min_as_of_date": min_as_of_date.isoformat() if isinstance(min_as_of_date, date) else None,
        "max_as_of_date": max_as_of_date.isoformat() if isinstance(max_as_of_date, date) else None,
        "min_created_at": min_created_at.isoformat() if isinstance(min_created_at, datetime) else None,
        "max_created_at": max_created_at.isoformat() if isinstance(max_created_at, datetime) else None,
        "empty_entity_type_rows": int(empty_entity_type or 0),
        "empty_entity_id_rows": int(empty_entity_id or 0),
        "empty_model_id_rows": int(empty_model_id or 0),
        "window_spec_not_object_rows": int(window_spec_not_object or 0),
        "empty_vector_ref_rows": int(empty_vector_ref or 0),
        "missing_vector_and_ref_rows": int(missing_vector_and_ref or 0),
        "empty_vector_bytes_rows": int(empty_vector_bytes or 0),
        "duplicate_key_rows": int(duplicate_key_rows or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show numeric_window_embeddings status and basic Layer 2 validation checks"
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
