"""Prometheus v2 – Show text_embeddings status (Layer 2 validation).

Validates basic Layer 2 contracts for ``text_embeddings``:
- source_type, source_id, model_id are non-empty
- vector_ref is either NULL or non-empty
- at least one of (vector, vector_ref) is present
- vector bytes are non-empty when present

Reports results for both runtime_db and historical_db.

Note: embedding dimension/model correctness is a higher-level audit.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
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
            MIN(created_at) AS min_created_at,
            MAX(created_at) AS max_created_at,
            SUM(CASE WHEN btrim(source_type) = '' THEN 1 ELSE 0 END) AS empty_source_type,
            SUM(CASE WHEN btrim(source_id) = '' THEN 1 ELSE 0 END) AS empty_source_id,
            SUM(CASE WHEN btrim(model_id) = '' THEN 1 ELSE 0 END) AS empty_model_id,
            SUM(CASE WHEN vector_ref IS NOT NULL AND btrim(vector_ref) = '' THEN 1 ELSE 0 END) AS empty_vector_ref,
            SUM(CASE WHEN vector IS NULL AND vector_ref IS NULL THEN 1 ELSE 0 END) AS missing_vector_and_ref,
            SUM(CASE WHEN vector IS NOT NULL AND octet_length(vector) = 0 THEN 1 ELSE 0 END) AS empty_vector_bytes
        FROM text_embeddings
    """

    sql_preview = """
        SELECT embedding_id, source_type, source_id, model_id, vector_ref, created_at
        FROM text_embeddings
        ORDER BY created_at DESC, embedding_id DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                min_created_at,
                max_created_at,
                empty_source_type,
                empty_source_id,
                empty_model_id,
                empty_vector_ref,
                missing_vector_and_ref,
                empty_vector_bytes,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = []
    for eid, stype, sid, mid, vref, created_at in preview_rows:
        preview.append(
            {
                "embedding_id": int(eid) if eid is not None else None,
                "source_type": str(stype),
                "source_id": str(sid),
                "model_id": str(mid),
                "vector_ref": str(vref) if vref is not None else None,
                "created_at": created_at.isoformat() if isinstance(created_at, datetime) else None,
            }
        )

    checks_passed = (
        int(empty_source_type or 0) == 0
        and int(empty_source_id or 0) == 0
        and int(empty_model_id or 0) == 0
        and int(empty_vector_ref or 0) == 0
        and int(missing_vector_and_ref or 0) == 0
        and int(empty_vector_bytes or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "min_created_at": min_created_at.isoformat() if isinstance(min_created_at, datetime) else None,
        "max_created_at": max_created_at.isoformat() if isinstance(max_created_at, datetime) else None,
        "empty_source_type_rows": int(empty_source_type or 0),
        "empty_source_id_rows": int(empty_source_id or 0),
        "empty_model_id_rows": int(empty_model_id or 0),
        "empty_vector_ref_rows": int(empty_vector_ref or 0),
        "missing_vector_and_ref_rows": int(missing_vector_and_ref or 0),
        "empty_vector_bytes_rows": int(empty_vector_bytes or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show text_embeddings status and basic Layer 2 validation checks"
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
