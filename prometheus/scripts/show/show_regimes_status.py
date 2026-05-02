"""Prometheus v2 – Show regimes status (Layer 2 validation).

Validates basic Layer 2 contracts for ``regimes``:
- regime_record_id, region, regime_label are non-empty
- regime_label is in a controlled set
- confidence is finite and within [0, 1]
- regime_embedding bytes are non-empty when present
- embedding_ref is either NULL or non-empty
- metadata is either NULL or a JSON object

Reports results for both runtime_db and historical_db.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from typing import Any, Optional, Sequence

from apatheon.core.database import get_db_manager

_ALLOWED_LABELS = ("CRISIS", "RISK_OFF", "CARRY", "NEUTRAL")
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
            COUNT(DISTINCT region) AS distinct_regions,
            MIN(as_of_date) AS min_as_of_date,
            MAX(as_of_date) AS max_as_of_date,
            MIN(created_at) AS min_created_at,
            MAX(created_at) AS max_created_at,
            SUM(CASE WHEN btrim(regime_record_id) = '' THEN 1 ELSE 0 END) AS empty_regime_record_id,
            SUM(CASE WHEN btrim(region) = '' THEN 1 ELSE 0 END) AS empty_region,
            SUM(CASE WHEN btrim(regime_label) = '' THEN 1 ELSE 0 END) AS empty_regime_label,
            SUM(CASE WHEN regime_label NOT IN {tuple(_ALLOWED_LABELS)!r} THEN 1 ELSE 0 END) AS bad_regime_label,
            SUM(CASE WHEN confidence IN {_NONFINITE} THEN 1 ELSE 0 END) AS confidence_nonfinite,
            SUM(CASE WHEN confidence < 0.0 OR confidence > 1.0 THEN 1 ELSE 0 END) AS confidence_out_of_range,
            SUM(CASE WHEN regime_embedding IS NOT NULL AND octet_length(regime_embedding) = 0 THEN 1 ELSE 0 END) AS empty_regime_embedding_bytes,
            SUM(CASE WHEN embedding_ref IS NOT NULL AND btrim(embedding_ref) = '' THEN 1 ELSE 0 END) AS empty_embedding_ref,
            SUM(CASE WHEN metadata IS NOT NULL AND jsonb_typeof(metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object
        FROM regimes
    """

    sql_preview = """
        SELECT regime_record_id, region, as_of_date, regime_label, confidence
        FROM regimes
        ORDER BY as_of_date DESC, region, regime_record_id
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
                empty_regime_record_id,
                empty_region,
                empty_regime_label,
                bad_regime_label,
                confidence_nonfinite,
                confidence_out_of_range,
                empty_regime_embedding_bytes,
                empty_embedding_ref,
                metadata_not_object,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = []
    for rid, region, as_of, label, conf in preview_rows:
        preview.append(
            {
                "regime_record_id": str(rid),
                "region": str(region),
                "as_of_date": as_of.isoformat() if isinstance(as_of, date) else None,
                "regime_label": str(label),
                "confidence": float(conf) if conf is not None else None,
            }
        )

    checks_passed = (
        int(empty_regime_record_id or 0) == 0
        and int(empty_region or 0) == 0
        and int(empty_regime_label or 0) == 0
        and int(bad_regime_label or 0) == 0
        and int(confidence_nonfinite or 0) == 0
        and int(confidence_out_of_range or 0) == 0
        and int(empty_regime_embedding_bytes or 0) == 0
        and int(empty_embedding_ref or 0) == 0
        and int(metadata_not_object or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_regions": int(distinct_regions or 0),
        "min_as_of_date": min_as_of_date.isoformat() if isinstance(min_as_of_date, date) else None,
        "max_as_of_date": max_as_of_date.isoformat() if isinstance(max_as_of_date, date) else None,
        "min_created_at": min_created_at.isoformat() if isinstance(min_created_at, datetime) else None,
        "max_created_at": max_created_at.isoformat() if isinstance(max_created_at, datetime) else None,
        "empty_regime_record_id_rows": int(empty_regime_record_id or 0),
        "empty_region_rows": int(empty_region or 0),
        "empty_regime_label_rows": int(empty_regime_label or 0),
        "bad_regime_label_rows": int(bad_regime_label or 0),
        "confidence_nonfinite_rows": int(confidence_nonfinite or 0),
        "confidence_out_of_range_rows": int(confidence_out_of_range or 0),
        "empty_regime_embedding_bytes_rows": int(empty_regime_embedding_bytes or 0),
        "empty_embedding_ref_rows": int(empty_embedding_ref or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show regimes status and basic Layer 2 validation checks")
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
