"""Prometheus v2 – Show macro_events status (Layer 1 validation).

Validates basic Layer 1 contracts for ``macro_events``:
- event_type is non-empty
- timestamp is present
- description is non-empty
- country is either NULL or non-empty
- text_ref is either NULL or non-empty
- metadata is either NULL or a JSON object

Reports results for both runtime_db and historical_db.

Note: timestamp semantics (e.g. scheduled vs realised) and source
normalisation are higher-level audits.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
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
            MIN(me.timestamp) AS min_ts,
            MAX(me.timestamp) AS max_ts,
            SUM(CASE WHEN btrim(me.event_type) = '' THEN 1 ELSE 0 END) AS empty_event_type,
            SUM(CASE WHEN me.country IS NOT NULL AND btrim(me.country) = '' THEN 1 ELSE 0 END) AS empty_country,
            SUM(CASE WHEN btrim(me.description) = '' THEN 1 ELSE 0 END) AS empty_description,
            SUM(CASE WHEN me.text_ref IS NOT NULL AND btrim(me.text_ref) = '' THEN 1 ELSE 0 END) AS empty_text_ref,
            SUM(CASE WHEN me.metadata IS NOT NULL AND jsonb_typeof(me.metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object
        FROM macro_events me
    """

    sql_preview = """
        SELECT event_id, timestamp, event_type, country, description
        FROM macro_events
        ORDER BY timestamp DESC, event_id DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                min_ts,
                max_ts,
                empty_event_type,
                empty_country,
                empty_description,
                empty_text_ref,
                metadata_not_object,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = []
    for eid, ts, etype, country, desc in preview_rows:
        preview.append(
            {
                "event_id": int(eid) if eid is not None else None,
                "timestamp": ts.isoformat() if isinstance(ts, datetime) else None,
                "event_type": str(etype),
                "country": str(country) if country is not None else None,
                "description": str(desc),
            }
        )

    checks_passed = (
        int(empty_event_type or 0) == 0
        and int(empty_country or 0) == 0
        and int(empty_description or 0) == 0
        and int(empty_text_ref or 0) == 0
        and int(metadata_not_object or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "min_timestamp": min_ts.isoformat() if isinstance(min_ts, datetime) else None,
        "max_timestamp": max_ts.isoformat() if isinstance(max_ts, datetime) else None,
        "empty_event_type_rows": int(empty_event_type or 0),
        "empty_country_rows": int(empty_country or 0),
        "empty_description_rows": int(empty_description or 0),
        "empty_text_ref_rows": int(empty_text_ref or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show macro_events status and basic Layer 1 validation checks"
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
