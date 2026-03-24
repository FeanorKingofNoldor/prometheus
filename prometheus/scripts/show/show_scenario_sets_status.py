"""Prometheus v2 – Show scenario_sets status (Layer 2 validation).

Validates basic Layer 2 contracts for ``scenario_sets``:
- scenario_set_id, name, category are non-empty
- category is in a controlled set (case-insensitive)
- horizon_days > 0 and num_paths > 0
- base_date_start <= base_date_end when both present
- base_universe_filter, generator_spec, metadata are JSON objects when present
- created_by is either NULL or non-empty

Reports results for both runtime_db and historical_db.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager

_ALLOWED_CATEGORIES = ("HISTORICAL", "BOOTSTRAP", "STRESSED")


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
            MIN(created_at) AS min_created_at,
            MAX(created_at) AS max_created_at,
            SUM(CASE WHEN btrim(scenario_set_id) = '' THEN 1 ELSE 0 END) AS empty_scenario_set_id,
            SUM(CASE WHEN btrim(name) = '' THEN 1 ELSE 0 END) AS empty_name,
            SUM(CASE WHEN btrim(category) = '' THEN 1 ELSE 0 END) AS empty_category,
            SUM(CASE WHEN upper(category) NOT IN {tuple(_ALLOWED_CATEGORIES)!r} THEN 1 ELSE 0 END) AS bad_category,
            SUM(CASE WHEN horizon_days <= 0 THEN 1 ELSE 0 END) AS bad_horizon_days,
            SUM(CASE WHEN num_paths <= 0 THEN 1 ELSE 0 END) AS bad_num_paths,
            SUM(CASE WHEN base_date_start IS NOT NULL AND base_date_end IS NOT NULL AND base_date_start > base_date_end THEN 1 ELSE 0 END) AS bad_base_date_window,
            SUM(CASE WHEN base_universe_filter IS NOT NULL AND jsonb_typeof(base_universe_filter) <> 'object' THEN 1 ELSE 0 END) AS base_universe_filter_not_object,
            SUM(CASE WHEN generator_spec IS NOT NULL AND jsonb_typeof(generator_spec) <> 'object' THEN 1 ELSE 0 END) AS generator_spec_not_object,
            SUM(CASE WHEN metadata IS NOT NULL AND jsonb_typeof(metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object,
            SUM(CASE WHEN created_by IS NOT NULL AND btrim(created_by) = '' THEN 1 ELSE 0 END) AS empty_created_by
        FROM scenario_sets
    """

    sql_preview = """
        SELECT scenario_set_id, name, category, horizon_days, num_paths, base_date_start, base_date_end, created_at
        FROM scenario_sets
        ORDER BY created_at DESC, scenario_set_id
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
                empty_scenario_set_id,
                empty_name,
                empty_category,
                bad_category,
                bad_horizon_days,
                bad_num_paths,
                bad_base_date_window,
                base_universe_filter_not_object,
                generator_spec_not_object,
                metadata_not_object,
                empty_created_by,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = []
    for sid, name, cat, horizon, num_paths, base_start, base_end, created_at in preview_rows:
        preview.append(
            {
                "scenario_set_id": str(sid),
                "name": str(name),
                "category": str(cat),
                "horizon_days": int(horizon) if horizon is not None else None,
                "num_paths": int(num_paths) if num_paths is not None else None,
                "base_date_start": base_start.isoformat() if isinstance(base_start, date) else None,
                "base_date_end": base_end.isoformat() if isinstance(base_end, date) else None,
                "created_at": created_at.isoformat() if isinstance(created_at, datetime) else None,
            }
        )

    checks_passed = (
        int(empty_scenario_set_id or 0) == 0
        and int(empty_name or 0) == 0
        and int(empty_category or 0) == 0
        and int(bad_category or 0) == 0
        and int(bad_horizon_days or 0) == 0
        and int(bad_num_paths or 0) == 0
        and int(bad_base_date_window or 0) == 0
        and int(base_universe_filter_not_object or 0) == 0
        and int(generator_spec_not_object or 0) == 0
        and int(metadata_not_object or 0) == 0
        and int(empty_created_by or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "min_created_at": min_created_at.isoformat() if isinstance(min_created_at, datetime) else None,
        "max_created_at": max_created_at.isoformat() if isinstance(max_created_at, datetime) else None,
        "empty_scenario_set_id_rows": int(empty_scenario_set_id or 0),
        "empty_name_rows": int(empty_name or 0),
        "empty_category_rows": int(empty_category or 0),
        "bad_category_rows": int(bad_category or 0),
        "bad_horizon_days_rows": int(bad_horizon_days or 0),
        "bad_num_paths_rows": int(bad_num_paths or 0),
        "bad_base_date_window_rows": int(bad_base_date_window or 0),
        "base_universe_filter_not_object_rows": int(base_universe_filter_not_object or 0),
        "generator_spec_not_object_rows": int(generator_spec_not_object or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "empty_created_by_rows": int(empty_created_by or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show scenario_sets status and basic Layer 2 validation checks"
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
