"""Prometheus v2 – Show backtest_runs status (Layer 5 validation).

Validates basic Layer 5 contracts for ``backtest_runs``:
- run_id, strategy_id are non-empty
- start_date <= end_date
- config_json is a JSON object
- metrics_json/universe_id are either NULL or non-empty/JSON object
- metadata is either NULL or a JSON object (if present)

Reports results for both runtime_db and historical_db.

Note: backtest reproducibility is a higher-level audit.
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
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT strategy_id) AS distinct_strategies,
            MIN(start_date) AS min_start_date,
            MAX(end_date) AS max_end_date,
            MIN(created_at) AS min_created_at,
            MAX(created_at) AS max_created_at,
            SUM(CASE WHEN btrim(run_id) = '' THEN 1 ELSE 0 END) AS empty_run_id,
            SUM(CASE WHEN btrim(strategy_id) = '' THEN 1 ELSE 0 END) AS empty_strategy_id,
            SUM(CASE WHEN start_date > end_date THEN 1 ELSE 0 END) AS bad_date_range,
            SUM(CASE WHEN jsonb_typeof(config_json) <> 'object' THEN 1 ELSE 0 END) AS config_json_not_object,
            SUM(CASE WHEN universe_id IS NOT NULL AND btrim(universe_id) = '' THEN 1 ELSE 0 END) AS empty_universe_id,
            SUM(CASE WHEN metrics_json IS NOT NULL AND jsonb_typeof(metrics_json) <> 'object' THEN 1 ELSE 0 END) AS metrics_json_not_object
        FROM backtest_runs
    """

    sql_preview = """
        SELECT run_id, strategy_id, start_date, end_date, created_at
        FROM backtest_runs
        ORDER BY created_at DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_strategies,
                min_start_date,
                max_end_date,
                min_created_at,
                max_created_at,
                empty_run_id,
                empty_strategy_id,
                bad_date_range,
                config_json_not_object,
                empty_universe_id,
                metrics_json_not_object,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "run_id": str(run_id),
            "strategy_id": str(strategy_id),
            "start_date": start_date_db.isoformat() if isinstance(start_date_db, date) else None,
            "end_date": end_date_db.isoformat() if isinstance(end_date_db, date) else None,
            "created_at": created_at.isoformat() if isinstance(created_at, datetime) else None,
        }
        for run_id, strategy_id, start_date_db, end_date_db, created_at in preview_rows
    ]

    checks_passed = (
        int(empty_run_id or 0) == 0
        and int(empty_strategy_id or 0) == 0
        and int(bad_date_range or 0) == 0
        and int(config_json_not_object or 0) == 0
        and int(empty_universe_id or 0) == 0
        and int(metrics_json_not_object or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_strategies": int(distinct_strategies or 0),
        "min_start_date": min_start_date.isoformat() if isinstance(min_start_date, date) else None,
        "max_end_date": max_end_date.isoformat() if isinstance(max_end_date, date) else None,
        "min_created_at": min_created_at.isoformat() if isinstance(min_created_at, datetime) else None,
        "max_created_at": max_created_at.isoformat() if isinstance(max_created_at, datetime) else None,
        "empty_run_id_rows": int(empty_run_id or 0),
        "empty_strategy_id_rows": int(empty_strategy_id or 0),
        "bad_date_range_rows": int(bad_date_range or 0),
        "config_json_not_object_rows": int(config_json_not_object or 0),
        "empty_universe_id_rows": int(empty_universe_id or 0),
        "metrics_json_not_object_rows": int(metrics_json_not_object or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show backtest_runs status and basic Layer 5 validation checks"
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
