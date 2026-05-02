"""Prometheus v2 – Show backtest_daily_equity status (Layer 5 validation).

Validates basic Layer 5 contracts for ``backtest_daily_equity``:
- run_id is non-empty
- equity_curve_value is finite
- drawdown is finite when present
- exposure_metrics_json is either NULL or a JSON object

Reports results for both runtime_db and historical_db.

Note: equity curve completeness is higher-level auditing.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
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
            COUNT(DISTINCT run_id) AS distinct_runs,
            MIN(date) AS min_date,
            MAX(date) AS max_date,
            SUM(CASE WHEN btrim(run_id) = '' THEN 1 ELSE 0 END) AS empty_run_id,
            SUM(CASE WHEN equity_curve_value IN {_NONFINITE} THEN 1 ELSE 0 END) AS equity_curve_value_nonfinite,
            SUM(CASE WHEN drawdown IS NOT NULL AND drawdown IN {_NONFINITE} THEN 1 ELSE 0 END) AS drawdown_nonfinite,
            SUM(CASE WHEN exposure_metrics_json IS NOT NULL AND jsonb_typeof(exposure_metrics_json) <> 'object' THEN 1 ELSE 0 END) AS exposure_metrics_json_not_object
        FROM backtest_daily_equity
    """

    sql_preview = """
        SELECT run_id, date, equity_curve_value, drawdown
        FROM backtest_daily_equity
        ORDER BY date DESC, run_id
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_runs,
                min_date,
                max_date,
                empty_run_id,
                equity_curve_value_nonfinite,
                drawdown_nonfinite,
                exposure_metrics_json_not_object,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "run_id": str(run_id),
            "date": date_db.isoformat() if isinstance(date_db, date) else None,
            "equity_curve_value": float(equity_curve_value) if equity_curve_value is not None else None,
            "drawdown": float(drawdown) if drawdown is not None else None,
        }
        for run_id, date_db, equity_curve_value, drawdown in preview_rows
    ]

    checks_passed = (
        int(empty_run_id or 0) == 0
        and int(equity_curve_value_nonfinite or 0) == 0
        and int(drawdown_nonfinite or 0) == 0
        and int(exposure_metrics_json_not_object or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_runs": int(distinct_runs or 0),
        "min_date": min_date.isoformat() if isinstance(min_date, date) else None,
        "max_date": max_date.isoformat() if isinstance(max_date, date) else None,
        "empty_run_id_rows": int(empty_run_id or 0),
        "equity_curve_value_nonfinite_rows": int(equity_curve_value_nonfinite or 0),
        "drawdown_nonfinite_rows": int(drawdown_nonfinite or 0),
        "exposure_metrics_json_not_object_rows": int(exposure_metrics_json_not_object or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show backtest_daily_equity status and basic Layer 5 validation checks"
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
