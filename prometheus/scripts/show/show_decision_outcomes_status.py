"""Prometheus v2 – Show decision_outcomes status (Layer 5 validation).

Validates basic Layer 5 contracts for ``decision_outcomes``:
- decision_id is non-empty
- horizon_days > 0
- realized metrics are finite when present
- metadata is either NULL or a JSON object

Reports results for both runtime_db and historical_db.

Note: decision linkage to engine_decisions is higher-level auditing.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
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
            COUNT(DISTINCT decision_id) AS distinct_decisions,
            MIN(created_at) AS min_created_at,
            MAX(created_at) AS max_created_at,
            SUM(CASE WHEN btrim(decision_id) = '' THEN 1 ELSE 0 END) AS empty_decision_id,
            SUM(CASE WHEN horizon_days <= 0 THEN 1 ELSE 0 END) AS horizon_days_nonpositive,
            SUM(CASE WHEN realized_return IS NOT NULL AND realized_return IN {_NONFINITE} THEN 1 ELSE 0 END) AS realized_return_nonfinite,
            SUM(CASE WHEN realized_pnl IS NOT NULL AND realized_pnl IN {_NONFINITE} THEN 1 ELSE 0 END) AS realized_pnl_nonfinite,
            SUM(CASE WHEN realized_drawdown IS NOT NULL AND realized_drawdown IN {_NONFINITE} THEN 1 ELSE 0 END) AS realized_drawdown_nonfinite,
            SUM(CASE WHEN realized_vol IS NOT NULL AND realized_vol IN {_NONFINITE} THEN 1 ELSE 0 END) AS realized_vol_nonfinite,
            SUM(CASE WHEN metadata IS NOT NULL AND jsonb_typeof(metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object
        FROM decision_outcomes
    """

    sql_preview = """
        SELECT decision_id, horizon_days, realized_return, realized_pnl, created_at
        FROM decision_outcomes
        ORDER BY created_at DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_decisions,
                min_created_at,
                max_created_at,
                empty_decision_id,
                horizon_days_nonpositive,
                realized_return_nonfinite,
                realized_pnl_nonfinite,
                realized_drawdown_nonfinite,
                realized_vol_nonfinite,
                metadata_not_object,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "decision_id": str(decision_id),
            "horizon_days": int(horizon_days) if horizon_days is not None else None,
            "realized_return": float(realized_return) if realized_return is not None else None,
            "realized_pnl": float(realized_pnl) if realized_pnl is not None else None,
            "created_at": created_at.isoformat() if isinstance(created_at, datetime) else None,
        }
        for decision_id, horizon_days, realized_return, realized_pnl, created_at in preview_rows
    ]

    checks_passed = (
        int(empty_decision_id or 0) == 0
        and int(horizon_days_nonpositive or 0) == 0
        and int(realized_return_nonfinite or 0) == 0
        and int(realized_pnl_nonfinite or 0) == 0
        and int(realized_drawdown_nonfinite or 0) == 0
        and int(realized_vol_nonfinite or 0) == 0
        and int(metadata_not_object or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_decisions": int(distinct_decisions or 0),
        "min_created_at": min_created_at.isoformat() if isinstance(min_created_at, datetime) else None,
        "max_created_at": max_created_at.isoformat() if isinstance(max_created_at, datetime) else None,
        "empty_decision_id_rows": int(empty_decision_id or 0),
        "horizon_days_nonpositive_rows": int(horizon_days_nonpositive or 0),
        "realized_return_nonfinite_rows": int(realized_return_nonfinite or 0),
        "realized_pnl_nonfinite_rows": int(realized_pnl_nonfinite or 0),
        "realized_drawdown_nonfinite_rows": int(realized_drawdown_nonfinite or 0),
        "realized_vol_nonfinite_rows": int(realized_vol_nonfinite or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show decision_outcomes status and basic Layer 5 validation checks"
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
