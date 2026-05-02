"""Prometheus v2 – Show risk_actions status (Layer 3 validation).

Validates basic Layer 3 contracts for ``risk_actions``:
- action_id and action_type are non-empty
- action_type is in a controlled set (OK/CAPPED/REJECTED/EXECUTION_REJECT)
- strategy_id/instrument_id/decision_id are either NULL or non-empty
- details_json is either NULL or a JSON object

Reports results for both runtime_db and historical_db.

Note: rationale completeness and linkage to decisions are higher-level audits.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any, Optional, Sequence

from apatheon.core.database import get_db_manager

_ALLOWED_ACTION_TYPES = ("OK", "CAPPED", "REJECTED", "EXECUTION_REJECT")


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
            COUNT(DISTINCT strategy_id) AS distinct_strategies,
            COUNT(DISTINCT instrument_id) AS distinct_instruments,
            MIN(created_at) AS min_created_at,
            MAX(created_at) AS max_created_at,
            SUM(CASE WHEN btrim(action_id) = '' THEN 1 ELSE 0 END) AS empty_action_id,
            SUM(CASE WHEN btrim(action_type) = '' THEN 1 ELSE 0 END) AS empty_action_type,
            SUM(CASE WHEN action_type NOT IN {tuple(_ALLOWED_ACTION_TYPES)!r} THEN 1 ELSE 0 END) AS bad_action_type,
            SUM(CASE WHEN strategy_id IS NOT NULL AND btrim(strategy_id) = '' THEN 1 ELSE 0 END) AS empty_strategy_id,
            SUM(CASE WHEN instrument_id IS NOT NULL AND btrim(instrument_id) = '' THEN 1 ELSE 0 END) AS empty_instrument_id,
            SUM(CASE WHEN decision_id IS NOT NULL AND btrim(decision_id) = '' THEN 1 ELSE 0 END) AS empty_decision_id,
            SUM(CASE WHEN details_json IS NOT NULL AND jsonb_typeof(details_json) <> 'object' THEN 1 ELSE 0 END) AS details_json_not_object
        FROM risk_actions
    """

    sql_preview = """
        SELECT created_at, action_type, strategy_id, instrument_id, decision_id
        FROM risk_actions
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
                distinct_instruments,
                min_created_at,
                max_created_at,
                empty_action_id,
                empty_action_type,
                bad_action_type,
                empty_strategy_id,
                empty_instrument_id,
                empty_decision_id,
                details_json_not_object,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "created_at": created_at.isoformat() if isinstance(created_at, datetime) else None,
            "action_type": str(action_type),
            "strategy_id": str(strategy_id) if strategy_id is not None else None,
            "instrument_id": str(instrument_id) if instrument_id is not None else None,
            "decision_id": str(decision_id) if decision_id is not None else None,
        }
        for created_at, action_type, strategy_id, instrument_id, decision_id in preview_rows
    ]

    checks_passed = (
        int(empty_action_id or 0) == 0
        and int(empty_action_type or 0) == 0
        and int(bad_action_type or 0) == 0
        and int(empty_strategy_id or 0) == 0
        and int(empty_instrument_id or 0) == 0
        and int(empty_decision_id or 0) == 0
        and int(details_json_not_object or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_strategies": int(distinct_strategies or 0),
        "distinct_instruments": int(distinct_instruments or 0),
        "min_created_at": min_created_at.isoformat() if isinstance(min_created_at, datetime) else None,
        "max_created_at": max_created_at.isoformat() if isinstance(max_created_at, datetime) else None,
        "empty_action_id_rows": int(empty_action_id or 0),
        "empty_action_type_rows": int(empty_action_type or 0),
        "bad_action_type_rows": int(bad_action_type or 0),
        "empty_strategy_id_rows": int(empty_strategy_id or 0),
        "empty_instrument_id_rows": int(empty_instrument_id or 0),
        "empty_decision_id_rows": int(empty_decision_id or 0),
        "details_json_not_object_rows": int(details_json_not_object or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show risk_actions status and basic Layer 3 validation checks"
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
