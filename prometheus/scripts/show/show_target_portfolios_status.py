"""Prometheus v2 – Show target_portfolios status (Layer 3 validation).

Validates basic Layer 3 contracts for ``target_portfolios``:
- target_id, strategy_id, portfolio_id are non-empty
- target_positions is a JSON object
- target_positions has key "weights" and it is a JSON object
- metadata is either NULL or a JSON object

Reports results for both runtime_db and historical_db.

Note: weights sum rules and lookahead safety are higher-level audits.
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
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT strategy_id) AS distinct_strategies,
            COUNT(DISTINCT portfolio_id) AS distinct_portfolios,
            MIN(as_of_date) AS min_as_of_date,
            MAX(as_of_date) AS max_as_of_date,
            MIN(created_at) AS min_created_at,
            MAX(created_at) AS max_created_at,
            SUM(CASE WHEN btrim(target_id) = '' THEN 1 ELSE 0 END) AS empty_target_id,
            SUM(CASE WHEN btrim(strategy_id) = '' THEN 1 ELSE 0 END) AS empty_strategy_id,
            SUM(CASE WHEN btrim(portfolio_id) = '' THEN 1 ELSE 0 END) AS empty_portfolio_id,
            SUM(CASE WHEN jsonb_typeof(target_positions) <> 'object' THEN 1 ELSE 0 END) AS target_positions_not_object,
            SUM(CASE WHEN NOT (target_positions ? 'weights') THEN 1 ELSE 0 END) AS missing_weights_key,
            SUM(
                CASE
                    WHEN (target_positions ? 'weights') AND jsonb_typeof(target_positions->'weights') <> 'object'
                    THEN 1
                    ELSE 0
                END
            ) AS weights_not_object,
            SUM(CASE WHEN metadata IS NOT NULL AND jsonb_typeof(metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object
        FROM target_portfolios
    """

    sql_preview = """
        SELECT
            strategy_id,
            portfolio_id,
            as_of_date,
            created_at
        FROM target_portfolios
        ORDER BY as_of_date DESC, created_at DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_strategies,
                distinct_portfolios,
                min_as_of_date,
                max_as_of_date,
                min_created_at,
                max_created_at,
                empty_target_id,
                empty_strategy_id,
                empty_portfolio_id,
                target_positions_not_object,
                missing_weights_key,
                weights_not_object,
                metadata_not_object,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "strategy_id": str(strategy_id),
            "portfolio_id": str(portfolio_id),
            "as_of_date": as_of_date_db.isoformat() if isinstance(as_of_date_db, date) else None,
            "created_at": created_at.isoformat() if isinstance(created_at, datetime) else None,
        }
        for strategy_id, portfolio_id, as_of_date_db, created_at in preview_rows
    ]

    checks_passed = (
        int(empty_target_id or 0) == 0
        and int(empty_strategy_id or 0) == 0
        and int(empty_portfolio_id or 0) == 0
        and int(target_positions_not_object or 0) == 0
        and int(missing_weights_key or 0) == 0
        and int(weights_not_object or 0) == 0
        and int(metadata_not_object or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_strategies": int(distinct_strategies or 0),
        "distinct_portfolios": int(distinct_portfolios or 0),
        "min_as_of_date": min_as_of_date.isoformat() if isinstance(min_as_of_date, date) else None,
        "max_as_of_date": max_as_of_date.isoformat() if isinstance(max_as_of_date, date) else None,
        "min_created_at": min_created_at.isoformat() if isinstance(min_created_at, datetime) else None,
        "max_created_at": max_created_at.isoformat() if isinstance(max_created_at, datetime) else None,
        "empty_target_id_rows": int(empty_target_id or 0),
        "empty_strategy_id_rows": int(empty_strategy_id or 0),
        "empty_portfolio_id_rows": int(empty_portfolio_id or 0),
        "target_positions_not_object_rows": int(target_positions_not_object or 0),
        "missing_weights_key_rows": int(missing_weights_key or 0),
        "weights_not_object_rows": int(weights_not_object or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show target_portfolios status and basic Layer 3 validation checks"
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
