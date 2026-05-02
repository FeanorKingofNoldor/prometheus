"""Prometheus v2 – Show engine_decisions status (Layer 3 validation).

Validates basic Layer 3 contracts for ``engine_decisions``:
- decision_id and engine_name are non-empty
- run_id/strategy_id/market_id/config_id are either NULL or non-empty
- input_refs/output_refs/metadata are JSON objects when present
- market_id exists in markets when present

Reports results for both runtime_db and historical_db.

Note: reproducibility of decisions is a higher-level audit.
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

    sql_markets_total = "SELECT COUNT(*) FROM markets"

    sql_without_markets_ref = """
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT engine_name) AS distinct_engines,
            MIN(as_of_date) AS min_as_of_date,
            MAX(as_of_date) AS max_as_of_date,
            MIN(created_at) AS min_created_at,
            MAX(created_at) AS max_created_at,
            SUM(CASE WHEN btrim(decision_id) = '' THEN 1 ELSE 0 END) AS empty_decision_id,
            SUM(CASE WHEN btrim(engine_name) = '' THEN 1 ELSE 0 END) AS empty_engine_name,
            SUM(CASE WHEN run_id IS NOT NULL AND btrim(run_id) = '' THEN 1 ELSE 0 END) AS empty_run_id,
            SUM(CASE WHEN strategy_id IS NOT NULL AND btrim(strategy_id) = '' THEN 1 ELSE 0 END) AS empty_strategy_id,
            SUM(CASE WHEN market_id IS NOT NULL AND btrim(market_id) = '' THEN 1 ELSE 0 END) AS empty_market_id,
            SUM(CASE WHEN config_id IS NOT NULL AND btrim(config_id) = '' THEN 1 ELSE 0 END) AS empty_config_id,
            SUM(CASE WHEN input_refs IS NOT NULL AND jsonb_typeof(input_refs) <> 'object' THEN 1 ELSE 0 END) AS input_refs_not_object,
            SUM(CASE WHEN output_refs IS NOT NULL AND jsonb_typeof(output_refs) <> 'object' THEN 1 ELSE 0 END) AS output_refs_not_object,
            SUM(CASE WHEN metadata IS NOT NULL AND jsonb_typeof(metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object
        FROM engine_decisions
    """

    sql_with_markets_ref = """
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT ed.engine_name) AS distinct_engines,
            MIN(ed.as_of_date) AS min_as_of_date,
            MAX(ed.as_of_date) AS max_as_of_date,
            MIN(ed.created_at) AS min_created_at,
            MAX(ed.created_at) AS max_created_at,
            SUM(CASE WHEN btrim(ed.decision_id) = '' THEN 1 ELSE 0 END) AS empty_decision_id,
            SUM(CASE WHEN btrim(ed.engine_name) = '' THEN 1 ELSE 0 END) AS empty_engine_name,
            SUM(CASE WHEN ed.run_id IS NOT NULL AND btrim(ed.run_id) = '' THEN 1 ELSE 0 END) AS empty_run_id,
            SUM(CASE WHEN ed.strategy_id IS NOT NULL AND btrim(ed.strategy_id) = '' THEN 1 ELSE 0 END) AS empty_strategy_id,
            SUM(CASE WHEN ed.market_id IS NOT NULL AND btrim(ed.market_id) = '' THEN 1 ELSE 0 END) AS empty_market_id,
            SUM(CASE WHEN ed.config_id IS NOT NULL AND btrim(ed.config_id) = '' THEN 1 ELSE 0 END) AS empty_config_id,
            SUM(CASE WHEN ed.input_refs IS NOT NULL AND jsonb_typeof(ed.input_refs) <> 'object' THEN 1 ELSE 0 END) AS input_refs_not_object,
            SUM(CASE WHEN ed.output_refs IS NOT NULL AND jsonb_typeof(ed.output_refs) <> 'object' THEN 1 ELSE 0 END) AS output_refs_not_object,
            SUM(CASE WHEN ed.metadata IS NOT NULL AND jsonb_typeof(ed.metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object,
            SUM(CASE WHEN ed.market_id IS NOT NULL AND m.market_id IS NULL THEN 1 ELSE 0 END) AS orphan_market_id
        FROM engine_decisions ed
        LEFT JOIN markets m ON m.market_id = ed.market_id
    """

    sql_preview = """
        SELECT decision_id, engine_name, strategy_id, market_id, as_of_date, created_at
        FROM engine_decisions
        ORDER BY created_at DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_markets_total)
            markets_total = int(cur.fetchone()[0] or 0)

            market_reference_check_skipped = markets_total == 0

            if market_reference_check_skipped:
                cur.execute(sql_without_markets_ref)
                (
                    total,
                    distinct_engines,
                    min_as_of_date,
                    max_as_of_date,
                    min_created_at,
                    max_created_at,
                    empty_decision_id,
                    empty_engine_name,
                    empty_run_id,
                    empty_strategy_id,
                    empty_market_id,
                    empty_config_id,
                    input_refs_not_object,
                    output_refs_not_object,
                    metadata_not_object,
                ) = cur.fetchone()
                orphan_market_id = 0
            else:
                cur.execute(sql_with_markets_ref)
                (
                    total,
                    distinct_engines,
                    min_as_of_date,
                    max_as_of_date,
                    min_created_at,
                    max_created_at,
                    empty_decision_id,
                    empty_engine_name,
                    empty_run_id,
                    empty_strategy_id,
                    empty_market_id,
                    empty_config_id,
                    input_refs_not_object,
                    output_refs_not_object,
                    metadata_not_object,
                    orphan_market_id,
                ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "decision_id": str(decision_id),
            "engine_name": str(engine_name),
            "strategy_id": str(strategy_id) if strategy_id is not None else None,
            "market_id": str(market_id) if market_id is not None else None,
            "as_of_date": as_of_date_db.isoformat() if isinstance(as_of_date_db, date) else None,
            "created_at": created_at.isoformat() if isinstance(created_at, datetime) else None,
        }
        for decision_id, engine_name, strategy_id, market_id, as_of_date_db, created_at in preview_rows
    ]

    checks_passed = (
        int(empty_decision_id or 0) == 0
        and int(empty_engine_name or 0) == 0
        and int(empty_run_id or 0) == 0
        and int(empty_strategy_id or 0) == 0
        and int(empty_market_id or 0) == 0
        and int(empty_config_id or 0) == 0
        and int(input_refs_not_object or 0) == 0
        and int(output_refs_not_object or 0) == 0
        and int(metadata_not_object or 0) == 0
        and (market_reference_check_skipped or int(orphan_market_id or 0) == 0)
    )

    return {
        "total_rows": int(total or 0),
        "distinct_engines": int(distinct_engines or 0),
        "min_as_of_date": min_as_of_date.isoformat() if isinstance(min_as_of_date, date) else None,
        "max_as_of_date": max_as_of_date.isoformat() if isinstance(max_as_of_date, date) else None,
        "min_created_at": min_created_at.isoformat() if isinstance(min_created_at, datetime) else None,
        "max_created_at": max_created_at.isoformat() if isinstance(max_created_at, datetime) else None,
        "empty_decision_id_rows": int(empty_decision_id or 0),
        "empty_engine_name_rows": int(empty_engine_name or 0),
        "empty_run_id_rows": int(empty_run_id or 0),
        "empty_strategy_id_rows": int(empty_strategy_id or 0),
        "empty_market_id_rows": int(empty_market_id or 0),
        "empty_config_id_rows": int(empty_config_id or 0),
        "input_refs_not_object_rows": int(input_refs_not_object or 0),
        "output_refs_not_object_rows": int(output_refs_not_object or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "orphan_market_id_rows": int(orphan_market_id or 0),
        "market_table_total_rows": int(markets_total),
        "market_reference_check_skipped": bool(market_reference_check_skipped),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show engine_decisions status and basic Layer 3 validation checks"
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
