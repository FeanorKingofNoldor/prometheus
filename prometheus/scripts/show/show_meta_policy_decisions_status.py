"""Prometheus v2 – Show meta policy decision status.

Audits basic Layer-2/3 expectations for meta book routing decisions written
into ``engine_decisions`` with engine_name='META_POLICY_V1'.

Checks (best-effort):
- decisions exist for a given market_id
- input_refs include market_situation and policy_version (may be null)
- output_refs include selected_book_id and selected_sleeve_id

Outputs a JSON summary suitable for CLI inspection.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any, Optional, Sequence

from apatheon.core.database import get_db_manager


def _summarise(db, *, market_id: str, limit: int) -> dict[str, Any]:
    sql_summary = """
        SELECT
            COUNT(*) AS total,
            MIN(as_of_date) AS min_as_of_date,
            MAX(as_of_date) AS max_as_of_date,
            SUM(CASE WHEN input_refs ? 'market_situation' THEN 0 ELSE 1 END) AS missing_market_situation,
            SUM(CASE WHEN input_refs ? 'policy_version' THEN 0 ELSE 1 END) AS missing_policy_version,
            SUM(CASE WHEN output_refs ? 'selected_book_id' THEN 0 ELSE 1 END) AS missing_selected_book_id,
            SUM(CASE WHEN output_refs ? 'selected_sleeve_id' THEN 0 ELSE 1 END) AS missing_selected_sleeve_id
        FROM engine_decisions
        WHERE engine_name = 'META_POLICY_V1'
          AND market_id = %s
    """

    sql_dist = """
        SELECT
            COALESCE(input_refs->>'market_situation', 'NULL') AS situation,
            COUNT(*) AS n
        FROM engine_decisions
        WHERE engine_name = 'META_POLICY_V1'
          AND market_id = %s
        GROUP BY 1
        ORDER BY n DESC, situation ASC
    """

    sql_preview = """
        SELECT decision_id,
               run_id,
               as_of_date,
               input_refs,
               output_refs,
               created_at
        FROM engine_decisions
        WHERE engine_name = 'META_POLICY_V1'
          AND market_id = %s
        ORDER BY as_of_date DESC, created_at DESC
        LIMIT %s
    """

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_summary, (market_id,))
            (
                total,
                min_as_of,
                max_as_of,
                missing_market_situation,
                missing_policy_version,
                missing_selected_book_id,
                missing_selected_sleeve_id,
            ) = cur.fetchone()

            cur.execute(sql_dist, (market_id,))
            dist_rows = cur.fetchall()

            cur.execute(sql_preview, (market_id, int(limit)))
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    dist = {str(sit): int(n) for sit, n in dist_rows}

    preview = []
    for decision_id, run_id, as_of_date_db, input_refs, output_refs, created_at in preview_rows:
        in_refs = input_refs or {}
        out_refs = output_refs or {}
        preview.append(
            {
                "decision_id": str(decision_id),
                "run_id": str(run_id) if run_id is not None else None,
                "as_of_date": as_of_date_db.isoformat() if isinstance(as_of_date_db, date) else None,
                "market_situation": in_refs.get("market_situation"),
                "policy_version": in_refs.get("policy_version"),
                "selected_book_id": out_refs.get("selected_book_id"),
                "selected_sleeve_id": out_refs.get("selected_sleeve_id"),
                "created_at": created_at.isoformat() if hasattr(created_at, "isoformat") else None,
            }
        )

    checks_passed = (
        int(total or 0) > 0
        and int(missing_market_situation or 0) == 0
        and int(missing_selected_book_id or 0) == 0
        and int(missing_selected_sleeve_id or 0) == 0
    )

    return {
        "market_id": market_id,
        "total_decisions": int(total or 0),
        "min_as_of_date": min_as_of.isoformat() if isinstance(min_as_of, date) else None,
        "max_as_of_date": max_as_of.isoformat() if isinstance(max_as_of, date) else None,
        "missing_market_situation_rows": int(missing_market_situation or 0),
        "missing_policy_version_rows": int(missing_policy_version or 0),
        "missing_selected_book_id_rows": int(missing_selected_book_id or 0),
        "missing_selected_sleeve_id_rows": int(missing_selected_sleeve_id or 0),
        "situation_distribution": dist,
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show meta policy routing decisions (engine_decisions: META_POLICY_V1)"
    )
    parser.add_argument(
        "--market-id",
        type=str,
        default="US_EQ",
        help="Market identifier (default: US_EQ)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Preview row limit (default: 20)",
    )

    args = parser.parse_args(argv)

    db = get_db_manager()
    report = _summarise(db, market_id=str(args.market_id).upper(), limit=int(args.limit))
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
