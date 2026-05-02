"""Prometheus v2 – Show meta_config_proposals status (Layer 0 validation).

Validates core Layer 0 contracts for the ``meta_config_proposals`` table:
- proposals are immutable (enforced by triggers; this script checks invariants)
- proposals have required non-empty identifiers/fields
- workflow state is represented via append-only ``meta_config_proposal_events``
  - every proposal should have exactly one CREATED event
  - at most one each of APPROVED/REJECTED/APPLIED/REVERTED

Reports results for both runtime_db and historical_db.
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Optional, Sequence

from apatheon.core.database import get_db_manager


def _summarise(db, which: str) -> dict[str, Any]:
    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    sql_counts = """
        SELECT
            COUNT(*) AS total_proposals,
            SUM(CASE WHEN btrim(proposal_id) = '' THEN 1 ELSE 0 END) AS empty_proposal_id_rows,
            SUM(CASE WHEN strategy_id IS NULL OR btrim(strategy_id) = '' THEN 1 ELSE 0 END) AS empty_strategy_id_rows,
            SUM(CASE WHEN proposal_type IS NULL OR btrim(proposal_type) = '' THEN 1 ELSE 0 END) AS empty_proposal_type_rows,
            SUM(CASE WHEN target_component IS NULL OR btrim(target_component) = '' THEN 1 ELSE 0 END) AS empty_target_component_rows,
            SUM(CASE WHEN confidence_score < 0.0 OR confidence_score > 1.0 THEN 1 ELSE 0 END) AS confidence_out_of_range_rows
        FROM meta_config_proposals
    """

    sql_event_counts = """
        SELECT
            COUNT(*) AS total_events,
            SUM(CASE WHEN event_type = 'CREATED' THEN 1 ELSE 0 END) AS created_events,
            SUM(CASE WHEN event_type = 'APPROVED' THEN 1 ELSE 0 END) AS approved_events,
            SUM(CASE WHEN event_type = 'REJECTED' THEN 1 ELSE 0 END) AS rejected_events,
            SUM(CASE WHEN event_type = 'APPLIED' THEN 1 ELSE 0 END) AS applied_events,
            SUM(CASE WHEN event_type = 'REVERTED' THEN 1 ELSE 0 END) AS reverted_events,
            SUM(CASE WHEN event_type NOT IN ('CREATED','APPROVED','REJECTED','APPLIED','REVERTED') THEN 1 ELSE 0 END) AS invalid_event_type_rows,
            SUM(CASE WHEN event_by IS NULL OR btrim(event_by) = '' THEN 1 ELSE 0 END) AS empty_event_by_rows
        FROM meta_config_proposal_events
    """

    sql_missing_created = """
        SELECT p.proposal_id
        FROM meta_config_proposals p
        LEFT JOIN meta_config_proposal_events e
          ON e.proposal_id = p.proposal_id
         AND e.event_type = 'CREATED'
        WHERE e.proposal_id IS NULL
        ORDER BY p.proposal_id
        LIMIT 50
    """

    sql_dupe_events = """
        SELECT proposal_id, event_type, COUNT(*) AS n
        FROM meta_config_proposal_events
        GROUP BY proposal_id, event_type
        HAVING COUNT(*) > 1
        ORDER BY n DESC
        LIMIT 50
    """

    sql_preview = """
        SELECT p.proposal_id, p.strategy_id,
               COALESCE(s.status, 'PENDING') AS status,
               p.expected_sharpe_improvement, p.confidence_score, p.created_at
        FROM meta_config_proposals p
        LEFT JOIN LATERAL (
            SELECT CASE WHEN e.event_type = 'CREATED' THEN 'PENDING' ELSE e.event_type END AS status
            FROM meta_config_proposal_events e
            WHERE e.proposal_id = p.proposal_id
            ORDER BY e.event_at DESC, e.event_id DESC
            LIMIT 1
        ) s ON TRUE
        ORDER BY p.created_at DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_counts)
            (
                total_proposals,
                empty_proposal_id_rows,
                empty_strategy_id_rows,
                empty_proposal_type_rows,
                empty_target_component_rows,
                confidence_out_of_range_rows,
            ) = cur.fetchone()

            cur.execute(sql_event_counts)
            (
                total_events,
                created_events,
                approved_events,
                rejected_events,
                applied_events,
                reverted_events,
                invalid_event_type_rows,
                empty_event_by_rows,
            ) = cur.fetchone()

            cur.execute(sql_missing_created)
            missing_created_rows = cur.fetchall()

            cur.execute(sql_dupe_events)
            dupe_rows = cur.fetchall()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    missing_created_preview = [str(r[0]) for r in missing_created_rows]

    dupe_preview = [
        {"proposal_id": str(pid), "event_type": str(et), "count": int(n)}
        for pid, et, n in dupe_rows
    ]

    preview = [
        {
            "proposal_id": str(pid),
            "strategy_id": str(sid),
            "status": str(st),
            "expected_sharpe_improvement": float(esi) if esi is not None else None,
            "confidence_score": float(cs) if cs is not None else None,
            "created_at": ca.isoformat() if ca is not None else None,
        }
        for pid, sid, st, esi, cs, ca in preview_rows
    ]

    checks_passed = (
        int(empty_proposal_id_rows or 0) == 0
        and int(empty_strategy_id_rows or 0) == 0
        and int(empty_proposal_type_rows or 0) == 0
        and int(empty_target_component_rows or 0) == 0
        and int(confidence_out_of_range_rows or 0) == 0
        and int(invalid_event_type_rows or 0) == 0
        and int(empty_event_by_rows or 0) == 0
        and len(missing_created_preview) == 0
        and len(dupe_preview) == 0
    )

    return {
        "total_meta_config_proposals": int(total_proposals or 0),
        "empty_proposal_id_rows": int(empty_proposal_id_rows or 0),
        "empty_strategy_id_rows": int(empty_strategy_id_rows or 0),
        "empty_proposal_type_rows": int(empty_proposal_type_rows or 0),
        "empty_target_component_rows": int(empty_target_component_rows or 0),
        "confidence_out_of_range_rows": int(confidence_out_of_range_rows or 0),
        "total_meta_config_proposal_events": int(total_events or 0),
        "events_by_type": {
            "CREATED": int(created_events or 0),
            "APPROVED": int(approved_events or 0),
            "REJECTED": int(rejected_events or 0),
            "APPLIED": int(applied_events or 0),
            "REVERTED": int(reverted_events or 0),
        },
        "invalid_event_type_rows": int(invalid_event_type_rows or 0),
        "empty_event_by_rows": int(empty_event_by_rows or 0),
        "missing_created_event_preview": missing_created_preview,
        "duplicate_event_type_preview": dupe_preview,
        "recent_proposals_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show meta_config_proposals status and Layer 0 validation checks"
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
