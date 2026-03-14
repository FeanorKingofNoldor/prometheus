"""Prometheus v2 – Show config_change_log status (Layer 0 validation).

Validates core Layer 0 contracts for the ``config_change_log`` table:
- append-only semantics (enforced by triggers; this script checks invariants)
- key identifiers are non-empty (change_id, strategy_id, change_type, target_component)
- applied_by is present
- reversion is represented via a REVERT row with reverts_change_id

Reports results for both runtime_db and historical_db.
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager


def _summarise(db, which: str) -> dict[str, Any]:
    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    sql_counts = """
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN btrim(change_id) = '' THEN 1 ELSE 0 END) AS empty_change_id_rows,
            SUM(CASE WHEN strategy_id IS NULL OR btrim(strategy_id) = '' THEN 1 ELSE 0 END) AS empty_strategy_id_rows,
            SUM(CASE WHEN change_type IS NULL OR btrim(change_type) = '' THEN 1 ELSE 0 END) AS empty_change_type_rows,
            SUM(CASE WHEN target_component IS NULL OR btrim(target_component) = '' THEN 1 ELSE 0 END) AS empty_target_component_rows,
            SUM(CASE WHEN applied_by IS NULL OR btrim(applied_by) = '' THEN 1 ELSE 0 END) AS empty_applied_by_rows,
            SUM(CASE WHEN applied_at IS NULL THEN 1 ELSE 0 END) AS null_applied_at_rows,
            SUM(CASE WHEN change_type = 'REVERT' AND reverts_change_id IS NULL THEN 1 ELSE 0 END) AS revert_missing_pointer_rows,
            SUM(CASE WHEN change_type <> 'REVERT' AND reverts_change_id IS NOT NULL THEN 1 ELSE 0 END) AS nonrevert_has_pointer_rows,
            SUM(CASE WHEN reverts_change_id IS NOT NULL AND reverts_change_id = change_id THEN 1 ELSE 0 END) AS self_revert_rows,
            SUM(CASE WHEN is_reverted = TRUE THEN 1 ELSE 0 END) AS legacy_is_reverted_true_rows
        FROM config_change_log
    """

    sql_dup_reverts = """
        SELECT reverts_change_id, COUNT(*) AS n
        FROM config_change_log
        WHERE reverts_change_id IS NOT NULL
        GROUP BY reverts_change_id
        HAVING COUNT(*) > 1
        ORDER BY n DESC
        LIMIT 50
    """

    sql_preview = """
        SELECT change_id, change_type, strategy_id, target_component, reverts_change_id, applied_by, applied_at
        FROM config_change_log
        ORDER BY applied_at DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_counts)
            (
                total_rows,
                empty_change_id_rows,
                empty_strategy_id_rows,
                empty_change_type_rows,
                empty_target_component_rows,
                empty_applied_by_rows,
                null_applied_at_rows,
                revert_missing_pointer_rows,
                nonrevert_has_pointer_rows,
                self_revert_rows,
                legacy_is_reverted_true_rows,
            ) = cur.fetchone()

            cur.execute(sql_dup_reverts)
            dup_rows = cur.fetchall()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    dup_preview = [
        {"reverts_change_id": str(cid), "count": int(n)} for cid, n in dup_rows
    ]

    preview = [
        {
            "change_id": str(cid),
            "change_type": str(ct),
            "strategy_id": str(sid),
            "target_component": str(tc),
            "reverts_change_id": str(rcid) if rcid is not None else None,
            "applied_by": str(ab),
            "applied_at": aa.isoformat() if aa is not None else None,
        }
        for cid, ct, sid, tc, rcid, ab, aa in preview_rows
    ]

    checks_passed = (
        int(empty_change_id_rows or 0) == 0
        and int(empty_strategy_id_rows or 0) == 0
        and int(empty_change_type_rows or 0) == 0
        and int(empty_target_component_rows or 0) == 0
        and int(empty_applied_by_rows or 0) == 0
        and int(null_applied_at_rows or 0) == 0
        and int(revert_missing_pointer_rows or 0) == 0
        and int(nonrevert_has_pointer_rows or 0) == 0
        and int(self_revert_rows or 0) == 0
        and len(dup_preview) == 0
    )

    return {
        "total_config_change_log_rows": int(total_rows or 0),
        "empty_change_id_rows": int(empty_change_id_rows or 0),
        "empty_strategy_id_rows": int(empty_strategy_id_rows or 0),
        "empty_change_type_rows": int(empty_change_type_rows or 0),
        "empty_target_component_rows": int(empty_target_component_rows or 0),
        "empty_applied_by_rows": int(empty_applied_by_rows or 0),
        "null_applied_at_rows": int(null_applied_at_rows or 0),
        "revert_missing_pointer_rows": int(revert_missing_pointer_rows or 0),
        "nonrevert_has_pointer_rows": int(nonrevert_has_pointer_rows or 0),
        "self_revert_rows": int(self_revert_rows or 0),
        "legacy_is_reverted_true_rows": int(legacy_is_reverted_true_rows or 0),
        "duplicate_reverts_change_id_preview": dup_preview,
        "recent_changes_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show config_change_log status and Layer 0 validation checks"
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
