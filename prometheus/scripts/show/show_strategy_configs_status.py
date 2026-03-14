"""Prometheus v2 – Show strategy_configs status (Layer 0 validation).

Validates core Layer 0 contracts for the ``strategy_configs`` table:
- append-only semantics: no UPDATE/DELETE (enforced by triggers; not tested here)
- config_hash is present and looks like an md5 hex string
- "active config" selection is explicit via strategies.active_strategy_config_id

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
            (SELECT COUNT(*) FROM strategy_configs) AS total_configs,
            (SELECT COUNT(DISTINCT strategy_id) FROM strategy_configs) AS distinct_strategies_with_configs,
            (
                SELECT COUNT(*)
                FROM strategy_configs
                WHERE config_hash IS NULL OR btrim(config_hash) = ''
            ) AS empty_config_hash_rows,
            (
                SELECT COUNT(*)
                FROM strategy_configs
                WHERE NOT (config_hash ~ '^[0-9a-f]{32}$')
            ) AS invalid_config_hash_rows,
            (
                SELECT COUNT(*)
                FROM strategies
                WHERE active_strategy_config_id IS NOT NULL
            ) AS strategies_with_active_config
    """

    sql_orphan_active = """
        SELECT s.strategy_id, s.active_strategy_config_id
        FROM strategies s
        LEFT JOIN strategy_configs sc
            ON sc.strategy_config_id = s.active_strategy_config_id
        WHERE s.active_strategy_config_id IS NOT NULL
          AND sc.strategy_config_id IS NULL
        ORDER BY s.strategy_id
        LIMIT 50
    """

    sql_missing_active = """
        SELECT sc.strategy_id, COUNT(*) AS n_configs
        FROM strategy_configs sc
        LEFT JOIN strategies s
            ON s.strategy_id = sc.strategy_id
        WHERE s.strategy_id IS NULL OR s.active_strategy_config_id IS NULL
        GROUP BY sc.strategy_id
        ORDER BY sc.strategy_id
        LIMIT 50
    """

    sql_preview = """
        SELECT strategy_id, config_hash, created_at, created_by
        FROM strategy_configs
        ORDER BY created_at DESC, strategy_config_id DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_counts)
            (
                total_configs,
                distinct_strategies_with_configs,
                empty_config_hash_rows,
                invalid_config_hash_rows,
                strategies_with_active_config,
            ) = cur.fetchone()

            cur.execute(sql_orphan_active)
            orphan_rows = cur.fetchall()

            cur.execute(sql_missing_active)
            missing_active_rows = cur.fetchall()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    orphan_preview = [
        {"strategy_id": str(sid), "active_strategy_config_id": int(cid)}
        for sid, cid in orphan_rows
    ]

    missing_active_preview = [
        {"strategy_id": str(sid), "n_configs": int(n)} for sid, n in missing_active_rows
    ]

    preview = [
        {
            "strategy_id": str(sid),
            "config_hash": str(ch),
            "created_at": ca.isoformat() if ca is not None else None,
            "created_by": str(cb),
        }
        for sid, ch, ca, cb in preview_rows
    ]

    checks_passed = (
        int(empty_config_hash_rows or 0) == 0
        and int(invalid_config_hash_rows or 0) == 0
        and len(orphan_preview) == 0
        and len(missing_active_preview) == 0
    )

    return {
        "total_strategy_configs": int(total_configs or 0),
        "distinct_strategies_with_configs": int(distinct_strategies_with_configs or 0),
        "empty_config_hash_rows": int(empty_config_hash_rows or 0),
        "invalid_config_hash_rows": int(invalid_config_hash_rows or 0),
        "strategies_with_active_config": int(strategies_with_active_config or 0),
        "orphan_active_configs_preview": orphan_preview,
        "strategies_with_configs_but_missing_active_preview": missing_active_preview,
        "recent_configs_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show strategy_configs status and Layer 0 validation checks"
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
