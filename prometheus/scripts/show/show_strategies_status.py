"""Prometheus v2 – Show strategies status (Layer 0 validation).

Validates core Layer 0 contracts for the ``strategies`` table:
- strategy_id is non-empty
- name is non-empty

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

    sql = """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN btrim(strategy_id) = '' THEN 1 ELSE 0 END) AS empty_strategy_id,
            SUM(CASE WHEN name IS NULL OR btrim(name) = '' THEN 1 ELSE 0 END) AS empty_name
        FROM strategies
    """

    sql_list = """
        SELECT strategy_id, name
        FROM strategies
        ORDER BY strategy_id
        LIMIT 50
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            total, empty_strategy_id, empty_name = cur.fetchone()

            cur.execute(sql_list)
            rows = cur.fetchall()
        finally:
            cur.close()

    preview = [{"strategy_id": str(sid), "name": str(nm)} for sid, nm in rows]

    checks_passed = int(empty_strategy_id or 0) == 0 and int(empty_name or 0) == 0

    return {
        "total_strategies": int(total or 0),
        "empty_strategy_id_rows": int(empty_strategy_id or 0),
        "empty_name_rows": int(empty_name or 0),
        "preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show strategies status and Layer 0 validation checks")
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
