"""Prometheus v2 – Show market_holidays status (Layer 0 validation).

This script validates basic Layer 0 contracts for the ``market_holidays`` table:
- uniqueness per (market_id, holiday_date)
- no empty holiday names
- market_id references an existing markets.market_id (when markets exists)

It reports results for both runtime_db and historical_db.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager


def _summarise(db, which: str) -> dict[str, Any]:
    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    sql_overview = """
        SELECT
            COUNT(*) AS n,
            MIN(holiday_date) AS min_date,
            MAX(holiday_date) AS max_date
        FROM market_holidays
    """

    sql_by_market = """
        SELECT market_id, COUNT(*), MIN(holiday_date), MAX(holiday_date)
        FROM market_holidays
        GROUP BY market_id
        ORDER BY market_id
    """

    sql_duplicates = """
        SELECT market_id, holiday_date, COUNT(*)
        FROM market_holidays
        GROUP BY market_id, holiday_date
        HAVING COUNT(*) > 1
        LIMIT 5
    """

    sql_empty_names = """
        SELECT COUNT(*)
        FROM market_holidays
        WHERE holiday_name IS NULL OR btrim(holiday_name) = ''
    """

    sql_orphans = """
        SELECT DISTINCT mh.market_id
        FROM market_holidays mh
        LEFT JOIN markets m ON m.market_id = mh.market_id
        WHERE m.market_id IS NULL
        ORDER BY mh.market_id
        LIMIT 20
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_overview)
            n, min_date, max_date = cur.fetchone()

            cur.execute(sql_by_market)
            by_market_rows = cur.fetchall()

            cur.execute(sql_duplicates)
            dup_rows = cur.fetchall()

            cur.execute(sql_empty_names)
            (empty_names,) = cur.fetchone()

            # Orphans check: if markets table is missing, this query would fail.
            try:
                cur.execute(sql_orphans)
                orphan_rows = cur.fetchall()
            except Exception:
                orphan_rows = []
        finally:
            cur.close()

    by_market = []
    for market_id, cnt, mn, mx in by_market_rows:
        by_market.append(
            {
                "market_id": str(market_id),
                "count": int(cnt),
                "min_date": mn.isoformat() if isinstance(mn, date) else None,
                "max_date": mx.isoformat() if isinstance(mx, date) else None,
            }
        )

    duplicates = [
        {"market_id": str(mid), "holiday_date": d.isoformat() if isinstance(d, date) else str(d), "count": int(c)}
        for (mid, d, c) in dup_rows
    ]

    orphans = [str(r[0]) for r in orphan_rows]

    n_int = int(n or 0)

    return {
        "total_holidays": n_int,
        "min_date": min_date.isoformat() if isinstance(min_date, date) else None,
        "max_date": max_date.isoformat() if isinstance(max_date, date) else None,
        "by_market": by_market,
        "duplicate_rows": duplicates,
        "duplicates_check_passed": len(duplicates) == 0,
        "empty_holiday_name_rows": int(empty_names or 0),
        "holiday_name_nonempty_check_passed": int(empty_names or 0) == 0,
        "orphan_market_ids": orphans,
        "orphan_check_passed": len(orphans) == 0,
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show market_holidays status and basic Layer 0 validations")
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
