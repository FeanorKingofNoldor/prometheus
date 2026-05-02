"""Prometheus v2 – Show portfolios status (Layer 0 validation).

Validates core Layer 0 contracts for the ``portfolios`` table:
- portfolio_id is non-empty
- name is non-empty
- base_currency is a 3-letter uppercase currency code

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
            SUM(CASE WHEN btrim(portfolio_id) = '' THEN 1 ELSE 0 END) AS empty_portfolio_id,
            SUM(CASE WHEN name IS NULL OR btrim(name) = '' THEN 1 ELSE 0 END) AS empty_name,
            SUM(CASE WHEN base_currency IS NULL OR base_currency !~ '^[A-Z]{3}$' THEN 1 ELSE 0 END) AS bad_base_currency
        FROM portfolios
    """

    sql_currency_breakdown = """
        SELECT base_currency, COUNT(*)
        FROM portfolios
        GROUP BY base_currency
        ORDER BY COUNT(*) DESC, base_currency
        LIMIT 50
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            total, empty_portfolio_id, empty_name, bad_base_currency = cur.fetchone()

            cur.execute(sql_currency_breakdown)
            rows = cur.fetchall()
        finally:
            cur.close()

    currency_breakdown = {str(c): int(n) for (c, n) in rows}

    checks_passed = (
        int(empty_portfolio_id or 0) == 0
        and int(empty_name or 0) == 0
        and int(bad_base_currency or 0) == 0
    )

    return {
        "total_portfolios": int(total or 0),
        "empty_portfolio_id_rows": int(empty_portfolio_id or 0),
        "empty_name_rows": int(empty_name or 0),
        "bad_base_currency_rows": int(bad_base_currency or 0),
        "base_currency_breakdown": currency_breakdown,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show portfolios status and Layer 0 validation checks")
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
