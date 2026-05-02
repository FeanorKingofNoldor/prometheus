"""Prometheus v2 – Show factors_daily status (Layer 1 validation).

Validates basic Layer 1 contracts for ``factors_daily``:
- unique per (factor_id, trade_date) (enforced by PK; not checked here)
- factor_id is non-empty
- factor values are finite (no NaN/Inf)
- factor values are bounded below by -1.0

Reports results for both runtime_db and historical_db.

Note: factor semantics (what value represents, how it is computed) are
validated via code review and higher-level audits.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
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
            COUNT(DISTINCT factor_id) AS distinct_factors,
            MIN(trade_date) AS min_trade_date,
            MAX(trade_date) AS max_trade_date,
            SUM(CASE WHEN EXTRACT(ISODOW FROM trade_date) IN (6,7) THEN 1 ELSE 0 END) AS weekend_trade_date,
            SUM(CASE WHEN btrim(factor_id) = '' THEN 1 ELSE 0 END) AS empty_factor_id,
            SUM(CASE WHEN value < -1.0 THEN 1 ELSE 0 END) AS value_lt_neg1,
            SUM(CASE WHEN value IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision) THEN 1 ELSE 0 END) AS value_nonfinite
        FROM factors_daily
    """

    sql_preview = """
        SELECT factor_id, trade_date, value
        FROM factors_daily
        ORDER BY trade_date DESC, factor_id
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_factors,
                min_trade_date,
                max_trade_date,
                weekend_trade_date,
                empty_factor_id,
                value_lt_neg1,
                value_nonfinite,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "factor_id": str(fid),
            "trade_date": td.isoformat() if isinstance(td, date) else None,
            "value": float(v) if v is not None else None,
        }
        for fid, td, v in preview_rows
    ]

    checks_passed = (
        int(weekend_trade_date or 0) == 0
        and int(empty_factor_id or 0) == 0
        and int(value_lt_neg1 or 0) == 0
        and int(value_nonfinite or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_factors": int(distinct_factors or 0),
        "min_trade_date": min_trade_date.isoformat() if isinstance(min_trade_date, date) else None,
        "max_trade_date": max_trade_date.isoformat() if isinstance(max_trade_date, date) else None,
        "weekend_trade_date_rows": int(weekend_trade_date or 0),
        "empty_factor_id_rows": int(empty_factor_id or 0),
        "value_less_than_neg1_rows": int(value_lt_neg1 or 0),
        "value_nonfinite_rows": int(value_nonfinite or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show factors_daily status and basic Layer 1 validation checks")
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
