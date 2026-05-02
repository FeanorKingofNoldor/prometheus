"""Prometheus v2 – Show returns_daily status (Layer 1 validation).

Validates basic Layer 1 contracts for ``returns_daily``:
- unique per (instrument_id, trade_date) (enforced by PK; not checked here)
- returns are finite (no NaN/Inf)
- returns are bounded below by -1.0

Reports results for both runtime_db and historical_db.

Note: consistency with prices_daily (exact recomputation equality) is a
higher-level audit.
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
            COUNT(DISTINCT instrument_id) AS distinct_instruments,
            MIN(trade_date) AS min_trade_date,
            MAX(trade_date) AS max_trade_date,
            SUM(CASE WHEN EXTRACT(ISODOW FROM trade_date) IN (6,7) THEN 1 ELSE 0 END) AS weekend_trade_date,
            SUM(CASE WHEN btrim(instrument_id) = '' THEN 1 ELSE 0 END) AS empty_instrument_id,
            SUM(CASE WHEN ret_1d < -1.0 THEN 1 ELSE 0 END) AS ret1_lt_neg1,
            SUM(CASE WHEN ret_5d < -1.0 THEN 1 ELSE 0 END) AS ret5_lt_neg1,
            SUM(CASE WHEN ret_21d < -1.0 THEN 1 ELSE 0 END) AS ret21_lt_neg1,
            SUM(CASE WHEN ret_1d IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision) THEN 1 ELSE 0 END) AS ret1_nonfinite,
            SUM(CASE WHEN ret_5d IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision) THEN 1 ELSE 0 END) AS ret5_nonfinite,
            SUM(CASE WHEN ret_21d IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision) THEN 1 ELSE 0 END) AS ret21_nonfinite
        FROM returns_daily
    """

    sql_preview = """
        SELECT instrument_id, trade_date, ret_1d, ret_5d, ret_21d
        FROM returns_daily
        ORDER BY trade_date DESC, instrument_id
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_instruments,
                min_trade_date,
                max_trade_date,
                weekend_trade_date,
                empty_instrument_id,
                ret1_lt_neg1,
                ret5_lt_neg1,
                ret21_lt_neg1,
                ret1_nonfinite,
                ret5_nonfinite,
                ret21_nonfinite,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "instrument_id": str(iid),
            "trade_date": td.isoformat() if isinstance(td, date) else None,
            "ret_1d": float(r1) if r1 is not None else None,
            "ret_5d": float(r5) if r5 is not None else None,
            "ret_21d": float(r21) if r21 is not None else None,
        }
        for iid, td, r1, r5, r21 in preview_rows
    ]

    checks_passed = (
        int(weekend_trade_date or 0) == 0
        and int(empty_instrument_id or 0) == 0
        and int(ret1_lt_neg1 or 0) == 0
        and int(ret5_lt_neg1 or 0) == 0
        and int(ret21_lt_neg1 or 0) == 0
        and int(ret1_nonfinite or 0) == 0
        and int(ret5_nonfinite or 0) == 0
        and int(ret21_nonfinite or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_instruments": int(distinct_instruments or 0),
        "min_trade_date": min_trade_date.isoformat() if isinstance(min_trade_date, date) else None,
        "max_trade_date": max_trade_date.isoformat() if isinstance(max_trade_date, date) else None,
        "weekend_trade_date_rows": int(weekend_trade_date or 0),
        "empty_instrument_id_rows": int(empty_instrument_id or 0),
        "ret_1d_less_than_neg1_rows": int(ret1_lt_neg1 or 0),
        "ret_5d_less_than_neg1_rows": int(ret5_lt_neg1 or 0),
        "ret_21d_less_than_neg1_rows": int(ret21_lt_neg1 or 0),
        "ret_1d_nonfinite_rows": int(ret1_nonfinite or 0),
        "ret_5d_nonfinite_rows": int(ret5_nonfinite or 0),
        "ret_21d_nonfinite_rows": int(ret21_nonfinite or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show returns_daily status and basic Layer 1 validation checks")
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
