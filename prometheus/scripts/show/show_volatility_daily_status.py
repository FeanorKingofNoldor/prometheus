"""Prometheus v2 – Show volatility_daily status (Layer 1 validation).

Validates basic Layer 1 contracts for ``volatility_daily``:
- unique per (instrument_id, trade_date) (enforced by PK; not checked here)
- volatility values are finite (no NaN/Inf)
- volatility values are non-negative

Reports results for both runtime_db and historical_db.
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
            SUM(CASE WHEN vol_21d < 0.0 THEN 1 ELSE 0 END) AS vol21_lt0,
            SUM(CASE WHEN vol_63d < 0.0 THEN 1 ELSE 0 END) AS vol63_lt0,
            SUM(CASE WHEN vol_21d IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision) THEN 1 ELSE 0 END) AS vol21_nonfinite,
            SUM(CASE WHEN vol_63d IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision) THEN 1 ELSE 0 END) AS vol63_nonfinite
        FROM volatility_daily
    """

    sql_preview = """
        SELECT instrument_id, trade_date, vol_21d, vol_63d
        FROM volatility_daily
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
                vol21_lt0,
                vol63_lt0,
                vol21_nonfinite,
                vol63_nonfinite,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "instrument_id": str(iid),
            "trade_date": td.isoformat() if isinstance(td, date) else None,
            "vol_21d": float(v21) if v21 is not None else None,
            "vol_63d": float(v63) if v63 is not None else None,
        }
        for iid, td, v21, v63 in preview_rows
    ]

    checks_passed = (
        int(weekend_trade_date or 0) == 0
        and int(empty_instrument_id or 0) == 0
        and int(vol21_lt0 or 0) == 0
        and int(vol63_lt0 or 0) == 0
        and int(vol21_nonfinite or 0) == 0
        and int(vol63_nonfinite or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_instruments": int(distinct_instruments or 0),
        "min_trade_date": min_trade_date.isoformat() if isinstance(min_trade_date, date) else None,
        "max_trade_date": max_trade_date.isoformat() if isinstance(max_trade_date, date) else None,
        "weekend_trade_date_rows": int(weekend_trade_date or 0),
        "empty_instrument_id_rows": int(empty_instrument_id or 0),
        "vol_21d_negative_rows": int(vol21_lt0 or 0),
        "vol_63d_negative_rows": int(vol63_lt0 or 0),
        "vol_21d_nonfinite_rows": int(vol21_nonfinite or 0),
        "vol_63d_nonfinite_rows": int(vol63_nonfinite or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show volatility_daily status and basic Layer 1 validation checks")
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
