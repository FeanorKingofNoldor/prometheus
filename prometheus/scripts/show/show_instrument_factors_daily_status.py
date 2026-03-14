"""Prometheus v2 – Show instrument_factors_daily status (Layer 1 validation).

Validates basic Layer 1 contracts for ``instrument_factors_daily``:
- unique per (instrument_id, trade_date, factor_id) (enforced by PK; not checked here)
- instrument_id and factor_id are non-empty
- exposures are finite (no NaN/Inf)

Reports results for both runtime_db and historical_db.

Note: consistency of exposures with the factor model and with universe
membership is a higher-level audit.
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

    sql = """
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT instrument_id) AS distinct_instruments,
            COUNT(DISTINCT factor_id) AS distinct_factors,
            MIN(trade_date) AS min_trade_date,
            MAX(trade_date) AS max_trade_date,
            SUM(CASE WHEN EXTRACT(ISODOW FROM trade_date) IN (6,7) THEN 1 ELSE 0 END) AS weekend_trade_date,
            SUM(CASE WHEN btrim(instrument_id) = '' THEN 1 ELSE 0 END) AS empty_instrument_id,
            SUM(CASE WHEN btrim(factor_id) = '' THEN 1 ELSE 0 END) AS empty_factor_id,
            SUM(CASE WHEN exposure IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision) THEN 1 ELSE 0 END) AS exposure_nonfinite
        FROM instrument_factors_daily
    """

    sql_preview = """
        SELECT instrument_id, trade_date, factor_id, exposure
        FROM instrument_factors_daily
        ORDER BY trade_date DESC, instrument_id, factor_id
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_instruments,
                distinct_factors,
                min_trade_date,
                max_trade_date,
                weekend_trade_date,
                empty_instrument_id,
                empty_factor_id,
                exposure_nonfinite,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "instrument_id": str(iid),
            "trade_date": td.isoformat() if isinstance(td, date) else None,
            "factor_id": str(fid),
            "exposure": float(ex) if ex is not None else None,
        }
        for iid, td, fid, ex in preview_rows
    ]

    checks_passed = (
        int(weekend_trade_date or 0) == 0
        and int(empty_instrument_id or 0) == 0
        and int(empty_factor_id or 0) == 0
        and int(exposure_nonfinite or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_instruments": int(distinct_instruments or 0),
        "distinct_factors": int(distinct_factors or 0),
        "min_trade_date": min_trade_date.isoformat() if isinstance(min_trade_date, date) else None,
        "max_trade_date": max_trade_date.isoformat() if isinstance(max_trade_date, date) else None,
        "weekend_trade_date_rows": int(weekend_trade_date or 0),
        "empty_instrument_id_rows": int(empty_instrument_id or 0),
        "empty_factor_id_rows": int(empty_factor_id or 0),
        "exposure_nonfinite_rows": int(exposure_nonfinite or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show instrument_factors_daily status and basic Layer 1 validation checks",
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
