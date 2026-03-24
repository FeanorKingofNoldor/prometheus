"""Prometheus v2 – Show portfolio_risk_reports status (Layer 3 validation).

Validates basic Layer 3 contracts for ``portfolio_risk_reports``:
- report_id, portfolio_id are non-empty
- portfolio_value/cash/net_exposure/gross_exposure/leverage are finite
- portfolio_value > 0; gross_exposure >= 0; leverage >= 0
- risk_metrics is a JSON object
- scenario_pnl/exposures_by_sector/exposures_by_factor/metadata are JSON objects when present

Reports results for both runtime_db and historical_db.

Note: risk model correctness and factor conventions are higher-level audits.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager

_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def _summarise(db, which: str) -> dict[str, Any]:
    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    sql = f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT portfolio_id) AS distinct_portfolios,
            MIN(as_of_date) AS min_as_of_date,
            MAX(as_of_date) AS max_as_of_date,
            SUM(CASE WHEN btrim(report_id) = '' THEN 1 ELSE 0 END) AS empty_report_id,
            SUM(CASE WHEN btrim(portfolio_id) = '' THEN 1 ELSE 0 END) AS empty_portfolio_id,
            SUM(CASE WHEN portfolio_value IN {_NONFINITE} THEN 1 ELSE 0 END) AS portfolio_value_nonfinite,
            SUM(CASE WHEN portfolio_value <= 0.0 THEN 1 ELSE 0 END) AS portfolio_value_nonpositive,
            SUM(CASE WHEN cash IN {_NONFINITE} THEN 1 ELSE 0 END) AS cash_nonfinite,
            SUM(CASE WHEN net_exposure IN {_NONFINITE} THEN 1 ELSE 0 END) AS net_exposure_nonfinite,
            SUM(CASE WHEN gross_exposure IN {_NONFINITE} THEN 1 ELSE 0 END) AS gross_exposure_nonfinite,
            SUM(CASE WHEN gross_exposure < 0.0 THEN 1 ELSE 0 END) AS gross_exposure_negative,
            SUM(CASE WHEN leverage IN {_NONFINITE} THEN 1 ELSE 0 END) AS leverage_nonfinite,
            SUM(CASE WHEN leverage < 0.0 THEN 1 ELSE 0 END) AS leverage_negative,
            SUM(CASE WHEN jsonb_typeof(risk_metrics) <> 'object' THEN 1 ELSE 0 END) AS risk_metrics_not_object,
            SUM(CASE WHEN scenario_pnl IS NOT NULL AND jsonb_typeof(scenario_pnl) <> 'object' THEN 1 ELSE 0 END) AS scenario_pnl_not_object,
            SUM(CASE WHEN exposures_by_sector IS NOT NULL AND jsonb_typeof(exposures_by_sector) <> 'object' THEN 1 ELSE 0 END) AS exposures_by_sector_not_object,
            SUM(CASE WHEN exposures_by_factor IS NOT NULL AND jsonb_typeof(exposures_by_factor) <> 'object' THEN 1 ELSE 0 END) AS exposures_by_factor_not_object,
            SUM(CASE WHEN metadata IS NOT NULL AND jsonb_typeof(metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object
        FROM portfolio_risk_reports
    """

    sql_preview = """
        SELECT portfolio_id, as_of_date, gross_exposure, net_exposure, leverage
        FROM portfolio_risk_reports
        ORDER BY as_of_date DESC, portfolio_id
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_portfolios,
                min_as_of_date,
                max_as_of_date,
                empty_report_id,
                empty_portfolio_id,
                portfolio_value_nonfinite,
                portfolio_value_nonpositive,
                cash_nonfinite,
                net_exposure_nonfinite,
                gross_exposure_nonfinite,
                gross_exposure_negative,
                leverage_nonfinite,
                leverage_negative,
                risk_metrics_not_object,
                scenario_pnl_not_object,
                exposures_by_sector_not_object,
                exposures_by_factor_not_object,
                metadata_not_object,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "portfolio_id": str(portfolio_id),
            "as_of_date": as_of_date_db.isoformat() if isinstance(as_of_date_db, date) else None,
            "gross_exposure": float(gross) if gross is not None else None,
            "net_exposure": float(net) if net is not None else None,
            "leverage": float(lev) if lev is not None else None,
        }
        for portfolio_id, as_of_date_db, gross, net, lev in preview_rows
    ]

    checks_passed = (
        int(empty_report_id or 0) == 0
        and int(empty_portfolio_id or 0) == 0
        and int(portfolio_value_nonfinite or 0) == 0
        and int(portfolio_value_nonpositive or 0) == 0
        and int(cash_nonfinite or 0) == 0
        and int(net_exposure_nonfinite or 0) == 0
        and int(gross_exposure_nonfinite or 0) == 0
        and int(gross_exposure_negative or 0) == 0
        and int(leverage_nonfinite or 0) == 0
        and int(leverage_negative or 0) == 0
        and int(risk_metrics_not_object or 0) == 0
        and int(scenario_pnl_not_object or 0) == 0
        and int(exposures_by_sector_not_object or 0) == 0
        and int(exposures_by_factor_not_object or 0) == 0
        and int(metadata_not_object or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_portfolios": int(distinct_portfolios or 0),
        "min_as_of_date": min_as_of_date.isoformat() if isinstance(min_as_of_date, date) else None,
        "max_as_of_date": max_as_of_date.isoformat() if isinstance(max_as_of_date, date) else None,
        "empty_report_id_rows": int(empty_report_id or 0),
        "empty_portfolio_id_rows": int(empty_portfolio_id or 0),
        "portfolio_value_nonfinite_rows": int(portfolio_value_nonfinite or 0),
        "portfolio_value_nonpositive_rows": int(portfolio_value_nonpositive or 0),
        "cash_nonfinite_rows": int(cash_nonfinite or 0),
        "net_exposure_nonfinite_rows": int(net_exposure_nonfinite or 0),
        "gross_exposure_nonfinite_rows": int(gross_exposure_nonfinite or 0),
        "gross_exposure_negative_rows": int(gross_exposure_negative or 0),
        "leverage_nonfinite_rows": int(leverage_nonfinite or 0),
        "leverage_negative_rows": int(leverage_negative or 0),
        "risk_metrics_not_object_rows": int(risk_metrics_not_object or 0),
        "scenario_pnl_not_object_rows": int(scenario_pnl_not_object or 0),
        "exposures_by_sector_not_object_rows": int(exposures_by_sector_not_object or 0),
        "exposures_by_factor_not_object_rows": int(exposures_by_factor_not_object or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show portfolio_risk_reports status and basic Layer 3 validation checks"
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
