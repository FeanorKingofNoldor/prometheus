"""Prometheus v2 – Show fundamental_ratios status (Layer 1 validation).

Validates basic Layer 1 contracts for ``fundamental_ratios``:
- issuer_id is non-empty
- frequency is non-empty and in a controlled set (ANNUAL/QUARTERLY)
- period_start < period_end
- numeric ratio fields are finite when present (no NaN/Inf)
- metrics is either NULL or a JSON object
- metadata is either NULL or a JSON object

Reports results for both runtime_db and historical_db.

Note: ratio definition/versioning is a higher-level audit.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager


_ALLOWED_FREQUENCIES = ("ANNUAL", "QUARTERLY")


def _summarise(db, which: str) -> dict[str, Any]:
    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    nonfinite_expr = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"

    sql = f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT fr.issuer_id) AS distinct_issuers,
            MIN(fr.period_end) AS min_period_end,
            MAX(fr.period_end) AS max_period_end,
            SUM(CASE WHEN btrim(fr.issuer_id) = '' THEN 1 ELSE 0 END) AS empty_issuer_id,
            SUM(CASE WHEN btrim(fr.frequency) = '' THEN 1 ELSE 0 END) AS empty_frequency,
            SUM(CASE WHEN fr.frequency NOT IN {tuple(_ALLOWED_FREQUENCIES)!r} THEN 1 ELSE 0 END) AS bad_frequency,
            SUM(CASE WHEN fr.period_start >= fr.period_end THEN 1 ELSE 0 END) AS bad_period_window,
            SUM(CASE WHEN fr.roe IN {nonfinite_expr} THEN 1 ELSE 0 END) AS roe_nonfinite,
            SUM(CASE WHEN fr.roic IN {nonfinite_expr} THEN 1 ELSE 0 END) AS roic_nonfinite,
            SUM(CASE WHEN fr.gross_margin IN {nonfinite_expr} THEN 1 ELSE 0 END) AS gross_margin_nonfinite,
            SUM(CASE WHEN fr.op_margin IN {nonfinite_expr} THEN 1 ELSE 0 END) AS op_margin_nonfinite,
            SUM(CASE WHEN fr.net_margin IN {nonfinite_expr} THEN 1 ELSE 0 END) AS net_margin_nonfinite,
            SUM(CASE WHEN fr.leverage IN {nonfinite_expr} THEN 1 ELSE 0 END) AS leverage_nonfinite,
            SUM(CASE WHEN fr.interest_coverage IN {nonfinite_expr} THEN 1 ELSE 0 END) AS interest_coverage_nonfinite,
            SUM(CASE WHEN fr.revenue_growth IN {nonfinite_expr} THEN 1 ELSE 0 END) AS revenue_growth_nonfinite,
            SUM(CASE WHEN fr.eps_growth IN {nonfinite_expr} THEN 1 ELSE 0 END) AS eps_growth_nonfinite,
            SUM(CASE WHEN fr.metrics IS NOT NULL AND jsonb_typeof(fr.metrics) <> 'object' THEN 1 ELSE 0 END) AS metrics_not_object,
            SUM(CASE WHEN fr.metadata IS NOT NULL AND jsonb_typeof(fr.metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object,
            SUM(CASE WHEN i.issuer_id IS NULL THEN 1 ELSE 0 END) AS orphan_issuer_id
        FROM fundamental_ratios fr
        LEFT JOIN issuers i ON i.issuer_id = fr.issuer_id
    """

    sql_preview = """
        SELECT issuer_id, period_start, period_end, frequency, roe, roic, gross_margin, net_margin
        FROM fundamental_ratios
        ORDER BY period_end DESC, issuer_id
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                distinct_issuers,
                min_period_end,
                max_period_end,
                empty_issuer_id,
                empty_frequency,
                bad_frequency,
                bad_period_window,
                roe_nonfinite,
                roic_nonfinite,
                gross_margin_nonfinite,
                op_margin_nonfinite,
                net_margin_nonfinite,
                leverage_nonfinite,
                interest_coverage_nonfinite,
                revenue_growth_nonfinite,
                eps_growth_nonfinite,
                metrics_not_object,
                metadata_not_object,
                orphan_issuer_id,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "issuer_id": str(issuer_id),
            "period_start": pstart.isoformat() if isinstance(pstart, date) else None,
            "period_end": pend.isoformat() if isinstance(pend, date) else None,
            "frequency": str(freq),
            "roe": float(roe) if roe is not None else None,
            "roic": float(roic) if roic is not None else None,
            "gross_margin": float(gm) if gm is not None else None,
            "net_margin": float(nm) if nm is not None else None,
        }
        for issuer_id, pstart, pend, freq, roe, roic, gm, nm in preview_rows
    ]

    checks_passed = (
        int(empty_issuer_id or 0) == 0
        and int(empty_frequency or 0) == 0
        and int(bad_frequency or 0) == 0
        and int(bad_period_window or 0) == 0
        and int(roe_nonfinite or 0) == 0
        and int(roic_nonfinite or 0) == 0
        and int(gross_margin_nonfinite or 0) == 0
        and int(op_margin_nonfinite or 0) == 0
        and int(net_margin_nonfinite or 0) == 0
        and int(leverage_nonfinite or 0) == 0
        and int(interest_coverage_nonfinite or 0) == 0
        and int(revenue_growth_nonfinite or 0) == 0
        and int(eps_growth_nonfinite or 0) == 0
        and int(metrics_not_object or 0) == 0
        and int(metadata_not_object or 0) == 0
        and int(orphan_issuer_id or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_issuers": int(distinct_issuers or 0),
        "min_period_end": min_period_end.isoformat() if isinstance(min_period_end, date) else None,
        "max_period_end": max_period_end.isoformat() if isinstance(max_period_end, date) else None,
        "empty_issuer_id_rows": int(empty_issuer_id or 0),
        "empty_frequency_rows": int(empty_frequency or 0),
        "bad_frequency_rows": int(bad_frequency or 0),
        "bad_period_window_rows": int(bad_period_window or 0),
        "roe_nonfinite_rows": int(roe_nonfinite or 0),
        "roic_nonfinite_rows": int(roic_nonfinite or 0),
        "gross_margin_nonfinite_rows": int(gross_margin_nonfinite or 0),
        "op_margin_nonfinite_rows": int(op_margin_nonfinite or 0),
        "net_margin_nonfinite_rows": int(net_margin_nonfinite or 0),
        "leverage_nonfinite_rows": int(leverage_nonfinite or 0),
        "interest_coverage_nonfinite_rows": int(interest_coverage_nonfinite or 0),
        "revenue_growth_nonfinite_rows": int(revenue_growth_nonfinite or 0),
        "eps_growth_nonfinite_rows": int(eps_growth_nonfinite or 0),
        "metrics_not_object_rows": int(metrics_not_object or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "orphan_issuer_id_rows": int(orphan_issuer_id or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show fundamental_ratios status and basic Layer 1 validation checks"
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
