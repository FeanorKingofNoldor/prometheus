"""Prometheus v2 – Show financial_statements status (Layer 1 validation).

Validates basic Layer 1 contracts for ``financial_statements``:
- issuer_id is non-empty
- fiscal_period is non-empty and follows a simple YYYY[A|Qn] format
- statement_type is one of IS/BS/CF
- period_end is present
- currency is either NULL or non-empty
- values is a JSON object
- metadata is either NULL or a JSON object

Reports results for both runtime_db and historical_db.

Note: statement content completeness and mapping consistency (e.g. GAAP tags)
are higher-level audits.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager


_ALLOWED_STATEMENT_TYPES = ("IS", "BS", "CF")
_FISCAL_PERIOD_RE = "^[0-9]{4}(A|Q[1-4])$"


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
            COUNT(DISTINCT fs.issuer_id) AS distinct_issuers,
            MIN(fs.period_end) AS min_period_end,
            MAX(fs.period_end) AS max_period_end,
            SUM(CASE WHEN btrim(fs.issuer_id) = '' THEN 1 ELSE 0 END) AS empty_issuer_id,
            SUM(CASE WHEN btrim(fs.fiscal_period) = '' THEN 1 ELSE 0 END) AS empty_fiscal_period,
            SUM(CASE WHEN fs.fiscal_period !~ '{_FISCAL_PERIOD_RE}' THEN 1 ELSE 0 END) AS bad_fiscal_period_format,
            SUM(CASE WHEN fs.statement_type NOT IN {tuple(_ALLOWED_STATEMENT_TYPES)!r} THEN 1 ELSE 0 END) AS bad_statement_type,
            SUM(CASE WHEN fs.period_end IS NULL THEN 1 ELSE 0 END) AS null_period_end,
            SUM(CASE WHEN fs.currency IS NOT NULL AND btrim(fs.currency) = '' THEN 1 ELSE 0 END) AS empty_currency,
            SUM(CASE WHEN jsonb_typeof(fs.values) <> 'object' THEN 1 ELSE 0 END) AS values_not_object,
            SUM(CASE WHEN fs.metadata IS NOT NULL AND jsonb_typeof(fs.metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object,
            SUM(CASE WHEN i.issuer_id IS NULL THEN 1 ELSE 0 END) AS orphan_issuer_id,
            SUM(CASE WHEN fs.period_end IS NOT NULL AND fs.report_date < fs.period_end THEN 1 ELSE 0 END) AS report_before_period_end
        FROM financial_statements fs
        LEFT JOIN issuers i ON i.issuer_id = fs.issuer_id
    """

    sql_preview = """
        SELECT statement_id, issuer_id, statement_type, fiscal_period, fiscal_year, period_end, report_date, currency
        FROM financial_statements
        ORDER BY period_end DESC NULLS LAST, issuer_id, statement_type, statement_id DESC
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
                empty_fiscal_period,
                bad_fiscal_period_format,
                bad_statement_type,
                null_period_end,
                empty_currency,
                values_not_object,
                metadata_not_object,
                orphan_issuer_id,
                report_before_period_end,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "statement_id": int(sid) if sid is not None else None,
            "issuer_id": str(issuer_id),
            "statement_type": str(stype),
            "fiscal_period": str(fper),
            "fiscal_year": int(fyear) if fyear is not None else None,
            "period_end": pend.isoformat() if isinstance(pend, date) else None,
            "report_date": rdate.isoformat() if isinstance(rdate, date) else None,
            "currency": str(cur) if cur is not None else None,
        }
        for sid, issuer_id, stype, fper, fyear, pend, rdate, cur in preview_rows
    ]

    checks_passed = (
        int(empty_issuer_id or 0) == 0
        and int(empty_fiscal_period or 0) == 0
        and int(bad_fiscal_period_format or 0) == 0
        and int(bad_statement_type or 0) == 0
        and int(null_period_end or 0) == 0
        and int(empty_currency or 0) == 0
        and int(values_not_object or 0) == 0
        and int(metadata_not_object or 0) == 0
        and int(orphan_issuer_id or 0) == 0
        and int(report_before_period_end or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "distinct_issuers": int(distinct_issuers or 0),
        "min_period_end": min_period_end.isoformat() if isinstance(min_period_end, date) else None,
        "max_period_end": max_period_end.isoformat() if isinstance(max_period_end, date) else None,
        "empty_issuer_id_rows": int(empty_issuer_id or 0),
        "empty_fiscal_period_rows": int(empty_fiscal_period or 0),
        "bad_fiscal_period_format_rows": int(bad_fiscal_period_format or 0),
        "bad_statement_type_rows": int(bad_statement_type or 0),
        "null_period_end_rows": int(null_period_end or 0),
        "empty_currency_rows": int(empty_currency or 0),
        "values_not_object_rows": int(values_not_object or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "orphan_issuer_id_rows": int(orphan_issuer_id or 0),
        "report_before_period_end_rows": int(report_before_period_end or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show financial_statements status and basic Layer 1 validation checks"
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
