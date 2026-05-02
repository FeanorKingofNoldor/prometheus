"""Prometheus v2 – Show news_factors_daily status (Layer 2 validation).

Validates basic Layer 2 contracts for ``news_factors_daily``:
- issuer_id, model_id, factor_name are non-empty
- factor_value is finite (no NaN/Inf)
- metadata is either NULL or a JSON object
- issuer_id exists in issuers (checked via LEFT JOIN) when the target DB
  has issuers populated

Reports results for both runtime_db and historical_db.

Note: lookahead safety and factor semantics are higher-level audits.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from typing import Any, Optional, Sequence

from apatheon.core.database import get_db_manager

_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def _summarise(db, which: str) -> dict[str, Any]:
    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    sql_issuers_total = "SELECT COUNT(*) FROM issuers"

    sql_without_issuers_ref = f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT nf.issuer_id) AS distinct_issuers,
            COUNT(DISTINCT nf.factor_name) AS distinct_factors,
            MIN(nf.as_of_date) AS min_as_of_date,
            MAX(nf.as_of_date) AS max_as_of_date,
            MIN(nf.created_at) AS min_created_at,
            MAX(nf.created_at) AS max_created_at,
            SUM(CASE WHEN btrim(nf.issuer_id) = '' THEN 1 ELSE 0 END) AS empty_issuer_id,
            SUM(CASE WHEN btrim(nf.model_id) = '' THEN 1 ELSE 0 END) AS empty_model_id,
            SUM(CASE WHEN btrim(nf.factor_name) = '' THEN 1 ELSE 0 END) AS empty_factor_name,
            SUM(CASE WHEN nf.factor_value IN {_NONFINITE} THEN 1 ELSE 0 END) AS factor_value_nonfinite,
            SUM(CASE WHEN nf.metadata IS NOT NULL AND jsonb_typeof(nf.metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object
        FROM news_factors_daily nf
    """

    sql_with_issuers_ref = f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT nf.issuer_id) AS distinct_issuers,
            COUNT(DISTINCT nf.factor_name) AS distinct_factors,
            MIN(nf.as_of_date) AS min_as_of_date,
            MAX(nf.as_of_date) AS max_as_of_date,
            MIN(nf.created_at) AS min_created_at,
            MAX(nf.created_at) AS max_created_at,
            SUM(CASE WHEN btrim(nf.issuer_id) = '' THEN 1 ELSE 0 END) AS empty_issuer_id,
            SUM(CASE WHEN btrim(nf.model_id) = '' THEN 1 ELSE 0 END) AS empty_model_id,
            SUM(CASE WHEN btrim(nf.factor_name) = '' THEN 1 ELSE 0 END) AS empty_factor_name,
            SUM(CASE WHEN nf.factor_value IN {_NONFINITE} THEN 1 ELSE 0 END) AS factor_value_nonfinite,
            SUM(CASE WHEN nf.metadata IS NOT NULL AND jsonb_typeof(nf.metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object,
            SUM(CASE WHEN i.issuer_id IS NULL THEN 1 ELSE 0 END) AS orphan_issuer_id
        FROM news_factors_daily nf
        LEFT JOIN issuers i ON i.issuer_id = nf.issuer_id
    """

    sql_preview = """
        SELECT issuer_id, as_of_date, model_id, factor_name, factor_value
        FROM news_factors_daily
        ORDER BY as_of_date DESC, issuer_id, factor_name
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_issuers_total)
            issuers_total = int(cur.fetchone()[0] or 0)

            issuer_reference_check_skipped = issuers_total == 0

            if issuer_reference_check_skipped:
                cur.execute(sql_without_issuers_ref)
                (
                    total,
                    distinct_issuers,
                    distinct_factors,
                    min_as_of_date,
                    max_as_of_date,
                    min_created_at,
                    max_created_at,
                    empty_issuer_id,
                    empty_model_id,
                    empty_factor_name,
                    factor_value_nonfinite,
                    metadata_not_object,
                ) = cur.fetchone()
                orphan_issuer_id = 0
            else:
                cur.execute(sql_with_issuers_ref)
                (
                    total,
                    distinct_issuers,
                    distinct_factors,
                    min_as_of_date,
                    max_as_of_date,
                    min_created_at,
                    max_created_at,
                    empty_issuer_id,
                    empty_model_id,
                    empty_factor_name,
                    factor_value_nonfinite,
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
            "as_of_date": as_of.isoformat() if isinstance(as_of, date) else None,
            "model_id": str(model_id),
            "factor_name": str(fname),
            "factor_value": float(fval) if fval is not None else None,
        }
        for issuer_id, as_of, model_id, fname, fval in preview_rows
    ]

    checks_passed = (
        int(empty_issuer_id or 0) == 0
        and int(empty_model_id or 0) == 0
        and int(empty_factor_name or 0) == 0
        and int(factor_value_nonfinite or 0) == 0
        and int(metadata_not_object or 0) == 0
        and (issuer_reference_check_skipped or int(orphan_issuer_id or 0) == 0)
    )

    return {
        "total_rows": int(total or 0),
        "distinct_issuers": int(distinct_issuers or 0),
        "distinct_factors": int(distinct_factors or 0),
        "min_as_of_date": min_as_of_date.isoformat() if isinstance(min_as_of_date, date) else None,
        "max_as_of_date": max_as_of_date.isoformat() if isinstance(max_as_of_date, date) else None,
        "min_created_at": min_created_at.isoformat() if isinstance(min_created_at, datetime) else None,
        "max_created_at": max_created_at.isoformat() if isinstance(max_created_at, datetime) else None,
        "empty_issuer_id_rows": int(empty_issuer_id or 0),
        "empty_model_id_rows": int(empty_model_id or 0),
        "empty_factor_name_rows": int(empty_factor_name or 0),
        "factor_value_nonfinite_rows": int(factor_value_nonfinite or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "orphan_issuer_id_rows": int(orphan_issuer_id or 0),
        "issuer_table_total_rows": int(issuers_total),
        "issuer_reference_check_skipped": bool(issuer_reference_check_skipped),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show news_factors_daily status and basic Layer 2 validation checks"
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
