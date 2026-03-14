"""Prometheus v2 – Show book_targets status (Layer 3 validation).

Validates basic Layer 3 contracts for ``book_targets``:
- target_id, book_id, region, entity_type, entity_id are non-empty
- target_weight is finite (no NaN/Inf)
- metadata is either NULL or a JSON object
- entity_id exists in instruments when entity_type=INSTRUMENT and the
  target DB has instruments populated

Reports results for both runtime_db and historical_db.

Note: portfolio-level sum rules are higher-level audits.
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

    sql_instruments_total = "SELECT COUNT(*) FROM instruments"

    sql_without_instruments_ref = f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT bt.book_id) AS distinct_books,
            COUNT(DISTINCT bt.entity_type) AS distinct_entity_types,
            MIN(bt.as_of_date) AS min_as_of_date,
            MAX(bt.as_of_date) AS max_as_of_date,
            SUM(CASE WHEN btrim(bt.target_id) = '' THEN 1 ELSE 0 END) AS empty_target_id,
            SUM(CASE WHEN btrim(bt.book_id) = '' THEN 1 ELSE 0 END) AS empty_book_id,
            SUM(CASE WHEN btrim(bt.region) = '' THEN 1 ELSE 0 END) AS empty_region,
            SUM(CASE WHEN btrim(bt.entity_type) = '' THEN 1 ELSE 0 END) AS empty_entity_type,
            SUM(CASE WHEN btrim(bt.entity_id) = '' THEN 1 ELSE 0 END) AS empty_entity_id,
            SUM(CASE WHEN bt.target_weight IN {_NONFINITE} THEN 1 ELSE 0 END) AS target_weight_nonfinite,
            SUM(CASE WHEN bt.metadata IS NOT NULL AND jsonb_typeof(bt.metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object
        FROM book_targets bt
    """

    sql_with_instruments_ref = f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT bt.book_id) AS distinct_books,
            COUNT(DISTINCT bt.entity_type) AS distinct_entity_types,
            MIN(bt.as_of_date) AS min_as_of_date,
            MAX(bt.as_of_date) AS max_as_of_date,
            SUM(CASE WHEN btrim(bt.target_id) = '' THEN 1 ELSE 0 END) AS empty_target_id,
            SUM(CASE WHEN btrim(bt.book_id) = '' THEN 1 ELSE 0 END) AS empty_book_id,
            SUM(CASE WHEN btrim(bt.region) = '' THEN 1 ELSE 0 END) AS empty_region,
            SUM(CASE WHEN btrim(bt.entity_type) = '' THEN 1 ELSE 0 END) AS empty_entity_type,
            SUM(CASE WHEN btrim(bt.entity_id) = '' THEN 1 ELSE 0 END) AS empty_entity_id,
            SUM(CASE WHEN bt.target_weight IN {_NONFINITE} THEN 1 ELSE 0 END) AS target_weight_nonfinite,
            SUM(CASE WHEN bt.metadata IS NOT NULL AND jsonb_typeof(bt.metadata) <> 'object' THEN 1 ELSE 0 END) AS metadata_not_object,
            SUM(CASE WHEN bt.entity_type = 'INSTRUMENT' AND i.instrument_id IS NULL THEN 1 ELSE 0 END) AS orphan_instrument_id
        FROM book_targets bt
        LEFT JOIN instruments i ON i.instrument_id = bt.entity_id
    """

    sql_preview = """
        SELECT book_id, as_of_date, region, entity_type, entity_id, target_weight
        FROM book_targets
        ORDER BY as_of_date DESC, book_id, region, entity_type, entity_id
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_instruments_total)
            instruments_total = int(cur.fetchone()[0] or 0)

            instrument_reference_check_skipped = instruments_total == 0

            if instrument_reference_check_skipped:
                cur.execute(sql_without_instruments_ref)
                (
                    total,
                    distinct_books,
                    distinct_entity_types,
                    min_as_of_date,
                    max_as_of_date,
                    empty_target_id,
                    empty_book_id,
                    empty_region,
                    empty_entity_type,
                    empty_entity_id,
                    target_weight_nonfinite,
                    metadata_not_object,
                ) = cur.fetchone()
                orphan_instrument_id = 0
            else:
                cur.execute(sql_with_instruments_ref)
                (
                    total,
                    distinct_books,
                    distinct_entity_types,
                    min_as_of_date,
                    max_as_of_date,
                    empty_target_id,
                    empty_book_id,
                    empty_region,
                    empty_entity_type,
                    empty_entity_id,
                    target_weight_nonfinite,
                    metadata_not_object,
                    orphan_instrument_id,
                ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "book_id": str(book_id),
            "as_of_date": as_of_date_db.isoformat() if isinstance(as_of_date_db, date) else None,
            "region": str(region),
            "entity_type": str(entity_type),
            "entity_id": str(entity_id),
            "target_weight": float(target_weight) if target_weight is not None else None,
        }
        for book_id, as_of_date_db, region, entity_type, entity_id, target_weight in preview_rows
    ]

    checks_passed = (
        int(empty_target_id or 0) == 0
        and int(empty_book_id or 0) == 0
        and int(empty_region or 0) == 0
        and int(empty_entity_type or 0) == 0
        and int(empty_entity_id or 0) == 0
        and int(target_weight_nonfinite or 0) == 0
        and int(metadata_not_object or 0) == 0
        and (instrument_reference_check_skipped or int(orphan_instrument_id or 0) == 0)
    )

    return {
        "total_rows": int(total or 0),
        "distinct_books": int(distinct_books or 0),
        "distinct_entity_types": int(distinct_entity_types or 0),
        "min_as_of_date": min_as_of_date.isoformat() if isinstance(min_as_of_date, date) else None,
        "max_as_of_date": max_as_of_date.isoformat() if isinstance(max_as_of_date, date) else None,
        "empty_target_id_rows": int(empty_target_id or 0),
        "empty_book_id_rows": int(empty_book_id or 0),
        "empty_region_rows": int(empty_region or 0),
        "empty_entity_type_rows": int(empty_entity_type or 0),
        "empty_entity_id_rows": int(empty_entity_id or 0),
        "target_weight_nonfinite_rows": int(target_weight_nonfinite or 0),
        "metadata_not_object_rows": int(metadata_not_object or 0),
        "orphan_instrument_id_rows": int(orphan_instrument_id or 0),
        "instrument_table_total_rows": int(instruments_total),
        "instrument_reference_check_skipped": bool(instrument_reference_check_skipped),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show book_targets status and basic Layer 3 validation checks"
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
