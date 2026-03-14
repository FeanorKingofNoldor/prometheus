"""Prometheus v2 – Show scenario_paths status (Layer 2 validation).

Validates basic Layer 2 contracts for ``scenario_paths``:
- scenario_set_id is non-empty and refers to an existing scenario_set (checked via LEFT JOIN)
- scenario_id >= 0 and horizon_index >= 0
- instrument_id, factor_id, macro_id are non-empty
- return_value is finite and >= -1.0
- price is either NULL or finite and >= 0.0
- shock_metadata is either NULL or a JSON object

Reports results for both runtime_db and historical_db.
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
            MIN(scenario_id) AS min_scenario_id,
            MAX(scenario_id) AS max_scenario_id,
            MIN(horizon_index) AS min_horizon_index,
            MAX(horizon_index) AS max_horizon_index,
            SUM(CASE WHEN btrim(sp.scenario_set_id) = '' THEN 1 ELSE 0 END) AS empty_scenario_set_id,
            SUM(CASE WHEN sp.scenario_id < 0 THEN 1 ELSE 0 END) AS neg_scenario_id,
            SUM(CASE WHEN sp.horizon_index < 0 THEN 1 ELSE 0 END) AS neg_horizon_index,
            SUM(CASE WHEN btrim(sp.instrument_id) = '' THEN 1 ELSE 0 END) AS empty_instrument_id,
            SUM(CASE WHEN btrim(sp.factor_id) = '' THEN 1 ELSE 0 END) AS empty_factor_id,
            SUM(CASE WHEN btrim(sp.macro_id) = '' THEN 1 ELSE 0 END) AS empty_macro_id,
            SUM(CASE WHEN sp.return_value IN {_NONFINITE} THEN 1 ELSE 0 END) AS return_value_nonfinite,
            SUM(CASE WHEN sp.return_value < -1.0 THEN 1 ELSE 0 END) AS return_value_lt_neg1,
            SUM(CASE WHEN sp.price IS NOT NULL AND sp.price IN {_NONFINITE} THEN 1 ELSE 0 END) AS price_nonfinite,
            SUM(CASE WHEN sp.price IS NOT NULL AND sp.price < 0.0 THEN 1 ELSE 0 END) AS price_negative,
            SUM(CASE WHEN sp.shock_metadata IS NOT NULL AND jsonb_typeof(sp.shock_metadata) <> 'object' THEN 1 ELSE 0 END) AS shock_metadata_not_object,
            SUM(CASE WHEN ss.scenario_set_id IS NULL THEN 1 ELSE 0 END) AS orphan_scenario_set_id
        FROM scenario_paths sp
        LEFT JOIN scenario_sets ss ON ss.scenario_set_id = sp.scenario_set_id
    """

    sql_preview = """
        SELECT scenario_set_id, scenario_id, horizon_index, instrument_id, return_value
        FROM scenario_paths
        ORDER BY scenario_id DESC, horizon_index DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                min_scenario_id,
                max_scenario_id,
                min_horizon_index,
                max_horizon_index,
                empty_scenario_set_id,
                neg_scenario_id,
                neg_horizon_index,
                empty_instrument_id,
                empty_factor_id,
                empty_macro_id,
                return_value_nonfinite,
                return_value_lt_neg1,
                price_nonfinite,
                price_negative,
                shock_metadata_not_object,
                orphan_scenario_set_id,
            ) = cur.fetchone()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    preview = [
        {
            "scenario_set_id": str(sid),
            "scenario_id": int(scid) if scid is not None else None,
            "horizon_index": int(hidx) if hidx is not None else None,
            "instrument_id": str(inst_id),
            "return_value": float(rv) if rv is not None else None,
        }
        for sid, scid, hidx, inst_id, rv in preview_rows
    ]

    checks_passed = (
        int(empty_scenario_set_id or 0) == 0
        and int(neg_scenario_id or 0) == 0
        and int(neg_horizon_index or 0) == 0
        and int(empty_instrument_id or 0) == 0
        and int(empty_factor_id or 0) == 0
        and int(empty_macro_id or 0) == 0
        and int(return_value_nonfinite or 0) == 0
        and int(return_value_lt_neg1 or 0) == 0
        and int(price_nonfinite or 0) == 0
        and int(price_negative or 0) == 0
        and int(shock_metadata_not_object or 0) == 0
        and int(orphan_scenario_set_id or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "min_scenario_id": int(min_scenario_id) if min_scenario_id is not None else None,
        "max_scenario_id": int(max_scenario_id) if max_scenario_id is not None else None,
        "min_horizon_index": int(min_horizon_index) if min_horizon_index is not None else None,
        "max_horizon_index": int(max_horizon_index) if max_horizon_index is not None else None,
        "empty_scenario_set_id_rows": int(empty_scenario_set_id or 0),
        "negative_scenario_id_rows": int(neg_scenario_id or 0),
        "negative_horizon_index_rows": int(neg_horizon_index or 0),
        "empty_instrument_id_rows": int(empty_instrument_id or 0),
        "empty_factor_id_rows": int(empty_factor_id or 0),
        "empty_macro_id_rows": int(empty_macro_id or 0),
        "return_value_nonfinite_rows": int(return_value_nonfinite or 0),
        "return_value_less_than_neg1_rows": int(return_value_lt_neg1 or 0),
        "price_nonfinite_rows": int(price_nonfinite or 0),
        "price_negative_rows": int(price_negative or 0),
        "shock_metadata_not_object_rows": int(shock_metadata_not_object or 0),
        "orphan_scenario_set_id_rows": int(orphan_scenario_set_id or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show scenario_paths status and basic Layer 2 validation checks")
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
