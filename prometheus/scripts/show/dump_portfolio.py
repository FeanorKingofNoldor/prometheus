"""Prometheus v2 – Portfolio & Risk inspection CLI.

This script provides a small helper for inspecting target portfolio
weights and associated risk metrics stored by the Portfolio & Risk
Engine. It reads from the ``target_portfolios`` and
``portfolio_risk_reports`` tables in the runtime database.

Example
-------

    # Show the latest snapshot for a portfolio
    python -m prometheus.scripts.dump_portfolio --portfolio-id US_CORE_LONG_EQ

    # Show a specific date and limit output to top 20 names
    python -m prometheus.scripts.dump_portfolio \
        --portfolio-id US_CORE_LONG_EQ \
        --as-of 2025-11-21 \
        --limit 20
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Dict, Optional, Sequence, Tuple

from apatheon.core.database import DatabaseManager, get_db_manager
from apatheon.core.logging import get_logger

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    """Parse a YYYY-MM-DD date string for CLI arguments."""

    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _find_latest_as_of(db_manager: DatabaseManager, portfolio_id: str) -> Optional[date]:
    """Return the most recent as_of_date for which targets exist.

    If no rows are present for the portfolio, returns None.
    """

    sql = """
        SELECT as_of_date
        FROM target_portfolios
        WHERE portfolio_id = %s
        ORDER BY as_of_date DESC
        LIMIT 1
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (portfolio_id,))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if not row:
        return None

    as_of_date: date = row[0]
    return as_of_date


def _load_target_snapshot(
    db_manager: DatabaseManager,
    portfolio_id: str,
    as_of: date,
) -> Tuple[Dict[str, float], Dict[str, object]]:
    """Load the latest target snapshot for a portfolio/date.

    Returns a tuple ``(weights, metadata)``. If no row is found, both
    dictionaries are empty.
    """

    sql = """
        SELECT target_positions, metadata
        FROM target_portfolios
        WHERE portfolio_id = %s
          AND as_of_date = %s
        ORDER BY created_at DESC
        LIMIT 1
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (portfolio_id, as_of))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if not row:
        return {}, {}

    positions, metadata = row
    if not isinstance(positions, dict):
        return {}, dict(metadata or {})

    raw_weights = positions.get("weights") or {}
    weights: Dict[str, float] = {str(k): float(v) for k, v in raw_weights.items()}

    return weights, dict(metadata or {})


def _load_risk_report(
    db_manager: DatabaseManager,
    portfolio_id: str,
    as_of: date,
) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float]]:
    """Load the latest risk report for a portfolio/date.

    Returns ``(risk_metrics, exposures, scenario_pnl)``. Empty dicts are
    returned if no report exists.
    """

    sql = """
        SELECT
            risk_metrics,
            exposures_by_sector,
            exposures_by_factor,
            scenario_pnl
        FROM portfolio_risk_reports
        WHERE portfolio_id = %s
          AND as_of_date = %s
        ORDER BY created_at DESC
        LIMIT 1
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (portfolio_id, as_of))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if not row:
        return {}, {}, {}

    risk_metrics_raw, exposures_sector_raw, exposures_factor_raw, scenario_pnl_raw = row

    risk_metrics: Dict[str, float] = {str(k): float(v) for k, v in (risk_metrics_raw or {}).items()}

    exposures: Dict[str, float] = {}
    for source in (exposures_sector_raw or {}, exposures_factor_raw or {}):
        for key, value in source.items():
            exposures[str(key)] = exposures.get(str(key), 0.0) + float(value)

    scenario_pnl: Dict[str, float] = {str(k): float(v) for k, v in (scenario_pnl_raw or {}).items()}

    return risk_metrics, exposures, scenario_pnl


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Entry point for the dump_portfolio CLI."""

    parser = argparse.ArgumentParser(
        description="Inspect target portfolio weights and risk metrics for a given portfolio/date",
    )

    parser.add_argument(
        "--portfolio-id",
        type=str,
        required=True,
        help="Portfolio identifier (e.g. US_CORE_LONG_EQ)",
    )
    parser.add_argument(
        "--as-of",
        dest="as_of",
        type=_parse_date,
        required=False,
        help="As-of date for the snapshot (YYYY-MM-DD). If omitted, uses the latest available date.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum number of instruments to display (default: 25)",
    )

    args = parser.parse_args(argv)

    db_manager = get_db_manager()

    as_of: Optional[date] = args.as_of
    if as_of is None:
        as_of = _find_latest_as_of(db_manager, args.portfolio_id)
        if as_of is None:
            print(f"No target_portfolios rows found for portfolio {args.portfolio_id!r}")
            return

    weights, metadata = _load_target_snapshot(db_manager, args.portfolio_id, as_of)
    if not weights:
        print(f"No target portfolio found for {args.portfolio_id!r} on {as_of}")
        return

    risk_metrics, exposures, scenario_pnl = _load_risk_report(db_manager, args.portfolio_id, as_of)

    print(f"Target portfolio for {args.portfolio_id} as of {as_of} (names={len(weights)})")
    if metadata:
        risk_model_id = metadata.get("risk_model_id") or metadata.get("model_id")
        if risk_model_id:
            print(f"  risk_model_id: {risk_model_id}")

    # Summary risk metrics
    if risk_metrics:
        print("\nRisk metrics:")
        for key in sorted(risk_metrics.keys()):
            print(f"  {key}: {risk_metrics[key]:.6f}")

    # Factor/sector exposures
    if exposures:
        print("\nExposures (sectors/factors):")
        for key in sorted(exposures.keys()):
            print(f"  {key}: {exposures[key]:.6f}")

    # Scenario P&L, if present
    if scenario_pnl:
        print("\nScenario P&L:")
        for key in sorted(scenario_pnl.keys()):
            print(f"  {key}: {scenario_pnl[key]:.6f}")

    # Top weights
    print("\nTop weights:")
    sorted_items = sorted(weights.items(), key=lambda kv: abs(kv[1]), reverse=True)
    for instrument_id, w in sorted_items[: max(1, args.limit)]:
        print(f"  {instrument_id}: weight={w:.6f}")


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
