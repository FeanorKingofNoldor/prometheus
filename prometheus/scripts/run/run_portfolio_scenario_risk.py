"""Compute scenario-based portfolio P&L and store it in portfolio_risk_reports.

This script wires the Synthetic Scenario Engine outputs into
``portfolio_risk_reports`` by applying a numeric scenario set to a
portfolio's target weights:

* For each selected (portfolio_id, as_of_date) row in
  ``portfolio_risk_reports``:

  - Load target weights from ``target_portfolios``.
  - Read instrument-level shocks from ``scenario_paths`` for a given
    ``scenario_set_id``.
  - Compute portfolio-level returns per scenario using
    :func:`prometheus.portfolio.scenario_risk.compute_portfolio_scenario_pnl`.
  - Write the resulting mapping into ``scenario_pnl`` and merge summary
    statistics into ``risk_metrics``.

It is safe to re-run this script: only the scenario-related keys in the
selected rows are overwritten.
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from psycopg2.extras import Json

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from prometheus.portfolio.scenario_risk import compute_portfolio_scenario_pnl


logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _load_portfolio_risk_rows(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str,
    start: Optional[date],
    end: Optional[date],
    limit: Optional[int],
) -> List[Tuple[date, Dict[str, Any], Dict[str, Any]]]:
    """Load (as_of_date, scenario_pnl, risk_metrics) for a portfolio.

    Existing JSON payloads are normalised to plain ``dict`` instances.
    """

    where_clauses = ["portfolio_id = %s"]
    params: List[Any] = [portfolio_id]

    if start is not None:
        where_clauses.append("as_of_date >= %s")
        params.append(start)
    if end is not None:
        where_clauses.append("as_of_date <= %s")
        params.append(end)

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT as_of_date, scenario_pnl, risk_metrics "
        "FROM portfolio_risk_reports" + where_sql + " ORDER BY as_of_date ASC"
    )

    if limit is not None and limit > 0:
        sql += " LIMIT %s"
        params.append(limit)

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    results: List[Tuple[date, Dict[str, Any], Dict[str, Any]]] = []
    for as_of_date_db, scenario_pnl_db, risk_metrics_db in rows:
        scen = scenario_pnl_db or {}
        if not isinstance(scen, Mapping):
            scen = {}
        rm = risk_metrics_db or {}
        if not isinstance(rm, Mapping):
            rm = {}
        results.append((as_of_date_db, dict(scen), dict(rm)))

    return results


def _load_target_weights(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str,
    as_of: date,
) -> Dict[str, float]:
    """Load instrument weights from target_portfolios for a given date.

    Expects target_positions JSON with a ``"weights"`` mapping.
    """

    sql = """
        SELECT target_positions
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

    if row is None:
        return {}

    (target_positions,) = row
    if not isinstance(target_positions, Mapping):
        return {}

    weights_payload = target_positions.get("weights")
    if not isinstance(weights_payload, Mapping):
        return {}

    weights: Dict[str, float] = {}
    for inst_id, w in weights_payload.items():
        try:
            w_f = float(w)
        except Exception:
            continue
        if w_f != 0.0:
            weights[str(inst_id)] = w_f

    return weights


def _update_portfolio_risk_row(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str,
    as_of: date,
    scenario_pnl: Dict[str, float],
    risk_metrics: Dict[str, Any],
) -> None:
    """Write updated JSON fields back into portfolio_risk_reports."""

    sql = """
        UPDATE portfolio_risk_reports
        SET scenario_pnl = %s,
            risk_metrics = %s
        WHERE portfolio_id = %s
          AND as_of_date = %s
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                sql,
                (
                    Json(scenario_pnl),
                    Json(risk_metrics),
                    portfolio_id,
                    as_of,
                ),
            )
            conn.commit()
        finally:
            cursor.close()


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compute scenario-based portfolio P&L for a scenario_set_id and "
            "store results in portfolio_risk_reports."
        ),
    )

    parser.add_argument(
        "--portfolio-id",
        type=str,
        required=True,
        help="portfolio_id whose risk reports should be updated",
    )
    parser.add_argument(
        "--scenario-set-id",
        type=str,
        required=True,
        help="scenario_set_id from scenario_sets/scenario_paths",
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Optional minimum as_of_date (YYYY-MM-DD) for risk reports",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        help="Optional maximum as_of_date (YYYY-MM-DD) for risk reports",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of risk report rows to process",
    )

    args = parser.parse_args(argv)

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive if provided")

    return args


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)

    config = get_config()
    db_manager = DatabaseManager(config)

    risk_rows = _load_portfolio_risk_rows(
        db_manager=db_manager,
        portfolio_id=args.portfolio_id,
        start=args.start,
        end=args.end,
        limit=args.limit,
    )

    if not risk_rows:
        logger.warning(
            "No portfolio_risk_reports rows found for portfolio_id=%s in the given range; nothing to do",
            args.portfolio_id,
        )
        return

    logger.info(
        "Computing scenario P&L for portfolio_id=%s scenario_set_id=%s rows=%d",
        args.portfolio_id,
        args.scenario_set_id,
        len(risk_rows),
    )

    for as_of_date_db, scenario_pnl_db, risk_metrics_db in risk_rows:
        weights = _load_target_weights(
            db_manager=db_manager,
            portfolio_id=args.portfolio_id,
            as_of=as_of_date_db,
        )
        if not weights:
            logger.debug(
                "No target_portfolios weights for portfolio_id=%s as_of=%s; skipping",
                args.portfolio_id,
                as_of_date_db,
            )
            continue

        try:
            result = compute_portfolio_scenario_pnl(
                db_manager=db_manager,
                scenario_set_id=args.scenario_set_id,
                as_of_date=as_of_date_db,
                weights=weights,
            )
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "Failed to compute scenario P&L for portfolio_id=%s as_of=%s scenario_set_id=%s",
                args.portfolio_id,
                as_of_date_db,
                args.scenario_set_id,
            )
            continue

        if not result.scenario_pnl:
            logger.debug(
                "No scenario P&L computed for portfolio_id=%s as_of=%s; skipping update",
                args.portfolio_id,
                as_of_date_db,
            )
            continue

        # Merge summary metrics into existing risk_metrics, but keep other
        # keys intact.
        merged_metrics = dict(risk_metrics_db or {})
        for key, value in result.summary_metrics.items():
            metric_key = f"{args.scenario_set_id}:{key}"
            merged_metrics[metric_key] = float(value)

        _update_portfolio_risk_row(
            db_manager=db_manager,
            portfolio_id=args.portfolio_id,
            as_of=as_of_date_db,
            scenario_pnl=result.scenario_pnl,
            risk_metrics=merged_metrics,
        )

    logger.info(
        "Scenario P&L backfill complete for portfolio_id=%s scenario_set_id=%s",
        args.portfolio_id,
        args.scenario_set_id,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
