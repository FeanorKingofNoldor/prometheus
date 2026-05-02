"""Backfill run-level STAB-scenario metrics into backtest_runs.

This script summarises per-date portfolio STAB-scenario exposure
metrics, previously written into ``portfolio_risk_reports.risk_metrics``
by ``backfill_portfolio_stab_scenario_metrics``, into run-level
statistics stored in ``backtest_runs.metrics_json``.

For each selected ``backtest_runs`` row, it:

- Infers the associated ``portfolio_id`` from ``config_json``.
- Determines the date range ``[start_date, end_date]``.
- Loads matching ``portfolio_risk_reports`` rows for that
  ``(portfolio_id, as_of_date)`` range.
- Extracts STAB scenario exposure fields from ``risk_metrics``:
  - ``stab_scenario_set_id``
  - ``stab_closest_scenario_cosine``
  - ``stab_portfolio_ctx_norm``
- Computes simple aggregates across the date range, e.g.:
  - mean / min / max of ``stab_closest_scenario_cosine``.
  - mean / max of ``stab_portfolio_ctx_norm``.
- Writes these into ``backtest_runs.metrics_json`` under
  ``stab_*`` keys, leaving existing metrics intact.

This is a v0 helper intended for offline research; it is safe to re-run
as it only overwrites the specific ``stab_*`` keys it manages.
"""

from __future__ import annotations

import argparse
from typing import Any, Dict, List, Mapping, Optional, Sequence

from apatheon.core.config import get_config
from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from psycopg2.extras import Json

logger = get_logger(__name__)


def _load_backtest_runs(
    db_manager: DatabaseManager,
    *,
    strategy_id: Optional[str],
    run_id: Optional[str],
) -> List[Dict[str, Any]]:
    """Load candidate backtest_runs rows to summarise."""

    where_clauses: List[str] = []
    params: List[Any] = []

    if run_id is not None:
        where_clauses.append("run_id = %s")
        params.append(run_id)
    if strategy_id is not None:
        where_clauses.append("strategy_id = %s")
        params.append(strategy_id)

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT run_id, strategy_id, config_json, start_date, end_date, metrics_json "
        "FROM backtest_runs" + where_sql + " ORDER BY created_at DESC"
    )

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    results: List[Dict[str, Any]] = []
    for run_id_db, strat_db, config_json, start_date, end_date, metrics_json in rows:
        cfg = config_json or {}
        if not isinstance(cfg, Mapping):
            cfg = {}
        metrics = metrics_json or {}
        if not isinstance(metrics, Mapping):
            metrics = {}
        results.append(
            {
                "run_id": str(run_id_db),
                "strategy_id": str(strat_db),
                "config_json": dict(cfg),
                "start_date": start_date,
                "end_date": end_date,
                "metrics_json": dict(metrics),
            }
        )

    return results


def _load_portfolio_risk_rows(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str,
    start_date,
    end_date,
) -> List[Dict[str, Any]]:
    """Load portfolio_risk_reports rows for a portfolio over a date range."""

    sql = """
        SELECT as_of_date, risk_metrics
        FROM portfolio_risk_reports
        WHERE portfolio_id = %s
          AND as_of_date >= %s
          AND as_of_date <= %s
        ORDER BY as_of_date
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (portfolio_id, start_date, end_date))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    results: List[Dict[str, Any]] = []
    for as_of_date, risk_metrics in rows:
        rm = risk_metrics or {}
        if not isinstance(rm, Mapping):
            rm = {}
        results.append({"as_of_date": as_of_date, "risk_metrics": dict(rm)})
    return results


def _summarise_stab_metrics(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Summarise per-date STAB metrics across a run."""

    if not rows:
        return {}

    cosines: List[float] = []
    norms: List[float] = []
    scenario_set_ids: List[str] = []

    for row in rows:
        rm = row.get("risk_metrics", {}) or {}
        if not isinstance(rm, Mapping):
            continue
        cos = rm.get("stab_closest_scenario_cosine")
        norm = rm.get("stab_portfolio_ctx_norm")
        scen_set = rm.get("stab_scenario_set_id")
        try:
            if cos is not None:
                cosines.append(float(cos))
        except Exception:
            pass
        try:
            if norm is not None:
                norms.append(float(norm))
        except Exception:
            pass
        if isinstance(scen_set, str):
            scenario_set_ids.append(scen_set)

    if not cosines and not norms:
        return {}

    from collections import Counter

    import numpy as np

    summary: Dict[str, Any] = {}

    if scenario_set_ids:
        scen_counts = Counter(scenario_set_ids)
        summary["stab_scenario_set_id"] = scen_counts.most_common(1)[0][0]

    if cosines:
        arr = np.array(cosines, dtype=float)
        summary["stab_closest_scenario_cosine_mean"] = float(arr.mean())
        summary["stab_closest_scenario_cosine_min"] = float(arr.min())
        summary["stab_closest_scenario_cosine_max"] = float(arr.max())

    if norms:
        arr = np.array(norms, dtype=float)
        summary["stab_portfolio_ctx_norm_mean"] = float(arr.mean())
        summary["stab_portfolio_ctx_norm_max"] = float(arr.max())

    summary["stab_num_days"] = len(rows)

    return summary


def _update_backtest_run_metrics(
    db_manager: DatabaseManager,
    *,
    run_id: str,
    new_metrics: Mapping[str, Any],
) -> None:
    """Merge new_metrics into backtest_runs.metrics_json for run_id."""

    sql_select = "SELECT metrics_json FROM backtest_runs WHERE run_id = %s"
    sql_update = "UPDATE backtest_runs SET metrics_json = %s WHERE run_id = %s"

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql_select, (run_id,))
            row = cursor.fetchone()
            existing = row[0] if row is not None else None
            if existing is None or not isinstance(existing, Mapping):
                merged: Dict[str, Any] = {}
            else:
                merged = dict(existing)

            # Overwrite only stab_* keys; leave everything else intact.
            for k, v in new_metrics.items():
                if k.startswith("stab_"):
                    merged[k] = v

            cursor.execute(sql_update, (Json(merged), run_id))
            conn.commit()
        finally:
            cursor.close()


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Summarise per-run STAB-scenario exposure metrics from "
            "portfolio_risk_reports into backtest_runs.metrics_json."
        ),
    )

    parser.add_argument(
        "--strategy-id",
        type=str,
        default=None,
        help=(
            "Optional strategy_id filter for backtest_runs. If omitted and "
            "--run-id is not provided, all runs are considered."
        ),
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Optional specific backtest run_id to summarise",
    )

    args = parser.parse_args(argv)

    return args


def summarise_backtest_stab_scenario_metrics(
    db_manager: DatabaseManager,
    *,
    strategy_id: str | None = None,
    run_id: str | None = None,
) -> int:
    """Summarise STAB-scenario metrics into backtest_runs for selected runs.

    Returns the number of ``backtest_runs`` rows updated.
    """

    runs = _load_backtest_runs(
        db_manager=db_manager,
        strategy_id=strategy_id,
        run_id=run_id,
    )

    if not runs:
        logger.warning("No backtest_runs matched the given filters; nothing to do")
        return 0

    logger.info("Found %d backtest_runs to summarise", len(runs))

    updated = 0

    for r in runs:
        run_id_db = r["run_id"]
        cfg = r["config_json"]
        portfolio_id = cfg.get("portfolio_id")
        if not portfolio_id:
            logger.debug(
                "Run %s missing portfolio_id in config_json; skipping STAB summary",
                run_id_db,
            )
            continue

        start_date = r["start_date"]
        end_date = r["end_date"]

        risk_rows = _load_portfolio_risk_rows(
            db_manager=db_manager,
            portfolio_id=portfolio_id,
            start_date=start_date,
            end_date=end_date,
        )
        if not risk_rows:
            logger.debug(
                "No portfolio_risk_reports rows for portfolio_id=%s in [%s, %s]; skipping run %s",
                portfolio_id,
                start_date,
                end_date,
                run_id_db,
            )
            continue

        summary = _summarise_stab_metrics(risk_rows)
        if not summary:
            logger.debug(
                "No usable STAB metrics for portfolio_id=%s in [%s, %s]; skipping run %s",
                portfolio_id,
                start_date,
                end_date,
                run_id_db,
            )
            continue

        _update_backtest_run_metrics(
            db_manager=db_manager,
            run_id=run_id_db,
            new_metrics=summary,
        )

        logger.info(
            "Updated run %s with STAB scenario summary: %s",
            run_id_db,
            {k: summary[k] for k in sorted(summary.keys())},
        )
        updated += 1

    return updated


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)

    config = get_config()
    db_manager = DatabaseManager(config)

    summarise_backtest_stab_scenario_metrics(
        db_manager=db_manager,
        strategy_id=args.strategy_id,
        run_id=args.run_id,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
