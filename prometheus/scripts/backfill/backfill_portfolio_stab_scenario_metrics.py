"""Backfill portfolio STAB-scenario metrics into portfolio_risk_reports.

This script augments existing portfolio risk reports with scenario-aware
metrics derived from joint STAB/fragility embeddings (``STAB_FRAGILITY_V0``):

- For each (portfolio_id, as_of_date) row in ``portfolio_risk_reports``:
  - Load the corresponding target portfolio weights from ``target_portfolios``.
  - Build a portfolio-level STAB vector ``z_portfolio`` from instrument
    embeddings (entity_type = "INSTRUMENT").
  - Load scenario-level STAB embeddings for a given ``scenario_set_id``
    (entity_type = "SCENARIO").
  - Compute cosine similarity and Euclidean distance between
    ``z_portfolio`` and each scenario.
  - Identify the closest scenario and simple summary statistics.
  - Write these metrics back into ``portfolio_risk_reports.risk_metrics``.

This is a v0 helper; it can be re-run safely as it overwrites only the
scenario-specific keys inside ``risk_metrics``.
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
from psycopg2.extras import Json

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger


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
) -> List[Tuple[date, Dict[str, Any]]]:
    """Load (as_of_date, risk_metrics) rows from portfolio_risk_reports."""

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
        "SELECT as_of_date, risk_metrics "
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

    results: List[Tuple[date, Dict[str, Any]]] = []
    for as_of_date_db, risk_metrics in rows:
        metrics = risk_metrics or {}
        if not isinstance(metrics, Mapping):
            metrics = {}
        results.append((as_of_date_db, dict(metrics)))

    return results


def _load_target_weights(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str,
    as_of: date,
) -> Dict[str, float]:
    """Load instrument weights from target_portfolios for a given date.

    Expects target_positions JSON with a "weights" mapping.
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


def _load_instrument_stab_embeddings(
    db_manager: DatabaseManager,
    *,
    instrument_ids: Sequence[str],
    as_of: date,
    model_id: str,
) -> Dict[str, np.ndarray]:
    """Load latest joint STAB embeddings for instruments up to as_of."""

    if not instrument_ids:
        return {}

    sql = """
        SELECT entity_scope, vector
        FROM joint_embeddings
        WHERE joint_type = 'STAB_FRAGILITY_V0'
          AND model_id = %s
          AND as_of_date <= %s
          AND (entity_scope->>'entity_type') = 'INSTRUMENT'
          AND (entity_scope->>'entity_id') = ANY(%s)
        ORDER BY as_of_date DESC, joint_id DESC
    """

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (model_id, as_of, list(instrument_ids)))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    emb_by_inst: Dict[str, np.ndarray] = {}
    for entity_scope, vector_bytes in rows:
        if not isinstance(entity_scope, Mapping) or vector_bytes is None:
            continue
        inst_id = entity_scope.get("entity_id")
        if not inst_id:
            continue
        key = str(inst_id)
        if key in emb_by_inst:
            continue
        vec = np.frombuffer(vector_bytes, dtype=np.float32)
        if vec.size == 0:
            continue
        emb_by_inst[key] = vec

    return emb_by_inst


def _compute_portfolio_vector(
    weights: Dict[str, float],
    embeddings: Dict[str, np.ndarray],
) -> Optional[np.ndarray]:
    """Compute weighted-average portfolio STAB vector, or None if empty."""

    items: List[Tuple[str, float]] = []
    for inst_id, w in weights.items():
        if inst_id in embeddings and w != 0.0:
            items.append((inst_id, float(w)))

    if not items:
        return None

    inst_ids = [inst for inst, _ in items]
    w_vec = np.array([w for _, w in items], dtype=np.float32)
    mats = np.stack([embeddings[i] for i in inst_ids], axis=0)

    total = float(np.sum(np.abs(w_vec)))
    if total <= 0.0:
        return None

    w_norm = w_vec / total
    z_portfolio = np.matmul(w_norm.astype(np.float32), mats)
    return z_portfolio.astype(np.float32)


def _load_scenario_embeddings(
    db_manager: DatabaseManager,
    *,
    scenario_set_id: str,
    model_id: str,
) -> List[Tuple[str, date, np.ndarray]]:
    """Load scenario-level joint STAB embeddings for a scenario_set_id."""

    sql = """
        SELECT as_of_date, entity_scope, vector
        FROM joint_embeddings
        WHERE joint_type = 'STAB_FRAGILITY_V0'
          AND model_id = %s
          AND (entity_scope->>'entity_type') = 'SCENARIO'
          AND (entity_scope->>'scenario_set_id') = %s
        ORDER BY as_of_date DESC, joint_id DESC
    """

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (model_id, scenario_set_id))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    results: Dict[str, Tuple[date, np.ndarray]] = {}
    for as_of_date_db, entity_scope, vector_bytes in rows:
        if not isinstance(entity_scope, Mapping) or vector_bytes is None:
            continue
        scenario_id = entity_scope.get("scenario_id")
        if not scenario_id:
            continue
        key = str(scenario_id)
        if key in results:
            continue
        vec = np.frombuffer(vector_bytes, dtype=np.float32)
        if vec.size == 0:
            continue
        results[key] = (as_of_date_db, vec)

    return [(sid, as_of, vec) for sid, (as_of, vec) in results.items()]


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    a_norm = np.linalg.norm(a)
    b_norm = np.linalg.norm(b)
    if a_norm == 0.0 or b_norm == 0.0:
        return 0.0
    return float(np.dot(a, b) / (a_norm * b_norm))


def _update_risk_metrics(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str,
    as_of: date,
    risk_metrics: Dict[str, Any],
) -> None:
    """Write updated risk_metrics back to portfolio_risk_reports."""

    sql = """
        UPDATE portfolio_risk_reports
        SET risk_metrics = %s
        WHERE portfolio_id = %s
          AND as_of_date = %s
    """

    payload = Json(risk_metrics)

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (payload, portfolio_id, as_of))
            conn.commit()
        finally:
            cursor.close()


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill portfolio STAB-scenario metrics into portfolio_risk_reports "
            "using STAB_FRAGILITY_V0 joint embeddings."
        ),
    )

    parser.add_argument(
        "--portfolio-id",
        type=str,
        required=True,
        help="portfolio_id whose risk reports should be augmented",
    )
    parser.add_argument(
        "--scenario-set-id",
        type=str,
        required=True,
        help="scenario_set_id to use for scenario-level STAB embeddings",
    )
    parser.add_argument(
        "--stab-model-id",
        type=str,
        default="joint-stab-fragility-v1",
        help=(
            "Joint STAB model_id for instrument/scenario embeddings "
            "(default: joint-stab-fragility-v1)"
        ),
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


def backfill_portfolio_stab_scenario_metrics_for_range(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str,
    scenario_set_id: str,
    stab_model_id: str = "joint-stab-fragility-v1",
    start: Optional[date] = None,
    end: Optional[date] = None,
    limit: Optional[int] = None,
) -> int:
    """Backfill STAB-scenario metrics for a portfolio over a date range.

    Returns the number of ``portfolio_risk_reports`` rows updated.
    """

    risk_rows = _load_portfolio_risk_rows(
        db_manager=db_manager,
        portfolio_id=portfolio_id,
        start=start,
        end=end,
        limit=limit,
    )

    if not risk_rows:
        logger.warning(
            "No portfolio_risk_reports rows found for portfolio_id=%s in the given range; nothing to do",
            portfolio_id,
        )
        return 0

    scenarios = _load_scenario_embeddings(
        db_manager=db_manager,
        scenario_set_id=scenario_set_id,
        model_id=stab_model_id,
    )
    if not scenarios:
        logger.warning(
            "No STAB_FRAGILITY_V0 scenario embeddings found for scenario_set_id=%s",
            scenario_set_id,
        )
        return 0

    logger.info(
        "Backfilling portfolio STAB-scenario metrics: portfolio=%s rows=%d scenario_set_id=%s",
        portfolio_id,
        len(risk_rows),
        scenario_set_id,
    )

    updated_count = 0

    for as_of_date_db, risk_metrics in risk_rows:
        weights = _load_target_weights(
            db_manager=db_manager,
            portfolio_id=portfolio_id,
            as_of=as_of_date_db,
        )
        if not weights:
            logger.debug(
                "No target_portfolios weights for portfolio_id=%s as_of=%s; skipping",
                portfolio_id,
                as_of_date_db,
            )
            continue

        inst_embeddings = _load_instrument_stab_embeddings(
            db_manager=db_manager,
            instrument_ids=list(weights.keys()),
            as_of=as_of_date_db,
            model_id=stab_model_id,
        )
        if not inst_embeddings:
            logger.debug(
                "No instrument STAB embeddings for portfolio_id=%s as_of=%s; skipping",
                portfolio_id,
                as_of_date_db,
            )
            continue

        z_portfolio = _compute_portfolio_vector(weights, inst_embeddings)
        if z_portfolio is None:
            logger.debug(
                "Failed to compute portfolio STAB vector for portfolio_id=%s as_of=%s; skipping",
                portfolio_id,
                as_of_date_db,
            )
            continue

        dim = int(z_portfolio.shape[0])
        portfolio_norm = float(np.linalg.norm(z_portfolio))

        scored: List[Tuple[float, float]] = []
        scenario_ids: List[str] = []
        for scenario_id, _as_of_scen, vec in scenarios:
            if vec.shape[0] != dim:
                continue
            cos = _cosine_similarity(z_portfolio, vec)
            dist = float(np.linalg.norm(z_portfolio - vec))
            scored.append((cos, dist))
            scenario_ids.append(scenario_id)

        if not scored:
            logger.debug(
                "No compatible scenarios for portfolio_id=%s as_of=%s; skipping",
                portfolio_id,
                as_of_date_db,
            )
            continue

        # Sort by descending cosine, then ascending distance.
        order = np.argsort([(-c, d) for c, d in scored])
        idx0 = int(order[0])
        best_cos, best_dist = scored[idx0]
        best_scenario_id = scenario_ids[idx0]

        # Simple summary over top 3 (if available).
        top_n = min(3, len(scored))
        top_cos = [scored[int(i)][0] for i in order[:top_n]]
        mean_top3_cos = float(sum(top_cos) / top_n)

        # Update risk_metrics with scenario-aware keys.
        updated_metrics = dict(risk_metrics or {})
        updated_metrics["stab_scenario_set_id"] = scenario_set_id
        updated_metrics["stab_closest_scenario_id"] = best_scenario_id
        updated_metrics["stab_closest_scenario_cosine"] = float(best_cos)
        updated_metrics["stab_closest_scenario_distance"] = float(best_dist)
        updated_metrics["stab_portfolio_ctx_norm"] = portfolio_norm
        updated_metrics["stab_top3_scenario_cosine_mean"] = mean_top3_cos

        _update_risk_metrics(
            db_manager=db_manager,
            portfolio_id=portfolio_id,
            as_of=as_of_date_db,
            risk_metrics=updated_metrics,
        )

        updated_count += 1

    logger.info(
        "Portfolio STAB-scenario metrics backfill complete for portfolio_id=%s (rows_updated=%d)",
        portfolio_id,
        updated_count,
    )

    return updated_count


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)

    config = get_config()
    db_manager = DatabaseManager(config)

    backfill_portfolio_stab_scenario_metrics_for_range(
        db_manager=db_manager,
        portfolio_id=args.portfolio_id,
        scenario_set_id=args.scenario_set_id,
        stab_model_id=args.stab_model_id,
        start=args.start,
        end=args.end,
        limit=args.limit,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
