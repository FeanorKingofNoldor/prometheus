"""Show portfolio-level exposure to STAB scenarios in joint space.

This script uses joint STAB/fragility embeddings (``STAB_FRAGILITY_V0``)
for instruments and scenarios to compute how close a portfolio's current
STAB state is to each scenario in a scenario set.

High-level behaviour:

- Load target portfolio weights from ``target_portfolios`` for a given
  ``portfolio_id`` and ``as_of_date``.
- For instruments with non-zero weights, load their joint STAB
  embeddings (entity_type = "INSTRUMENT").
- Compute a weighted average portfolio STAB vector ``z_portfolio``.
- Load scenario-level STAB embeddings for a given ``scenario_set_id``
  (entity_type = "SCENARIO").
- Compute cosine similarity and Euclidean distance between
  ``z_portfolio`` and each scenario vector.
- Print the top-K closest scenarios as a CSV.

This is a diagnostic tool for portfolio & risk analysis; it does not
change any stored risk reports.
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
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


def _load_portfolio_weights(
    db_manager: DatabaseManager,
    portfolio_id: str,
    as_of: date,
) -> Dict[str, float]:
    """Load instrument weights for a portfolio/as_of from target_portfolios.

    Expects ``target_positions`` JSON to contain a ``{"weights": {...}}``
    mapping, as written by PortfolioStorage.save_target_portfolio.
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
        raise RuntimeError(
            f"No target_portfolios row found for portfolio_id={portfolio_id!r} as_of={as_of}"
        )

    (target_positions,) = row
    if not isinstance(target_positions, Mapping):
        raise RuntimeError("target_positions is not a JSON object as expected")

    weights_payload = target_positions.get("weights")
    if not isinstance(weights_payload, Mapping):
        raise RuntimeError("target_positions['weights'] is missing or not a mapping")

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
    """Load latest joint STAB embeddings for instruments up to as_of.

    Returns a mapping instrument_id -> vector. If multiple rows exist per
    instrument_id, the most recent by (as_of_date, joint_id) is used.
    """

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
        # Take first occurrence per instrument_id due to ORDER BY (latest first).
        if key in emb_by_inst:
            continue
        vec = np.frombuffer(vector_bytes, dtype=np.float32)
        if vec.size == 0:
            continue
        emb_by_inst[key] = vec

    return emb_by_inst


def _compute_portfolio_stab_vector(
    weights: Dict[str, float],
    embeddings: Dict[str, np.ndarray],
) -> Optional[np.ndarray]:
    """Compute weighted-average portfolio STAB vector.

    Uses weights only for instruments that have an embedding. If no
    instruments survive, returns None.
    """

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
        # Keep first occurrence per scenario_id (latest due to ORDER BY).
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


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Show portfolio-level exposure to STAB scenarios in joint "
            "STAB_FRAGILITY_V0 space."
        ),
    )

    parser.add_argument(
        "--portfolio-id",
        type=str,
        required=True,
        help="Portfolio_id whose target weights should be analysed",
    )
    parser.add_argument(
        "--as-of",
        type=_parse_date,
        required=True,
        help="As-of date (YYYY-MM-DD) matching target_portfolios.as_of_date",
    )
    parser.add_argument(
        "--scenario-set-id",
        type=str,
        required=True,
        help="Scenario_set_id to use for scenario-level STAB embeddings",
    )
    parser.add_argument(
        "--stab-model-id",
        type=str,
        default="joint-stab-fragility-v1",
        help=(
            "Joint STAB model_id for both instrument and scenario "
            "embeddings (default: joint-stab-fragility-v1)"
        ),
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=20,
        help="Number of closest scenarios to display (default: 20)",
    )

    args = parser.parse_args(argv)

    if args.top_k <= 0:
        parser.error("--top-k must be positive")

    config = get_config()
    db_manager = DatabaseManager(config)

    # 1) Load portfolio weights
    weights = _load_portfolio_weights(
        db_manager=db_manager,
        portfolio_id=args.portfolio_id,
        as_of=args.as_of,
    )
    if not weights:
        logger.warning(
            "No non-zero weights found for portfolio_id=%s as_of=%s",
            args.portfolio_id,
            args.as_of,
        )
        return

    # 2) Load instrument-level STAB embeddings and compute portfolio vector.
    inst_embeddings = _load_instrument_stab_embeddings(
        db_manager=db_manager,
        instrument_ids=list(weights.keys()),
        as_of=args.as_of,
        model_id=args.stab_model_id,
    )
    if not inst_embeddings:
        logger.warning(
            "No STAB_FRAGILITY_V0 instrument embeddings found for portfolio_id=%s as_of=%s",
            args.portfolio_id,
            args.as_of,
        )
        return

    z_portfolio = _compute_portfolio_stab_vector(weights, inst_embeddings)
    if z_portfolio is None:
        logger.warning("Failed to compute portfolio STAB vector; nothing to do")
        return

    portfolio_norm = float(np.linalg.norm(z_portfolio))

    # 3) Load scenario embeddings for the requested scenario_set_id.
    scenarios = _load_scenario_embeddings(
        db_manager=db_manager,
        scenario_set_id=args.scenario_set_id,
        model_id=args.stab_model_id,
    )
    if not scenarios:
        logger.warning(
            "No STAB_FRAGILITY_V0 scenario embeddings found for scenario_set_id=%s",
            args.scenario_set_id,
        )
        return

    dim = int(z_portfolio.shape[0])
    scored: List[Tuple[float, float, str, date]] = []
    for scenario_id, as_of_scen, vec in scenarios:
        if vec.shape[0] != dim:
            logger.warning(
                "Skipping scenario_id=%s due to dimension mismatch: %s != %s",
                scenario_id,
                vec.shape[0],
                dim,
            )
            continue
        cos = _cosine_similarity(z_portfolio, vec)
        dist = float(np.linalg.norm(z_portfolio - vec))
        scored.append((cos, dist, scenario_id, as_of_scen))

    if not scored:
        logger.warning("No compatible scenario embeddings after dimension filtering.")
        return

    scored.sort(key=lambda t: (-t[0], t[1]))
    top_k = min(args.top_k, len(scored))
    top = scored[:top_k]

    print(
        "cosine,euclidean,scenario_set_id,scenario_id,scenario_as_of,"
        "portfolio_id,portfolio_ctx_norm,num_instruments_used",
    )
    num_used = len(inst_embeddings)
    for cos, dist, scenario_id, as_of_scen in top:
        print(
            f"{cos:.6f},{dist:.6f},{args.scenario_set_id},{scenario_id},{as_of_scen},"
            f"{args.portfolio_id},{portfolio_norm:.6f},{num_used}"
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
