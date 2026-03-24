"""Backfill soft-target (STAB) state-change risk series.

This offline script computes a simple *soft-target state-change risk*
series for a given ``entity_type`` (typically ``"INSTRUMENT"``), using
an empirical transition matrix over :class:`SoftTargetClass` values
stored in the runtime database.

For each soft-target state ``(entity_type, entity_id, as_of_date, class)``
observed in ``soft_target_classes`` between the requested start and end
dates, it computes:

- The probability of ending in a strictly *more fragile* class after a
  given horizon ``H`` (``p_worsen_any``).
- The probability of being in TARGETABLE or BREAKER at horizon ``H``
  (``p_to_targetable_or_breaker``).
- The probability of being in BREAKER at horizon ``H`` (``p_to_breaker``).
- A scalar ``stability_risk_score`` in [0, 1], currently equal to
  ``p_to_targetable_or_breaker``.

These metrics are derived from a homogeneous Markov chain over
soft-target classes, with one-step transition probabilities obtained via
:func:`StabilityStorage.get_transition_matrix`. Missing transition rows
are treated as identity (no-change).

The output is a CSV with one row per
``(entity_type, entity_id, as_of_date)`` in the window. This CSV can
later be joined onto other research tables or used directly as a
soft-target fragility feature.

This script is offline/research only; it is not part of the live
pipeline.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd
from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.stability.state_change import _SOFT_TARGET_ORDER, _build_transition_matrix
from apathis.stability.storage import StabilityStorage
from apathis.stability.types import SoftTargetClass, SoftTargetState

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


@dataclass(frozen=True)
class SoftTargetRiskPoint:
    """Single soft-target risk observation for an entity/date."""

    as_of_date: date
    entity_type: str
    entity_id: str
    current_soft_target_class: str
    horizon_steps: int
    p_worsen_any: float
    p_improve_any: float
    p_to_targetable_or_breaker: float
    p_to_breaker: float
    stability_risk_score: float


def _compute_soft_target_risk_series(
    storage: StabilityStorage,
    *,
    entity_type: str,
    entity_ids: Sequence[str],
    start_date: date,
    end_date: date,
    horizon_steps: int,
) -> List[SoftTargetRiskPoint]:
    """Compute soft-target risk points for each stored state in window.

    This function:

    - Fetches soft-target history for each entity between ``start_date``
      and ``end_date``.
    - Builds a Markov transition matrix from
      :meth:`StabilityStorage.get_transition_matrix`.
    - Raises the matrix to ``horizon_steps`` via
      :func:`numpy.linalg.matrix_power`.
    - For each soft-target state in history, reads off the horizon
      distribution and derives risk metrics.
    """

    if horizon_steps <= 0:
        raise ValueError("horizon_steps must be a positive integer")

    matrix_dict = storage.get_transition_matrix(entity_type)
    P, index_by_class = _build_transition_matrix(matrix_dict)

    try:
        P_h = np.linalg.matrix_power(P, horizon_steps)
    except ValueError as exc:  # pragma: no cover - defensive
        raise RuntimeError("Failed to compute matrix power for soft-target transitions") from exc

    labels = list(index_by_class.keys())

    points: List[SoftTargetRiskPoint] = []

    for entity_id in entity_ids:
        history: List[SoftTargetState] = storage.get_history(
            entity_type=entity_type,
            entity_id=entity_id,
            start_date=start_date,
            end_date=end_date,
        )
        if not history:
            continue

        for state in history:
            current_class = state.soft_target_class
            current_idx = index_by_class[current_class]
            row = P_h[current_idx, :]

            distribution: Dict[SoftTargetClass, float] = {
                label: float(row[index_by_class[label]]) for label in labels
            }

            current_rank = _SOFT_TARGET_ORDER[current_class]
            p_worsen_any = 0.0
            p_improve_any = 0.0
            p_to_targetable_or_breaker = 0.0
            p_to_breaker = 0.0

            for label, prob in distribution.items():
                rank = _SOFT_TARGET_ORDER[label]
                if rank > current_rank:
                    p_worsen_any += prob
                elif rank < current_rank:
                    p_improve_any += prob

                if label in (SoftTargetClass.TARGETABLE, SoftTargetClass.BREAKER):
                    p_to_targetable_or_breaker += prob
                if label == SoftTargetClass.BREAKER:
                    p_to_breaker += prob

            def _clamp(x: float) -> float:
                return float(max(0.0, min(1.0, x)))

            p_worsen_any = _clamp(p_worsen_any)
            p_improve_any = _clamp(p_improve_any)
            p_to_targetable_or_breaker = _clamp(p_to_targetable_or_breaker)
            p_to_breaker = _clamp(p_to_breaker)

            stability_risk_score = p_to_targetable_or_breaker

            points.append(
                SoftTargetRiskPoint(
                    as_of_date=state.as_of_date,
                    entity_type=state.entity_type,
                    entity_id=state.entity_id,
                    current_soft_target_class=state.soft_target_class.value,
                    horizon_steps=horizon_steps,
                    p_worsen_any=p_worsen_any,
                    p_improve_any=p_improve_any,
                    p_to_targetable_or_breaker=p_to_targetable_or_breaker,
                    p_to_breaker=p_to_breaker,
                    stability_risk_score=stability_risk_score,
                )
            )

    return points


def _load_entity_ids_with_soft_targets(
    storage: StabilityStorage,
    *,
    entity_type: str,
    start_date: date,
    end_date: date,
) -> List[str]:
    """Return distinct entity_ids with soft-target states in window."""

    sql = """
        SELECT DISTINCT entity_id
        FROM soft_target_classes
        WHERE entity_type = %s
          AND as_of_date BETWEEN %s AND %s
        ORDER BY entity_id
    """

    with storage.db_manager.get_runtime_connection() as conn:  # type: ignore[attr-defined]
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (entity_type, start_date, end_date))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return [str(entity_id) for (entity_id,) in rows]


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill a soft-target (STAB) state-change risk series for an entity_type "
            "using the empirical transition matrix."
        ),
    )

    parser.add_argument(
        "--entity-type",
        type=str,
        default="INSTRUMENT",
        help="Entity type in soft_target_classes to process (default: INSTRUMENT)",
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        required=True,
        help="Start date (YYYY-MM-DD) for soft-target history window",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        required=True,
        help="End date (YYYY-MM-DD) for soft-target history window",
    )
    parser.add_argument(
        "--horizon-steps",
        type=int,
        default=1,
        help="Horizon in Markov steps (typically days) for risk forecast (default: 1)",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Path to output CSV file for soft-target risk series",
    )

    args = parser.parse_args(argv)

    start_date: date = args.start
    end_date: date = args.end
    if end_date < start_date:
        parser.error("--end must be >= --start")

    if args.horizon_steps <= 0:
        parser.error("--horizon-steps must be a positive integer")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    config = get_config()
    db_manager = DatabaseManager(config)
    storage = StabilityStorage(db_manager=db_manager)

    logger.info(
        "Computing soft-target risk series for entity_type=%s, start=%s, end=%s, horizon_steps=%d",
        args.entity_type,
        start_date,
        end_date,
        args.horizon_steps,
    )

    entity_ids = _load_entity_ids_with_soft_targets(
        storage,
        entity_type=args.entity_type,
        start_date=start_date,
        end_date=end_date,
    )
    if not entity_ids:
        logger.warning(
            "No soft-target states found for entity_type=%s between %s and %s; nothing to do",
            args.entity_type,
            start_date,
            end_date,
        )
        return

    points = _compute_soft_target_risk_series(
        storage,
        entity_type=args.entity_type,
        entity_ids=entity_ids,
        start_date=start_date,
        end_date=end_date,
        horizon_steps=args.horizon_steps,
    )

    if not points:
        logger.warning("No soft-target risk points computed; nothing to write")
        return

    df_out = pd.DataFrame(
        [
            {
                "as_of_date": p.as_of_date,
                "entity_type": p.entity_type,
                "entity_id": p.entity_id,
                "current_soft_target_class": p.current_soft_target_class,
                "horizon_steps": p.horizon_steps,
                "p_worsen_any": p.p_worsen_any,
                "p_improve_any": p.p_improve_any,
                "p_to_targetable_or_breaker": p.p_to_targetable_or_breaker,
                "p_to_breaker": p.p_to_breaker,
                "stability_risk_score": p.stability_risk_score,
            }
            for p in points
        ]
    )

    df_out.sort_values(["as_of_date", "entity_type", "entity_id"], inplace=True)
    df_out.to_csv(out_path, index=False)

    logger.info(
        "Wrote %d soft-target risk observations for entity_type=%s to %s",
        df_out.shape[0],
        args.entity_type,
        out_path,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
