"""Backfill regime state-change risk series for a region.

This offline script computes a simple *regime state-change risk* time
series for a given region, using the empirical regime transition matrix
stored in the runtime database.

For each regime state ``(region, as_of_date, regime_label)`` observed in
``regimes`` between the requested start and end dates, it computes:

- The probability of *any* regime change over a given horizon ``H``.
- The probability of being in ``CRISIS`` or ``RISK_OFF`` at horizon ``H``.
- The probability of being in ``CARRY`` at horizon ``H``.
- A scalar ``regime_risk_score`` in [0, 1], currently equal to the
  stressed-probability ``P(CRISIS or RISK_OFF)``.

The output is a CSV with one row per ``(region, as_of_date)`` and the
above metrics. This CSV can later be joined onto other research tables
(e.g. lambda_t(x) clusters) by ``as_of_date`` and used as an additional
feature.

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
from apatheon.core.config import get_config
from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from apatheon.regime.state_change import _build_transition_matrix
from apatheon.regime.storage import RegimeStorage
from apatheon.regime.types import RegimeLabel, RegimeState

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


@dataclass(frozen=True)
class RegimeRiskPoint:
    """Single regime risk observation for (region, as_of_date)."""

    as_of_date: date
    region: str
    current_regime_label: str
    horizon_steps: int
    p_change_any: float
    p_to_crisis_or_risk_off: float
    p_to_carry: float
    regime_risk_score: float


def _compute_regime_risk_series(
    storage: RegimeStorage,
    *,
    region: str,
    start_date: date,
    end_date: date,
    horizon_steps: int,
) -> List[RegimeRiskPoint]:
    """Compute regime risk points for each stored regime state in window.

    This function:

    - Fetches regime history for ``region`` between ``start_date`` and
      ``end_date``.
    - Builds a Markov transition matrix from
      :meth:`RegimeStorage.get_transition_matrix`.
    - Raises the matrix to ``horizon_steps`` via ``numpy.linalg.matrix_power``.
    - For each regime state in history, reads off the horizon
      distribution and derives risk metrics.
    """

    history: List[RegimeState] = storage.get_history(region, start_date, end_date)
    if not history:
        logger.warning(
            "No regime history found for region=%s between %s and %s; nothing to do",
            region,
            start_date,
            end_date,
        )
        return []

    matrix_dict = storage.get_transition_matrix(region)
    P, index_by_label = _build_transition_matrix(matrix_dict)

    try:
        P_h = np.linalg.matrix_power(P, horizon_steps)
    except ValueError as exc:  # pragma: no cover - defensive
        raise RuntimeError("Failed to compute matrix power for regime transitions") from exc

    labels = list(index_by_label.keys())

    points: List[RegimeRiskPoint] = []
    for state in history:
        current_label = state.regime_label
        current_idx = index_by_label[current_label]
        row = P_h[current_idx, :]

        # Build distribution over labels at the horizon.
        distribution: Dict[RegimeLabel, float] = {
            label: float(row[index_by_label[label]]) for label in labels
        }

        p_stay = distribution.get(current_label, 0.0)
        p_change_any = float(max(0.0, min(1.0, 1.0 - p_stay)))

        p_crisis = distribution.get(RegimeLabel.CRISIS, 0.0)
        p_risk_off = distribution.get(RegimeLabel.RISK_OFF, 0.0)
        p_carry = distribution.get(RegimeLabel.CARRY, 0.0)

        p_to_crisis_or_risk_off = float(max(0.0, min(1.0, p_crisis + p_risk_off)))
        regime_risk_score = p_to_crisis_or_risk_off

        points.append(
            RegimeRiskPoint(
                as_of_date=state.as_of_date,
                region=state.region,
                current_regime_label=current_label.value,
                horizon_steps=horizon_steps,
                p_change_any=p_change_any,
                p_to_crisis_or_risk_off=p_to_crisis_or_risk_off,
                p_to_carry=float(max(0.0, min(1.0, p_carry))),
                regime_risk_score=regime_risk_score,
            )
        )

    return points


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill a regime state-change risk time series for a region "
            "using the empirical transition matrix."
        ),
    )

    parser.add_argument(
        "--region",
        type=str,
        default="US",
        help="Region label for regime engine (default: US)",
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        required=True,
        help="Start date (YYYY-MM-DD) for regime history window",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        required=True,
        help="End date (YYYY-MM-DD) for regime history window",
    )
    parser.add_argument(
        "--horizon-steps",
        type=int,
        default=1,
        help="Horizon in Markov steps (typically trading days) for risk forecast (default: 1)",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Path to output CSV file for regime risk series",
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
    storage = RegimeStorage(db_manager=db_manager)

    logger.info(
        "Computing regime risk series for region=%s, start=%s, end=%s, horizon_steps=%d",
        args.region,
        start_date,
        end_date,
        args.horizon_steps,
    )

    points = _compute_regime_risk_series(
        storage,
        region=args.region,
        start_date=start_date,
        end_date=end_date,
        horizon_steps=args.horizon_steps,
    )

    if not points:
        logger.warning("No regime risk points computed; nothing to write")
        return

    df_out = pd.DataFrame(
        [
            {
                "as_of_date": p.as_of_date,
                "region": p.region,
                "current_regime_label": p.current_regime_label,
                "horizon_steps": p.horizon_steps,
                "p_change_any": p.p_change_any,
                "p_to_crisis_or_risk_off": p.p_to_crisis_or_risk_off,
                "p_to_carry": p.p_to_carry,
                "regime_risk_score": p.regime_risk_score,
            }
            for p in points
        ]
    )

    df_out.sort_values(["as_of_date", "region"], inplace=True)
    df_out.to_csv(out_path, index=False)

    logger.info(
        "Wrote %d regime risk observations for region=%s to %s",
        df_out.shape[0],
        args.region,
        out_path,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
