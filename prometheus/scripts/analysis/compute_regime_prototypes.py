"""Compute numeric regime prototypes from stored regime embeddings.

This script helps "tighten" the semantics of NumericRegimeModel by
constructing regime prototypes (e.g. NEUTRAL, CRISIS) from actual
regime embeddings stored in the ``regimes`` table.

Given a region and one or more date windows, it computes mean regime
embeddings per window and emits a JSON prototype configuration that can
be used to initialise NumericRegimeModel in a more meaningful way.

The output does not modify the database; it is intended for offline
calibration and inspection.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Sequence

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


@dataclass(frozen=True)
class WindowSpec:
    name: str
    start_date: date
    end_date: date


def _compute_mean_embedding_for_window(
    db_manager: DatabaseManager,
    region: str,
    window: WindowSpec,
) -> Optional[np.ndarray]:
    sql = """
        SELECT regime_embedding
        FROM regimes
        WHERE region = %s
          AND as_of_date BETWEEN %s AND %s
        ORDER BY as_of_date ASC
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (region, window.start_date, window.end_date))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    if not rows:
        logger.warning(
            "No regime embeddings found for region=%s window=%s [%s,%s]",
            region,
            window.name,
            window.start_date,
            window.end_date,
        )
        return None

    vectors = [np.frombuffer(row[0], dtype=np.float32) for row in rows if row[0] is not None]
    if not vectors:
        return None

    first_shape = vectors[0].shape
    for v in vectors[1:]:
        if v.shape != first_shape:
            raise ValueError(
                "Inconsistent regime embedding shapes for window="
                f"{window.name}: {v.shape} vs {first_shape}"
            )

    stacked = np.stack(vectors, axis=0)
    return stacked.mean(axis=0).astype(np.float32)


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Compute regime prototypes (e.g. NEUTRAL/CRISIS) for a region "
            "by averaging stored regime embeddings over date windows."
        ),
    )

    parser.add_argument(
        "--region",
        type=str,
        required=True,
        help="Region identifier as stored in the regimes table (e.g. US)",
    )

    parser.add_argument(
        "--neutral-start",
        type=_parse_date,
        required=True,
        help="Start date (YYYY-MM-DD) for NEUTRAL calibration window",
    )
    parser.add_argument(
        "--neutral-end",
        type=_parse_date,
        required=True,
        help="End date (YYYY-MM-DD) for NEUTRAL calibration window",
    )

    parser.add_argument(
        "--crisis-start",
        type=_parse_date,
        default=None,
        help="Optional start date for CRISIS calibration window",
    )
    parser.add_argument(
        "--crisis-end",
        type=_parse_date,
        default=None,
        help="Optional end date for CRISIS calibration window",
    )

    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help=(
            "Optional path to write prototypes JSON. If omitted, the JSON "
            "is printed to stdout."
        ),
    )

    args = parser.parse_args(argv)

    if args.neutral_end < args.neutral_start:
        parser.error("--neutral-end must be >= --neutral-start")

    if (args.crisis_start is None) ^ (args.crisis_end is None):
        parser.error("--crisis-start and --crisis-end must be provided together or not at all")

    if args.crisis_start and args.crisis_end and args.crisis_end < args.crisis_start:
        parser.error("--crisis-end must be >= --crisis-start")

    config = get_config()
    db_manager = DatabaseManager(config)

    windows: List[WindowSpec] = [
        WindowSpec(
            name="NEUTRAL",
            start_date=args.neutral_start,
            end_date=args.neutral_end,
        )
    ]

    if args.crisis_start and args.crisis_end:
        windows.append(
            WindowSpec(
                name="CRISIS",
                start_date=args.crisis_start,
                end_date=args.crisis_end,
            )
        )

    embeddings: Dict[str, Any] = {}
    dim: Optional[int] = None

    for w in windows:
        vec = _compute_mean_embedding_for_window(db_manager, args.region, w)
        if vec is None:
            continue

        if dim is None:
            dim = int(vec.shape[0])
        elif dim != int(vec.shape[0]):
            raise ValueError(
                f"Embedding dimension mismatch between windows; expected {dim}, got {vec.shape[0]}"
            )

        embeddings[w.name] = {
            "center": vec.tolist(),
            "window": {
                "start_date": w.start_date.isoformat(),
                "end_date": w.end_date.isoformat(),
            },
            "l2_norm": float(np.linalg.norm(vec)),
        }

    if not embeddings:
        logger.warning("No prototypes computed; check input windows and data availability")
        return

    output_obj: Dict[str, Any] = {
        "region": args.region,
        "embedding_dim": dim,
        "prototypes": embeddings,
    }

    text = json.dumps(output_obj, indent=2, sort_keys=True)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(text + "\n")
        logger.info("Wrote regime prototypes to %s", args.output)
    else:
        print(text)


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
