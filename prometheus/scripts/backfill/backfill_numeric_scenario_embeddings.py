"""Backfill numeric scenario embeddings (num-scenario-core-v1).

This script reads instrument-level scenario paths from the runtime
`scenario_paths` table, constructs numeric representations for each
scenario in a given `scenario_set_id`, and stores 384-dim embeddings into
`numeric_window_embeddings` using the `num-scenario-core-v1` encoder
interface.

For v0, the encoder is implemented as a simple flatten + pad/truncate
projection of the scenario return panel into `R^384` using
`PadToDimNumericEmbeddingModel`.

Embeddings are written into `historical_db.numeric_window_embeddings`
with:

- `entity_type = 'SCENARIO'`.
- `entity_id = '{scenario_set_id}:{scenario_id}'`.
- `window_spec` describing the horizon and set id.
- `model_id = 'num-scenario-core-v1'` (by default).

Examples
--------

    # Backfill scenario embeddings for a specific scenario set
    python -m prometheus.scripts.backfill_numeric_scenario_embeddings \
        --scenario-set-id SET_ABC123 \
        --model-id num-scenario-core-v1 \
        --limit 100
"""

from __future__ import annotations

import argparse
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.encoders import NumericEmbeddingStore, NumericWindowSpec
from apathis.encoders.models_simple_numeric import PadToDimNumericEmbeddingModel

logger = get_logger(__name__)


def _load_scenario_set_metadata(
    db_manager: DatabaseManager,
    scenario_set_id: str,
) -> Tuple[int, Optional[str]]:
    """Return (horizon_days, base_date_end_str) for a scenario set.

    base_date_end is converted to an ISO date string if present; this is
    later used as the `as_of_date` for embeddings.
    """

    sql = """
        SELECT horizon_days, base_date_end
        FROM scenario_sets
        WHERE scenario_set_id = %s
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (scenario_set_id,))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if row is None:
        raise ValueError(f"scenario_set not found: {scenario_set_id}")

    horizon_days, base_date_end = row
    horizon = int(horizon_days)
    as_of_date_str: Optional[str]
    if base_date_end is not None:
        as_of_date_str = base_date_end.isoformat()
    else:
        as_of_date_str = None
    return horizon, as_of_date_str


def _load_scenario_ids(
    db_manager: DatabaseManager,
    scenario_set_id: str,
    limit: Optional[int] = None,
) -> List[int]:
    """Return distinct scenario_id values for a scenario_set_id."""

    sql = """
        SELECT DISTINCT scenario_id
        FROM scenario_paths
        WHERE scenario_set_id = %s
        ORDER BY scenario_id
    """

    params: List[object] = [scenario_set_id]
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

    return [int(scenario_id) for (scenario_id,) in rows]


def _load_scenario_panel(
    db_manager: DatabaseManager,
    scenario_set_id: str,
    scenario_id: int,
    horizon_days: int,
) -> Optional[np.ndarray]:
    """Load a scenario return panel as a 2D array of shape (H, N).

    We consider only instrument-level rows (instrument_id non-null) and
    treat `horizon_index` as the time axis. Returns are ordered by
    horizon_index ascending, then instrument_id ascending.

    If the panel does not have at least `horizon_days` steps or has zero
    instruments, None is returned.
    """

    sql = """
        SELECT horizon_index, instrument_id, return_value
        FROM scenario_paths
        WHERE scenario_set_id = %s
          AND scenario_id = %s
          AND instrument_id IS NOT NULL
        ORDER BY horizon_index ASC, instrument_id ASC
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (scenario_set_id, scenario_id))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    if not rows:
        return None

    # Collect unique instruments and max horizon_index.
    instruments: List[str] = []
    seen_instruments: set[str] = set()
    max_h: int = 0
    for h, inst_id, _ in rows:
        max_h = max(max_h, int(h))
        inst = str(inst_id)
        if inst not in seen_instruments:
            seen_instruments.add(inst)
            instruments.append(inst)

    if max_h < horizon_days:
        logger.debug(
            "Scenario %s:%d has insufficient horizon length %d < %d; skipping",
            scenario_set_id,
            scenario_id,
            max_h,
            horizon_days,
        )
        return None

    if not instruments:
        return None

    inst_index: Dict[str, int] = {inst: i for i, inst in enumerate(instruments)}
    H = horizon_days
    N = len(instruments)

    panel = np.zeros((H, N), dtype=np.float32)

    for h, inst_id, ret in rows:
        h_idx = int(h)
        if h_idx <= 0 or h_idx > H:
            # horizon_index=0 holds baseline rows; we ignore and only
            # consume 1..H for returns.
            continue
        inst = str(inst_id)
        j = inst_index[inst]
        panel[h_idx - 1, j] = float(ret)

    return panel


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill numeric scenario embeddings (num-scenario-core-v1) "
            "into numeric_window_embeddings for a given scenario_set_id."
        ),
    )

    parser.add_argument(
        "--scenario-set-id",
        type=str,
        required=True,
        help="Identifier of the scenario set to embed",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of scenarios (scenario_id values) to process (default: 100)",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="num-scenario-core-v1",
        help="Model identifier to tag embeddings with (default: num-scenario-core-v1)",
    )

    args = parser.parse_args(argv)

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive")

    return args


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)

    config = get_config()
    db_manager = DatabaseManager(config)

    horizon_days, as_of_date_str = _load_scenario_set_metadata(
        db_manager=db_manager,
        scenario_set_id=args.scenario_set_id,
    )

    scenario_ids = _load_scenario_ids(
        db_manager=db_manager,
        scenario_set_id=args.scenario_set_id,
        limit=args.limit,
    )

    if not scenario_ids:
        logger.warning("No scenarios found for scenario_set_id=%s; nothing to do", args.scenario_set_id)
        return

    logger.info(
        "Backfilling numeric scenario embeddings: set=%s horizon_days=%d scenarios=%d model_id=%s",
        args.scenario_set_id,
        horizon_days,
        len(scenario_ids),
        args.model_id,
    )

    store = NumericEmbeddingStore(db_manager=db_manager)
    model = PadToDimNumericEmbeddingModel(target_dim=384)

    # Determine as_of_date to use in numeric_window_embeddings. Prefer
    # base_date_end from scenario_sets; fall back to today if absent.
    from datetime import date as _date_cls

    if as_of_date_str is not None:
        as_of_date = _date_cls.fromisoformat(as_of_date_str)
    else:
        as_of_date = _date_cls.today()

    success = 0
    failures = 0

    for scenario_id in scenario_ids:
        panel = _load_scenario_panel(
            db_manager=db_manager,
            scenario_set_id=args.scenario_set_id,
            scenario_id=scenario_id,
            horizon_days=horizon_days,
        )
        if panel is None:
            continue

        try:
            embedding = model.encode(panel)
        except Exception as exc:  # pragma: no cover - defensive
            failures += 1
            logger.exception(
                "Failed to encode scenario %s:%d: %s",
                args.scenario_set_id,
                scenario_id,
                exc,
            )
            continue

        spec = NumericWindowSpec(
            entity_type="SCENARIO",
            entity_id=f"{args.scenario_set_id}:{scenario_id}",
            window_days=horizon_days,
        )

        try:
            store.save_embedding(
                spec=spec,
                as_of_date=as_of_date,
                model_id=args.model_id,
                vector=embedding,
            )
            success += 1
        except Exception as exc:  # pragma: no cover - defensive
            failures += 1
            logger.exception(
                "Failed to save embedding for scenario %s:%d: %s",
                args.scenario_set_id,
                scenario_id,
                exc,
            )

    logger.info(
        "Numeric scenario embeddings backfill complete: success=%d failures=%d",
        success,
        failures,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
