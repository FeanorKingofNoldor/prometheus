"""Backfill Meta Config+Env joint embeddings (joint-meta-config-env-v1).

This script builds joint Meta Config+Env embeddings for backtest runs by
combining up to three numeric branches derived from `backtest_runs`:

- Config branch  (num-config-core-v1):  numeric encoding of config_json.
- Env branch     (num-env-core-v1):     environment-oriented features from
  config_json (market/universe/assessment fields).
- Outcome branch (num-outcome-core-v1): numeric encoding of metrics_json
  (backtest summary metrics).

All branches are constructed as 384-dim vectors via a simple
flatten+pad/truncate model. For each backtest run the script:

1. Extracts config/env/outcome feature vectors from JSON.
2. Encodes each into `R^384`.
3. Combines available branches via a weighted average into a single
   `z_meta ∈ R^384`.
4. Stores the result in `historical_db.joint_embeddings` with:

   - joint_type = 'META_CONFIG_ENV_V0'.
   - model_id   = 'joint-meta-config-env-v1' (by default).

The combination logic lives in this script; the joint model used by
JointEmbeddingService is an IdentityNumericJointModel that simply
passes the combined numeric embedding through.

Examples
--------

    # Backfill Meta Config+Env embeddings for a strategy
    python -m prometheus.scripts.backfill_joint_meta_config_env \
        --strategy-id US_EQ_CORE_LONG_EQ \
        --limit 200 \
        --w-config 1.0 --w-env 1.0 --w-outcome 1.0 \
        --joint-model-id joint-meta-config-env-v1
"""

from __future__ import annotations

import argparse
import hashlib
from datetime import date
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.encoders.joint import JointEmbeddingService, JointEmbeddingStore, JointExample
from apathis.encoders.models_joint_simple import IdentityNumericJointModel
from apathis.encoders.models_simple_numeric import PadToDimNumericEmbeddingModel

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _stable_hash_to_float(s: str) -> float:
    """Map a string to a deterministic float in [-1, 1]."""

    h = hashlib.sha1(s.encode("utf-8")).hexdigest()
    # Use first 16 hex chars (64 bits) for stability.
    v = int(h[:16], 16)
    denom = float(2**63 - 1)
    x = (v % (2**63 - 1)) / denom  # in [0, 1]
    return float(x * 2.0 - 1.0)


def _flatten_dict(
    data: Mapping[str, Any],
    *,
    prefix: str = "",
    out: Optional[List[Tuple[str, Any]]] = None,
) -> List[Tuple[str, Any]]:
    """Flatten a nested dict into (key_path, value) pairs.

    Lists/tuples are traversed with numeric indices. Only leaf values are
    included; intermediate dict/list nodes are not.
    """

    if out is None:
        out = []

    for key in sorted(data.keys()):
        value = data[key]
        if prefix:
            path = f"{prefix}.{key}"
        else:
            path = str(key)

        if isinstance(value, dict):
            _flatten_dict(value, prefix=path, out=out)
        elif isinstance(value, (list, tuple)):
            for idx, item in enumerate(value):
                item_path = f"{path}[{idx}]"
                if isinstance(item, dict):
                    _flatten_dict(item, prefix=item_path, out=out)
                else:
                    out.append((item_path, item))
        else:
            out.append((path, value))

    return out


def _build_numeric_features_from_config(config: Mapping[str, Any]) -> np.ndarray:
    """Build a numeric feature vector from config_json.

    - Numeric/boolean values are used directly.
    - String values are converted via a stable hash of "path=value".
    """

    pairs = _flatten_dict(config)
    values: List[float] = []

    for path, value in pairs:
        if isinstance(value, bool):
            values.append(1.0 if value else 0.0)
        elif isinstance(value, (int, float)):
            values.append(float(value))
        elif isinstance(value, str):
            token = f"{path}={value}"
            values.append(_stable_hash_to_float(token))
        else:
            # Ignore other types (None, objects, etc.).
            continue

    return np.asarray(values, dtype=np.float32)


_ENV_KEYS = {
    "market_id",
    "universe_id",
    "assessment_strategy_id",
    "assessment_horizon_days",
}


def _build_env_features_from_config(config: Mapping[str, Any]) -> np.ndarray:
    """Build environment-oriented features from config_json.

    This focuses on keys that describe *where* the run was executed
    (market/universe/assessment) rather than how.
    """

    pairs = _flatten_dict(config)
    values: List[float] = []

    for path, value in pairs:
        if not any(key in path for key in _ENV_KEYS):
            continue

        if isinstance(value, bool):
            values.append(1.0 if value else 0.0)
        elif isinstance(value, (int, float)):
            values.append(float(value))
        elif isinstance(value, str):
            token = f"{path}={value}"
            values.append(_stable_hash_to_float(token))
        else:
            continue

    return np.asarray(values, dtype=np.float32)


def _build_outcome_features(metrics: Mapping[str, Any]) -> np.ndarray:
    """Build a numeric feature vector from metrics_json.

    Numeric values (int/float/bool) are included, sorted by key. Strings
    and other types are ignored.
    """

    values: List[float] = []
    for key in sorted(metrics.keys()):
        v = metrics.get(key)
        if isinstance(v, bool):
            values.append(1.0 if v else 0.0)
        elif isinstance(v, (int, float)):
            values.append(float(v))
        else:
            continue

    return np.asarray(values, dtype=np.float32)


def _encode_to_384(features: np.ndarray, model: PadToDimNumericEmbeddingModel) -> Optional[np.ndarray]:
    """Encode a 1D feature vector into R^384 using PadToDimNumericEmbeddingModel.

    Returns None if the input vector is empty.
    """

    if features.size == 0:
        return None

    window = features.reshape(1, -1).astype(np.float32)
    return model.encode(window)


def _combine_branches(
    components: List[Optional[np.ndarray]],
    weights: List[float],
) -> Optional[np.ndarray]:
    """Weighted average combination of context branches.

    Components with None value or non-positive weight are skipped. If no
    valid components remain, returns None. Otherwise a weighted average is
    computed and returned as float32.
    """

    valid: List[Tuple[np.ndarray, float]] = []
    for comp, w in zip(components, weights, strict=True):
        if comp is None or w <= 0.0:  # type: ignore[truthy-function]
            continue
        valid.append((comp, w))

    if not valid:
        return None

    base_shape = valid[0][0].shape
    for comp, _ in valid[1:]:
        if comp.shape != base_shape:
            raise ValueError(
                f"Meta branches have mismatched shapes: {base_shape} vs {comp.shape}"
            )

    stacked = np.stack([c for (c, _) in valid], axis=0)
    w = np.array([w for (_, w) in valid], dtype=np.float32).reshape(-1, 1)
    z = (w * stacked).sum(axis=0) / w.sum()
    return z.astype(np.float32)


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill Meta Config+Env joint embeddings (META_CONFIG_ENV_V0) "
            "from backtest_runs by combining config, env, and outcome branches."
        ),
    )

    parser.add_argument(
        "--strategy-id",
        dest="strategy_ids",
        action="append",
        help="Strategy_id to include (can be specified multiple times). If omitted, all strategies are used.",
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Optional minimum backtest end_date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        help="Optional maximum backtest end_date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Maximum number of backtest runs to process (default: 500)",
    )

    parser.add_argument(
        "--w-config",
        type=float,
        default=1.0,
        help="Weight for config branch (default: 1.0)",
    )
    parser.add_argument(
        "--w-env",
        type=float,
        default=1.0,
        help="Weight for environment branch (default: 1.0)",
    )
    parser.add_argument(
        "--w-outcome",
        type=float,
        default=1.0,
        help="Weight for outcome branch (default: 1.0)",
    )

    parser.add_argument(
        "--joint-model-id",
        type=str,
        default="joint-meta-config-env-v1",
        help="Model_id to tag joint embeddings with (default: joint-meta-config-env-v1)",
    )

    args = parser.parse_args(argv)

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive")

    return args


def _load_backtest_runs(
    db_manager: DatabaseManager,
    *,
    strategy_ids: Optional[List[str]] = None,
    start: Optional[date] = None,
    end: Optional[date] = None,
    limit: Optional[int] = None,
) -> List[Tuple[str, str, Optional[str], date, Dict[str, Any], Dict[str, Any]]]:
    """Load backtest_runs rows with non-null metrics_json.

    Returns tuples of (run_id, strategy_id, universe_id, end_date,
    config_json, metrics_json).
    """

    where_clauses = ["metrics_json IS NOT NULL"]
    params: List[Any] = []

    if strategy_ids:
        where_clauses.append("strategy_id = ANY(%s)")
        params.append(strategy_ids)

    if start is not None:
        where_clauses.append("end_date >= %s")
        params.append(start)

    if end is not None:
        where_clauses.append("end_date <= %s")
        params.append(end)

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT run_id, strategy_id, universe_id, end_date, config_json, metrics_json "
        "FROM backtest_runs" + where_sql + " ORDER BY end_date DESC, run_id DESC"
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

    results: List[Tuple[str, str, Optional[str], date, Dict[str, Any], Dict[str, Any]]] = []
    for run_id, strategy_id, universe_id, end_date_db, config_json, metrics_json in rows:
        cfg = config_json or {}
        m = metrics_json or {}
        results.append(
            (
                str(run_id),
                str(strategy_id),
                str(universe_id) if universe_id is not None else None,
                end_date_db,
                cfg,
                m,
            )
        )

    return results


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)

    config = get_config()
    db_manager = DatabaseManager(config)

    runs = _load_backtest_runs(
        db_manager=db_manager,
        strategy_ids=args.strategy_ids,
        start=args.start,
        end=args.end,
        limit=args.limit,
    )

    if not runs:
        logger.warning("No backtest_runs rows found for the given filters; nothing to do")
        return

    logger.info(
        "Backfilling Meta Config+Env embeddings: runs=%d strategies=%s joint_model=%s",
        len(runs),
        ",".join(sorted({sid for _, sid, _, _, _, _ in runs})),
        args.joint_model_id,
    )

    pad_model = PadToDimNumericEmbeddingModel(target_dim=384)
    store = JointEmbeddingStore(db_manager=db_manager)
    joint_model = IdentityNumericJointModel()
    service = JointEmbeddingService(model=joint_model, store=store, model_id=args.joint_model_id)

    examples: List[JointExample] = []

    for run_id, strategy_id, universe_id, end_date_db, cfg, metrics in runs:
        # Branch encoders
        z_config_raw = _build_numeric_features_from_config(cfg)
        z_env_raw = _build_env_features_from_config(cfg)
        z_outcome_raw = _build_outcome_features(metrics)

        z_config = _encode_to_384(z_config_raw, pad_model)
        z_env = _encode_to_384(z_env_raw, pad_model)
        z_outcome = _encode_to_384(z_outcome_raw, pad_model)

        z_meta = _combine_branches(
            components=[z_config, z_env, z_outcome],
            weights=[args.w_config, args.w_env, args.w_outcome],
        )
        if z_meta is None:
            logger.debug("No usable branches for run_id=%s; skipping", run_id)
            continue

        source_parts: List[str] = []
        if z_config is not None and args.w_config > 0.0:
            source_parts.append("config")
        if z_env is not None and args.w_env > 0.0:
            source_parts.append("env")
        if z_outcome is not None and args.w_outcome > 0.0:
            source_parts.append("outcome")

        entity_scope: Mapping[str, object] = {
            "run_id": run_id,
            "strategy_id": strategy_id,
            "universe_id": universe_id,
            "source": "+".join(source_parts) if source_parts else "<none>",
        }

        ex = JointExample(
            joint_type="META_CONFIG_ENV_V0",
            as_of_date=end_date_db,
            entity_scope=entity_scope,
            numeric_embedding=z_meta,
            text_embedding=None,
        )
        examples.append(ex)

    if not examples:
        logger.warning("No Meta Config+Env examples constructed; nothing to write")
        return

    _ = service.embed_and_store(examples)
    logger.info(
        "Meta Config+Env backfill complete: wrote %d embeddings with model_id=%s",
        len(examples),
        args.joint_model_id,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
