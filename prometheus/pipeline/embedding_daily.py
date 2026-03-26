"""Prometheus -- Daily Embedding Generation Pipeline.

Generates numeric window embeddings for all active instruments on a given
date, then computes cross-sectional features (distance from universe mean)
used by the assessment model.

Tier 1: Numeric embeddings only (no external model files needed).
Tier 2: + Text embeddings + joint (requires torch + sentence-transformers).

Called by the daily pipeline during the signals phase.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional

import numpy as np

from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class CrossSectionalScore:
    """Cross-sectional embedding features for one instrument."""
    instrument_id: str
    cosine_distance: float    # Distance from universe mean (0 = typical, 1 = outlier)
    percentile_rank: float    # Where this instrument ranks (0 = most typical, 1 = most outlier)


def generate_numeric_embeddings(
    db_manager: DatabaseManager,
    as_of_date: date,
    market_id: str = "US_EQ",
    window_days: int = 63,
    model_id: str = "num-regime-core-v1",
) -> int:
    """Generate numeric window embeddings for all active instruments.

    Uses PadToDimNumericEmbeddingModel (384-dim, deterministic, no external files).
    Returns number of embeddings generated.
    """
    from apathis.core.time import TradingCalendar
    from apathis.data.reader import DataReader
    from apathis.encoders import (
        NumericWindowBuilder,
        NumericWindowEncoder,
        NumericWindowSpec,
        NumericEmbeddingStore,
    )
    from apathis.encoders.models_simple_numeric import PadToDimNumericEmbeddingModel

    reader = DataReader(db_manager=db_manager)
    calendar = TradingCalendar()
    builder = NumericWindowBuilder(data_reader=reader, calendar=calendar)
    model = PadToDimNumericEmbeddingModel(target_dim=384)
    store = NumericEmbeddingStore(db_manager=db_manager)
    encoder = NumericWindowEncoder(builder=builder, model=model, store=store, model_id=model_id)

    # Get active instruments
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT instrument_id FROM instruments
                WHERE market_id = %s AND status = 'ACTIVE' AND asset_class = 'EQUITY'
                ORDER BY instrument_id
            """, (market_id,))
            instruments = [r[0] for r in cur.fetchall()]

    count = 0
    for instrument_id in instruments:
        spec = NumericWindowSpec(
            entity_type="INSTRUMENT",
            entity_id=instrument_id,
            window_days=window_days,
        )
        try:
            encoder.embed_and_store(spec, as_of_date)
            count += 1
        except Exception:
            # Skip instruments with insufficient price history
            pass

    logger.info(
        "Numeric embeddings: generated %d/%d for %s on %s",
        count, len(instruments), market_id, as_of_date,
    )
    return count


def compute_cross_sectional_scores(
    db_manager: DatabaseManager,
    instrument_ids: List[str],
    as_of_date: date,
    model_id: str = "num-regime-core-v1",
) -> Dict[str, CrossSectionalScore]:
    """Compute cross-sectional embedding features for instruments.

    For each instrument, computes cosine distance from the universe mean
    embedding. Instruments far from the mean are in unusual states relative
    to the rest of the universe.

    Returns: {instrument_id: CrossSectionalScore}
    """
    # Batch-load all embeddings for the date
    with db_manager.get_historical_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT entity_id, vector
                FROM numeric_window_embeddings
                WHERE model_id = %s
                  AND as_of_date = %s
                  AND entity_type = 'INSTRUMENT'
                  AND entity_id = ANY(%s)
            """, (model_id, as_of_date, list(instrument_ids)))
            rows = cur.fetchall()

    if len(rows) < 10:
        logger.debug("Too few embeddings (%d) for cross-sectional scoring on %s", len(rows), as_of_date)
        return {}

    # Parse vectors
    vectors: Dict[str, np.ndarray] = {}
    for entity_id, vec_bytes in rows:
        vectors[entity_id] = np.frombuffer(vec_bytes, dtype=np.float32).copy()

    # Compute universe mean
    all_vecs = np.stack(list(vectors.values()))
    universe_mean = np.mean(all_vecs, axis=0)
    mean_norm = np.linalg.norm(universe_mean)

    if mean_norm < 1e-10:
        return {}

    # Compute cosine distances
    distances: Dict[str, float] = {}
    for inst_id, vec in vectors.items():
        vec_norm = np.linalg.norm(vec)
        if vec_norm < 1e-10:
            distances[inst_id] = 1.0  # Degenerate vector = maximum outlier
        else:
            cosine_sim = float(np.dot(vec, universe_mean) / (vec_norm * mean_norm))
            distances[inst_id] = max(0.0, 1.0 - cosine_sim)  # 0 = identical, 2 = opposite

    # Compute percentile ranks
    sorted_dists = sorted(distances.values())
    n = len(sorted_dists)

    results: Dict[str, CrossSectionalScore] = {}
    for inst_id, dist in distances.items():
        # Rank: what fraction of instruments are closer to the mean
        rank = sum(1 for d in sorted_dists if d <= dist) / n
        results[inst_id] = CrossSectionalScore(
            instrument_id=inst_id,
            cosine_distance=dist,
            percentile_rank=rank,
        )

    return results
