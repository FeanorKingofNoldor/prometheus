"""Prometheus -- Daily Embedding Generation Pipeline.

Generates numeric window embeddings (``num-regime-core-v1``) for all active
instruments on a given date. These embeddings are consumed downstream by the
joint-context / joint-profile backfills (see ``scripts/backfill/``) and the
standalone ``run_numeric_regime`` job.

(Historically these embeddings also fed a cross-sectional outlier penalty in
the assessment model. That penalty was removed — it carried no information
beyond raw vol/return — so the cross-sectional scoring helper was deleted. The
embedding *generation* is retained because the joint backfills still read the
``numeric_window_embeddings`` table.)

Numeric-only by design: 63 days of price/volume/returns patterns capture
actual market behavior. Text/news embeddings were evaluated and rejected —
they dilute the price signal with stale, semantic noise that doesn't
translate to alpha. Use LLM structured extraction for text signals instead
(earnings guidance direction, central bank hawkish/dovish, etc.).

No external model files needed — pure deterministic feature transforms.
Called by the daily pipeline during the signals phase.
"""

from __future__ import annotations

from datetime import date

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger

logger = get_logger(__name__)


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
    from apatheon.core.time import TradingCalendar
    from apatheon.data.reader import DataReader
    from apatheon.encoders import (
        NumericEmbeddingStore,
        NumericWindowBuilder,
        NumericWindowEncoder,
        NumericWindowSpec,
    )
    from apatheon.encoders.models_simple_numeric import PadToDimNumericEmbeddingModel

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
