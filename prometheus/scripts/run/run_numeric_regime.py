"""Run numeric Regime Engine using numeric window embeddings.

This script wires together:

- NumericWindowEncoder (prices → numeric window → embedding → persisted
  into ``numeric_window_embeddings``)
- NumericRegimeModel (embedding → regime label)
- RegimeEngine/RegimeStorage (persist regime state and transitions into
  runtime ``regimes`` / ``regime_transitions`` tables).

For Iteration 1 the regime model is deliberately simple:

- It uses a single prototype labelled ``NEUTRAL`` whose centre is the
  embedding for the requested (region, as_of_date).
- As a result, all classifications produced by this script will be
  ``NEUTRAL`` with confidence 1.0, but the full pipeline from prices to
  embeddings to regime state is exercised and persisted.

Examples
--------

    # Run numeric regime for US region using AAPL.US as proxy
    python -m prometheus.scripts.run_numeric_regime \
        --region US \
        --instrument-id AAPL.US \
        --as-of 2025-11-21 \
        --window-days 63
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Optional, Sequence

import numpy as np

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from apathis.core.time import TradingCalendar, TradingCalendarConfig, US_EQ
from apathis.data.reader import DataReader
from apathis.encoders import (
    NumericWindowSpec,
    NumericWindowBuilder,
    NumericEmbeddingStore,
    NumericWindowEncoder,
    FlattenNumericEmbeddingModel,
    PadToDimNumericEmbeddingModel,
)
from apathis.regime import (
    RegimeEngine,
    RegimeStorage,
    NumericRegimeModel,
    RegimePrototype,
    RegimeLabel,
)


logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run numeric Regime Engine for a single region/date",
    )

    parser.add_argument(
        "--region",
        type=str,
        default="US",
        help="Region label for regime engine (default: US)",
    )
    parser.add_argument(
        "--instrument-id",
        type=str,
        required=True,
        help="Instrument ID used as numeric proxy for the region (e.g. AAPL.US)",
    )
    parser.add_argument(
        "--as-of",
        dest="as_of",
        type=_parse_date,
        required=True,
        help="As-of date for regime classification (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=63,
        help="Number of observed price rows in the numeric window (default: 63)",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="num-regime-core-v1",
        help=(
            "Model identifier for numeric embeddings (default: num-regime-core-v1). "
            "Use 'numeric-simple-v1' to store raw flattened windows."
        ),
    )
    parser.add_argument(
        "--market",
        type=str,
        default=US_EQ,
        help="Trading calendar market code for prices (default: US_EQ)",
    )

    args = parser.parse_args(argv)

    db_manager = get_db_manager()

    # ------------------------------------------------------------------
    # Build numeric encoder
    # ------------------------------------------------------------------

    reader = DataReader(db_manager=db_manager)
    calendar = TradingCalendar(TradingCalendarConfig(market=args.market))
    builder = NumericWindowBuilder(reader, calendar)
    store = NumericEmbeddingStore(db_manager=db_manager)

    # Choose the numeric embedding model based on model_id. For
    # ``num-regime-core-v1`` we use a padded encoder that produces
    # 384-dimensional vectors; otherwise we fall back to a pure flattening
    # model for debugging and exploratory runs.
    if args.model_id == "num-regime-core-v1":
        model = PadToDimNumericEmbeddingModel(target_dim=384)
    else:
        model = FlattenNumericEmbeddingModel()

    encoder = NumericWindowEncoder(builder=builder, model=model, store=store, model_id=args.model_id)

    spec = NumericWindowSpec(
        entity_type="INSTRUMENT",
        entity_id=args.instrument_id,
        window_days=args.window_days,
    )

    # Build an initial embedding to determine dimensionality and serve as
    # the NEUTRAL prototype centre.
    base_embedding = encoder.embed_and_store(spec, args.as_of)
    center = np.asarray(base_embedding, dtype=np.float32)

    prototypes = [
        RegimePrototype(
            label=RegimeLabel.NEUTRAL,
            center=center,
        ),
    ]

    regime_model = NumericRegimeModel(
        encoder=encoder,
        region_instruments={args.region: args.instrument_id},
        window_days=args.window_days,
        prototypes=prototypes,
        temperature=1.0,
    )

    storage = RegimeStorage(db_manager=db_manager)
    engine = RegimeEngine(model=regime_model, storage=storage)

    state = engine.get_regime(as_of_date=args.as_of, region=args.region)

    logger.info(
        "Numeric regime run complete: date=%s region=%s label=%s confidence=%.3f",
        state.as_of_date,
        state.region,
        state.regime_label.value,
        state.confidence,
    )

    # Also print a concise summary to stdout for ad-hoc use.
    print(
        f"Regime as of {state.as_of_date} for region {state.region}: "
        f"label={state.regime_label.value}, confidence={state.confidence:.3f}",
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
