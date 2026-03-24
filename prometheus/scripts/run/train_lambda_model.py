"""Train and save a LambdaPoly2Model from raw lambda CSV data.

This script loads a raw lambda_t(x) CSV (produced by
``backfill_opportunity_density.py``), prepares lambda_next pairs,
trains a global poly2 model on all available data, and saves the
model coefficients to a JSON file for daily pipeline use.

Run this periodically (e.g. weekly) or after backfills to refresh
the model. The daily pipeline (``lambda_daily.run_daily_lambda``)
will load the saved model for real-time predictions.

Usage::

    python -m prometheus.scripts.run.train_lambda_model \
        --input data/lambda_US_EQ_raw.csv \
        --output data/lambda_model_US_EQ.json \
        --experiment-id US_EQ_GL_POLY2_V0
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
from apathis.core.logging import get_logger

from prometheus.opportunity.lambda_model import (
    LambdaPoly2Model,
    prepare_next_lambda,
)

logger = get_logger(__name__)


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Train and save a lambda poly2 model."""
    parser = argparse.ArgumentParser(
        description="Train a LambdaPoly2Model from raw lambda CSV and save coefficients.",
    )

    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to raw lambda_t(x) CSV (from backfill_opportunity_density.py)",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Path to save model coefficients JSON",
    )
    parser.add_argument(
        "--experiment-id",
        type=str,
        default="US_EQ_GL_POLY2_V0",
        help="Experiment identifier to embed in the saved model (default: US_EQ_GL_POLY2_V0)",
    )

    args = parser.parse_args(argv)

    csv_path = Path(args.input)
    out_path = Path(args.output)

    if not csv_path.exists():
        raise SystemExit(f"Input CSV not found: {csv_path}")

    logger.info("Loading raw lambda data from %s", csv_path)
    df = pd.read_csv(csv_path)
    df["as_of_date"] = pd.to_datetime(df["as_of_date"]).dt.date

    logger.info("Loaded %d raw observations", df.shape[0])

    # Prepare lambda_next pairs for training.
    df_pairs = prepare_next_lambda(df)
    logger.info("Prepared %d lambda pairs for training", df_pairs.shape[0])

    if df_pairs.empty:
        raise SystemExit("No valid lambda pairs — cannot train model")

    # Train model.
    model = LambdaPoly2Model(experiment_id=args.experiment_id)
    model.train(df_pairs)

    # Save.
    model.save(out_path)

    logger.info(
        "Model trained and saved: %d pairs, %d weights → %s",
        model.train_rows,
        len(model.weights),
        out_path,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
