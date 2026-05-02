"""Summarise lambda opportunity-density experiment results.

This script reads the CSV produced by ``run_opportunity_density_experiment.py``
and prints a simple top-k ranking of experiments according to a chosen
metric (e.g. Pearson correlation or average uplift).
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
from apatheon.core.logging import get_logger

logger = get_logger(__name__)


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Summarise lambda opportunity-density experiments from a results "
            "CSV and print the top-k rows by a given metric."
        ),
    )

    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to experiment results CSV (from run_opportunity_density_experiment.py)",
    )
    parser.add_argument(
        "--sort-by",
        type=str,
        default="pearson_corr",
        help=(
            "Metric column to sort by (descending). Typical choices: "
            "pearson_corr, spearman_corr, avg_uplift. Default: pearson_corr."
        ),
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=20,
        help="Number of top experiments to print (default: 20)",
    )

    args = parser.parse_args(argv)

    csv_path = Path(args.input)
    if not csv_path.exists():
        raise SystemExit(f"Input CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    if df.empty:
        print(f"No experiment rows in {csv_path}")
        return

    if args.sort_by not in df.columns:
        raise SystemExit(
            f"sort-by column {args.sort_by!r} not found in CSV columns: {sorted(df.columns)}"
        )

    k = max(1, int(args.top_k))

    df_sorted = df.sort_values(by=args.sort_by, ascending=False).head(k)

    cols_to_show = [
        "experiment_id",
        "model",
        "train_start",
        "train_end",
        "test_start",
        "test_end",
        "top_quantile",
        "num_pairs_train",
        "num_pairs_test",
        "num_days_uplift",
        "pearson_corr",
        "spearman_corr",
        "avg_uplift",
        "uplift_std_err",
    ]
    cols_present = [c for c in cols_to_show if c in df_sorted.columns]

    print(f"Top {k} experiments in {csv_path} by {args.sort_by} (descending):")
    print("-")
    print(df_sorted[cols_present].to_string(index=False))


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
