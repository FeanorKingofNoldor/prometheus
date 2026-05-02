"""Evaluate a simple baseline forecast for opportunity-density (lambda_t).

This script loads a CSV produced by ``backfill_opportunity_density.py`` and
evaluates how well a naive one-step-ahead forecast

    lambda_hat_{t+1}(x) = lambda_t(x)

performs. It reports:

- Overall Pearson and Spearman correlations between lambda_t and
  realised lambda_{t+1} across all cluster-date observations.
- The average uplift in realised lambda_{t+1} for clusters in the top
  quantile of lambda_t compared to the rest (per date), as a sanity check
  that lambda_t carries information about where structure will be
  tomorrow.

This is an offline/research tool and is not part of the live pipeline.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence

import numpy as np
import pandas as pd
from apatheon.core.logging import get_logger
from scipy.stats import spearmanr

logger = get_logger(__name__)


@dataclass(frozen=True)
class BaselineEvalResult:
    pearson_corr: float
    spearman_corr: float
    avg_uplift: float
    uplift_std_err: float
    num_days_used: int


def _load_lambda_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "as_of_date" not in df.columns:
        raise ValueError("Input CSV must have an 'as_of_date' column")

    df["as_of_date"] = pd.to_datetime(df["as_of_date"]).dt.date
    required_cols = {
        "market_id",
        "sector",
        "soft_target_class",
        "lambda_value",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Input CSV missing required columns: {sorted(missing)}")

    return df


def _prepare_next_lambda(df: pd.DataFrame) -> pd.DataFrame:
    """Add lambda_next column aligned by cluster and date.

    For each cluster (market_id, sector, soft_target_class), we compute
    lambda_next(x, t) = lambda(x, t+1) and attach it to the row at t.
    Rows without a t+1 observation for the same cluster are dropped.
    """

    df_sorted = df.sort_values(
        ["market_id", "sector", "soft_target_class", "as_of_date"],
    ).copy()

    group_keys = ["market_id", "sector", "soft_target_class"]
    df_sorted["lambda_next"] = (
        df_sorted.groupby(group_keys)["lambda_value"].shift(-1)
    )

    valid = df_sorted.dropna(subset=["lambda_next"]).copy()
    # Ensure numeric dtype
    valid["lambda_value"] = valid["lambda_value"].astype(float)
    valid["lambda_next"] = valid["lambda_next"].astype(float)
    return valid


def _evaluate_baseline(
    df: pd.DataFrame,
    *,
    top_quantile: float = 0.2,
) -> BaselineEvalResult:
    """Evaluate simple baseline lambda_hat_{t+1} = lambda_t.

    Args:
        df: DataFrame with columns as_of_date, market_id, sector,
            soft_target_class, lambda_value, lambda_next.
        top_quantile: Fraction of clusters per date to treat as
            "predicted-high" lambda_t when computing uplift.
    """

    if df.empty:
        raise ValueError("No valid lambda/lambda_next pairs to evaluate")

    # Overall correlations across all cluster-date pairs
    pearson_corr = float(df["lambda_value"].corr(df["lambda_next"]))
    spearman_corr, _ = spearmanr(df["lambda_value"], df["lambda_next"])

    # Per-date uplift analysis: for each as_of_date, compare realised
    # lambda_next between the top quantile of lambda_t and the rest.
    uplifts: List[float] = []
    by_date = df.groupby("as_of_date")
    for as_of, g in by_date:
        if g.shape[0] < 5:
            # Too few clusters to say anything meaningful.
            continue

        cutoff = g["lambda_value"].quantile(1.0 - top_quantile)
        high = g[g["lambda_value"] >= cutoff]
        low = g[g["lambda_value"] < cutoff]
        if high.empty or low.empty:
            continue

        uplift = float(high["lambda_next"].mean() - low["lambda_next"].mean())
        uplifts.append(uplift)

    if not uplifts:
        avg_uplift = float("nan")
        uplift_std_err = float("nan")
        num_days_used = 0
    else:
        uplifts_arr = np.asarray(uplifts, dtype=float)
        avg_uplift = float(uplifts_arr.mean())
        uplift_std_err = float(uplifts_arr.std(ddof=1) / np.sqrt(len(uplifts_arr)))
        num_days_used = len(uplifts)

    return BaselineEvalResult(
        pearson_corr=pearson_corr,
        spearman_corr=float(spearman_corr),
        avg_uplift=avg_uplift,
        uplift_std_err=uplift_std_err,
        num_days_used=num_days_used,
    )


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate a simple one-step-ahead baseline for opportunity-"
            "density (lambda_t) using a backfilled CSV."
        ),
    )

    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to lambda_t(x) CSV produced by backfill_opportunity_density.py",
    )
    parser.add_argument(
        "--top-quantile",
        type=float,
        default=0.2,
        help=(
            "Top quantile (0-1) of lambda_t per date to treat as predicted-"
            "high when computing uplift (default: 0.2)."
        ),
    )

    args = parser.parse_args(argv)

    csv_path = Path(args.input)
    if not csv_path.exists():
        raise SystemExit(f"Input CSV not found: {csv_path}")

    if not (0.0 < args.top_quantile < 1.0):
        raise SystemExit("--top-quantile must be between 0 and 1")

    logger.info("Loading lambda_t(x) from %s", csv_path)
    df = _load_lambda_csv(csv_path)

    logger.info("Preparing lambda_next and evaluating baseline forecast")
    df_pairs = _prepare_next_lambda(df)
    result = _evaluate_baseline(df_pairs, top_quantile=args.top_quantile)

    print("Baseline lambda_hat_{t+1} = lambda_t evaluation")
    print("------------------------------------------------")
    print(f"Input file        : {csv_path}")
    print(f"Num pairs         : {df_pairs.shape[0]}")
    print(f"Num days (uplift) : {result.num_days_used}")
    print()
    print(f"Pearson corr(lambda_t, lambda_{'{'}t+1{'}'})   : {result.pearson_corr:.4f}")
    print(f"Spearman corr(lambda_t, lambda_{'{'}t+1{'}'})  : {result.spearman_corr:.4f}")
    print()
    print(
        "Avg uplift in next-day lambda for top-quantile vs rest: "
        f"{result.avg_uplift:.6f} (std err {result.uplift_std_err:.6f})"
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
