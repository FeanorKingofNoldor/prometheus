"""Run simple opportunity-density (lambda) experiments with train/test splits.

This script is an experiment harness for lambda_t(x) (opportunity density)
backfilled by ``backfill_opportunity_density.py``. It allows you to run
simple models and evaluate them on a train/test split using the same
metrics as the baseline evaluator:

- Pearson and Spearman correlations between lambda_hat and realised
  lambda_{t+1}.
- Average uplift in realised lambda_{t+1} for top-quantile vs rest
  clusters (per date) based on lambda_hat.

Models supported in this iteration:

- ``persistence``: lambda_hat_{t+1} = lambda_t (baseline).
- ``cluster_mean``: lambda_hat_{t+1}(x) = mean_train lambda_{t+1}(x)
  per cluster, computed on the train window.
- ``global_ar1``: lambda_hat_{t+1} = a + b * lambda_t (global AR(1)).
- ``global_linear_full``: global linear regression over multiple
  numeric features available in the lambda CSV (e.g. lambda_t,
  num_instruments, dispersion, avg_vol_window).

The script writes a single CSV row with experiment metadata and metrics,
so you can append many experiment runs into one results file on the new
server. Optionally, it can also dump per-row predictions for the test
window to a separate CSV for deeper analysis.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import numpy as np
import pandas as pd

# scipy is optional; tests import helpers from this module in lightweight
# environments where scipy may not be installed.
try:  # pragma: no cover - optional dependency
    from scipy.stats import spearmanr  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    spearmanr = None

from apathis.core.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class LambdaExperimentResult:
    experiment_id: str
    model: str
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    top_quantile: float
    num_pairs_train: int
    num_pairs_test: int
    num_days_uplift: int
    pearson_corr: float
    spearman_corr: float
    avg_uplift: float
    uplift_std_err: float


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


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
    """Add lambda_next and simple dynamics features aligned by cluster/date.

    For each cluster (market_id, sector, soft_target_class), compute
    lambda_next(x, t) = lambda(x, t+1) and attach it to the row at t.
    Rows without a t+1 observation for the same cluster are dropped.

    In addition, compute:
    - lambda_prev: lambda(x, t-1) aligned to t.
    - lambda_trend: lambda(x, t) - lambda_prev(x, t).
    These are optional numeric features used by richer models; missing
    values are later treated as zeros by the feature builder.
    """

    df_sorted = df.sort_values(
        ["market_id", "sector", "soft_target_class", "as_of_date"],
    ).copy()

    group_keys = ["market_id", "sector", "soft_target_class"]
    # Next-step target
    df_sorted["lambda_next"] = df_sorted.groupby(group_keys)["lambda_value"].shift(-1)
    # Previous-step context and simple trend
    df_sorted["lambda_prev"] = df_sorted.groupby(group_keys)["lambda_value"].shift(1)
    df_sorted["lambda_trend"] = df_sorted["lambda_value"] - df_sorted["lambda_prev"]

    valid = df_sorted.dropna(subset=["lambda_next"]).copy()
    # Ensure numeric dtype for core target and optional dynamics features.
    valid["lambda_value"] = valid["lambda_value"].astype(float)
    valid["lambda_next"] = valid["lambda_next"].astype(float)
    # lambda_prev / lambda_trend may still contain NaNs for the first
    # observation in each cluster; these are handled downstream by
    # _build_feature_matrix via np.nan_to_num.
    return valid


def _train_test_split(
    df_pairs: pd.DataFrame,
    *,
    train_start: date,
    train_end: date,
    test_start: date,
    test_end: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split lambda pairs into train and test windows by as_of_date."""

    mask_train = (df_pairs["as_of_date"] >= train_start) & (df_pairs["as_of_date"] <= train_end)
    mask_test = (df_pairs["as_of_date"] >= test_start) & (df_pairs["as_of_date"] <= test_end)

    df_train = df_pairs.loc[mask_train].copy()
    df_test = df_pairs.loc[mask_test].copy()

    if df_train.empty:
        raise ValueError("Train window produced no lambda pairs; adjust dates")
    if df_test.empty:
        raise ValueError("Test window produced no lambda pairs; adjust dates")

    return df_train, df_test


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


_NUMERIC_FEATURE_COLS: list[str] = [
    "lambda_value",
    # Simple one-step dynamics features engineered in _prepare_next_lambda.
    # These give the models access to local momentum and level context.
    "lambda_prev",
    "lambda_trend",
    # Cluster composition and cross-sectional structure.
    "num_instruments",
    "dispersion",
    "avg_vol_window",
    # Optional state-aware features. These columns may or may not be
    # present in the input CSV; when absent, they are treated as zeros.
    # ``regime_risk_score`` is produced by
    # ``backfill_regime_change_risk.py`` and represents the probability
    # of being in a stressed (CRISIS or RISK_OFF) regime at a given
    # horizon.
    "regime_risk_score",
    # Optional STAB state-change risk features. These can be joined onto
    # the lambda_t(x) CSV by date (or cluster) using outputs from
    # ``backfill_stability_change_risk.py`` or related aggregation
    # scripts. When absent, they are treated as zeros.
    "stab_risk_score",
    "stab_p_worsen_any",
]


def _build_feature_matrix(df: pd.DataFrame, feature_cols: Iterable[str]) -> np.ndarray:
    """Return feature matrix X for the given columns.

    Missing columns are filled with zeros so the interface is robust to
    older lambda CSVs that may not contain all engineered features.

    Any NaN values in the available columns are also treated as zeros to
    avoid propagating NaNs into the linear models.
    """

    n_rows = df.shape[0]
    if n_rows == 0:
        return np.zeros((0, 0), dtype=float)

    cols: list[np.ndarray] = []
    for col in feature_cols:
        if col in df.columns:
            vals = df[col].to_numpy(dtype=float)
            # Replace NaNs with zeros so that missing values in optional
            # features (e.g. regime_risk_score on dates without regime
            # history) do not break the regressions.
            vals = np.nan_to_num(vals, nan=0.0)
        else:
            vals = np.zeros(n_rows, dtype=float)
        cols.append(vals)

    return np.vstack(cols).T


def _predict_persistence(df: pd.DataFrame) -> np.ndarray:
    """Baseline: lambda_hat_{t+1} = lambda_t."""

    return df["lambda_value"].to_numpy(dtype=float)


def _predict_cluster_mean(df_train: pd.DataFrame, df_test: pd.DataFrame) -> np.ndarray:
    """Cluster-mean model: lambda_hat_{t+1}(x) = mean_train lambda_{t+1}(x).

    If a cluster has no training data, fall back to the global mean of
    lambda_next in the training set.
    """

    group_keys = ["market_id", "sector", "soft_target_class"]

    train_means = (
        df_train.groupby(group_keys)["lambda_next"].mean().rename("lambda_next_mean")
    )
    global_mean = float(df_train["lambda_next"].mean())

    df_test_keyed = df_test.set_index(group_keys, drop=False)
    # Align cluster means with test clusters
    means_aligned = train_means.reindex(df_test_keyed.index)

    preds = means_aligned.to_numpy(dtype=float)
    # Where we have no cluster mean (NaN), use global mean
    mask_nan = np.isnan(preds)
    if np.any(mask_nan):
        preds[mask_nan] = global_mean

    return preds


def _predict_global_ar1(df_train: pd.DataFrame, df_test: pd.DataFrame) -> np.ndarray:
    """Global AR(1) model: lambda_hat_{t+1} = a + b * lambda_t.

    The parameters (a, b) are estimated on the entire training set via
    ordinary least squares, pooling all clusters together.
    """

    x_train = df_train["lambda_value"].to_numpy(dtype=float)
    y_train = df_train["lambda_next"].to_numpy(dtype=float)

    if x_train.size == 0:
        raise ValueError("Training data empty in _predict_global_ar1")

    # Design matrix with intercept term
    X = np.vstack([np.ones_like(x_train), x_train]).T
    coef, _, _, _ = np.linalg.lstsq(X, y_train, rcond=None)
    a, b = coef

    x_test = df_test["lambda_value"].to_numpy(dtype=float)
    return a + b * x_test


def _predict_global_linear_full(
    df_train: pd.DataFrame,
    df_test: pd.DataFrame,
    feature_cols: Iterable[str] | None = None,
) -> np.ndarray:
    """Global linear regression over multiple numeric features.

    This model fits lambda_next ~ a + w^T * x, where x contains the
    numeric feature columns from ``feature_cols`` (defaulting to
    ``_NUMERIC_FEATURE_COLS``) and an intercept term. All clusters are
    pooled together when estimating the coefficients.
    """

    if feature_cols is None:
        feature_cols = _NUMERIC_FEATURE_COLS

    y_train = df_train["lambda_next"].to_numpy(dtype=float)
    if y_train.size == 0:
        raise ValueError("Training data empty in _predict_global_linear_full")

    X_train = _build_feature_matrix(df_train, feature_cols)
    if X_train.shape[0] == 0:
        raise ValueError("No rows in training feature matrix for _predict_global_linear_full")

    # Add intercept column
    ones = np.ones((X_train.shape[0], 1), dtype=float)
    X_design = np.hstack([ones, X_train])
    coef, _, _, _ = np.linalg.lstsq(X_design, y_train, rcond=None)

    # Split intercept and feature weights
    a = float(coef[0])
    w = coef[1:]

    X_test = _build_feature_matrix(df_test, feature_cols)
    if X_test.shape[1] != w.shape[0]:
        raise ValueError("Feature dimension mismatch between train and test in global_linear_full")

    return a + X_test @ w


def _predict_global_poly2(
    df_train: pd.DataFrame,
    df_test: pd.DataFrame,
    feature_cols: Iterable[str] | None = None,
) -> np.ndarray:
    """Global quadratic model over numeric features.

    This model applies a simple second-order polynomial expansion to the
    feature matrix before fitting a linear model:

        lambda_next ~ a + w1^T * x + w2^T * (x^2)

    where x is built from ``feature_cols``. This provides a lightweight
    non-linear baseline without introducing external ML dependencies.
    """

    if feature_cols is None:
        feature_cols = _NUMERIC_FEATURE_COLS

    y_train = df_train["lambda_next"].to_numpy(dtype=float)
    if y_train.size == 0:
        raise ValueError("Training data empty in _predict_global_poly2")

    X_base_train = _build_feature_matrix(df_train, feature_cols)
    if X_base_train.shape[0] == 0:
        raise ValueError("No rows in training feature matrix for _predict_global_poly2")

    # Build [x, x^2] feature block.
    X_sq_train = X_base_train ** 2
    X_train = np.hstack([X_base_train, X_sq_train])

    ones = np.ones((X_train.shape[0], 1), dtype=float)
    X_design = np.hstack([ones, X_train])
    coef, _, _, _ = np.linalg.lstsq(X_design, y_train, rcond=None)

    a = float(coef[0])
    w = coef[1:]

    X_base_test = _build_feature_matrix(df_test, feature_cols)
    if X_base_test.shape[0] == 0:
        raise ValueError("No rows in test feature matrix for _predict_global_poly2")

    X_sq_test = X_base_test ** 2
    X_test = np.hstack([X_base_test, X_sq_test])

    if X_test.shape[1] != w.shape[0]:
        raise ValueError("Feature dimension mismatch between train and test in global_poly2")

    return a + X_test @ w


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


def _spearman_corr(x: np.ndarray, y: np.ndarray) -> float:
    """Return Spearman correlation between x and y without requiring scipy."""

    if x.size < 2 or y.size < 2:
        return float("nan")

    if spearmanr is not None:
        corr, _p = spearmanr(x, y)
        return float(corr)

    # Fallback: Pearson correlation of average ranks.
    rx = pd.Series(x).rank(method="average").to_numpy(dtype=float)
    ry = pd.Series(y).rank(method="average").to_numpy(dtype=float)

    corr = float(np.corrcoef(rx, ry)[0, 1])
    return corr


def _evaluate_predictions(
    df_test: pd.DataFrame,
    y_hat: np.ndarray,
    *,
    top_quantile: float,
) -> tuple[float, float, float, float, int]:
    """Compute correlations and uplift metrics for predictions on df_test.

    Returns (pearson_corr, spearman_corr, avg_uplift, uplift_std_err,
    num_days_uplift).
    """

    if df_test.empty:
        raise ValueError("df_test is empty in _evaluate_predictions")

    if y_hat.shape[0] != df_test.shape[0]:
        raise ValueError("Prediction vector length does not match df_test rows")

    df_eval = df_test.copy()
    df_eval["lambda_hat"] = y_hat.astype(float)

    # Correlations between predicted and realised next lambda
    pearson_corr = float(df_eval["lambda_hat"].corr(df_eval["lambda_next"]))
    spearman_corr = _spearman_corr(
        df_eval["lambda_hat"].to_numpy(dtype=float),
        df_eval["lambda_next"].to_numpy(dtype=float),
    )

    # Uplift: per date, compare realised lambda_next for top quantile of
    # lambda_hat vs the rest.
    uplifts: List[float] = []
    by_date = df_eval.groupby("as_of_date")
    for as_of, g in by_date:
        if g.shape[0] < 5:
            continue
        cutoff = g["lambda_hat"].quantile(1.0 - top_quantile)
        high = g[g["lambda_hat"] >= cutoff]
        low = g[g["lambda_hat"] < cutoff]
        if high.empty or low.empty:
            continue
        uplift = float(high["lambda_next"].mean() - low["lambda_next"].mean())
        uplifts.append(uplift)

    if not uplifts:
        avg_uplift = float("nan")
        uplift_std_err = float("nan")
        num_days = 0
    else:
        uplifts_arr = np.asarray(uplifts, dtype=float)
        avg_uplift = float(uplifts_arr.mean())
        uplift_std_err = float(uplifts_arr.std(ddof=1) / np.sqrt(len(uplifts_arr)))
        num_days = len(uplifts_arr)

    return pearson_corr, float(spearman_corr), avg_uplift, uplift_std_err, num_days


# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run a lambda opportunity-density experiment with a simple model "
            "and train/test split, writing a single CSV row of metrics."
        ),
    )

    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to lambda_t(x) CSV (from backfill_opportunity_density.py)",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Path to append experiment results CSV (will be created if missing)",
    )
    parser.add_argument(
        "--experiment-id",
        type=str,
        required=True,
        help="Logical experiment identifier to record in the results CSV",
    )
    parser.add_argument(
        "--model",
        type=str,
        choices=[
            "persistence",
            "cluster_mean",
            "global_ar1",
            "global_linear_full",
            "global_poly2",
        ],
        required=True,
        help=(
            "Which model to run (persistence, cluster_mean, global_ar1, "
            "global_linear_full, or global_poly2 for a quadratic feature baseline)"
        ),
    )
    parser.add_argument(
        "--train-start",
        type=_parse_date,
        required=True,
        help="Train window start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--train-end",
        type=_parse_date,
        required=True,
        help="Train window end date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--test-start",
        type=_parse_date,
        required=True,
        help="Test window start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--test-end",
        type=_parse_date,
        required=True,
        help="Test window end date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--top-quantile",
        type=float,
        default=0.2,
        help=(
            "Top quantile (0-1) of predicted lambda per date to treat as "
            "high when computing uplift (default: 0.2)."
        ),
    )
    parser.add_argument(
        "--predictions-output",
        type=str,
        default=None,
        help=(
            "Optional path to write per-row test predictions. If provided, "
            "the script appends test rows with a lambda_hat column and "
            "experiment_id."
        ),
    )
    parser.add_argument(
        "--regime-risk-csv",
        type=str,
        default=None,
        help=(
            "Optional CSV with regime state-change risk series (as produced by "
            "backfill_regime_change_risk.py). When provided, the script will "
            "join a 'regime_risk_score' column onto the lambda_t(x) data by "
            "as_of_date and expose it as an additional numeric feature."
        ),
    )

    args = parser.parse_args(argv)

    csv_path = Path(args.input)
    out_path = Path(args.output)
    predictions_path = Path(args.predictions_output) if args.predictions_output else None

    if not csv_path.exists():
        raise SystemExit(f"Input CSV not found: {csv_path}")
    if not (0.0 < args.top_quantile < 1.0):
        raise SystemExit("--top-quantile must be between 0 and 1")

    logger.info("Loading lambda_t(x) from %s", csv_path)
    df = _load_lambda_csv(csv_path)

    # Optionally join regime state-change risk features by as_of_date.
    if args.regime_risk_csv is not None:
        risk_path = Path(args.regime_risk_csv)
        if not risk_path.exists():
            raise SystemExit(f"Regime risk CSV not found: {risk_path}")
        logger.info("Loading regime risk series from %s", risk_path)
        df_risk = pd.read_csv(risk_path)
        if "as_of_date" not in df_risk.columns:
            raise SystemExit("Regime risk CSV must contain an 'as_of_date' column")
        if "regime_risk_score" not in df_risk.columns:
            raise SystemExit(
                "Regime risk CSV must contain a 'regime_risk_score' column; "
                "use backfill_regime_change_risk.py to generate it."
            )
        df_risk["as_of_date"] = pd.to_datetime(df_risk["as_of_date"]).dt.date
        df = df.merge(df_risk[["as_of_date", "regime_risk_score"]], on="as_of_date", how="left")

    logger.info("Preparing lambda_next and splitting train/test")
    df_pairs = _prepare_next_lambda(df)

    df_train, df_test = _train_test_split(
        df_pairs,
        train_start=args.train_start,
        train_end=args.train_end,
        test_start=args.test_start,
        test_end=args.test_end,
    )

    logger.info(
        "Running model=%s on train [%s, %s] and test [%s, %s]",
        args.model,
        args.train_start,
        args.train_end,
        args.test_start,
        args.test_end,
    )

    if args.model == "persistence":
        y_hat = _predict_persistence(df_test)
    elif args.model == "cluster_mean":
        y_hat = _predict_cluster_mean(df_train, df_test)
    elif args.model == "global_ar1":
        y_hat = _predict_global_ar1(df_train, df_test)
    elif args.model == "global_linear_full":
        y_hat = _predict_global_linear_full(df_train, df_test)
    elif args.model == "global_poly2":
        y_hat = _predict_global_poly2(df_train, df_test)
    else:  # pragma: no cover - defensive
        raise SystemExit(f"Unsupported model: {args.model}")

    pearson_corr, spearman_corr, avg_uplift, uplift_std_err, num_days = _evaluate_predictions(
        df_test,
        y_hat,
        top_quantile=args.top_quantile,
    )

    result = LambdaExperimentResult(
        experiment_id=args.experiment_id,
        model=args.model,
        train_start=args.train_start,
        train_end=args.train_end,
        test_start=args.test_start,
        test_end=args.test_end,
        top_quantile=args.top_quantile,
        num_pairs_train=int(df_train.shape[0]),
        num_pairs_test=int(df_test.shape[0]),
        num_days_uplift=num_days,
        pearson_corr=pearson_corr,
        spearman_corr=spearman_corr,
        avg_uplift=avg_uplift,
        uplift_std_err=uplift_std_err,
    )

    # Append or create output CSV with aggregate experiment metrics.
    out_path.parent.mkdir(parents=True, exist_ok=True)

    row = {
        "experiment_id": result.experiment_id,
        "model": result.model,
        "train_start": result.train_start.isoformat(),
        "train_end": result.train_end.isoformat(),
        "test_start": result.test_start.isoformat(),
        "test_end": result.test_end.isoformat(),
        "top_quantile": result.top_quantile,
        "num_pairs_train": result.num_pairs_train,
        "num_pairs_test": result.num_pairs_test,
        "num_days_uplift": result.num_days_uplift,
        "pearson_corr": result.pearson_corr,
        "spearman_corr": result.spearman_corr,
        "avg_uplift": result.avg_uplift,
        "uplift_std_err": result.uplift_std_err,
    }

    if out_path.exists():
        df_out = pd.read_csv(out_path)
        df_out = pd.concat([df_out, pd.DataFrame([row])], ignore_index=True)
    else:
        df_out = pd.DataFrame([row])

    df_out.to_csv(out_path, index=False)

    # Optionally dump per-row test predictions.
    if predictions_path is not None:
        predictions_path.parent.mkdir(parents=True, exist_ok=True)
        df_pred = df_test.copy()
        df_pred["lambda_hat"] = y_hat.astype(float)
        df_pred["experiment_id"] = args.experiment_id
        if predictions_path.exists():
            df_pred_existing = pd.read_csv(predictions_path)
            df_pred_combined = pd.concat([df_pred_existing, df_pred], ignore_index=True)
        else:
            df_pred_combined = df_pred
        df_pred_combined.to_csv(predictions_path, index=False)

        logger.info(
            "Experiment %s predictions appended to %s", args.experiment_id, predictions_path
        )

    logger.info("Experiment %s complete; results appended to %s", args.experiment_id, out_path)


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
