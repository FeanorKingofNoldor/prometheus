"""Train a lambda prediction model (GBT) from historical data.

This script:
1. Loads cluster-level lambda from universe_members.reasons
2. Loads regime history for regime risk features
3. Engineers enhanced features (rolling stats, z-scores, etc.)
4. Runs walk-forward validation (expanding window, 1-month steps)
5. Trains a final model on all data
6. Saves model artifact + walk-forward predictions CSV

Usage::

    python -m prometheus.scripts.train.train_lambda_model \\
        --market US_EQ \\
        --output-model data/lambda_model_US_EQ.json \\
        --output-predictions data/lambda_predictions_US_EQ.csv

Author: Prometheus Team
Created: 2026-03-16
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd
from apatheon.core.config import get_config
from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger

from prometheus.opportunity.lambda_model import (
    LambdaGBTModel,
    build_enhanced_features,
    prepare_next_lambda,
)

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_cluster_lambda_from_db(
    db_manager: DatabaseManager,
    *,
    market_id: str = "US_EQ",
    min_date: date | None = None,
    max_date: date | None = None,
) -> pd.DataFrame:
    """Load cluster-level lambda time series from universe_members.

    Aggregates instrument-level features from the reasons JSON
    to cluster level, including STAB risk, volume, volatility,
    and assessment scores.
    """
    sql = """
        SELECT
            um.as_of_date,
            (um.reasons->>'cluster_id')::text AS cluster_id,
            count(*) AS num_instruments,
            avg((um.reasons->>'lambda_score')::numeric) AS lambda_value,
            stddev_samp((um.reasons->>'lambda_score')::numeric) AS dispersion,
            avg(COALESCE((um.reasons->>'stab_risk_score')::numeric, 0)) AS stab_risk_score,
            avg(COALESCE((um.reasons->>'stab_p_worsen_any')::numeric, 0)) AS stab_p_worsen_any,
            avg(COALESCE((um.reasons->>'stab_p_to_targetable_or_breaker')::numeric, 0))
                AS stab_p_to_targetable,
            count(*) FILTER (
                WHERE COALESCE((um.reasons->>'stab_risk_score')::numeric, 0) > 0.5
            )::float / GREATEST(count(*), 1) AS cluster_pct_high_risk,
            avg(COALESCE((um.reasons->>'realised_vol_63d')::numeric, 0))
                AS avg_realised_vol_63d,
            avg(COALESCE((um.reasons->>'avg_volume_63d')::numeric, 0))
                AS avg_volume_63d,
            avg(COALESCE((um.reasons->>'assessment_score')::numeric, 0))
                AS avg_assessment_score
        FROM universe_members um
        WHERE um.included = TRUE
          AND um.reasons ? 'cluster_id'
          AND um.reasons ? 'lambda_score'
    """
    params: list = []
    if min_date:
        sql += " AND um.as_of_date >= %s"
        params.append(min_date)
    if max_date:
        sql += " AND um.as_of_date <= %s"
        params.append(max_date)
    sql += """
        GROUP BY um.as_of_date, (um.reasons->>'cluster_id')
        ORDER BY um.as_of_date, (um.reasons->>'cluster_id')
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    if not rows:
        logger.warning("No cluster lambda data found in universe_members")
        return pd.DataFrame()

    df = pd.DataFrame(
        rows,
        columns=[
            "as_of_date", "cluster_id", "num_instruments",
            "lambda_value", "dispersion", "stab_risk_score",
            "stab_p_worsen_any", "stab_p_to_targetable",
            "cluster_pct_high_risk", "avg_realised_vol_63d",
            "avg_volume_63d", "avg_assessment_score",
        ],
    )

    # Convert types (DB returns Decimal).
    for col in df.columns:
        if col in ("as_of_date", "cluster_id"):
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["num_instruments"] = df["num_instruments"].fillna(0).astype(int)

    # Parse cluster_id → market_id, sector, soft_target_class.
    parts = df["cluster_id"].str.split("|", expand=True)
    if parts.shape[1] >= 3:
        df["market_id"] = parts[0]
        df["sector"] = parts[1]
        df["soft_target_class"] = parts[2]
    else:
        logger.warning("Unexpected cluster_id format; cannot parse")
        return pd.DataFrame()

    if market_id:
        df = df[df["market_id"] == market_id].copy()

    logger.info(
        "Loaded %d cluster-date rows (%d dates, %d clusters) for %s",
        len(df),
        df["as_of_date"].nunique(),
        df["cluster_id"].nunique(),
        market_id,
    )
    return df


def load_regime_data(
    db_manager: DatabaseManager,
    region: str = "US",
) -> pd.DataFrame:
    """Load regime time series with both label and numeric risk score."""
    sql = """
        SELECT as_of_date, regime_label
        FROM regimes
        WHERE region = %s
        ORDER BY as_of_date
    """
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (region,))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    if not rows:
        return pd.DataFrame(columns=["as_of_date", "regime_label", "regime_risk_score"])

    df = pd.DataFrame(rows, columns=["as_of_date", "regime_label"])

    risk_map = {
        "CRISIS": 1.0,
        "RISK_OFF": 0.8,
        "NEUTRAL": 0.3,
        "RECOVERY": 0.3,
        "RISK_ON": 0.2,
        "CARRY": 0.0,
    }
    df["regime_risk_score"] = df["regime_label"].map(risk_map).fillna(0.3)

    logger.info("Loaded %d regime observations for %s", len(df), region)
    return df[["as_of_date", "regime_label", "regime_risk_score"]]


# ---------------------------------------------------------------------------
# Walk-forward validation
# ---------------------------------------------------------------------------


def _compute_metrics(
    predicted: np.ndarray,
    actual: np.ndarray,
    current: np.ndarray,
) -> Dict[str, float]:
    """Compute prediction quality metrics."""
    from scipy.stats import spearmanr

    n = len(predicted)
    if n == 0:
        return {}

    errors = predicted - actual
    mae = float(np.mean(np.abs(errors)))
    rmse = float(np.sqrt(np.mean(errors ** 2)))

    ss_res = float(np.sum(errors ** 2))
    ss_tot = float(np.sum((actual - np.mean(actual)) ** 2))
    r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

    pred_dir = np.sign(predicted - current)
    actual_dir = np.sign(actual - current)
    direction_accuracy = float(np.mean(pred_dir == actual_dir))

    rho, _ = spearmanr(predicted, actual)

    return {
        "n": n,
        "mae": mae,
        "rmse": rmse,
        "r_squared": r_squared,
        "direction_accuracy": direction_accuracy,
        "spearman_rho": float(rho),
    }


def walk_forward_validate(
    df_pairs: pd.DataFrame,
    *,
    min_train_dates: int = 60,
    step_days: int = 14,
    target_mode: str = "change",
) -> Tuple[pd.DataFrame, Dict[str, float], List[Dict[str, float]]]:
    """Run walk-forward validation with expanding window and 2-week steps.

    Returns:
        (oos_predictions_df, aggregate_metrics, per_fold_metrics_list)
    """
    dates = sorted(df_pairs["as_of_date"].unique())
    n_dates = len(dates)

    if n_dates < min_train_dates + 10:
        logger.warning(
            "Only %d dates; need %d + 10 for walk-forward", n_dates, min_train_dates,
        )
        return pd.DataFrame(), {}, []

    first_cutoff = dates[min_train_dates - 1]

    # Build fold boundaries with step_days increments.
    folds: List[Tuple[date, date]] = []
    cutoff = first_cutoff
    while True:
        test_end = cutoff + timedelta(days=step_days)
        test_dates = [d for d in dates if cutoff < d <= test_end]
        if not test_dates:
            # Try extending to find the next available date.
            remaining = [d for d in dates if d > cutoff]
            if not remaining:
                break
            test_end = remaining[min(len(remaining) - 1, 5)]
            test_dates = [d for d in dates if cutoff < d <= test_end]
            if not test_dates:
                break
        folds.append((cutoff, test_end))
        cutoff = test_end
        if cutoff >= dates[-2]:
            break

    logger.info(
        "Walk-forward: %d folds, step=%dd, first_cutoff=%s, last_test=%s",
        len(folds), step_days, first_cutoff,
        folds[-1][1] if folds else "N/A",
    )

    all_oos: List[pd.DataFrame] = []
    fold_metrics: List[Dict[str, float]] = []

    for fold_idx, (train_end, test_end) in enumerate(folds):
        df_train = df_pairs[df_pairs["as_of_date"] <= train_end].copy()
        df_test = df_pairs[
            (df_pairs["as_of_date"] > train_end)
            & (df_pairs["as_of_date"] <= test_end)
        ].copy()

        if df_train.empty or df_test.empty:
            continue

        model = LambdaGBTModel(
            experiment_id=f"wf_fold_{fold_idx}",
            target_mode=target_mode,
            max_depth=3,
            min_samples_leaf=40,
            l2_regularization=3.0,
            learning_rate=0.03,
            max_iter=500,
        )
        model.train(df_train)
        preds = model.predict(df_test)

        df_test = df_test.copy()
        df_test["lambda_hat"] = preds.astype(float)

        # Per-fold metrics.
        fm = _compute_metrics(
            preds,
            df_test["lambda_next"].to_numpy(dtype=float),
            df_test["lambda_value"].to_numpy(dtype=float),
        )
        fm["fold"] = fold_idx
        fm["train_end"] = str(train_end)
        fm["test_end"] = str(test_end)
        fm["train_rows"] = len(df_train)

        # Momentum baseline: predict same direction as last observed change.
        momentum_pred = (
            df_test["lambda_value"].to_numpy(dtype=float)
            + df_test["lambda_trend"].fillna(0).to_numpy(dtype=float)
        )
        mm = _compute_metrics(
            momentum_pred,
            df_test["lambda_next"].to_numpy(dtype=float),
            df_test["lambda_value"].to_numpy(dtype=float),
        )
        fm["baseline_dir_acc"] = mm.get("direction_accuracy", 0.0)
        fm["lift_vs_baseline"] = fm.get("direction_accuracy", 0) - fm["baseline_dir_acc"]

        fold_metrics.append(fm)
        all_oos.append(df_test)

    if not all_oos:
        return pd.DataFrame(), {}, []

    df_oos = pd.concat(all_oos, ignore_index=True)

    # Aggregate metrics.
    agg = _compute_metrics(
        df_oos["lambda_hat"].to_numpy(dtype=float),
        df_oos["lambda_next"].to_numpy(dtype=float),
        df_oos["lambda_value"].to_numpy(dtype=float),
    )
    agg["n_folds"] = len(folds)

    # Momentum baseline on full OOS: predict same direction as last change.
    momentum_full = (
        df_oos["lambda_value"].to_numpy(dtype=float)
        + df_oos["lambda_trend"].fillna(0).to_numpy(dtype=float)
    )
    mm_full = _compute_metrics(
        momentum_full,
        df_oos["lambda_next"].to_numpy(dtype=float),
        df_oos["lambda_value"].to_numpy(dtype=float),
    )
    agg["baseline_dir_acc"] = mm_full.get("direction_accuracy", 0.0)
    agg["lift_vs_baseline"] = agg["direction_accuracy"] - agg["baseline_dir_acc"]

    return df_oos, agg, fold_metrics


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Train a lambda GBT prediction model with walk-forward validation.",
    )
    parser.add_argument(
        "--market", type=str, default="US_EQ",
        help="Market ID (default: US_EQ)",
    )
    parser.add_argument(
        "--region", type=str, default="US",
        help="Regime region (default: US)",
    )
    parser.add_argument(
        "--output-model", type=str, default=None,
        help="Path for model JSON (default: data/lambda_model_{market}.json)",
    )
    parser.add_argument(
        "--output-predictions", type=str, default=None,
        help="Path for predictions CSV (default: data/lambda_predictions_{market}.csv)",
    )
    parser.add_argument(
        "--experiment-id", type=str, default="US_EQ_GBT_V1",
        help="Experiment ID to tag predictions",
    )
    parser.add_argument(
        "--min-train-dates", type=int, default=60,
        help="Minimum training dates before first WF fold (default: 60)",
    )
    parser.add_argument(
        "--step-days", type=int, default=14,
        help="Walk-forward step size in days (default: 14)",
    )

    args = parser.parse_args(argv)
    market_id = args.market

    # Resolve output paths.
    model_path = Path(
        args.output_model
        or str(PROJECT_ROOT / f"data/lambda_model_{market_id}.json")
    )
    predictions_path = Path(
        args.output_predictions
        or str(PROJECT_ROOT / f"data/lambda_predictions_{market_id}.csv")
    )

    # Connect to DB.
    config = get_config()
    db_manager = DatabaseManager(config)

    # Step 1: Load data.
    print(f"\n{'='*60}")
    print(f"  Lambda GBT Model Training — {market_id}")
    print(f"{'='*60}\n")

    print("Loading cluster lambda from universe_members...")
    df_raw = load_cluster_lambda_from_db(db_manager, market_id=market_id)
    if df_raw.empty:
        print("ERROR: No lambda data found. Run the pipeline first.")
        return

    print(f"  → {len(df_raw)} cluster-date rows, "
          f"{df_raw['as_of_date'].nunique()} dates, "
          f"{df_raw['cluster_id'].nunique()} clusters")

    print("Loading regime history...")
    df_regime = load_regime_data(db_manager, region=args.region)
    print(f"  → {len(df_regime)} regime observations")

    # Join regime label + risk score.
    if not df_regime.empty:
        df_raw = df_raw.merge(df_regime, on="as_of_date", how="left")
        df_raw["regime_risk_score"] = df_raw["regime_risk_score"].fillna(0.3)
        df_raw["regime_label"] = df_raw["regime_label"].fillna("NEUTRAL")

    # Step 2: Feature engineering.
    print("Engineering features...")

    # avg_vol_window not available from DB aggregate; leave as NaN (GBT handles it).
    if "avg_vol_window" not in df_raw.columns:
        df_raw["avg_vol_window"] = np.nan

    # Prepare lambda_next target + lag features.
    df_pairs = prepare_next_lambda(df_raw)
    print(f"  → {len(df_pairs)} rows with lambda_next target")

    # Enhanced features.
    df_pairs = build_enhanced_features(df_pairs)
    print(f"  → {len(df_pairs.columns)} total columns after feature engineering")

    # Step 3: Walk-forward validation.
    print(f"\nRunning walk-forward validation "
          f"(min_train={args.min_train_dates}, step={args.step_days}d)...")
    df_oos, metrics, fold_metrics = walk_forward_validate(
        df_pairs,
        min_train_dates=args.min_train_dates,
        step_days=args.step_days,
        target_mode="change",
    )

    if metrics:
        n_pred = metrics.get('n', 0)
        print(f"\n  Walk-Forward Results ({metrics['n_folds']} folds, {n_pred} OOS predictions):")
        print(f"  {'GBT Direction Acc':>22}: {metrics['direction_accuracy']:.4f}")
        print(f"  {'Momentum Baseline':>22}: {metrics['baseline_dir_acc']:.4f}")
        print(f"  {'Lift vs Baseline':>22}: {metrics['lift_vs_baseline']:+.4f}")
        print(f"  {'MAE':>22}: {metrics['mae']:.6f}")
        print(f"  {'RMSE':>22}: {metrics['rmse']:.6f}")
        print(f"  {'R²':>22}: {metrics['r_squared']:.4f}")
        print(f"  {'Spearman ρ':>22}: {metrics['spearman_rho']:.4f}")

        # Per-fold breakdown.
        if fold_metrics:
            print(f"\n  Per-Fold Breakdown ({len(fold_metrics)} folds):")
            print(f"  {'Fold':>4} {'Train→':>12} {'Test→':>12} {'DirAcc':>7} {'Base':>7} {'Lift':>7} {'N':>5}")
            for fm in fold_metrics:
                print(
                    f"  {fm['fold']:>4} {fm['train_end']:>12} {fm['test_end']:>12} "
                    f"{fm['direction_accuracy']:>7.4f} {fm['baseline_dir_acc']:>7.4f} "
                    f"{fm['lift_vs_baseline']:>+7.4f} {fm['n']:>5}"
                )
    else:
        print("  WARNING: Walk-forward validation produced no results")

    # Step 4: Train final model on all data.
    print("\nTraining final model on full dataset...")
    final_model = LambdaGBTModel(
        experiment_id=args.experiment_id,
        target_mode="change",
        max_depth=3,
        min_samples_leaf=40,
        l2_regularization=3.0,
        learning_rate=0.03,
        max_iter=500,
    )
    final_model.train(df_pairs)

    # Step 5: Save outputs.
    print(f"\nSaving model to {model_path}")
    final_model.save(model_path)

    # Save walk-forward predictions CSV for scorecard consumption.
    if not df_oos.empty:
        df_oos["experiment_id"] = args.experiment_id

        # Also add final model's in-sample predictions for the most recent
        # data that wasn't covered by walk-forward (last fold's training window).
        # The scorecard will use this CSV.
        predictions_path.parent.mkdir(parents=True, exist_ok=True)

        # Keep only the columns expected by the scorecard/provider.
        keep_cols = [
            "as_of_date", "market_id", "sector", "soft_target_class",
            "lambda_value", "lambda_hat", "experiment_id",
        ]
        # Add optional columns if present.
        for extra in ["num_instruments", "dispersion", "avg_vol_window", "lambda_next"]:
            if extra in df_oos.columns:
                keep_cols.append(extra)

        df_save = df_oos[[c for c in keep_cols if c in df_oos.columns]].copy()
        df_save.to_csv(predictions_path, index=False)
        print(f"Saved {len(df_save)} walk-forward predictions to {predictions_path}")
    else:
        print("WARNING: No OOS predictions to save")

    # Step 6: Feature importance (permutation-based for HistGBT).
    if final_model._estimator is not None:
        try:
            from sklearn.inspection import permutation_importance
            X_final = final_model._build_X(df_pairs)
            y_final = (df_pairs["lambda_next"] - df_pairs["lambda_value"]).to_numpy(dtype=float)
            perm_result = permutation_importance(
                final_model._estimator, X_final, y_final,
                n_repeats=5, random_state=42, n_jobs=-1,
            )
            importances = perm_result.importances_mean
            feat_imp = sorted(
                zip(final_model.feature_cols, importances),
                key=lambda x: -x[1],
            )
            print("\n  Top Feature Importances (permutation):")
            for name, imp in feat_imp[:10]:
                bar = "█" * int(max(0, imp) * 500)
                print(f"    {name:>28}: {imp:.4f}  {bar}")
        except Exception as exc:
            print(f"\n  (Feature importance unavailable: {exc})")

    print(f"\n{'='*60}")
    print("  Training complete.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
