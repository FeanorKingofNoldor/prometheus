"""Prometheus v2 – Lambda Opportunity-Density Model.

Provides a trainable quadratic (poly2) model for predicting
lambda_hat_{t+1} from realised lambda_t(x) cluster features.

The model is a lightweight linear regression over a second-order
polynomial expansion of numeric features — no external ML dependencies.
Coefficients are JSON-serialisable for persistence and daily reuse.

Usage::

    from prometheus.opportunity.lambda_model import LambdaPoly2Model

    model = LambdaPoly2Model()
    model.train(df_pairs)            # fit on historical lambda pairs
    model.save("data/lambda_model_US_EQ.json")

    model2 = LambdaPoly2Model.load("data/lambda_model_US_EQ.json")
    preds = model2.predict(df_today)  # predict lambda_hat for new rows

Key responsibilities:
- Train a global poly2 regression on lambda cluster pairs
- Predict lambda_hat for new cluster feature rows
- Persist and load model coefficients as JSON

External dependencies:
- numpy: linear algebra
- pandas: data frames

Thread safety: Not thread-safe (stateful coefficients)

Author: Prometheus Team
Created: 2026-03-11
Last Modified: 2026-03-11
Status: Development
Version: v0.1.0
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

import joblib
import numpy as np
import pandas as pd
from apathis.core.logging import get_logger
from sklearn.ensemble import HistGradientBoostingRegressor

logger = get_logger(__name__)


# ============================================================================
# Shared Feature Definitions
# ============================================================================

NUMERIC_FEATURE_COLS: List[str] = [
    "lambda_value",
    "lambda_prev",
    "lambda_trend",
    "num_instruments",
    "dispersion",
    "avg_vol_window",
    "regime_risk_score",
    "stab_risk_score",
    "stab_p_worsen_any",
    "sector_health_score",
]

# Extended features for the GBT model — includes rolling stats, z-scores,
# cross-cluster context, regime, STAB, and calendar effects.
GBT_FEATURE_COLS: List[str] = [
    # Core lambda + deeper lags
    "lambda_value",
    "lambda_prev",
    "lambda_trend",
    "lambda_lag2",
    "lambda_lag3",
    "lambda_lag5",
    "lambda_accel",
    "lambda_trend_lag2",
    "lambda_trend_lag3",
    "lambda_trend_lag5",
    # Cluster composition
    "num_instruments",
    "dispersion",
    "avg_vol_window",
    # Rolling statistics (per cluster)
    "lambda_roll_mean_5",
    "lambda_roll_std_5",
    "lambda_roll_mean_21",
    "lambda_roll_std_21",
    # Derived signals
    "lambda_zscore_21",
    "lambda_mean_reversion_21",
    "lambda_rank_pct",
    # Cross-cluster context (market-level)
    "market_lambda_mean",
    "market_lambda_std",
    "market_lambda_range",
    # Regime (one-hot + dynamics)
    "regime_risk_score",
    "regime_CARRY",
    "regime_CRISIS",
    "regime_RISK_OFF",
    "regime_changed_5d",
    # STAB risk (cluster-level aggregates)
    "stab_risk_score",
    "stab_p_worsen_any",
    "stab_p_to_targetable",
    "cluster_pct_high_risk",
    # Volume / volatility (cluster-level)
    "avg_realised_vol_63d",
    "avg_volume_63d",
    # Assessment
    "avg_assessment_score",
    # Calendar
    "day_of_week",
    "month",
]


def build_feature_matrix(df: pd.DataFrame, feature_cols: Iterable[str] | None = None) -> np.ndarray:
    """Return feature matrix X for the given columns.

    Missing columns are filled with zeros so the interface is robust to
    older lambda CSVs that may not contain all engineered features.
    NaN values are replaced with zeros.

    Args:
        df: Input DataFrame with cluster rows.
        feature_cols: Columns to use. Defaults to NUMERIC_FEATURE_COLS.

    Returns:
        2D numpy array of shape (n_rows, n_features).
    """
    if feature_cols is None:
        feature_cols = NUMERIC_FEATURE_COLS

    n_rows = df.shape[0]
    if n_rows == 0:
        return np.zeros((0, 0), dtype=float)

    cols: list[np.ndarray] = []
    for col in feature_cols:
        if col in df.columns:
            vals = df[col].to_numpy(dtype=float)
            vals = np.nan_to_num(vals, nan=0.0)
        else:
            vals = np.zeros(n_rows, dtype=float)
        cols.append(vals)

    return np.vstack(cols).T


def prepare_next_lambda(df: pd.DataFrame) -> pd.DataFrame:
    """Add lambda_next and dynamics features aligned by cluster/date.

    For each cluster (market_id, sector, soft_target_class), computes
    lambda_next(x, t) = lambda(x, t+1) and attaches it to the row at t.
    Rows without a t+1 observation are dropped.

    Also computes lambda_prev and lambda_trend.

    Args:
        df: Raw lambda observations with as_of_date and lambda_value.

    Returns:
        DataFrame with lambda_next, lambda_prev, lambda_trend columns.
        Rows without a valid lambda_next are dropped.
    """
    df_sorted = df.sort_values(
        ["market_id", "sector", "soft_target_class", "as_of_date"],
    ).copy()

    group_keys = ["market_id", "sector", "soft_target_class"]
    df_sorted["lambda_next"] = df_sorted.groupby(group_keys)["lambda_value"].shift(-1)
    df_sorted["lambda_prev"] = df_sorted.groupby(group_keys)["lambda_value"].shift(1)
    df_sorted["lambda_trend"] = df_sorted["lambda_value"] - df_sorted["lambda_prev"]

    valid = df_sorted.dropna(subset=["lambda_next"]).copy()
    valid["lambda_value"] = valid["lambda_value"].astype(float)
    valid["lambda_next"] = valid["lambda_next"].astype(float)
    return valid


# ============================================================================
# Enhanced Feature Engineering
# ============================================================================


def build_enhanced_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add rolling stats, deeper lags, regime encoding, STAB and calendar features.

    Expects columns:
    - as_of_date, market_id, sector, soft_target_class, lambda_value

    Optional columns used when present:
    - regime_label, regime_risk_score, stab_risk_score, stab_p_worsen_any,
      stab_p_to_targetable, cluster_pct_high_risk, avg_realised_vol_63d,
      avg_volume_63d, avg_assessment_score

    Missing base columns are left as NaN; HistGBT handles them natively.

    Args:
        df: DataFrame with cluster-level lambda observations.

    Returns:
        DataFrame with additional feature columns.
    """
    out = df.copy()
    out = out.sort_values(
        ["market_id", "sector", "soft_target_class", "as_of_date"],
    )

    group_keys = ["market_id", "sector", "soft_target_class"]
    grp = out.groupby(group_keys)["lambda_value"]

    # --- Deeper autoregressive lags ---
    out["lambda_lag2"] = grp.transform(lambda s: s.shift(2))
    out["lambda_lag3"] = grp.transform(lambda s: s.shift(3))
    out["lambda_lag5"] = grp.transform(lambda s: s.shift(5))

    # Lambda acceleration (second derivative of trend).
    if "lambda_trend" in out.columns:
        out["lambda_accel"] = grp.transform(
            lambda s: s.diff(),
        )  # diff of lambda_value; we want diff of trend
        # Recompute: accel = trend_t - trend_{t-1}
        trend = out["lambda_value"] - out.groupby(group_keys)["lambda_value"].shift(1)
        prev_trend = trend.groupby(out.groupby(group_keys).ngroup()).shift(1)
        out["lambda_accel"] = trend - prev_trend
    else:
        # Compute trend inline then acceleration
        prev = grp.transform(lambda s: s.shift(1))
        trend = out["lambda_value"] - prev
        prev_trend = trend.groupby(out.groupby(group_keys).ngroup()).shift(1)
        out["lambda_accel"] = trend - prev_trend

    # --- Trend lags (momentum persistence at different horizons) ---
    if "lambda_trend" in out.columns:
        trend_series = out["lambda_trend"]
    else:
        trend_series = out["lambda_value"] - grp.transform(lambda s: s.shift(1))
    grp_idx = out.groupby(group_keys).ngroup()
    out["lambda_trend_lag2"] = trend_series.groupby(grp_idx).shift(2)
    out["lambda_trend_lag3"] = trend_series.groupby(grp_idx).shift(3)
    out["lambda_trend_lag5"] = trend_series.groupby(grp_idx).shift(5)

    # --- Rolling statistics per cluster ---
    out["lambda_roll_mean_5"] = grp.transform(
        lambda s: s.rolling(5, min_periods=2).mean(),
    )
    out["lambda_roll_std_5"] = grp.transform(
        lambda s: s.rolling(5, min_periods=2).std(),
    )
    out["lambda_roll_mean_21"] = grp.transform(
        lambda s: s.rolling(21, min_periods=5).mean(),
    )
    out["lambda_roll_std_21"] = grp.transform(
        lambda s: s.rolling(21, min_periods=5).std(),
    )

    # Z-score: how far current lambda is from its 21d mean.
    roll_std_safe = out["lambda_roll_std_21"].replace(0.0, np.nan)
    out["lambda_zscore_21"] = (
        (out["lambda_value"] - out["lambda_roll_mean_21"]) / roll_std_safe
    )

    # Mean reversion signal: positive = below mean (expect increase).
    out["lambda_mean_reversion_21"] = out["lambda_roll_mean_21"] - out["lambda_value"]

    # --- Cross-cluster rank (percentile within each date) ---
    out["lambda_rank_pct"] = out.groupby(["market_id", "as_of_date"])[
        "lambda_value"
    ].rank(pct=True)

    # --- Cross-cluster context: market-level stats on each date ---
    market_stats = (
        out.groupby(["market_id", "as_of_date"])["lambda_value"]
        .agg(["mean", "std", lambda x: x.max() - x.min()])
    )
    market_stats.columns = [
        "market_lambda_mean", "market_lambda_std", "market_lambda_range",
    ]
    market_stats = market_stats.reset_index()
    out = out.merge(market_stats, on=["market_id", "as_of_date"], how="left")

    # --- Regime one-hot encoding ---
    if "regime_label" in out.columns:
        for label in ["CARRY", "CRISIS", "RISK_OFF"]:
            out[f"regime_{label}"] = (out["regime_label"] == label).astype(float)

        # Regime changed in last 5 days.
        regime_changed = out.drop_duplicates(
            subset=["as_of_date", "market_id"],
        ).set_index("as_of_date")["regime_label"]
        regime_changed = regime_changed.ne(regime_changed.shift(1))
        regime_changed_5d = regime_changed.rolling(5, min_periods=1).max()
        regime_map = regime_changed_5d.to_dict()
        out["regime_changed_5d"] = out["as_of_date"].map(regime_map).fillna(0.0)
    else:
        for label in ["CARRY", "CRISIS", "RISK_OFF"]:
            out[f"regime_{label}"] = np.nan
        out["regime_changed_5d"] = np.nan

    # --- Calendar features ---
    if hasattr(out["as_of_date"].iloc[0], "weekday"):
        out["day_of_week"] = out["as_of_date"].apply(lambda d: d.weekday())
        out["month"] = out["as_of_date"].apply(lambda d: d.month)
    else:
        dates = pd.to_datetime(out["as_of_date"])
        out["day_of_week"] = dates.dt.weekday
        out["month"] = dates.dt.month

    return out


# ============================================================================
# Model Class: Poly2
# ============================================================================


@dataclass
class LambdaPoly2Model:
    """Global quadratic model for lambda_hat prediction.

    Fits lambda_next ~ a + w1^T * x + w2^T * (x^2) where x is built
    from ``feature_cols``.

    Attributes:
        intercept: Fitted intercept term.
        weights: Fitted coefficient vector for [x, x^2] features.
        feature_cols: Feature column names used during training.
        trained_at: ISO timestamp of last training.
        train_rows: Number of rows used in training.
        experiment_id: Logical experiment identifier.
    """

    intercept: float = 0.0
    weights: list[float] = field(default_factory=list)
    feature_cols: list[str] = field(default_factory=lambda: list(NUMERIC_FEATURE_COLS))
    trained_at: str = ""
    train_rows: int = 0
    experiment_id: str = ""

    @property
    def is_trained(self) -> bool:
        """Whether the model has been fitted."""
        return len(self.weights) > 0

    # ========================================================================
    # Training
    # ========================================================================

    def train(self, df_pairs: pd.DataFrame) -> None:
        """Fit the poly2 model on lambda pairs.

        Args:
            df_pairs: DataFrame with lambda_value, lambda_next, and
                feature columns. Typically produced by
                ``prepare_next_lambda()``.

        Raises:
            ValueError: If training data is empty or invalid.
        """
        y = df_pairs["lambda_next"].to_numpy(dtype=float)
        if y.size == 0:
            raise ValueError("Training data is empty")

        X_base = build_feature_matrix(df_pairs, self.feature_cols)
        if X_base.shape[0] == 0:
            raise ValueError("No rows in training feature matrix")

        # Poly2 expansion: [x, x^2]
        X_sq = X_base ** 2
        X_expanded = np.hstack([X_base, X_sq])

        # Add intercept column
        ones = np.ones((X_expanded.shape[0], 1), dtype=float)
        X_design = np.hstack([ones, X_expanded])

        coef, _, _, _ = np.linalg.lstsq(X_design, y, rcond=None)

        self.intercept = float(coef[0])
        self.weights = [float(w) for w in coef[1:]]
        self.train_rows = int(y.size)
        self.trained_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        logger.info(
            "LambdaPoly2Model.train: fitted on %d rows, %d features (poly2 → %d weights)",
            self.train_rows,
            len(self.feature_cols),
            len(self.weights),
        )

    # ========================================================================
    # Prediction
    # ========================================================================

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        """Predict lambda_hat for new cluster rows.

        Args:
            df: DataFrame with feature columns (lambda_value, etc.).
                Does NOT need lambda_next.

        Returns:
            1D numpy array of predicted lambda_hat values.

        Raises:
            RuntimeError: If the model has not been trained.
            ValueError: If feature dimensions don't match.
        """
        if not self.is_trained:
            raise RuntimeError("Model has not been trained — call train() or load() first")

        X_base = build_feature_matrix(df, self.feature_cols)
        X_sq = X_base ** 2
        X_expanded = np.hstack([X_base, X_sq])

        w = np.array(self.weights, dtype=float)
        if X_expanded.shape[1] != w.shape[0]:
            raise ValueError(
                f"Feature dimension mismatch: model expects {w.shape[0]} "
                f"features (poly2), got {X_expanded.shape[1]}"
            )

        return self.intercept + X_expanded @ w

    # ========================================================================
    # Persistence
    # ========================================================================

    def save(self, path: str | Path) -> None:
        """Save model coefficients to a JSON file.

        Args:
            path: Output file path.
        """
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)

        data = {
            "model_type": "global_poly2",
            "intercept": self.intercept,
            "weights": self.weights,
            "feature_cols": self.feature_cols,
            "trained_at": self.trained_at,
            "train_rows": self.train_rows,
            "experiment_id": self.experiment_id,
        }

        with open(p, "w") as f:
            json.dump(data, f, indent=2)

        logger.info("LambdaPoly2Model.save: wrote model to %s", p)

    @classmethod
    def load(cls, path: str | Path) -> LambdaPoly2Model:
        """Load model coefficients from a JSON file.

        Args:
            path: Path to saved model JSON.

        Returns:
            A trained LambdaPoly2Model instance.

        Raises:
            FileNotFoundError: If the model file doesn't exist.
        """
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Lambda model file not found: {p}")

        with open(p) as f:
            data = json.load(f)

        model = cls(
            intercept=float(data.get("intercept", 0.0)),
            weights=[float(w) for w in data.get("weights", [])],
            feature_cols=list(data.get("feature_cols", NUMERIC_FEATURE_COLS)),
            trained_at=str(data.get("trained_at", "")),
            train_rows=int(data.get("train_rows", 0)),
            experiment_id=str(data.get("experiment_id", "")),
        )

        logger.info(
            "LambdaPoly2Model.load: loaded from %s (trained_at=%s, %d weights, %d train_rows)",
            p, model.trained_at, len(model.weights), model.train_rows,
        )
        return model


# ============================================================================
# Model Class: Gradient Boosted Trees
# ============================================================================


@dataclass
class LambdaGBTModel:
    """Gradient Boosted Trees model for lambda_hat prediction.

    Uses sklearn's HistGradientBoostingRegressor which:
    - Handles NaN natively (no imputation needed)
    - Fast training via histogram-based splitting
    - Good regularization out of the box

    Attributes:
        feature_cols: Feature column names.
        trained_at: ISO timestamp of last training.
        train_rows: Number of rows used in training.
        experiment_id: Logical experiment identifier.
        model_path: Path to the saved sklearn artifact (joblib).
    """

    feature_cols: list[str] = field(default_factory=lambda: list(GBT_FEATURE_COLS))
    trained_at: str = ""
    train_rows: int = 0
    experiment_id: str = ""

    # Hyperparameters (conservative for ~15K rows).
    max_iter: int = 300
    max_depth: int = 5
    learning_rate: float = 0.05
    min_samples_leaf: int = 20
    l2_regularization: float = 1.0
    target_mode: str = "change"  # "change" predicts delta; "level" predicts raw lambda_next

    # Internal: the fitted sklearn model (not serialised in JSON).
    _estimator: HistGradientBoostingRegressor | None = field(
        default=None, repr=False,
    )

    @property
    def is_trained(self) -> bool:
        return self._estimator is not None

    # ========================================================================
    # Training
    # ========================================================================

    def train(self, df_pairs: pd.DataFrame) -> None:
        """Fit the GBT model on lambda pairs.

        Args:
            df_pairs: DataFrame with lambda_next (target) and feature
                columns. Produced by ``prepare_next_lambda()`` +
                ``build_enhanced_features()``.
        """
        if self.target_mode == "residual":
            # Target = actual change minus momentum (lambda_trend).
            # Model learns corrections to momentum — mean-reversion, regime shifts, etc.
            actual_change = (df_pairs["lambda_next"] - df_pairs["lambda_value"]).to_numpy(dtype=float)
            momentum = df_pairs["lambda_trend"].fillna(0).to_numpy(dtype=float)
            y = actual_change - momentum
        elif self.target_mode == "change":
            y = (df_pairs["lambda_next"] - df_pairs["lambda_value"]).to_numpy(dtype=float)
        else:
            y = df_pairs["lambda_next"].to_numpy(dtype=float)
        if y.size == 0:
            raise ValueError("Training data is empty")

        X = self._build_X(df_pairs)

        est = HistGradientBoostingRegressor(
            max_iter=self.max_iter,
            max_depth=self.max_depth,
            learning_rate=self.learning_rate,
            min_samples_leaf=self.min_samples_leaf,
            l2_regularization=self.l2_regularization,
            random_state=42,
            early_stopping=True,
            validation_fraction=0.1,
            n_iter_no_change=20,
        )
        est.fit(X, y)

        self._estimator = est
        self.train_rows = int(y.size)
        self.trained_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        logger.info(
            "LambdaGBTModel.train: fitted on %d rows, %d features, %d iterations",
            self.train_rows,
            len(self.feature_cols),
            est.n_iter_,
        )

    # ========================================================================
    # Prediction
    # ========================================================================

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        """Predict lambda_hat for new cluster rows.

        Args:
            df: DataFrame with feature columns.

        Returns:
            1D numpy array of predicted lambda_hat.
        """
        if not self.is_trained:
            raise RuntimeError("Model not trained — call train() or load()")

        X = self._build_X(df)
        raw = self._estimator.predict(X)  # type: ignore[union-attr]
        if self.target_mode == "residual":
            base = df["lambda_value"].to_numpy(dtype=float)
            momentum = (
                df["lambda_trend"].fillna(0).to_numpy(dtype=float)
                if "lambda_trend" in df.columns
                else np.zeros(len(raw))
            )
            return base + momentum + raw
        if self.target_mode == "change":
            return df["lambda_value"].to_numpy(dtype=float) + raw
        return raw

    def _build_X(self, df: pd.DataFrame) -> np.ndarray:
        """Build feature matrix. NaN-friendly (HistGBT handles them)."""
        n = df.shape[0]
        cols: list[np.ndarray] = []
        for col in self.feature_cols:
            if col in df.columns:
                vals = df[col].to_numpy(dtype=float)
            else:
                vals = np.full(n, np.nan, dtype=float)
            cols.append(vals)
        return np.vstack(cols).T

    # ========================================================================
    # Persistence
    # ========================================================================

    def save(self, path: str | Path) -> None:
        """Save model: JSON metadata + joblib artifact.

        Creates two files:
        - ``path`` (JSON): metadata + hyperparams
        - ``path.with_suffix('.joblib')``): sklearn estimator
        """
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)

        joblib_path = p.with_suffix(".joblib")

        data = {
            "model_type": "gbt",
            "feature_cols": self.feature_cols,
            "trained_at": self.trained_at,
            "train_rows": self.train_rows,
            "experiment_id": self.experiment_id,
            "max_iter": self.max_iter,
            "max_depth": self.max_depth,
            "learning_rate": self.learning_rate,
            "min_samples_leaf": self.min_samples_leaf,
            "l2_regularization": self.l2_regularization,
            "target_mode": self.target_mode,
            "sklearn_artifact": str(joblib_path.name),
        }

        with open(p, "w") as f:
            json.dump(data, f, indent=2)

        if self._estimator is not None:
            joblib.dump(self._estimator, joblib_path)

        logger.info("LambdaGBTModel.save: wrote model to %s + %s", p, joblib_path)

    @classmethod
    def load(cls, path: str | Path) -> LambdaGBTModel:
        """Load model from JSON metadata + joblib artifact."""
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Lambda GBT model file not found: {p}")

        with open(p) as f:
            data = json.load(f)

        model = cls(
            feature_cols=list(data.get("feature_cols", GBT_FEATURE_COLS)),
            trained_at=str(data.get("trained_at", "")),
            train_rows=int(data.get("train_rows", 0)),
            experiment_id=str(data.get("experiment_id", "")),
            max_iter=int(data.get("max_iter", 300)),
            max_depth=int(data.get("max_depth", 5)),
            learning_rate=float(data.get("learning_rate", 0.05)),
            min_samples_leaf=int(data.get("min_samples_leaf", 20)),
            l2_regularization=float(data.get("l2_regularization", 1.0)),
            target_mode=str(data.get("target_mode", "level")),
        )

        # Load sklearn artifact.
        artifact_name = data.get("sklearn_artifact")
        if artifact_name:
            joblib_path = p.parent / artifact_name
        else:
            joblib_path = p.with_suffix(".joblib")

        if joblib_path.exists():
            model._estimator = joblib.load(joblib_path)
        else:
            logger.warning(
                "LambdaGBTModel.load: joblib artifact not found at %s",
                joblib_path,
            )

        logger.info(
            "LambdaGBTModel.load: loaded from %s (trained_at=%s, %d train_rows)",
            p, model.trained_at, model.train_rows,
        )
        return model


def load_lambda_model(path: str | Path) -> LambdaPoly2Model | LambdaGBTModel:
    """Auto-detect and load the correct model type from JSON metadata."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Model file not found: {p}")

    with open(p) as f:
        data = json.load(f)

    model_type = data.get("model_type", "global_poly2")
    if model_type == "gbt":
        return LambdaGBTModel.load(p)
    return LambdaPoly2Model.load(p)


__all__ = [
    "LambdaPoly2Model",
    "LambdaGBTModel",
    "NUMERIC_FEATURE_COLS",
    "GBT_FEATURE_COLS",
    "build_feature_matrix",
    "build_enhanced_features",
    "prepare_next_lambda",
    "load_lambda_model",
]
