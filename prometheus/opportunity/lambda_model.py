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
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import numpy as np
import pandas as pd

from apathis.core.logging import get_logger

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
# Model Class
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


__all__ = [
    "LambdaPoly2Model",
    "NUMERIC_FEATURE_COLS",
    "build_feature_matrix",
    "prepare_next_lambda",
]
