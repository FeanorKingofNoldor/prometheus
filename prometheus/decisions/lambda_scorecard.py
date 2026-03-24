"""Prometheus v2 – Lambda Prediction Scorecard.

Evaluates the accuracy of lambda_hat (predicted next-day opportunity
density) against realised lambda at t+1 for each (sector × STAB class)
cluster.

Metrics produced:
- MAE, RMSE: absolute prediction error
- R²: coefficient of determination
- Direction accuracy: did lambda_hat correctly predict increase vs decrease?
- Per-cluster breakdown: which clusters predict best/worst?

The scorecard can work from:
1. A predictions CSV (if available) — contains lambda_hat and lambda_value
2. Direct DB computation — recomputes lambda from raw prices/STAB data

Usage::

    from prometheus.decisions.lambda_scorecard import LambdaScorecard
    scorecard = LambdaScorecard(db_manager=db)
    report = scorecard.build_scorecard(market_id="US_EQ")

"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger

logger = get_logger(__name__)

# Default CSV path pattern (relative to project root).
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PREDICTIONS_PATH = "data/lambda_predictions_{market_id}.csv"

# Cluster key columns.
CLUSTER_KEYS = ["market_id", "sector", "soft_target_class"]


@dataclass(frozen=True)
class LambdaClusterAccuracy:
    """Prediction accuracy for one cluster."""

    cluster_key: str  # "market_id|sector|stab_class"
    sector: str
    soft_target_class: str
    mae: float
    rmse: float
    direction_accuracy: float
    count: int
    avg_predicted: float
    avg_actual: float


@dataclass(frozen=True)
class LambdaScorecardReport:
    """Complete lambda prediction scorecard."""

    market_id: str
    total_predictions: int
    mae: float
    rmse: float
    r_squared: float
    direction_accuracy: float
    avg_predicted: float
    avg_actual: float
    cluster_breakdown: List[LambdaClusterAccuracy]
    date_range: Tuple[date, date]
    data_source: str  # "csv" or "db"


@dataclass
class LambdaScorecard:
    """Builds lambda prediction accuracy reports."""

    db_manager: DatabaseManager

    def build_scorecard(
        self,
        *,
        market_id: str = "US_EQ",
        max_dates: int = 200,
        start_date: date | None = None,
        end_date: date | None = None,
        predictions_csv: str | Path | None = None,
    ) -> LambdaScorecardReport:
        """Build a lambda prediction scorecard.

        Tries the predictions CSV first; falls back to DB computation
        if the CSV doesn't exist or is empty.

        Args:
            market_id: Market to evaluate.
            max_dates: Maximum number of dates to include.
            start_date: Optional start of evaluation window.
            end_date: Optional end.
            predictions_csv: Path to predictions CSV. Defaults to
                data/lambda_predictions_{market_id}.csv.

        Returns:
            Complete LambdaScorecardReport.
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)

        # Try CSV first.
        if predictions_csv is None:
            csv_path = PROJECT_ROOT / DEFAULT_PREDICTIONS_PATH.format(
                market_id=market_id,
            )
        else:
            csv_path = Path(predictions_csv)
            if not csv_path.is_absolute():
                csv_path = PROJECT_ROOT / csv_path

        if csv_path.exists() and csv_path.stat().st_size > 0:
            return self._build_from_csv(
                csv_path,
                market_id=market_id,
                max_dates=max_dates,
                start_date=start_date,
                end_date=end_date,
            )

        # Fallback: compute from DB.
        return self._build_from_db(
            market_id=market_id,
            max_dates=max_dates,
            start_date=start_date,
            end_date=end_date,
        )

    def _build_from_csv(
        self,
        csv_path: Path,
        *,
        market_id: str,
        max_dates: int,
        start_date: date | None,
        end_date: date | None,
    ) -> LambdaScorecardReport:
        """Build scorecard from a predictions CSV.

        The CSV has columns: as_of_date, market_id, sector,
        soft_target_class, lambda_value, lambda_hat, etc.

        We join each row's lambda_hat with the actual lambda_value from
        the next observation for the same cluster.
        """
        df = pd.read_csv(csv_path)
        df["as_of_date"] = pd.to_datetime(df["as_of_date"]).dt.date

        if market_id:
            df = df[df["market_id"] == market_id]
        if start_date:
            df = df[df["as_of_date"] >= start_date]
        if end_date:
            df = df[df["as_of_date"] <= end_date]

        if df.empty or "lambda_hat" not in df.columns:
            return self._empty_report(market_id, end_date or date.today(), "csv")

        # Use lambda_next from CSV if available (accurate walk-forward target),
        # otherwise compute via shift(-1) on lambda_value.
        df = df.sort_values(CLUSTER_KEYS + ["as_of_date"])
        if "lambda_next" in df.columns:
            df["lambda_actual_next"] = df["lambda_next"].astype(float)
        else:
            df["lambda_actual_next"] = df.groupby(CLUSTER_KEYS)["lambda_value"].shift(-1)
        df = df.dropna(subset=["lambda_hat", "lambda_actual_next"])

        if df.empty:
            return self._empty_report(market_id, end_date or date.today(), "csv")

        # Limit to most recent dates.
        dates_sorted = sorted(df["as_of_date"].unique(), reverse=True)
        if len(dates_sorted) > max_dates:
            cutoff = dates_sorted[max_dates - 1]
            df = df[df["as_of_date"] >= cutoff]

        return self._compute_report(df, market_id, "csv")

    def _build_from_db(
        self,
        *,
        market_id: str,
        max_dates: int,
        start_date: date | None,
        end_date: date | None,
    ) -> LambdaScorecardReport:
        """Build scorecard by recomputing lambda from raw DB data.

        Uses the same logic as compute_lambda_for_date but loads
        historical cluster-level lambda values from universe_members
        reasons JSON (which stores lambda_score and cluster_id).
        """
        # Load daily lambda observations from the universe_members table.
        # Each included member has reasons.cluster_id and reasons.lambda_score.
        # We aggregate to cluster level.
        sql = """
            SELECT
                um.as_of_date,
                (um.reasons->>'cluster_id')::text AS cluster_id,
                avg((um.reasons->>'lambda_score')::numeric) AS avg_lambda
            FROM universe_members um
            WHERE um.included = TRUE
              AND um.reasons ? 'cluster_id'
              AND um.reasons ? 'lambda_score'
              AND um.as_of_date <= %s
        """
        params: list = [end_date or date.today()]
        if start_date:
            sql += " AND um.as_of_date >= %s"
            params.append(start_date)
        sql += """
            GROUP BY um.as_of_date, (um.reasons->>'cluster_id')
            ORDER BY um.as_of_date DESC
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, tuple(params))
                rows = cursor.fetchall()
            finally:
                cursor.close()

        if not rows:
            logger.info(
                "LambdaScorecard: no lambda data in universe_members for %s",
                market_id,
            )
            return self._empty_report(
                market_id, end_date or date.today(), "db",
            )

        # Build DataFrame.
        df = pd.DataFrame(rows, columns=["as_of_date", "cluster_id", "lambda_value"])
        df["lambda_value"] = df["lambda_value"].astype(float)

        # Parse cluster_id → market_id, sector, soft_target_class.
        parts = df["cluster_id"].str.split("|", expand=True)
        if parts.shape[1] >= 3:
            df["market_id"] = parts[0]
            df["sector"] = parts[1]
            df["soft_target_class"] = parts[2]
        else:
            return self._empty_report(
                market_id, end_date or date.today(), "db",
            )

        if market_id:
            df = df[df["market_id"] == market_id]

        if df.empty:
            return self._empty_report(
                market_id, end_date or date.today(), "db",
            )

        # Limit dates.
        dates_sorted = sorted(df["as_of_date"].unique(), reverse=True)
        if len(dates_sorted) > max_dates + 1:
            cutoff = dates_sorted[max_dates]
            df = df[df["as_of_date"] >= cutoff]

        # Sort and create "predicted" (today's value as naive forecast)
        # and "actual" (next day's value).
        df = df.sort_values(CLUSTER_KEYS + ["as_of_date"])
        df["lambda_hat"] = df.groupby(CLUSTER_KEYS)["lambda_value"].shift(0)
        df["lambda_actual_next"] = df.groupby(CLUSTER_KEYS)["lambda_value"].shift(-1)
        df = df.dropna(subset=["lambda_hat", "lambda_actual_next"])

        if df.empty:
            return self._empty_report(
                market_id, end_date or date.today(), "db",
            )

        return self._compute_report(df, market_id, "db")

    def _compute_report(
        self,
        df: pd.DataFrame,
        market_id: str,
        source: str,
    ) -> LambdaScorecardReport:
        """Compute metrics from a DataFrame with lambda_hat and lambda_actual_next."""
        predicted = df["lambda_hat"].to_numpy(dtype=float)
        actual = df["lambda_actual_next"].to_numpy(dtype=float)
        n = len(predicted)

        errors = predicted - actual
        mae = float(np.mean(np.abs(errors)))
        rmse = float(np.sqrt(np.mean(errors ** 2)))

        # R².
        ss_res = float(np.sum(errors ** 2))
        ss_tot = float(np.sum((actual - np.mean(actual)) ** 2))
        r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

        # Direction accuracy: did prediction correctly forecast
        # increase vs decrease relative to current lambda?
        # lambda_hat predicts t+1 value. We check if the predicted
        # direction of change (lambda_hat - lambda_value_t) matches
        # the actual direction (lambda_actual_next - lambda_value_t).
        if "lambda_value" in df.columns:
            current = df["lambda_value"].to_numpy(dtype=float)
            pred_direction = np.sign(predicted - current)
            actual_direction = np.sign(actual - current)
            direction_hits = np.sum(pred_direction == actual_direction)
            direction_accuracy = float(direction_hits / n) if n > 0 else 0.0
        else:
            direction_accuracy = 0.0

        # Per-cluster breakdown.
        cluster_breakdown: List[LambdaClusterAccuracy] = []
        for (sector, stab_class), group in df.groupby(
            ["sector", "soft_target_class"],
        ):
            g_pred = group["lambda_hat"].to_numpy(dtype=float)
            g_actual = group["lambda_actual_next"].to_numpy(dtype=float)
            g_n = len(g_pred)
            g_errors = g_pred - g_actual
            g_mae = float(np.mean(np.abs(g_errors)))
            g_rmse = float(np.sqrt(np.mean(g_errors ** 2)))

            g_dir_acc = 0.0
            if "lambda_value" in group.columns:
                g_curr = group["lambda_value"].to_numpy(dtype=float)
                g_pred_dir = np.sign(g_pred - g_curr)
                g_act_dir = np.sign(g_actual - g_curr)
                g_dir_hits = np.sum(g_pred_dir == g_act_dir)
                g_dir_acc = float(g_dir_hits / g_n) if g_n > 0 else 0.0

            cluster_breakdown.append(
                LambdaClusterAccuracy(
                    cluster_key=f"{market_id}|{sector}|{stab_class}",
                    sector=str(sector),
                    soft_target_class=str(stab_class),
                    mae=g_mae,
                    rmse=g_rmse,
                    direction_accuracy=g_dir_acc,
                    count=g_n,
                    avg_predicted=float(np.mean(g_pred)),
                    avg_actual=float(np.mean(g_actual)),
                )
            )

        # Sort: worst direction accuracy first.
        cluster_breakdown.sort(key=lambda c: c.direction_accuracy)

        min_date = df["as_of_date"].min()
        max_date = df["as_of_date"].max()

        return LambdaScorecardReport(
            market_id=market_id,
            total_predictions=n,
            mae=mae,
            rmse=rmse,
            r_squared=r_squared,
            direction_accuracy=direction_accuracy,
            avg_predicted=float(np.mean(predicted)),
            avg_actual=float(np.mean(actual)),
            cluster_breakdown=cluster_breakdown,
            date_range=(min_date, max_date),
            data_source=source,
        )

    @staticmethod
    def _empty_report(
        market_id: str,
        as_of: date,
        source: str,
    ) -> LambdaScorecardReport:
        return LambdaScorecardReport(
            market_id=market_id,
            total_predictions=0,
            mae=0.0,
            rmse=0.0,
            r_squared=0.0,
            direction_accuracy=0.0,
            avg_predicted=0.0,
            avg_actual=0.0,
            cluster_breakdown=[],
            date_range=(as_of, as_of),
            data_source=source,
        )
