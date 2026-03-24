"""Prometheus v2 – Daily Lambda Pipeline Step.

Computes raw lambda_t(x) for today's clusters and predicts lambda_hat
using a pre-trained LambdaPoly2Model, then appends predictions to the
predictions CSV for consumption by CsvLambdaClusterScoreProvider.

This module is designed to be called as a non-fatal step in the daily
pipeline (between SIGNALS and UNIVERSES). If the model file is missing
or any computation fails, the pipeline continues without lambda.

Usage::

    from prometheus.opportunity.lambda_daily import run_daily_lambda

    result = run_daily_lambda(
        db_manager=db,
        as_of_date=date(2026, 3, 10),
        market_id="US_EQ",
    )

Key responsibilities:
- Compute raw lambda_t(x) for today's STAB clusters
- Predict lambda_hat using saved model coefficients
- Append predictions to CSV for downstream consumption

External dependencies:
- numpy, pandas: computation
- psycopg2: database access (via DatabaseManager)

Database tables accessed:
- instruments: Read (runtime)
- issuer_classifications: Read (runtime)
- soft_target_classes: Read (runtime)
- sector_health_daily: Read (runtime)
- prices_daily: Read (historical)

Thread safety: Not thread-safe

Author: Prometheus Team
Created: 2026-03-11
Last Modified: 2026-03-11
Status: Development
Version: v0.1.0
"""

from __future__ import annotations

import fcntl
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Sequence

import numpy as np
import pandas as pd
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.core.time import TradingCalendar, TradingCalendarConfig
from apathis.data.reader import DataReader

from prometheus.opportunity.lambda_model import (
    LambdaGBTModel,
    build_enhanced_features,
    load_lambda_model,
)

logger = get_logger(__name__)

# Project root for resolving relative paths.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Default paths (relative to PROJECT_ROOT).
DEFAULT_MODEL_PATH = "data/lambda_model_{market_id}.json"
DEFAULT_PREDICTIONS_PATH = "data/lambda_predictions_{market_id}.csv"


# ============================================================================
# Raw Lambda Computation
# ============================================================================


@dataclass(frozen=True)
class LambdaClusterRow:
    """Single raw lambda_t(x) observation for one cluster on one date."""

    as_of_date: date
    market_id: str
    sector: str
    soft_target_class: str
    num_instruments: int
    dispersion: float
    avg_vol_window: float
    lambda_value: float
    sector_health_score: float = 0.0


# Maximum staleness (in days) for STAB lookback in the daily lambda query.
_STAB_LOOKBACK_DAYS = 10


def _load_instruments_with_stab(
    db_manager: DatabaseManager,
    *,
    as_of_date: date,
    market_ids: Sequence[str],
    stab_lookback_days: int = _STAB_LOOKBACK_DAYS,
) -> pd.DataFrame:
    """Load active equity instruments with sector and STAB state.

    Uses the most recent STAB classification within a window of
    ``stab_lookback_days`` calendar days.  The daily pipeline may only
    re-classify a small subset of instruments on each run, so a strict
    exact-date match would leave most instruments without a STAB state.

    Excludes SYNTH* instruments to prevent synthetic reality leakage.

    Returns DataFrame with columns:
        instrument_id, issuer_id, sector, market_id, soft_target_class
    """
    stab_start = as_of_date - timedelta(days=stab_lookback_days)

    sql = """
        SELECT
            i.instrument_id,
            i.issuer_id,
            COALESCE(NULLIF(ic.sector, ''), 'UNKNOWN') AS sector,
            i.market_id,
            st.soft_target_class
        FROM instruments AS i
        LEFT JOIN issuer_classifications AS ic
          ON ic.issuer_id = i.issuer_id
        LEFT JOIN LATERAL (
            SELECT stc.soft_target_class
            FROM soft_target_classes AS stc
            WHERE stc.entity_type = 'INSTRUMENT'
              AND stc.entity_id = i.instrument_id
              AND stc.as_of_date BETWEEN %s AND %s
            ORDER BY stc.as_of_date DESC
            LIMIT 1
        ) AS st ON TRUE
        WHERE i.market_id = ANY(%s)
          AND i.asset_class = 'EQUITY'
          AND i.status = 'ACTIVE'
          AND i.instrument_id NOT LIKE 'SYNTH%%'
          AND ic.sector IS NOT NULL
          AND ic.sector != ''
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (stab_start, as_of_date, list(market_ids)))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    df = pd.DataFrame(
        rows,
        columns=["instrument_id", "issuer_id", "sector", "market_id", "soft_target_class"],
    )
    if df.empty:
        return df

    df["sector"] = df["sector"].astype(str)
    df["market_id"] = df["market_id"].astype(str)
    df["soft_target_class"] = df["soft_target_class"].fillna("UNKNOWN").astype(str)
    return df


def compute_lambda_for_date(
    db_manager: DatabaseManager,
    data_reader: DataReader,
    calendar: TradingCalendar,
    *,
    as_of_date: date,
    market_ids: Sequence[str],
    lookback_days: int = 20,
    min_cluster_size: int = 3,
) -> List[LambdaClusterRow]:
    """Compute raw lambda_t(x) for all clusters on a single date.

    Args:
        db_manager: Database manager.
        data_reader: Price data reader.
        calendar: Trading calendar.
        as_of_date: Date to compute for.
        market_ids: Markets to include.
        lookback_days: Trailing window for vol computation.
        min_cluster_size: Minimum instruments per cluster.

    Returns:
        List of LambdaClusterRow observations.
    """
    inst_df = _load_instruments_with_stab(
        db_manager, as_of_date=as_of_date, market_ids=market_ids,
    )
    if inst_df.empty:
        logger.info("compute_lambda_for_date: no instruments for %s", as_of_date)
        return []

    # Filter to instruments that have a STAB state (not UNKNOWN).
    inst_df = inst_df[inst_df["soft_target_class"] != "UNKNOWN"]
    if inst_df.empty:
        logger.info("compute_lambda_for_date: no instruments with STAB state for %s", as_of_date)
        return []

    # Trading day window.
    search_start = as_of_date - timedelta(days=lookback_days * 3)
    trading_days = calendar.trading_days_between(search_start, as_of_date)
    if len(trading_days) < lookback_days:
        logger.info("compute_lambda_for_date: insufficient trading days before %s", as_of_date)
        return []

    window_days = trading_days[-lookback_days:]
    start_date = window_days[0]

    instrument_ids = sorted(inst_df["instrument_id"].unique().tolist())
    prices = data_reader.read_prices(
        instrument_ids=instrument_ids,
        start_date=start_date,
        end_date=as_of_date,
    )
    if prices.empty:
        logger.info("compute_lambda_for_date: no prices for %s", as_of_date)
        return []

    # Compute simple daily returns.
    prices = prices[["instrument_id", "trade_date", "close"]].copy()
    prices["close"] = prices["close"].astype(float)
    prices.sort_values(["instrument_id", "trade_date"], inplace=True)
    prices["ret"] = prices.groupby("instrument_id")["close"].pct_change()

    # Latest-day return per instrument.
    latest = prices.groupby("instrument_id").tail(1).copy()
    latest = latest[latest["trade_date"] == as_of_date]
    if latest.empty:
        logger.info("compute_lambda_for_date: no prices on %s", as_of_date)
        return []

    # Realised vol per instrument over window.
    vol = (
        prices.groupby("instrument_id")["ret"]
        .std(ddof=1)
        .rename("realised_vol_window")
        .reset_index()
    )

    feat = latest.merge(vol, on="instrument_id", how="left")
    feat = feat.merge(inst_df, on="instrument_id", how="left")
    feat = feat.dropna(subset=["ret", "realised_vol_window"])
    if feat.empty:
        return []

    # Load sector health scores for this date (graceful fallback to empty).
    sector_health: Dict[str, float] = {}
    try:
        sql_shi = "SELECT sector_name, score FROM sector_health_daily WHERE as_of_date = %s"
        with db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql_shi, (as_of_date,))
                sector_health = {str(r[0]): float(r[1]) for r in cursor.fetchall()}
            finally:
                cursor.close()
    except Exception:
        logger.debug("compute_lambda_for_date: sector_health_daily unavailable for %s", as_of_date)

    # Aggregate per cluster.
    results: List[LambdaClusterRow] = []
    grouped = feat.groupby(["market_id", "sector", "soft_target_class"], dropna=False)

    for (market_id, sector, soft_class), g in grouped:
        n = int(g.shape[0])
        if n < min_cluster_size:
            continue

        disp = float(np.std(g["ret"].to_numpy(), ddof=1))
        avg_vol = float(g["realised_vol_window"].mean())
        lambda_value = disp + avg_vol
        shi_score = sector_health.get(str(sector), 0.0)

        results.append(LambdaClusterRow(
            as_of_date=as_of_date,
            market_id=str(market_id),
            sector=str(sector),
            soft_target_class=str(soft_class),
            num_instruments=n,
            dispersion=disp,
            avg_vol_window=avg_vol,
            lambda_value=lambda_value,
            sector_health_score=shi_score,
        ))

    logger.info(
        "compute_lambda_for_date: date=%s clusters=%d instruments=%d",
        as_of_date, len(results), len(inst_df),
    )
    return results


# ============================================================================
# Daily Lambda Pipeline Step
# ============================================================================


@dataclass
class DailyLambdaResult:
    """Result of the daily lambda computation."""

    as_of_date: date
    market_id: str
    n_clusters: int
    n_predictions: int
    predictions_csv: str
    success: bool
    error: str | None = None


def run_daily_lambda(
    db_manager: DatabaseManager,
    as_of_date: date,
    market_id: str = "US_EQ",
    *,
    model_path: str | Path | None = None,
    predictions_csv: str | Path | None = None,
    experiment_id: str = "US_EQ_GL_POLY2_V0",
    lookback_days: int = 20,
    min_cluster_size: int = 3,
) -> DailyLambdaResult:
    """Compute and predict daily lambda for a market.

    This is the main entry point for the daily pipeline. It:
    1. Computes raw lambda_t(x) for today's clusters.
    2. Loads the trained model from disk.
    3. Predicts lambda_hat for each cluster.
    4. Appends predictions to the CSV.

    Args:
        db_manager: Database manager.
        as_of_date: Date to compute for.
        market_id: Market identifier.
        model_path: Path to saved model JSON. Defaults to
            data/lambda_model_{market_id}.json.
        predictions_csv: Path to append predictions to. Defaults to
            data/lambda_predictions_{market_id}.csv.
        experiment_id: Experiment ID to tag predictions with.
        lookback_days: Trailing window for vol.
        min_cluster_size: Minimum instruments per cluster.

    Returns:
        DailyLambdaResult with status and counts.
    """
    # Resolve default paths.
    if model_path is None:
        model_path = PROJECT_ROOT / DEFAULT_MODEL_PATH.format(market_id=market_id)
    else:
        model_path = Path(model_path)
        if not model_path.is_absolute():
            model_path = PROJECT_ROOT / model_path

    if predictions_csv is None:
        predictions_csv = PROJECT_ROOT / DEFAULT_PREDICTIONS_PATH.format(market_id=market_id)
    else:
        predictions_csv = Path(predictions_csv)
        if not predictions_csv.is_absolute():
            predictions_csv = PROJECT_ROOT / predictions_csv

    def fail(msg):
        return DailyLambdaResult(
            as_of_date=as_of_date, market_id=market_id,
            n_clusters=0, n_predictions=0,
            predictions_csv=str(predictions_csv),
            success=False, error=msg,
        )

    # Step 1: Load model (auto-detects Poly2 vs GBT from JSON metadata).
    try:
        model = load_lambda_model(model_path)
    except FileNotFoundError:
        logger.warning(
            "run_daily_lambda: model file not found at %s; skipping lambda for %s",
            model_path, as_of_date,
        )
        return fail(f"Model file not found: {model_path}")
    except Exception as exc:
        logger.warning(
            "run_daily_lambda: failed to load model from %s: %s", model_path, exc,
        )
        return fail(f"Model load error: {exc}")

    # Step 2: Compute raw lambda.
    calendar = TradingCalendar(TradingCalendarConfig(market=market_id))
    data_reader = DataReader(db_manager=db_manager)

    clusters = compute_lambda_for_date(
        db_manager, data_reader, calendar,
        as_of_date=as_of_date,
        market_ids=[market_id],
        lookback_days=lookback_days,
        min_cluster_size=min_cluster_size,
    )

    if not clusters:
        logger.info("run_daily_lambda: no clusters for %s; skipping", as_of_date)
        return fail("No clusters computed")

    # Step 3: Build features DataFrame and predict.
    df_clusters = pd.DataFrame([
        {
            "as_of_date": c.as_of_date,
            "market_id": c.market_id,
            "sector": c.sector,
            "soft_target_class": c.soft_target_class,
            "num_instruments": c.num_instruments,
            "dispersion": c.dispersion,
            "avg_vol_window": c.avg_vol_window,
            "lambda_value": c.lambda_value,
            "sector_health_score": c.sector_health_score,
        }
        for c in clusters
    ])

    # For GBT models, compute enhanced features before prediction.
    if isinstance(model, LambdaGBTModel):
        try:
            df_clusters = build_enhanced_features(df_clusters)
        except Exception as exc:
            logger.warning(
                "run_daily_lambda: enhanced feature engineering failed: %s", exc,
            )
            # Fall through — GBT handles NaN natively, so partial features
            # are better than skipping entirely.

    try:
        preds = model.predict(df_clusters)
    except Exception as exc:
        logger.warning("run_daily_lambda: prediction failed: %s", exc)
        return fail(f"Prediction error: {exc}")

    df_clusters["lambda_hat"] = preds.astype(float)
    df_clusters["experiment_id"] = experiment_id

    # Step 4: Append to predictions CSV (file-locked for safety).
    predictions_csv = Path(predictions_csv)
    predictions_csv.parent.mkdir(parents=True, exist_ok=True)

    try:
        _append_predictions(predictions_csv, df_clusters, as_of_date, experiment_id)
    except Exception as exc:
        logger.warning(
            "run_daily_lambda: failed to write predictions CSV: %s", exc,
        )
        return fail(f"CSV write error: {exc}")

    logger.info(
        "run_daily_lambda: date=%s market=%s clusters=%d predictions=%d → %s",
        as_of_date, market_id, len(clusters), len(preds), predictions_csv,
    )

    return DailyLambdaResult(
        as_of_date=as_of_date,
        market_id=market_id,
        n_clusters=len(clusters),
        n_predictions=len(preds),
        predictions_csv=str(predictions_csv),
        success=True,
    )


def _append_predictions(
    csv_path: Path,
    df_new: pd.DataFrame,
    as_of_date: date,
    experiment_id: str,
) -> None:
    """Append new predictions to CSV, replacing any existing rows for the same date/experiment.

    Uses file locking to prevent concurrent writes from corrupting the CSV.
    """
    with open(csv_path, "a") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            if csv_path.exists() and csv_path.stat().st_size > 0:
                df_existing = pd.read_csv(csv_path)
                df_existing["as_of_date"] = pd.to_datetime(df_existing["as_of_date"]).dt.date

                # Remove any existing rows for this date + experiment to avoid duplicates.
                mask = ~(
                    (df_existing["as_of_date"] == as_of_date)
                    & (df_existing["experiment_id"] == experiment_id)
                )
                df_existing = df_existing[mask]
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            else:
                df_combined = df_new

            df_combined.to_csv(csv_path, index=False)
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


__all__ = [
    "run_daily_lambda",
    "compute_lambda_for_date",
    "DailyLambdaResult",
    "LambdaClusterRow",
]
