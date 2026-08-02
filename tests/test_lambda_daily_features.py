"""Lambda daily GBT feature integrity.

Regression: daily GBT inference built engineered features (lags/rolling/
zscore) on a SINGLE date, so they were all NaN and predictions collapsed to
~intercept. The pipeline now computes features over a trailing cluster-history
window and selects the as_of_date rows. When history is insufficient it must
detect the all-NaN condition and log a WARNING rather than emit a degenerate
prediction silently.
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

import prometheus.opportunity.lambda_daily as ld
from prometheus.opportunity.lambda_daily import LambdaClusterRow, run_daily_lambda
from prometheus.opportunity.lambda_model import LambdaGBTModel


class _CapturingGBT(LambdaGBTModel):
    """GBT model that records the DataFrame it is asked to predict on."""

    def __init__(self) -> None:
        super().__init__()
        self.seen: pd.DataFrame | None = None

    def predict(self, df: pd.DataFrame) -> np.ndarray:  # type: ignore[override]
        self.seen = df.copy()
        return np.full(len(df), 0.5, dtype=float)


def _cluster(d: date, lv: float) -> LambdaClusterRow:
    return LambdaClusterRow(
        as_of_date=d, market_id="US_EQ", sector="Tech",
        soft_target_class="A", num_instruments=5, dispersion=0.1,
        avg_vol_window=0.2, lambda_value=lv, sector_health_score=0.5,
    )


class _StubCalendar:
    def __init__(self, *a, **k) -> None:
        pass

    def trading_days_between(self, start: date, end: date) -> list[date]:
        days: list[date] = []
        d = start
        while d <= end:
            if d.weekday() < 5:
                days.append(d)
            d += timedelta(days=1)
        return days


def _patch(monkeypatch, model, per_date):
    """Patch model loading + per-date cluster computation + IO."""
    monkeypatch.setattr(ld, "load_lambda_model", lambda *_a, **_k: model)
    monkeypatch.setattr(ld, "TradingCalendar", lambda *_a, **_k: _StubCalendar())
    monkeypatch.setattr(ld, "TradingCalendarConfig", lambda *_a, **_k: object())
    monkeypatch.setattr(ld, "DataReader", lambda *_a, **_k: object())

    def _compute(db, reader, cal, *, as_of_date, market_ids, lookback_days, min_cluster_size):
        return per_date(as_of_date)

    monkeypatch.setattr(ld, "compute_lambda_for_date", _compute)
    monkeypatch.setattr(ld, "_append_predictions", lambda *a, **k: None)


def test_trailing_history_yields_non_nan_features(monkeypatch, tmp_path) -> None:
    model = _CapturingGBT()
    as_of = date(2026, 2, 27)

    # Every trading day in the trailing window has a cluster observation.
    def per_date(d: date):
        return [_cluster(d, 0.30 + 0.001 * d.toordinal())]

    _patch(monkeypatch, model, per_date)

    res = run_daily_lambda(
        db_manager=object(), as_of_date=as_of, market_id="US_EQ",
        model_path=tmp_path / "m.json",
        predictions_csv=tmp_path / "p.csv",
    )

    assert res.success
    assert model.seen is not None
    # Only the as_of_date rows are scored.
    assert (model.seen["as_of_date"] == as_of).all()
    # Engineered features must be populated (non-NaN), proving the trailing
    # history fed the lag/rolling/zscore computations.
    for col in ("lambda_prev", "lambda_roll_mean_21", "lambda_zscore_21"):
        assert not model.seen[col].isna().all(), f"{col} should be non-NaN"


def test_single_date_history_flags_degenerate(monkeypatch, tmp_path, caplog) -> None:
    model = _CapturingGBT()
    as_of = date(2026, 2, 27)

    # Only as_of_date yields a cluster; all prior days are empty, so engineered
    # features cannot be computed and the degenerate guard must fire.
    def per_date(d: date):
        return [_cluster(d, 0.30)] if d == as_of else []

    _patch(monkeypatch, model, per_date)

    with caplog.at_level("WARNING"):
        res = run_daily_lambda(
            db_manager=object(), as_of_date=as_of, market_id="US_EQ",
            model_path=tmp_path / "m.json",
            predictions_csv=tmp_path / "p.csv",
        )

    assert res.success  # still emits, but flagged
    assert any("degenerate" in r.message.lower() or "NaN" in r.message
               for r in caplog.records), "degenerate prediction must be warned"
