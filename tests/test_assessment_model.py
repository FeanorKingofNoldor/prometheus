"""Tests for BasicAssessmentModel (prometheus.assessment.model_basic)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from prometheus.assessment.model_basic import BasicAssessmentModel


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_price_df(instrument_id: str, closes: list[float], start_date: date) -> pd.DataFrame:
    """Build a DataFrame mimicking DataReader.read_prices output."""
    rows = []
    for i, c in enumerate(closes):
        rows.append(
            {
                "instrument_id": instrument_id,
                "trade_date": start_date + timedelta(days=i),
                "close": c,
            }
        )
    return pd.DataFrame(rows)


def _trading_days(start: date, end: date) -> list[date]:
    """Return every calendar day between start and end (inclusive) as a simple stub."""
    days = []
    d = start
    while d <= end:
        days.append(d)
        d += timedelta(days=1)
    return days


def _build_model(
    prices_df: pd.DataFrame | None = None,
    stab_state=None,
    window_days: int = 21,
    momentum_ref: float = 0.20,
    fragility_penalty_weight: float = 0.15,
    weak_profile_penalty_multiplier: float = 0.5,
    buy_threshold: float = 0.01,
    strong_buy_threshold: float = 0.03,
    sell_threshold: float = 0.01,
    strong_sell_threshold: float = 0.03,
) -> BasicAssessmentModel:
    """Construct a BasicAssessmentModel with mocked dependencies."""
    data_reader = MagicMock()
    if prices_df is not None:
        data_reader.read_prices.return_value = prices_df
    else:
        data_reader.read_prices.return_value = pd.DataFrame()

    calendar = MagicMock()
    calendar.trading_days_between.side_effect = _trading_days

    stability_storage = None
    if stab_state is not None:
        stability_storage = MagicMock()
        stability_storage.get_latest_state.return_value = stab_state

    model = BasicAssessmentModel(
        data_reader=data_reader,
        calendar=calendar,
        stability_storage=stability_storage,
        db_manager=None,
        momentum_window_days=window_days,
        momentum_ref=momentum_ref,
        fragility_penalty_weight=fragility_penalty_weight,
        weak_profile_penalty_multiplier=weak_profile_penalty_multiplier,
        buy_threshold=buy_threshold,
        strong_buy_threshold=strong_buy_threshold,
        sell_threshold=sell_threshold,
        strong_sell_threshold=strong_sell_threshold,
    )
    return model


@dataclass
class FakeSoftTargetState:
    soft_target_score: float
    weak_profile: bool
    soft_target_class: "FakeEnum"


@dataclass
class FakeEnum:
    value: str


# ---------------------------------------------------------------------------
# _compute_price_features tests
# ---------------------------------------------------------------------------


class TestComputePriceFeatures:
    """Tests for BasicAssessmentModel._compute_price_features."""

    def test_normal_upward_momentum(self):
        """Positive price trend produces positive momentum."""
        closes = [100.0 + i for i in range(25)]  # 100 to 124
        as_of = date(2025, 6, 30)
        start = as_of - timedelta(days=24)
        df = _make_price_df("AAPL", closes, start)
        model = _build_model(df, window_days=21)

        momentum, vol = model._compute_price_features("AAPL", as_of, 21)
        # Price went from 100 to 124 → momentum ~ 0.24
        assert momentum > 0.0
        assert vol > 0.0

    def test_normal_downward_momentum(self):
        """Negative price trend produces negative momentum."""
        closes = [200.0 - i * 2 for i in range(25)]  # 200 down to 152
        as_of = date(2025, 6, 30)
        start = as_of - timedelta(days=24)
        df = _make_price_df("AAPL", closes, start)
        model = _build_model(df, window_days=21)

        momentum, vol = model._compute_price_features("AAPL", as_of, 21)
        assert momentum < 0.0

    def test_flat_prices_zero_momentum(self):
        """Flat prices produce zero momentum and near-zero vol."""
        closes = [100.0] * 25
        as_of = date(2025, 6, 30)
        start = as_of - timedelta(days=24)
        df = _make_price_df("AAPL", closes, start)
        model = _build_model(df, window_days=21)

        momentum, vol = model._compute_price_features("AAPL", as_of, 21)
        assert momentum == 0.0
        assert vol == 0.0

    def test_zero_start_price_returns_zero_momentum(self):
        """If the first close is 0, momentum should be 0 (not division error)."""
        closes = [0.0] + [100.0] * 24
        as_of = date(2025, 6, 30)
        start = as_of - timedelta(days=24)
        df = _make_price_df("AAPL", closes, start)
        model = _build_model(df, window_days=21)

        momentum, vol = model._compute_price_features("AAPL", as_of, 21)
        assert momentum == 0.0

    def test_insufficient_data_raises(self):
        """Too few rows raises ValueError."""
        closes = [100.0, 101.0]  # Only 2 rows, need 85% of 21 = 17
        as_of = date(2025, 6, 30)
        start = as_of - timedelta(days=1)
        df = _make_price_df("AAPL", closes, start)
        model = _build_model(df, window_days=21)

        with pytest.raises(ValueError, match="Insufficient price rows"):
            model._compute_price_features("AAPL", as_of, 21)

    def test_empty_dataframe_raises(self):
        """Empty DataFrame raises ValueError."""
        model = _build_model(pd.DataFrame(), window_days=21)

        with pytest.raises(ValueError, match="Insufficient price rows"):
            model._compute_price_features("AAPL", date(2025, 6, 30), 21)

    def test_window_days_zero_raises(self):
        """window_days=0 raises ValueError."""
        model = _build_model(pd.DataFrame(), window_days=21)
        with pytest.raises(ValueError, match="window_days must be positive"):
            model._compute_price_features("AAPL", date(2025, 6, 30), 0)

    def test_negative_window_days_raises(self):
        """Negative window_days raises ValueError."""
        model = _build_model(pd.DataFrame(), window_days=21)
        with pytest.raises(ValueError, match="window_days must be positive"):
            model._compute_price_features("AAPL", date(2025, 6, 30), -5)

    def test_85_percent_threshold_passes(self):
        """Exactly 85% of window_days rows should be accepted."""
        window = 20
        min_required = max(2, int(window * 0.85))  # 17
        closes = [100.0 + i * 0.5 for i in range(min_required)]
        as_of = date(2025, 6, 30)
        start = as_of - timedelta(days=min_required - 1)
        df = _make_price_df("AAPL", closes, start)
        model = _build_model(df, window_days=window)

        # Should not raise
        momentum, vol = model._compute_price_features("AAPL", as_of, window)
        assert isinstance(momentum, float)

    def test_just_below_85_percent_threshold_fails(self):
        """One fewer row than the 85% threshold should raise."""
        window = 20
        min_required = max(2, int(window * 0.85))  # 17
        closes = [100.0 + i for i in range(min_required - 1)]
        as_of = date(2025, 6, 30)
        start = as_of - timedelta(days=min_required - 2)
        df = _make_price_df("AAPL", closes, start)
        model = _build_model(df, window_days=window)

        with pytest.raises(ValueError, match="Insufficient price rows"):
            model._compute_price_features("AAPL", as_of, window)

    def test_realised_vol_positive_for_varying_prices(self):
        """Realised vol is positive when prices vary."""
        np.random.seed(42)
        closes = (100 + np.cumsum(np.random.randn(25))).tolist()
        as_of = date(2025, 6, 30)
        start = as_of - timedelta(days=24)
        df = _make_price_df("AAPL", closes, start)
        model = _build_model(df, window_days=21)

        _, vol = model._compute_price_features("AAPL", as_of, 21)
        assert vol > 0.0


# ---------------------------------------------------------------------------
# _build_score tests
# ---------------------------------------------------------------------------


class TestBuildScore:
    """Tests for BasicAssessmentModel._build_score."""

    def _make_model_with_prices(self, closes, stab_state=None, **kwargs):
        as_of = date(2025, 6, 30)
        start = as_of - timedelta(days=len(closes) - 1)
        df = _make_price_df("AAPL", closes, start)
        return _build_model(df, stab_state=stab_state, **kwargs), as_of

    def test_positive_momentum_buy_signal(self):
        """Strong positive momentum should yield BUY or STRONG_BUY."""
        # 10% rise over window
        closes = np.linspace(100, 110, 25).tolist()
        model, as_of = self._make_model_with_prices(closes, window_days=21)

        score = model._build_score("AAPL", "strat1", "US_EQ", as_of, 21)
        assert score.signal_label in ("BUY", "STRONG_BUY")
        assert score.score > 0.0
        assert score.expected_return > 0.0

    def test_negative_momentum_sell_signal(self):
        """Strong negative momentum should yield SELL or STRONG_SELL."""
        closes = np.linspace(100, 85, 25).tolist()
        model, as_of = self._make_model_with_prices(closes, window_days=21)

        score = model._build_score("AAPL", "strat1", "US_EQ", as_of, 21)
        assert score.signal_label in ("SELL", "STRONG_SELL")
        assert score.score < 0.0

    def test_flat_momentum_hold_signal(self):
        """Near-zero momentum should yield HOLD."""
        closes = [100.0] * 25
        model, as_of = self._make_model_with_prices(closes, window_days=21)

        score = model._build_score("AAPL", "strat1", "US_EQ", as_of, 21)
        assert score.signal_label == "HOLD"
        assert score.score == 0.0

    def test_score_clamped_to_minus_one(self):
        """Very negative momentum should clamp score to -1."""
        closes = np.linspace(100, 30, 25).tolist()  # -70% drop
        model, as_of = self._make_model_with_prices(
            closes, window_days=21, momentum_ref=0.10
        )

        score = model._build_score("AAPL", "strat1", "US_EQ", as_of, 21)
        assert score.score == -1.0

    def test_score_clamped_to_plus_one(self):
        """Very positive momentum should clamp score to +1."""
        closes = np.linspace(100, 200, 25).tolist()  # +100% rise
        model, as_of = self._make_model_with_prices(
            closes, window_days=21, momentum_ref=0.10
        )

        score = model._build_score("AAPL", "strat1", "US_EQ", as_of, 21)
        assert score.score == 1.0

    def test_confidence_zero_for_insufficient_history(self):
        """When price history is insufficient, confidence should be 0."""
        model = _build_model(pd.DataFrame(), window_days=21)

        score = model._build_score("AAPL", "strat1", "US_EQ", date(2025, 6, 30), 21)
        assert score.confidence == 0.0
        assert score.signal_label == "HOLD"
        assert score.metadata["insufficient_history"] is True

    def test_confidence_clamped_to_one(self):
        """Confidence should not exceed 1.0."""
        closes = np.linspace(100, 200, 25).tolist()  # huge momentum
        model, as_of = self._make_model_with_prices(
            closes, window_days=21, momentum_ref=0.10
        )

        score = model._build_score("AAPL", "strat1", "US_EQ", as_of, 21)
        assert score.confidence <= 1.0

    def test_confidence_zero_for_flat_prices(self):
        """Zero momentum should yield zero confidence."""
        closes = [100.0] * 25
        model, as_of = self._make_model_with_prices(closes, window_days=21)

        score = model._build_score("AAPL", "strat1", "US_EQ", as_of, 21)
        assert score.confidence == 0.0

    def test_fragility_penalty_applied(self):
        """STAB state with high fragility should reduce the score."""
        closes = np.linspace(100, 110, 25).tolist()
        stab = FakeSoftTargetState(
            soft_target_score=80.0,
            weak_profile=False,
            soft_target_class=FakeEnum("HIGH"),
        )
        model_no_stab, as_of = self._make_model_with_prices(closes, window_days=21)
        model_stab, _ = self._make_model_with_prices(
            closes, stab_state=stab, window_days=21
        )

        score_clean = model_no_stab._build_score("AAPL", "strat1", "US_EQ", as_of, 21)
        score_penalised = model_stab._build_score("AAPL", "strat1", "US_EQ", as_of, 21)

        assert score_penalised.score < score_clean.score

    def test_weak_profile_amplifies_penalty(self):
        """weak_profile=True should amplify the fragility penalty."""
        closes = np.linspace(100, 110, 25).tolist()
        stab_normal = FakeSoftTargetState(
            soft_target_score=50.0,
            weak_profile=False,
            soft_target_class=FakeEnum("MEDIUM"),
        )
        stab_weak = FakeSoftTargetState(
            soft_target_score=50.0,
            weak_profile=True,
            soft_target_class=FakeEnum("MEDIUM"),
        )
        model_normal, as_of = self._make_model_with_prices(
            closes, stab_state=stab_normal, window_days=21
        )
        model_weak, _ = self._make_model_with_prices(
            closes, stab_state=stab_weak, window_days=21
        )

        score_normal = model_normal._build_score("AAPL", "strat1", "US_EQ", as_of, 21)
        score_weak = model_weak._build_score("AAPL", "strat1", "US_EQ", as_of, 21)

        assert score_weak.score < score_normal.score

    def test_signal_label_strong_buy(self):
        """Adjusted score above strong_buy_threshold → STRONG_BUY."""
        closes = np.linspace(100, 120, 25).tolist()  # 20% rise
        model, as_of = self._make_model_with_prices(
            closes, window_days=21, strong_buy_threshold=0.03
        )

        score = model._build_score("AAPL", "strat1", "US_EQ", as_of, 21)
        assert score.signal_label == "STRONG_BUY"

    def test_signal_label_strong_sell(self):
        """Adjusted score below -strong_sell_threshold → STRONG_SELL."""
        closes = np.linspace(100, 80, 25).tolist()  # -20% drop
        model, as_of = self._make_model_with_prices(
            closes, window_days=21, strong_sell_threshold=0.03
        )

        score = model._build_score("AAPL", "strat1", "US_EQ", as_of, 21)
        assert score.signal_label == "STRONG_SELL"

    def test_metadata_contains_window_days(self):
        """Metadata should record the window_days used."""
        closes = [100.0] * 25
        model, as_of = self._make_model_with_prices(closes, window_days=21)

        score = model._build_score("AAPL", "strat1", "US_EQ", as_of, 21)
        assert score.metadata["window_days"] == 21

    def test_alpha_components_present(self):
        """Alpha components should contain momentum and fragility_penalty."""
        closes = np.linspace(100, 110, 25).tolist()
        model, as_of = self._make_model_with_prices(closes, window_days=21)

        score = model._build_score("AAPL", "strat1", "US_EQ", as_of, 21)
        assert "momentum" in score.alpha_components
        assert "fragility_penalty" in score.alpha_components

    def test_horizon_days_passed_through(self):
        """horizon_days should be recorded in the InstrumentScore."""
        closes = [100.0] * 25
        model, as_of = self._make_model_with_prices(closes, window_days=21)

        score = model._build_score("AAPL", "strat1", "US_EQ", as_of, 42)
        assert score.horizon_days == 42


# ---------------------------------------------------------------------------
# score_instruments tests
# ---------------------------------------------------------------------------


class TestScoreInstruments:
    """Tests for BasicAssessmentModel.score_instruments."""

    def test_scores_multiple_instruments(self):
        """Should return scores for all valid instruments."""
        closes = np.linspace(100, 110, 25).tolist()
        as_of = date(2025, 6, 30)
        start = as_of - timedelta(days=24)

        # Build a DF that works for any instrument_id
        data_reader = MagicMock()
        data_reader.read_prices.return_value = _make_price_df("ANY", closes, start)

        calendar = MagicMock()
        calendar.trading_days_between.side_effect = _trading_days

        model = BasicAssessmentModel(
            data_reader=data_reader,
            calendar=calendar,
            momentum_window_days=21,
        )

        scores = model.score_instruments("strat1", "US_EQ", ["AAPL", "GOOG", "MSFT"], as_of, 21)
        assert len(scores) == 3
        assert set(scores.keys()) == {"AAPL", "GOOG", "MSFT"}

    def test_horizon_days_zero_raises(self):
        """horizon_days <= 0 should raise ValueError."""
        model = _build_model(pd.DataFrame(), window_days=21)
        with pytest.raises(ValueError, match="horizon_days must be positive"):
            model.score_instruments("strat1", "US_EQ", ["AAPL"], date(2025, 6, 30), 0)

    def test_mix_valid_and_insufficient_history(self):
        """Instruments with insufficient data should still return a score (HOLD)."""
        as_of = date(2025, 6, 30)
        start = as_of - timedelta(days=24)

        call_count = [0]
        good_df = _make_price_df("AAPL", np.linspace(100, 110, 25).tolist(), start)
        empty_df = pd.DataFrame()

        def mock_read_prices(ids, start_d, end_d):
            call_count[0] += 1
            if call_count[0] % 2 == 1:
                return good_df
            return empty_df

        data_reader = MagicMock()
        data_reader.read_prices.side_effect = mock_read_prices

        calendar = MagicMock()
        calendar.trading_days_between.side_effect = _trading_days

        model = BasicAssessmentModel(
            data_reader=data_reader,
            calendar=calendar,
            momentum_window_days=21,
        )

        scores = model.score_instruments("strat1", "US_EQ", ["AAPL", "BAD"], as_of, 21)
        assert len(scores) == 2
        # The "BAD" instrument should have insufficient_history
        assert scores["BAD"].metadata["insufficient_history"] is True
        assert scores["BAD"].confidence == 0.0

    def test_empty_instrument_list(self):
        """Empty instrument list returns empty dict."""
        model = _build_model(pd.DataFrame(), window_days=21)
        scores = model.score_instruments("strat1", "US_EQ", [], date(2025, 6, 30), 21)
        assert scores == {}

    def test_single_instrument(self):
        """Single instrument should work fine."""
        closes = [100.0] * 25
        as_of = date(2025, 6, 30)
        start = as_of - timedelta(days=24)
        df = _make_price_df("AAPL", closes, start)
        model = _build_model(df, window_days=21)

        scores = model.score_instruments("strat1", "US_EQ", ["AAPL"], as_of, 21)
        assert len(scores) == 1
        assert "AAPL" in scores
