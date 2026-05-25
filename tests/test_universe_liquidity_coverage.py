"""Unit tests for the universe liquidity-feature history tolerance.

Regression coverage for the failure mode where a few missed upstream
price-ingestion days collapsed the entire universe to the only
always-ingested name (SPY.US): ``_compute_liquidity_features`` used to
require a price on *every* one of the last ``window_days`` trading days.
It now tolerates a small gap (``min_history_coverage``).
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from prometheus.universe.engine import BasicUniverseModel


class _StubCalendar:
    """Returns a contiguous run of weekday "trading days"."""

    def trading_days_between(self, start: date, end: date) -> list[date]:
        days: list[date] = []
        d = start
        while d <= end:
            if d.weekday() < 5:
                days.append(d)
            d += timedelta(days=1)
        return days


class _StubReader:
    """Yields a price frame covering ``present`` of the requested days."""

    def __init__(self, present: int) -> None:
        self._present = present

    def read_prices(self, instrument_ids, start: date, end: date) -> pd.DataFrame:
        cal = _StubCalendar()
        days = cal.trading_days_between(start, end)[: self._present]
        return pd.DataFrame(
            {
                "instrument_id": [instrument_ids[0]] * len(days),
                "trade_date": days,
                "close": [100.0 + i for i in range(len(days))],
                "volume": [1_000_000.0] * len(days),
            }
        )


def _model(present: int, *, coverage: float = 0.9) -> BasicUniverseModel:
    return BasicUniverseModel(
        db_manager=None,
        calendar=_StubCalendar(),
        data_reader=_StubReader(present),
        profile_service=None,
        stability_storage=None,
        window_days=63,
        min_history_coverage=coverage,
    )


AS_OF = date(2026, 5, 18)


def test_full_coverage_passes() -> None:
    feats = _model(63)._compute_liquidity_features("SPY.US", AS_OF)
    assert feats and feats["avg_volume_63d"] == 1_000_000.0


def test_small_gap_now_passes() -> None:
    # 60/63 days present (~95%): used to be rejected, must now pass.
    feats = _model(60)._compute_liquidity_features("XLE.US", AS_OF)
    assert feats, "instrument with a small ingestion gap should remain eligible"


def test_large_gap_still_excluded() -> None:
    # 30/63 days (~48%): well below tolerance, must still be excluded.
    assert _model(30)._compute_liquidity_features("THIN.US", AS_OF) == {}


def test_coverage_threshold_is_configurable() -> None:
    # With a strict 100% threshold the same small gap is rejected again.
    assert _model(60, coverage=1.0)._compute_liquidity_features("XLE.US", AS_OF) == {}
