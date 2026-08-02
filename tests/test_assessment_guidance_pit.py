"""Point-in-time guard for the sector-guidance read in the assessment model.

Regression: the ``corporate_guidance`` query filtered ``filing_date >=
as_of_date - 90d`` with NO upper bound, so backtests/backfills could include
filings dated AFTER as_of_date (look-ahead). The query must now also bound
``filing_date <= as_of_date``.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date

import pandas as pd

from prometheus.assessment.model_basic import BasicAssessmentModel


class _RecordingCursor:
    def __init__(self, sink: list) -> None:
        self._sink = sink

    def execute(self, sql, params=None):
        self._sink.append((str(sql), params))

    def fetchall(self):
        return []

    def fetchone(self):
        return None

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _RecordingConn:
    def __init__(self, sink: list) -> None:
        self._sink = sink

    def cursor(self):
        return _RecordingCursor(self._sink)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _StubReader:
    def read_prices(self, instrument_ids, start_date, end_date):
        return pd.DataFrame()


class _StubCalendar:
    def trading_days_between(self, start, end):
        return []


def _build_db(sink):
    class _DB:
        @contextmanager
        def get_runtime_connection(self):
            yield _RecordingConn(sink)

    return _DB()


def test_guidance_query_bounds_filing_date_at_as_of_date() -> None:
    sink: list = []
    model = BasicAssessmentModel(
        data_reader=_StubReader(),
        calendar=_StubCalendar(),
        db_manager=_build_db(sink),
    )
    as_of = date(2020, 6, 15)

    model.score_instruments(
        strategy_id="S", market_id="US_EQ",
        instrument_ids=["AAA.US"], as_of_date=as_of, horizon_days=21,
    )

    guidance_calls = [
        (sql, params) for sql, params in sink if "corporate_guidance" in sql
    ]
    assert guidance_calls, "guidance query should have been issued"
    sql, params = guidance_calls[0]
    assert "filing_date <= %s" in sql, "guidance query must bound the upper date"
    # Upper bound parameter must be exactly as_of_date (point-in-time).
    assert as_of in tuple(params)
    # Lower bound (90d window) must remain strictly before as_of_date.
    lower = min(p for p in params if isinstance(p, date))
    assert lower < as_of
