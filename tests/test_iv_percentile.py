"""Tests for IV percentile reader.

DB access is patched so we test the logic without a live DB.
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from prometheus.calendar.iv_percentile import (
    is_iv_cheap,
    is_iv_rich,
    iv_percentile,
)


def _mock_db_rows(rows):
    """Build a context-manager mock that returns ``rows`` from
    ``cur.fetchall()``."""
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur_cm = MagicMock(__enter__=MagicMock(return_value=cur),
                       __exit__=MagicMock(return_value=False))
    conn = MagicMock(cursor=MagicMock(return_value=cur_cm))
    conn_cm = MagicMock(__enter__=MagicMock(return_value=conn),
                        __exit__=MagicMock(return_value=False))
    db = MagicMock(get_runtime_connection=MagicMock(return_value=conn_cm))
    return db


def test_iv_percentile_returns_none_with_no_db():
    # Pretend get_db_manager throws — the function must swallow it
    with patch("apatheon.core.database.get_db_manager",
               side_effect=RuntimeError("no db")):
        assert iv_percentile("SPY", date(2026, 6, 9)) is None


def test_iv_percentile_returns_none_when_too_few_observations():
    today = date(2026, 6, 9)
    rows = [(today - timedelta(days=i), 0.20) for i in range(10)]
    rows.append((today, 0.20))
    with patch("apatheon.core.database.get_db_manager",
               return_value=_mock_db_rows(rows)):
        # min_observations defaults to 30; we have 11
        assert iv_percentile("SPY", today) is None


def test_iv_percentile_at_median_returns_around_half():
    today = date(2026, 6, 9)
    # 40 days at IV=0.10 + today at IV=0.15 → today's 0.15 is above all
    # historical → percentile ~ 1.0
    rows = [(today - timedelta(days=i + 1), 0.10) for i in range(40)]
    rows.append((today, 0.15))
    with patch("apatheon.core.database.get_db_manager",
               return_value=_mock_db_rows(rows)):
        pct = iv_percentile("SPY", today, min_observations=20)
        assert pct == pytest.approx(1.0)


def test_iv_percentile_today_below_history_returns_zero():
    today = date(2026, 6, 9)
    rows = [(today - timedelta(days=i + 1), 0.50) for i in range(40)]
    rows.append((today, 0.10))
    with patch("apatheon.core.database.get_db_manager",
               return_value=_mock_db_rows(rows)):
        pct = iv_percentile("SPY", today, min_observations=20)
        assert pct == pytest.approx(0.0)


def test_iv_percentile_today_at_median():
    today = date(2026, 6, 9)
    # 20 days at 0.10, 20 days at 0.30, today at 0.20 → ~half below
    rows = []
    rows.extend((today - timedelta(days=i + 1), 0.10) for i in range(20))
    rows.extend((today - timedelta(days=i + 21), 0.30) for i in range(20))
    rows.append((today, 0.20))
    with patch("apatheon.core.database.get_db_manager",
               return_value=_mock_db_rows(rows)):
        pct = iv_percentile("SPY", today, min_observations=10)
        assert pct == pytest.approx(0.5, abs=0.05)


def test_iv_percentile_returns_none_when_today_missing():
    today = date(2026, 6, 9)
    rows = [(today - timedelta(days=i + 1), 0.20) for i in range(40)]
    # No row for today
    with patch("apatheon.core.database.get_db_manager",
               return_value=_mock_db_rows(rows)):
        assert iv_percentile("SPY", today, min_observations=20) is None


# ── Convenience predicates ──────────────────────────────────────────


def test_is_iv_rich_passes_when_pct_above_threshold():
    today = date(2026, 6, 9)
    rows = [(today - timedelta(days=i + 1), 0.10) for i in range(40)]
    rows.append((today, 0.30))
    with patch("apatheon.core.database.get_db_manager",
               return_value=_mock_db_rows(rows)):
        assert is_iv_rich("SPY", today, threshold=0.80) is True


def test_is_iv_cheap_passes_when_pct_below_threshold():
    today = date(2026, 6, 9)
    rows = [(today - timedelta(days=i + 1), 0.30) for i in range(40)]
    rows.append((today, 0.05))
    with patch("apatheon.core.database.get_db_manager",
               return_value=_mock_db_rows(rows)):
        assert is_iv_cheap("SPY", today, threshold=0.20) is True


def test_is_iv_rich_returns_none_when_no_signal():
    """Insufficient data → None, not False — caller distinguishes."""
    with patch("apatheon.core.database.get_db_manager",
               side_effect=RuntimeError("no db")):
        assert is_iv_rich("SPY", date(2026, 6, 9)) is None
        assert is_iv_cheap("SPY", date(2026, 6, 9)) is None
