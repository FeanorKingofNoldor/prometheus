"""Prometheus – Account-reset run boundary.

Paper-account resets are recorded in ``account_resets`` (see
``prometheus.scripts.maintenance.wipe_paper_trading``). Any analytics that
aggregate *live* performance must never mix data from before a reset with
data from after it — the account NAV, positions, and even the strategy
config were discontinuous at the boundary.

This module is the single source of truth for "when did the current run
start". Every lookback query in the feedback/self-calibration machinery
clamps its window start with :func:`clamp_window_start`.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from apatheon.core.logging import get_logger

logger = get_logger(__name__)


def current_run_start(db_manager: Any) -> date | None:
    """Return the start date of the current paper run.

    Reads ``MAX(reset_at)::date`` from ``account_resets``.

    Args:
        db_manager: DatabaseManager (runtime DB).

    Returns:
        Date of the most recent account reset, or ``None`` when the table
        is empty/missing (no reset has ever happened — no lower bound).
    """
    try:
        with db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute("SELECT MAX(reset_at)::date FROM account_resets")
                row = cur.fetchone()
            finally:
                cur.close()
        value = row[0] if row else None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return None
    except Exception as exc:
        logger.warning("[run_boundary] current_run_start failed: %s", exc)
        return None


def clamp_window_start(window_start: date, run_start: date | None) -> date:
    """Clamp a lookback window start to the current run boundary.

    Returns ``max(window_start, run_start)``; a ``None`` run_start leaves
    the window untouched.
    """
    if run_start is None:
        return window_start
    return max(window_start, run_start)
