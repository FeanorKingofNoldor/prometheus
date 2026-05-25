"""Backtest-vs-live performance drift tracking.

Revision ID: 0101_backtest_live_drift
Revises: 0100_notifications
Create Date: 2026-05-25

Stores daily snapshots of the drift between what the most-recent
backtest predicted for a strategy and what live trading actually
produced. The drift comparator computes Sharpe/return/max-drawdown
deltas per (strategy, horizon) and a coarse severity bucket; the
notifications rule engine fires a ``drift_alert`` notification on
warning + critical severities.

One row per (as_of_date, strategy_id, horizon_days). Re-running the
comparator on the same day overwrites that row (UPSERT via the
unique index).
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0101_backtest_live_drift"
down_revision: Union[str, None] = "0100_notifications"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "backtest_live_drift",
        sa.Column("drift_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("horizon_days", sa.Integer, nullable=False),
        # Sample sizes
        sa.Column("n_live_outcomes", sa.Integer, nullable=False, server_default="0"),
        sa.Column("backtest_run_id", sa.String(length=64), nullable=True),
        # Sharpe
        sa.Column("live_sharpe", sa.Float, nullable=True),
        sa.Column("backtest_sharpe", sa.Float, nullable=True),
        sa.Column("sharpe_delta", sa.Float, nullable=True),
        # Cumulative return (live = sum realized_return; backtest = run's metric)
        sa.Column("live_return", sa.Float, nullable=True),
        sa.Column("backtest_return", sa.Float, nullable=True),
        sa.Column("return_delta", sa.Float, nullable=True),
        # Max drawdown (positive numbers; live = LivePerformanceTracker; backtest = run metric)
        sa.Column("live_max_drawdown", sa.Float, nullable=True),
        sa.Column("backtest_max_drawdown", sa.Float, nullable=True),
        sa.Column("max_drawdown_delta", sa.Float, nullable=True),
        # Severity bucket for alerting: info / warning / critical
        sa.Column("severity", sa.String(length=16), nullable=False, server_default="info"),
        sa.Column("notes", sa.String(length=300), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )

    # One row per (as_of, strategy, horizon). UPSERT key.
    op.create_index(
        "ux_drift_date_strategy_horizon",
        "backtest_live_drift",
        ["as_of_date", "strategy_id", "horizon_days"],
        unique=True,
    )
    # Common dashboard query: "drift for strategy X over time"
    op.create_index(
        "idx_drift_strategy_date",
        "backtest_live_drift",
        ["strategy_id", "as_of_date"],
    )
    # Severity scan for the alert rule
    op.create_index(
        "idx_drift_severity_date",
        "backtest_live_drift",
        ["severity", "as_of_date"],
    )


def downgrade() -> None:
    op.drop_index("idx_drift_severity_date", table_name="backtest_live_drift")
    op.drop_index("idx_drift_strategy_date", table_name="backtest_live_drift")
    op.drop_index("ux_drift_date_strategy_horizon", table_name="backtest_live_drift")
    op.drop_table("backtest_live_drift")
