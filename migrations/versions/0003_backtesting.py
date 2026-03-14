"""backtesting result tables

Revision ID: 0003
Revises: 0002
Create Date: 2025-11-24

This migration creates basic tables used to store backtest metadata,
trades, and equity curves:

- backtest_runs
- backtest_trades
- backtest_daily_equity
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create backtesting result tables."""

    op.create_table(
        "backtest_runs",
        sa.Column("run_id", sa.String(length=64), primary_key=True),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("config_json", postgresql.JSONB, nullable=False),
        sa.Column("start_date", sa.Date, nullable=False),
        sa.Column("end_date", sa.Date, nullable=False),
        sa.Column("universe_id", sa.String(length=64), nullable=True),
        sa.Column("metrics_json", postgresql.JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )

    op.create_table(
        "backtest_trades",
        sa.Column("trade_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("trade_date", sa.Date, nullable=False),
        sa.Column("ticker", sa.String(length=50), nullable=False),
        sa.Column("direction", sa.String(length=8), nullable=False),
        sa.Column("size", sa.Float, nullable=False),
        sa.Column("price", sa.Float, nullable=False),
        sa.Column("regime_id", sa.String(length=64), nullable=True),
        sa.Column("universe_id", sa.String(length=64), nullable=True),
        sa.Column("profile_version_id", sa.BigInteger, nullable=True),
        sa.Column("decision_metadata_json", postgresql.JSONB, nullable=True),
        sa.ForeignKeyConstraint(["run_id"], ["backtest_runs.run_id"], name="fk_backtest_trades_run"),
    )

    op.create_index(
        "idx_backtest_trades_run_date",
        "backtest_trades",
        ["run_id", "trade_date"],
    )

    op.create_table(
        "backtest_daily_equity",
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("equity_curve_value", sa.Float, nullable=False),
        sa.Column("drawdown", sa.Float, nullable=True),
        sa.Column("exposure_metrics_json", postgresql.JSONB, nullable=True),
        sa.ForeignKeyConstraint(["run_id"], ["backtest_runs.run_id"], name="fk_backtest_equity_run"),
        sa.PrimaryKeyConstraint("run_id", "date", name="pk_backtest_daily_equity"),
    )


def downgrade() -> None:
    """Drop backtesting result tables."""

    op.drop_table("backtest_daily_equity")
    op.drop_index("idx_backtest_trades_run_date", table_name="backtest_trades")
    op.drop_table("backtest_trades")
    op.drop_table("backtest_runs")
