"""execution core tables

Revision ID: 0020
Revises: 0019
Create Date: 2025-11-28

This migration creates core execution tables in the runtime database:

- orders
- fills
- positions_snapshots

These tables are shared across LIVE, PAPER, and BACKTEST modes. The
schema is aligned with the execution and backtesting spec (015) and the
database schema overview (30).
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0020"
down_revision: Union[str, None] = "0019"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create execution core tables and indexes."""

    # orders: logical orders emitted by the execution planner/router.
    op.create_table(
        "orders",
        sa.Column("order_id", sa.String(length=64), primary_key=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("instrument_id", sa.String(length=64), nullable=False),
        sa.Column("side", sa.String(length=8), nullable=False),
        sa.Column("order_type", sa.String(length=16), nullable=False),
        sa.Column("quantity", sa.Float, nullable=False),
        sa.Column("limit_price", sa.Float, nullable=True),
        sa.Column("stop_price", sa.Float, nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),  # LIVE / PAPER / BACKTEST
        sa.Column("portfolio_id", sa.String(length=64), nullable=True),
        sa.Column("decision_id", sa.String(length=64), nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
    )

    op.create_index(
        "idx_orders_timestamp",
        "orders",
        ["timestamp"],
    )
    op.create_index(
        "idx_orders_portfolio",
        "orders",
        ["portfolio_id"],
    )

    # fills: concrete executions (partial or full) of orders.
    op.create_table(
        "fills",
        sa.Column("fill_id", sa.String(length=64), primary_key=True),
        sa.Column("order_id", sa.String(length=64), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("instrument_id", sa.String(length=64), nullable=False),
        sa.Column("side", sa.String(length=8), nullable=False),
        sa.Column("quantity", sa.Float, nullable=False),
        sa.Column("price", sa.Float, nullable=False),
        sa.Column("commission", sa.Float, nullable=True),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.ForeignKeyConstraint(["order_id"], ["orders.order_id"], name="fk_fills_order"),
    )

    op.create_index(
        "idx_fills_timestamp",
        "fills",
        ["timestamp"],
    )

    # positions_snapshots: point-in-time portfolio holdings.
    op.create_table(
        "positions_snapshots",
        sa.Column("snapshot_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("portfolio_id", sa.String(length=64), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("instrument_id", sa.String(length=64), nullable=False),
        sa.Column("quantity", sa.Float, nullable=False),
        sa.Column("avg_cost", sa.Float, nullable=False),
        sa.Column("market_value", sa.Float, nullable=False),
        sa.Column("unrealized_pnl", sa.Float, nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
    )

    op.create_index(
        "idx_positions_snapshots_portfolio_ts",
        "positions_snapshots",
        ["portfolio_id", "timestamp"],
    )


def downgrade() -> None:
    """Drop execution core tables and indexes."""

    op.drop_index("idx_positions_snapshots_portfolio_ts", table_name="positions_snapshots")
    op.drop_table("positions_snapshots")

    op.drop_index("idx_fills_timestamp", table_name="fills")
    op.drop_table("fills")

    op.drop_index("idx_orders_portfolio", table_name="orders")
    op.drop_index("idx_orders_timestamp", table_name="orders")
    op.drop_table("orders")
