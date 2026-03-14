"""market data tables

Revision ID: 0002
Revises: 0001
Create Date: 2025-11-24

This migration creates market data tables in the historical database:

- prices_daily
- returns_daily
- volatility_daily
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create market data tables and indexes."""

    # prices_daily
    op.create_table(
        "prices_daily",
        sa.Column("instrument_id", sa.String(length=50), nullable=False),
        sa.Column("trade_date", sa.Date, nullable=False),
        sa.Column("open", sa.Float, nullable=False),
        sa.Column("high", sa.Float, nullable=False),
        sa.Column("low", sa.Float, nullable=False),
        sa.Column("close", sa.Float, nullable=False),
        sa.Column("adjusted_close", sa.Float, nullable=False),
        sa.Column("volume", sa.Float, nullable=False),
        sa.Column("currency", sa.String(length=10), nullable=False),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.PrimaryKeyConstraint("instrument_id", "trade_date", name="pk_prices_daily"),
    )

    # returns_daily
    op.create_table(
        "returns_daily",
        sa.Column("instrument_id", sa.String(length=50), nullable=False),
        sa.Column("trade_date", sa.Date, nullable=False),
        sa.Column("ret_1d", sa.Float, nullable=False),
        sa.Column("ret_5d", sa.Float, nullable=False),
        sa.Column("ret_21d", sa.Float, nullable=False),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.PrimaryKeyConstraint("instrument_id", "trade_date", name="pk_returns_daily"),
    )

    # volatility_daily
    op.create_table(
        "volatility_daily",
        sa.Column("instrument_id", sa.String(length=50), nullable=False),
        sa.Column("trade_date", sa.Date, nullable=False),
        sa.Column("vol_21d", sa.Float, nullable=False),
        sa.Column("vol_63d", sa.Float, nullable=False),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.PrimaryKeyConstraint("instrument_id", "trade_date", name="pk_volatility_daily"),
    )

    # Helpful indexes
    op.create_index("idx_prices_daily_date", "prices_daily", ["trade_date"])
    op.create_index("idx_returns_daily_date", "returns_daily", ["trade_date"])
    op.create_index("idx_volatility_daily_date", "volatility_daily", ["trade_date"])


def downgrade() -> None:
    """Drop market data tables and indexes."""

    op.drop_index("idx_volatility_daily_date", table_name="volatility_daily")
    op.drop_index("idx_returns_daily_date", table_name="returns_daily")
    op.drop_index("idx_prices_daily_date", table_name="prices_daily")

    op.drop_table("volatility_daily")
    op.drop_table("returns_daily")
    op.drop_table("prices_daily")
