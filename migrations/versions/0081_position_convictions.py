"""add position_convictions table for conviction-based exits

Revision ID: 0081_position_convictions
Revises: 0080_nation_relationships_position_occupancy_l0
Create Date: 2026-03-01

Stores per-instrument conviction state used by the ConvictionTracker
to implement conviction-based position lifecycle management.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0081_position_convictions"
down_revision: Union[str, None] = "0080_nation_relationships_position_occupancy_l0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "position_convictions",
        sa.Column("portfolio_id", sa.String(length=64), nullable=False),
        sa.Column("instrument_id", sa.String(length=64), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("conviction_score", sa.Float, nullable=False),
        sa.Column("entry_date", sa.Date, nullable=False),
        sa.Column("avg_entry_price", sa.Float, nullable=False),
        sa.Column("consecutive_selected", sa.Integer, nullable=False, server_default=sa.text("0")),
        sa.Column("is_scaled_up", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("portfolio_id", "instrument_id", "as_of_date"),
    )

    # Fast lookup: load latest states per portfolio.
    op.create_index(
        "idx_position_convictions_portfolio_date",
        "position_convictions",
        ["portfolio_id", "as_of_date"],
    )

    # Per-instrument time series.
    op.create_index(
        "idx_position_convictions_instrument_date",
        "position_convictions",
        ["instrument_id", "as_of_date"],
    )


def downgrade() -> None:
    op.drop_index("idx_position_convictions_instrument_date", table_name="position_convictions")
    op.drop_index("idx_position_convictions_portfolio_date", table_name="position_convictions")
    op.drop_table("position_convictions")
