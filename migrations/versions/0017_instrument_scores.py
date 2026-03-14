"""instrument scores table

Revision ID: 0017
Revises: 0016
Create Date: 2025-11-25

This migration creates the ``instrument_scores`` table in the runtime
database. The table stores per-instrument assessment outputs produced by
the Assessment Engine.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0017"
down_revision: Union[str, None] = "0016"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create instrument_scores table and indexes."""

    op.create_table(
        "instrument_scores",
        sa.Column("score_id", sa.String(length=64), primary_key=True),
        sa.Column("strategy_id", sa.String(length=50), nullable=False),
        sa.Column("market_id", sa.String(length=50), nullable=False),
        sa.Column("instrument_id", sa.String(length=50), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("horizon_days", sa.Integer, nullable=False),
        sa.Column("expected_return", sa.Float, nullable=False),
        sa.Column("score", sa.Float, nullable=False),
        sa.Column("confidence", sa.Float, nullable=False),
        sa.Column("signal_label", sa.String(length=32), nullable=False),
        sa.Column("alpha_components", postgresql.JSONB, nullable=False),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    # Index to support common lookups by strategy and date, matching
    # docs/architecture/30_database_schema.md.
    op.create_index(
        "idx_instrument_scores_strategy_date",
        "instrument_scores",
        ["strategy_id", "as_of_date"],
    )


def downgrade() -> None:
    """Drop instrument_scores table and indexes."""

    op.drop_index("idx_instrument_scores_strategy_date", table_name="instrument_scores")
    op.drop_table("instrument_scores")
