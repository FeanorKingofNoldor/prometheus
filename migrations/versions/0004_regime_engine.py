"""regime engine tables

Revision ID: 0004
Revises: 0003
Create Date: 2025-11-24

This migration creates tables used by the simplified Regime Engine:

- regimes
- regime_transitions
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create regimes and regime_transitions tables."""

    op.create_table(
        "regimes",
        sa.Column("regime_record_id", sa.String(length=64), primary_key=True),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("region", sa.String(length=32), nullable=False),
        sa.Column("regime_label", sa.String(length=32), nullable=False),
        sa.Column("regime_embedding", postgresql.BYTEA, nullable=True),
        sa.Column("embedding_ref", sa.String(length=255), nullable=True),
        sa.Column("confidence", sa.Float, nullable=False),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_index(
        "idx_regimes_region_date",
        "regimes",
        ["region", "as_of_date"],
    )

    op.create_table(
        "regime_transitions",
        sa.Column("transition_id", sa.String(length=64), primary_key=True),
        sa.Column("region", sa.String(length=32), nullable=False),
        sa.Column("from_regime_label", sa.String(length=32), nullable=False),
        sa.Column("to_regime_label", sa.String(length=32), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_index(
        "idx_regime_transitions_region_date",
        "regime_transitions",
        ["region", "as_of_date"],
    )


def downgrade() -> None:
    """Drop regimes and regime_transitions tables."""

    op.drop_index("idx_regime_transitions_region_date", table_name="regime_transitions")
    op.drop_table("regime_transitions")

    op.drop_index("idx_regimes_region_date", table_name="regimes")
    op.drop_table("regimes")
