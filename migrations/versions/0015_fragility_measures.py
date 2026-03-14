"""fragility measures table

Revision ID: 0015
Revises: 0014
Create Date: 2025-11-25

This migration creates the ``fragility_measures`` table used by the
Fragility Alpha Engine (135_spec) in the runtime database.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0015"
down_revision: Union[str, None] = "0014"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create fragility_measures table."""

    op.create_table(
        "fragility_measures",
        sa.Column("fragility_id", sa.String(length=64), primary_key=True),
        sa.Column("entity_type", sa.String(length=32), nullable=False),
        sa.Column("entity_id", sa.String(length=64), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("fragility_score", sa.Float, nullable=False),
        sa.Column("scenario_losses", postgresql.JSONB, nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_index(
        "idx_fragility_measures_entity_date",
        "fragility_measures",
        ["entity_type", "entity_id", "as_of_date"],
        unique=False,
    )


def downgrade() -> None:
    """Drop fragility_measures table."""

    op.drop_index("idx_fragility_measures_entity_date", table_name="fragility_measures")
    op.drop_table("fragility_measures")
