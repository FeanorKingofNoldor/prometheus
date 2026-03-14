"""universe tables

Revision ID: 0008
Revises: 0007
Create Date: 2025-11-24

This migration creates tables used by the Universe engine in the runtime
database:

- universe_members
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0008"
down_revision: Union[str, None] = "0007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create universe_members table and indexes."""

    op.create_table(
        "universe_members",
        sa.Column("universe_member_id", sa.String(length=64), primary_key=True),
        sa.Column("universe_id", sa.String(length=64), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("entity_type", sa.String(length=32), nullable=False),
        sa.Column("entity_id", sa.String(length=64), nullable=False),
        sa.Column("tier", sa.String(length=16), nullable=False, server_default="EXCLUDED"),
        sa.Column("included", sa.Boolean, nullable=False),
        sa.Column("score", sa.Float, nullable=False),
        sa.Column("reasons", postgresql.JSONB, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    # Fast lookups for a given universe/date and entity type.
    op.create_index(
        "idx_universe_members_universe_date",
        "universe_members",
        ["universe_id", "as_of_date", "entity_type", "included"],
    )

    # Ensure one row per (universe_id, as_of_date, entity_type, entity_id).
    op.create_index(
        "ux_universe_members_unique_entity",
        "universe_members",
        ["universe_id", "as_of_date", "entity_type", "entity_id"],
        unique=True,
    )


def downgrade() -> None:
    """Drop universe_members table and indexes."""

    op.drop_index("ux_universe_members_unique_entity", table_name="universe_members")
    op.drop_index("idx_universe_members_universe_date", table_name="universe_members")
    op.drop_table("universe_members")
