"""book targets

Revision ID: 0010
Revises: 0009
Create Date: 2025-11-24

This migration creates the ``book_targets`` table used to store per-book
per-date target weights for entities (e.g. instruments).
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0010"
down_revision: Union[str, None] = "0009"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create book_targets table and indexes."""

    op.create_table(
        "book_targets",
        sa.Column("target_id", sa.String(length=64), primary_key=True),
        sa.Column("book_id", sa.String(length=64), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("region", sa.String(length=32), nullable=False),
        sa.Column("entity_type", sa.String(length=32), nullable=False),
        sa.Column("entity_id", sa.String(length=64), nullable=False),
        sa.Column("target_weight", sa.Float, nullable=False),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    # One target row per (book_id, date, region, entity) with upserts.
    op.create_index(
        "ux_book_targets_book_date_entity",
        "book_targets",
        ["book_id", "as_of_date", "region", "entity_type", "entity_id"],
        unique=True,
    )

    op.create_index(
        "idx_book_targets_book_date",
        "book_targets",
        ["book_id", "as_of_date"],
        unique=False,
    )


def downgrade() -> None:
    """Drop book_targets table and indexes."""

    op.drop_index("idx_book_targets_book_date", table_name="book_targets")
    op.drop_index("ux_book_targets_book_date_entity", table_name="book_targets")
    op.drop_table("book_targets")
