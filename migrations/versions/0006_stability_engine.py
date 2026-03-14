"""stability engine tables

Revision ID: 0006
Revises: 0005
Create Date: 2025-11-24

This migration creates tables used by the Stability / Soft Target
(STAB) engine in the runtime database:

- stability_vectors
- soft_target_classes
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0006"
down_revision: Union[str, None] = "0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create stability and soft target tables."""

    # stability_vectors: continuous stability / fragility vectors per entity.
    op.create_table(
        "stability_vectors",
        sa.Column("stability_id", sa.String(length=64), primary_key=True),
        sa.Column("entity_type", sa.String(length=32), nullable=False),
        sa.Column("entity_id", sa.String(length=64), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("vector_components", postgresql.JSONB, nullable=False),
        sa.Column("overall_score", sa.Float, nullable=False),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_index(
        "idx_stability_vectors_entity_date",
        "stability_vectors",
        ["entity_type", "entity_id", "as_of_date"],
    )

    # soft_target_classes: discrete soft-target state derived from stability.
    op.create_table(
        "soft_target_classes",
        sa.Column("soft_target_id", sa.String(length=64), primary_key=True),
        sa.Column("entity_type", sa.String(length=32), nullable=False),
        sa.Column("entity_id", sa.String(length=64), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("soft_target_class", sa.String(length=32), nullable=False),
        sa.Column("soft_target_score", sa.Float, nullable=False),
        sa.Column("weak_profile", sa.Boolean, nullable=False),
        sa.Column("instability", sa.Float, nullable=False),
        sa.Column("high_fragility", sa.Float, nullable=False),
        sa.Column("complacent_pricing", sa.Float, nullable=False),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_index(
        "idx_soft_target_classes_entity_date",
        "soft_target_classes",
        ["entity_type", "entity_id", "as_of_date"],
    )


def downgrade() -> None:
    """Drop stability and soft target tables."""

    op.drop_index("idx_soft_target_classes_entity_date", table_name="soft_target_classes")
    op.drop_table("soft_target_classes")

    op.drop_index("idx_stability_vectors_entity_date", table_name="stability_vectors")
    op.drop_table("stability_vectors")
