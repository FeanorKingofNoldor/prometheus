"""profiles table

Revision ID: 0007
Revises: 0006
Create Date: 2025-11-24

This migration creates the `profiles` table in the runtime database as
specified in 020_data_model.md.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0007"
down_revision: Union[str, None] = "0006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create the profiles table."""

    op.create_table(
        "profiles",
        sa.Column("profile_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("issuer_id", sa.String(length=50), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("structured", postgresql.JSONB, nullable=False),
        sa.Column("embedding_vector_ref", sa.String(length=255), nullable=True),
        sa.Column("risk_flags", postgresql.JSONB, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    # Enforce uniqueness of issuer/as_of_date pairs for upserts.
    op.create_index(
        "idx_profiles_issuer_date",
        "profiles",
        ["issuer_id", "as_of_date"],
        unique=True,
    )


def downgrade() -> None:
    """Drop the profiles table."""

    op.drop_index("idx_profiles_issuer_date", table_name="profiles")
    op.drop_table("profiles")