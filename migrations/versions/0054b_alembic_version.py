"""Widen alembic_version.version_num to support long revision IDs

Revision ID: 0054b_alembic_version
Revises: 0054_text_embeddings_l2
Create Date: 2025-12-16

Alembic's default version table uses VARCHAR(32) for ``version_num``.
Our revision identifiers are descriptive and can exceed 32 characters
(e.g. ``0055_numeric_window_embeddings_l2``), which would otherwise fail
when Alembic updates the version table.

This migration widens ``alembic_version.version_num`` to VARCHAR(255).
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "0054b_alembic_version"
down_revision: Union[str, None] = "0054_text_embeddings_l2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "alembic_version",
        "version_num",
        existing_type=sa.String(length=32),
        type_=sa.String(length=255),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "alembic_version",
        "version_num",
        existing_type=sa.String(length=255),
        type_=sa.String(length=32),
        existing_nullable=False,
    )
