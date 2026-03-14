"""ensure universe_members.tier column exists

Revision ID: 0016
Revises: 0015
Create Date: 2025-11-25

This migration adds the ``tier`` column to ``universe_members`` if it is
missing. Earlier development iterations created the table without this
column; later iterations and the UniverseEngine expect it to be present.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0016"
down_revision: Union[str, None] = "0015"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add tier column to universe_members if it does not exist."""

    # Use a raw ALTER TABLE with IF NOT EXISTS for compatibility with
    # databases that may already have the column from earlier migrations.
    op.execute(
        sa.text(
            """
            ALTER TABLE universe_members
            ADD COLUMN IF NOT EXISTS tier VARCHAR(16)
                NOT NULL
                DEFAULT 'EXCLUDED'
            """
        )
    )


def downgrade() -> None:
    """Drop tier column from universe_members if present."""

    op.execute(
        sa.text(
            """
            ALTER TABLE universe_members
            DROP COLUMN IF EXISTS tier
            """
        )
    )