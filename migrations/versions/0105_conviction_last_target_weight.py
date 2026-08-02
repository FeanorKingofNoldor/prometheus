"""add last_target_weight to position_convictions

Revision ID: 0105_conviction_last_target_weight
Revises: 0104_instrument_scores_dedup_index
Create Date: 2026-06-11

Persists the last real target weight assigned to a conviction-held
position so that positions kept alive by conviction (held but not in
today's selection) re-use their own last weight instead of being
silently re-sized to the average weight of the selected names.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0105_conviction_last_target_weight"
down_revision: Union[str, None] = "0104_instrument_scores_dedup_index"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "position_convictions",
        sa.Column("last_target_weight", sa.Float, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("position_convictions", "last_target_weight")
