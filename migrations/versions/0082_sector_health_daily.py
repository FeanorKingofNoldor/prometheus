"""add sector_health_daily table for live pipeline sector health tracking

Revision ID: 0082_sector_health_daily
Revises: 0081_position_convictions
Create Date: 2026-03-01

Stores daily per-sector health index (SHI) scores computed by the
SectorHealthEngine and consumed by the SectorAllocator during the
live daily pipeline.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision: str = "0082_sector_health_daily"
down_revision: Union[str, None] = "0081_position_convictions"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "sector_health_daily",
        sa.Column("sector_name", sa.String(length=64), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("score", sa.Float, nullable=False),
        sa.Column("raw_composite", sa.Float, nullable=True),
        sa.Column("signals", JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("sector_name", "as_of_date"),
    )

    # Fast lookup by date (load all sectors for a given day).
    op.create_index(
        "idx_sector_health_daily_date",
        "sector_health_daily",
        ["as_of_date"],
    )


def downgrade() -> None:
    op.drop_index("idx_sector_health_daily_date", table_name="sector_health_daily")
    op.drop_table("sector_health_daily")
