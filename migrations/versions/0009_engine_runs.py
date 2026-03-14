"""engine runs state machine

Revision ID: 0009
Revises: 0008
Create Date: 2025-11-24

This migration creates the ``engine_runs`` table used by the pipeline
state machine to track per-date, per-region engine phases.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0009"
down_revision: Union[str, None] = "0008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create engine_runs table and indexes."""

    op.create_table(
        "engine_runs",
        sa.Column("run_id", sa.String(length=64), primary_key=True),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("region", sa.String(length=32), nullable=False),
        sa.Column("phase", sa.String(length=32), nullable=False),
        sa.Column("error", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("phase_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("phase_completed_at", sa.DateTime(timezone=True), nullable=True),
    )

    # One logical run per (as_of_date, region) for now. If we ever want
    # multiple runs per (date, region), this uniqueness constraint can be
    # relaxed in a later migration.
    op.create_index(
        "ux_engine_runs_date_region",
        "engine_runs",
        ["as_of_date", "region"],
        unique=True,
    )


def downgrade() -> None:
    """Drop engine_runs table and indexes."""

    op.drop_index("ux_engine_runs_date_region", table_name="engine_runs")
    op.drop_table("engine_runs")
