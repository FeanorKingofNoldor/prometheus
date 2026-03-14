"""
Migration 0021: Create market_holidays table and unique index.

Author: Prometheus Team
Created: 2025-12-01
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0023_market_holidays_table"
down_revision = "0022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "market_holidays",
        sa.Column("market_id", sa.String(length=50), nullable=False),
        sa.Column("holiday_date", sa.Date(), nullable=False),
        sa.Column("holiday_name", sa.String(length=200), nullable=False),
        sa.Column("source", sa.String(length=50), nullable=False, server_default="eodhd"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("NOW()")),
        schema=None,
    )
    op.create_unique_constraint(
        "uq_market_holidays_market_date",
        "market_holidays",
        ["market_id", "holiday_date"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_market_holidays_market_date", "market_holidays", type_="unique")
    op.drop_table("market_holidays")