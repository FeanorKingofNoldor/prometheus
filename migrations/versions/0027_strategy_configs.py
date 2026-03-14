"""Create strategy_configs table for applied configuration state

Revision ID: 0027_strategy_configs
Revises: 0026_meta_intelligence_tables
Create Date: 2025-12-02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic
revision = "0027_strategy_configs"
down_revision = "0026_meta_intelligence_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "strategy_configs",
        sa.Column("strategy_id", sa.String(length=64), primary_key=True, nullable=False),
        sa.Column("config_json", postgresql.JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_strategy_configs_strategy", "strategy_configs", ["strategy_id"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_strategy_configs_strategy", table_name="strategy_configs")
    op.drop_table("strategy_configs")
