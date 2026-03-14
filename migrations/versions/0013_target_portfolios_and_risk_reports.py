"""target portfolios and portfolio risk reports

Revision ID: 0013
Revises: 0012
Create Date: 2025-11-24

This migration creates the ``target_portfolios`` and
``portfolio_risk_reports`` tables used by the Portfolio & Risk Engine in
the runtime database, as described in the architecture schema docs.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0013"
down_revision: Union[str, None] = "0012"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create target_portfolios and portfolio_risk_reports tables."""

    op.create_table(
        "target_portfolios",
        sa.Column("target_id", sa.String(length=64), primary_key=True),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("portfolio_id", sa.String(length=64), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("target_positions", postgresql.JSONB, nullable=False),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_index(
        "idx_target_portfolios_portfolio_date",
        "target_portfolios",
        ["portfolio_id", "as_of_date"],
        unique=False,
    )

    op.create_table(
        "portfolio_risk_reports",
        sa.Column("report_id", sa.String(length=64), primary_key=True),
        sa.Column("portfolio_id", sa.String(length=64), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("portfolio_value", sa.Float, nullable=False),
        sa.Column("cash", sa.Float, nullable=False),
        sa.Column("net_exposure", sa.Float, nullable=False),
        sa.Column("gross_exposure", sa.Float, nullable=False),
        sa.Column("leverage", sa.Float, nullable=False),
        sa.Column("risk_metrics", postgresql.JSONB, nullable=False),
        sa.Column("scenario_pnl", postgresql.JSONB, nullable=True),
        sa.Column("exposures_by_sector", postgresql.JSONB, nullable=True),
        sa.Column("exposures_by_factor", postgresql.JSONB, nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_index(
        "idx_portfolio_risk_reports_portfolio_date",
        "portfolio_risk_reports",
        ["portfolio_id", "as_of_date"],
        unique=False,
    )


def downgrade() -> None:
    """Drop target_portfolios and portfolio_risk_reports tables."""

    op.drop_index("idx_portfolio_risk_reports_portfolio_date", table_name="portfolio_risk_reports")
    op.drop_table("portfolio_risk_reports")

    op.drop_index("idx_target_portfolios_portfolio_date", table_name="target_portfolios")
    op.drop_table("target_portfolios")
