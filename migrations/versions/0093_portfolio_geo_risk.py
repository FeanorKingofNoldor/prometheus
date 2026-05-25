"""Add portfolio_geo_risk_snapshots table.

Revision ID: 0093_portfolio_geo_risk
Revises: 0092_compound_pressure_alerts
Create Date: 2026-05-05

Persists per-portfolio geopolitical risk snapshots produced by calling
``apatheon.portfolio.exposure.analyze_exposure`` against the live IBKR
holdings.  One row per (portfolio_id, as_of_date) — refreshed on each
scan, with the full PortfolioExposure JSON kept as the source of truth.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0093_portfolio_geo_risk"
down_revision: Union[str, None] = "0092_compound_pressure_alerts"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "portfolio_geo_risk_snapshots",
        sa.Column("snapshot_id", sa.String(), primary_key=True),
        sa.Column("portfolio_id", sa.String(), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("overall_risk_score", sa.Float(), nullable=False),
        sa.Column("conflict_risk", sa.Float(), nullable=False, server_default="0"),
        sa.Column("chokepoint_risk", sa.Float(), nullable=False, server_default="0"),
        sa.Column("sovereign_risk", sa.Float(), nullable=False, server_default="0"),
        sa.Column("sector_risk", sa.Float(), nullable=False, server_default="0"),
        sa.Column("ticker_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "exposure",
            sa.dialects.postgresql.JSONB(),
            nullable=False,
            server_default="{}",
        ),
        sa.Column("decision_id", sa.String(), nullable=True),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "portfolio_id",
            "as_of_date",
            name="uq_portfolio_geo_risk_snapshots_pid_date",
        ),
        sa.CheckConstraint(
            "overall_risk_score >= 0 AND overall_risk_score <= 100",
            name="ck_portfolio_geo_risk_overall_range",
        ),
        sa.CheckConstraint(
            "btrim(portfolio_id) <> ''",
            name="ck_portfolio_geo_risk_portfolio_id_nonempty",
        ),
    )

    op.create_index(
        "ix_portfolio_geo_risk_snapshots_as_of_date",
        "portfolio_geo_risk_snapshots",
        ["as_of_date"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_portfolio_geo_risk_snapshots_as_of_date",
        table_name="portfolio_geo_risk_snapshots",
    )
    op.drop_table("portfolio_geo_risk_snapshots")
