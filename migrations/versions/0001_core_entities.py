"""core entities

Revision ID: 0001
Revises: None
Create Date: 2025-11-24

This migration creates the core entity tables used throughout Prometheus:

- markets
- issuers
- instruments
- portfolios
- strategies
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create core entity tables and indexes."""

    # markets
    op.create_table(
        "markets",
        sa.Column("market_id", sa.String(length=50), primary_key=True),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("region", sa.String(length=50), nullable=False),
        sa.Column("timezone", sa.String(length=50), nullable=False),
        sa.Column("calendar_spec", postgresql.JSONB, nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )

    # issuers
    op.create_table(
        "issuers",
        sa.Column("issuer_id", sa.String(length=50), primary_key=True),
        sa.Column("issuer_type", sa.String(length=50), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("country", sa.String(length=50), nullable=True),
        sa.Column("sector", sa.String(length=100), nullable=True),
        sa.Column("industry", sa.String(length=100), nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )

    # instruments
    op.create_table(
        "instruments",
        sa.Column("instrument_id", sa.String(length=50), primary_key=True),
        sa.Column("issuer_id", sa.String(length=50), sa.ForeignKey("issuers.issuer_id")),
        sa.Column("market_id", sa.String(length=50), sa.ForeignKey("markets.market_id")),
        sa.Column("asset_class", sa.String(length=50), nullable=False),
        sa.Column("symbol", sa.String(length=50), nullable=False),
        sa.Column("exchange", sa.String(length=50), nullable=True),
        sa.Column("currency", sa.String(length=10), nullable=False),
        sa.Column("multiplier", sa.Float, nullable=True),
        sa.Column("maturity_date", sa.Date, nullable=True),
        sa.Column("underlying_instrument_id", sa.String(length=50), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="ACTIVE"),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )

    # portfolios
    op.create_table(
        "portfolios",
        sa.Column("portfolio_id", sa.String(length=50), primary_key=True),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("base_currency", sa.String(length=10), nullable=False),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )

    # strategies
    op.create_table(
        "strategies",
        sa.Column("strategy_id", sa.String(length=50), primary_key=True),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )

    # Indexes for common lookup patterns
    op.create_index("idx_instruments_issuer", "instruments", ["issuer_id"])
    op.create_index("idx_instruments_market", "instruments", ["market_id"])
    op.create_index("idx_instruments_status", "instruments", ["status"])


def downgrade() -> None:
    """Drop core entity tables and indexes."""

    op.drop_index("idx_instruments_status", table_name="instruments")
    op.drop_index("idx_instruments_market", table_name="instruments")
    op.drop_index("idx_instruments_issuer", table_name="instruments")

    op.drop_table("strategies")
    op.drop_table("portfolios")
    op.drop_table("instruments")
    op.drop_table("issuers")
    op.drop_table("markets")
