"""historical fundamentals tables

Revision ID: 0012
Revises: 0011
Create Date: 2025-11-24

This migration adds fundamentals storage tables to the historical
database as described in the data model and ingestion specs:

- financial_statements
- fundamental_ratios
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0012"
down_revision: Union[str, None] = "0011"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create financial_statements and fundamental_ratios tables."""

    op.create_table(
        "financial_statements",
        sa.Column("statement_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("issuer_id", sa.String(length=50), nullable=False),
        sa.Column("fiscal_period", sa.String(length=16), nullable=False),
        sa.Column("fiscal_year", sa.Integer, nullable=False),
        sa.Column("statement_type", sa.String(length=16), nullable=False),
        sa.Column("report_date", sa.Date, nullable=False),
        sa.Column("period_start", sa.Date, nullable=True),
        sa.Column("period_end", sa.Date, nullable=True),
        sa.Column("currency", sa.String(length=10), nullable=True),
        sa.Column("values", postgresql.JSONB, nullable=False),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
    )

    op.create_index(
        "idx_financial_statements_issuer_type_end",
        "financial_statements",
        ["issuer_id", "statement_type", "period_end"],
        unique=True,
    )

    op.create_table(
        "fundamental_ratios",
        sa.Column("issuer_id", sa.String(length=50), nullable=False),
        sa.Column("period_start", sa.Date, nullable=False),
        sa.Column("period_end", sa.Date, nullable=False),
        sa.Column("frequency", sa.String(length=16), nullable=False),
        sa.Column("roe", sa.Float, nullable=True),
        sa.Column("roic", sa.Float, nullable=True),
        sa.Column("gross_margin", sa.Float, nullable=True),
        sa.Column("op_margin", sa.Float, nullable=True),
        sa.Column("net_margin", sa.Float, nullable=True),
        sa.Column("leverage", sa.Float, nullable=True),
        sa.Column("interest_coverage", sa.Float, nullable=True),
        sa.Column("revenue_growth", sa.Float, nullable=True),
        sa.Column("eps_growth", sa.Float, nullable=True),
        sa.Column("metrics", postgresql.JSONB, nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.PrimaryKeyConstraint(
            "issuer_id", "period_start", "period_end", "frequency", name="pk_fundamental_ratios"
        ),
    )


def downgrade() -> None:
    """Drop fundamentals tables."""

    op.drop_table("fundamental_ratios")
    op.drop_index("idx_financial_statements_issuer_type_end", table_name="financial_statements")
    op.drop_table("financial_statements")
