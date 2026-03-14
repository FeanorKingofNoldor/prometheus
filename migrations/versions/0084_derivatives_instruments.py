"""Add derivative-specific columns to instruments table

Revision ID: 0084_derivatives_instruments
Revises: 0083_engine_runs_execution_done_phase
Create Date: 2026-03-03

Adds columns for caching discovered options, futures, and futures-options
contracts from IBKR:

- strike: Option/FOP strike price
- right: Option/FOP right ("C" or "P")
- expiry: Expiration date YYYYMMDD (distinct from maturity_date which is a DATE)
- underlying_symbol: Underlying ticker (e.g. "AAPL" for an AAPL option)
- ibkr_con_id: IBKR contract ID for fast re-qualification

Also adds a composite index for efficient chain lookups.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0084_derivatives_instruments"
down_revision: Union[str, None] = "0083_engine_runs_execution_done_phase"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add derivative columns and indexes."""

    # New columns on instruments
    op.add_column(
        "instruments",
        sa.Column("strike", sa.Float, nullable=True),
    )
    op.add_column(
        "instruments",
        sa.Column("right", sa.String(length=2), nullable=True),
    )
    op.add_column(
        "instruments",
        sa.Column("expiry", sa.String(length=8), nullable=True),
    )
    op.add_column(
        "instruments",
        sa.Column("underlying_symbol", sa.String(length=50), nullable=True),
    )
    op.add_column(
        "instruments",
        sa.Column("ibkr_con_id", sa.BigInteger, nullable=True),
    )

    # Composite index for chain lookups:
    # "give me all OPT instruments for AAPL expiring 20260620"
    op.create_index(
        "idx_instruments_deriv_chain",
        "instruments",
        ["asset_class", "underlying_symbol", "expiry"],
    )

    # Unique index on ibkr_con_id for fast contract resolution
    op.create_index(
        "idx_instruments_ibkr_con_id",
        "instruments",
        ["ibkr_con_id"],
        unique=True,
        postgresql_where=sa.text("ibkr_con_id IS NOT NULL"),
    )


def downgrade() -> None:
    """Remove derivative columns and indexes."""

    op.drop_index("idx_instruments_ibkr_con_id", table_name="instruments")
    op.drop_index("idx_instruments_deriv_chain", table_name="instruments")

    op.drop_column("instruments", "ibkr_con_id")
    op.drop_column("instruments", "underlying_symbol")
    op.drop_column("instruments", "expiry")
    op.drop_column("instruments", "right")
    op.drop_column("instruments", "strike")
