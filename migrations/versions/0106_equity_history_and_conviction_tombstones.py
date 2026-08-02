"""portfolio_equity_history table + conviction exit tombstones

Revision ID: 0106_equity_history_and_conviction_tombstones
Revises: 0105_conviction_last_target_weight
Create Date: 2026-07-03

Two audit fixes:

1. ``portfolio_equity_history`` — daily NAV per portfolio. The
   RiskCheckingBroker drawdown circuit breaker reads its trailing peak
   from this table; the table never existed, so the breaker silently
   skipped its check on every order since inception. Populated daily by
   the snapshot_positions daemon job.

2. ``position_convictions.exited_at`` — exit tombstones. Exited
   positions previously left their last live row as the "latest" state
   forever, so they kept re-emitting decay exits and were resurrected
   with stale scale-up status and entry prices when re-selected.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0106_equity_history_and_conviction_tombstones"
down_revision: Union[str, None] = "0105_conviction_last_target_weight"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "portfolio_equity_history",
        sa.Column("portfolio_id", sa.Text, nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("equity", sa.Numeric(18, 2), nullable=False),
        sa.Column("cash", sa.Numeric(18, 2), nullable=True),
        sa.Column("gross_position_value", sa.Numeric(18, 2), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("portfolio_id", "as_of_date"),
    )
    op.create_index(
        "ix_portfolio_equity_history_date",
        "portfolio_equity_history",
        ["as_of_date"],
    )

    op.add_column(
        "position_convictions",
        sa.Column("exited_at", sa.Date, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("position_convictions", "exited_at")
    op.drop_index(
        "ix_portfolio_equity_history_date", table_name="portfolio_equity_history"
    )
    op.drop_table("portfolio_equity_history")
