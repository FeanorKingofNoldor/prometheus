"""Tighten strategies + portfolios Layer 0 contracts

Revision ID: 0033_strategies_portfolios_l0
Revises: 0032_issuers_instruments_l0
Create Date: 2025-12-16

This migration tightens Layer 0 contracts for:
- strategies
- portfolios

Changes
-------
strategies:
- Require strategy_id to be non-empty (PK ensures uniqueness but not non-empty).
- Require name to be non-empty.

portfolios:
- Require portfolio_id to be non-empty.
- Require name to be non-empty.
- Require base_currency to look like a 3-letter uppercase currency code.

Rationale
---------
These tables represent canonical entities used throughout the system.
Basic non-empty checks prevent silent misconfigurations.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0033_strategies_portfolios_l0"
down_revision: Union[str, None] = "0032_issuers_instruments_l0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # strategies
    op.create_check_constraint(
        "ck_strategies_strategy_id_nonempty",
        "strategies",
        "char_length(btrim(strategy_id)) > 0",
    )
    op.create_check_constraint(
        "ck_strategies_name_nonempty",
        "strategies",
        "char_length(btrim(name)) > 0",
    )

    # portfolios
    op.create_check_constraint(
        "ck_portfolios_portfolio_id_nonempty",
        "portfolios",
        "char_length(btrim(portfolio_id)) > 0",
    )
    op.create_check_constraint(
        "ck_portfolios_name_nonempty",
        "portfolios",
        "char_length(btrim(name)) > 0",
    )
    op.create_check_constraint(
        "ck_portfolios_base_currency_format",
        "portfolios",
        "base_currency ~ '^[A-Z]{3}$'",
    )


def downgrade() -> None:
    # portfolios
    op.drop_constraint(
        "ck_portfolios_base_currency_format",
        "portfolios",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolios_name_nonempty",
        "portfolios",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolios_portfolio_id_nonempty",
        "portfolios",
        type_="check",
    )

    # strategies
    op.drop_constraint(
        "ck_strategies_name_nonempty",
        "strategies",
        type_="check",
    )
    op.drop_constraint(
        "ck_strategies_strategy_id_nonempty",
        "strategies",
        type_="check",
    )
