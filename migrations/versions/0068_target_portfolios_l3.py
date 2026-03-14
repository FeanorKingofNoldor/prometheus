"""Layer 3: tighten target_portfolios contracts

Revision ID: 0068_target_portfolios_l3
Revises: 0067_book_targets_l3
Create Date: 2025-12-16

Layer 3 contract for ``target_portfolios``:
- target_id/strategy_id/portfolio_id are non-empty
- target_positions is a JSON object
- target_positions must contain key 'weights' and it must be a JSON object
- metadata is either NULL or a JSON object

Note: weight sum rules and lookahead safety are higher-level audits.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0068_target_portfolios_l3"
down_revision: Union[str, None] = "0067_book_targets_l3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_target_portfolios_target_id_nonempty",
        "target_portfolios",
        "btrim(target_id) <> ''",
    )

    op.create_check_constraint(
        "ck_target_portfolios_strategy_id_nonempty",
        "target_portfolios",
        "btrim(strategy_id) <> ''",
    )

    op.create_check_constraint(
        "ck_target_portfolios_portfolio_id_nonempty",
        "target_portfolios",
        "btrim(portfolio_id) <> ''",
    )

    op.create_check_constraint(
        "ck_target_portfolios_target_positions_object",
        "target_portfolios",
        "jsonb_typeof(target_positions) = 'object'",
    )

    op.create_check_constraint(
        "ck_target_portfolios_target_positions_weights_object",
        "target_portfolios",
        "(target_positions ? 'weights') AND jsonb_typeof(target_positions->'weights') = 'object'",
    )

    op.create_check_constraint(
        "ck_target_portfolios_metadata_object_or_null",
        "target_portfolios",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_target_portfolios_metadata_object_or_null",
        "target_portfolios",
        type_="check",
    )
    op.drop_constraint(
        "ck_target_portfolios_target_positions_weights_object",
        "target_portfolios",
        type_="check",
    )
    op.drop_constraint(
        "ck_target_portfolios_target_positions_object",
        "target_portfolios",
        type_="check",
    )
    op.drop_constraint(
        "ck_target_portfolios_portfolio_id_nonempty",
        "target_portfolios",
        type_="check",
    )
    op.drop_constraint(
        "ck_target_portfolios_strategy_id_nonempty",
        "target_portfolios",
        type_="check",
    )
    op.drop_constraint(
        "ck_target_portfolios_target_id_nonempty",
        "target_portfolios",
        type_="check",
    )
