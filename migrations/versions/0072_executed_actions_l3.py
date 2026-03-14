"""Layer 3: tighten executed_actions contracts

Revision ID: 0072_executed_actions_l3
Revises: 0071_risk_actions_l3
Create Date: 2025-12-16

Layer 3 contract for ``executed_actions``:
- action_id and side are non-empty
- side is in an allowed set
- quantity is finite and > 0
- price is finite and >= 0
- slippage/fees are finite when present
- decision_id/run_id/portfolio_id/instrument_id are either NULL or non-empty
- metadata is either NULL or a JSON object

Note: executed_actions may contain synthetic instrument_id values; strict
referential checks are validated at the application layer.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0072_executed_actions_l3"
down_revision: Union[str, None] = "0071_risk_actions_l3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_executed_actions_action_id_nonempty",
        "executed_actions",
        "btrim(action_id) <> ''",
    )

    op.create_check_constraint(
        "ck_executed_actions_side_nonempty",
        "executed_actions",
        "btrim(side) <> ''",
    )

    op.create_check_constraint(
        "ck_executed_actions_side_allowed",
        "executed_actions",
        "side IN ('BUY', 'SELL')",
    )

    op.create_check_constraint(
        "ck_executed_actions_quantity_finite",
        "executed_actions",
        f"quantity NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_executed_actions_quantity_positive",
        "executed_actions",
        "quantity > 0.0",
    )

    op.create_check_constraint(
        "ck_executed_actions_price_finite",
        "executed_actions",
        f"price NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_executed_actions_price_nonnegative",
        "executed_actions",
        "price >= 0.0",
    )

    op.create_check_constraint(
        "ck_executed_actions_slippage_finite_when_present",
        "executed_actions",
        f"slippage IS NULL OR slippage NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_executed_actions_fees_finite_when_present",
        "executed_actions",
        f"fees IS NULL OR fees NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_executed_actions_decision_id_nonempty_when_present",
        "executed_actions",
        "decision_id IS NULL OR btrim(decision_id) <> ''",
    )

    op.create_check_constraint(
        "ck_executed_actions_run_id_nonempty_when_present",
        "executed_actions",
        "run_id IS NULL OR btrim(run_id) <> ''",
    )

    op.create_check_constraint(
        "ck_executed_actions_portfolio_id_nonempty_when_present",
        "executed_actions",
        "portfolio_id IS NULL OR btrim(portfolio_id) <> ''",
    )

    op.create_check_constraint(
        "ck_executed_actions_instrument_id_nonempty_when_present",
        "executed_actions",
        "instrument_id IS NULL OR btrim(instrument_id) <> ''",
    )

    op.create_check_constraint(
        "ck_executed_actions_metadata_object_or_null",
        "executed_actions",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_executed_actions_metadata_object_or_null",
        "executed_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_executed_actions_instrument_id_nonempty_when_present",
        "executed_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_executed_actions_portfolio_id_nonempty_when_present",
        "executed_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_executed_actions_run_id_nonempty_when_present",
        "executed_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_executed_actions_decision_id_nonempty_when_present",
        "executed_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_executed_actions_fees_finite_when_present",
        "executed_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_executed_actions_slippage_finite_when_present",
        "executed_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_executed_actions_price_nonnegative",
        "executed_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_executed_actions_price_finite",
        "executed_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_executed_actions_quantity_positive",
        "executed_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_executed_actions_quantity_finite",
        "executed_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_executed_actions_side_allowed",
        "executed_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_executed_actions_side_nonempty",
        "executed_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_executed_actions_action_id_nonempty",
        "executed_actions",
        type_="check",
    )
