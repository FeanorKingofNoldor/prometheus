"""Layer 5: tighten decision_outcomes contracts

Revision ID: 0079_decision_outcomes_l5
Revises: 0078_backtest_daily_equity_l5
Create Date: 2025-12-16

Layer 5 contract for ``decision_outcomes``:
- decision_id is non-empty
- horizon_days > 0
- realized metrics are finite when present
- metadata is either NULL or a JSON object
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "0079_decision_outcomes_l5"
down_revision: Union[str, None] = "0078_backtest_daily_equity_l5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_decision_outcomes_decision_id_nonempty",
        "decision_outcomes",
        "btrim(decision_id) <> ''",
    )

    op.create_check_constraint(
        "ck_decision_outcomes_horizon_days_positive",
        "decision_outcomes",
        "horizon_days > 0",
    )

    op.create_check_constraint(
        "ck_decision_outcomes_realized_return_finite_when_present",
        "decision_outcomes",
        f"realized_return IS NULL OR realized_return NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_decision_outcomes_realized_pnl_finite_when_present",
        "decision_outcomes",
        f"realized_pnl IS NULL OR realized_pnl NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_decision_outcomes_realized_drawdown_finite_when_present",
        "decision_outcomes",
        f"realized_drawdown IS NULL OR realized_drawdown NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_decision_outcomes_realized_vol_finite_when_present",
        "decision_outcomes",
        f"realized_vol IS NULL OR realized_vol NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_decision_outcomes_metadata_object_or_null",
        "decision_outcomes",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint("ck_decision_outcomes_metadata_object_or_null", "decision_outcomes", type_="check")
    op.drop_constraint("ck_decision_outcomes_realized_vol_finite_when_present", "decision_outcomes", type_="check")
    op.drop_constraint("ck_decision_outcomes_realized_drawdown_finite_when_present", "decision_outcomes", type_="check")
    op.drop_constraint("ck_decision_outcomes_realized_pnl_finite_when_present", "decision_outcomes", type_="check")
    op.drop_constraint("ck_decision_outcomes_realized_return_finite_when_present", "decision_outcomes", type_="check")
    op.drop_constraint("ck_decision_outcomes_horizon_days_positive", "decision_outcomes", type_="check")
    op.drop_constraint("ck_decision_outcomes_decision_id_nonempty", "decision_outcomes", type_="check")
