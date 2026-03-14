"""Layer 3: tighten risk_actions contracts

Revision ID: 0071_risk_actions_l3
Revises: 0070_engine_decisions_l3
Create Date: 2025-12-16

Layer 3 contract for ``risk_actions``:
- action_id and action_type are non-empty
- action_type is in an allowed set
- strategy_id/instrument_id/decision_id are either NULL or non-empty
- details_json is either NULL or a JSON object
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0071_risk_actions_l3"
down_revision: Union[str, None] = "0070_engine_decisions_l3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_risk_actions_action_id_nonempty",
        "risk_actions",
        "btrim(action_id) <> ''",
    )

    op.create_check_constraint(
        "ck_risk_actions_action_type_nonempty",
        "risk_actions",
        "btrim(action_type) <> ''",
    )

    op.create_check_constraint(
        "ck_risk_actions_action_type_allowed",
        "risk_actions",
        "action_type IN ('OK', 'CAPPED', 'REJECTED', 'EXECUTION_REJECT')",
    )

    op.create_check_constraint(
        "ck_risk_actions_strategy_id_nonempty_when_present",
        "risk_actions",
        "strategy_id IS NULL OR btrim(strategy_id) <> ''",
    )

    op.create_check_constraint(
        "ck_risk_actions_instrument_id_nonempty_when_present",
        "risk_actions",
        "instrument_id IS NULL OR btrim(instrument_id) <> ''",
    )

    op.create_check_constraint(
        "ck_risk_actions_decision_id_nonempty_when_present",
        "risk_actions",
        "decision_id IS NULL OR btrim(decision_id) <> ''",
    )

    op.create_check_constraint(
        "ck_risk_actions_details_json_object_or_null",
        "risk_actions",
        "details_json IS NULL OR jsonb_typeof(details_json) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_risk_actions_details_json_object_or_null",
        "risk_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_risk_actions_decision_id_nonempty_when_present",
        "risk_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_risk_actions_instrument_id_nonempty_when_present",
        "risk_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_risk_actions_strategy_id_nonempty_when_present",
        "risk_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_risk_actions_action_type_allowed",
        "risk_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_risk_actions_action_type_nonempty",
        "risk_actions",
        type_="check",
    )
    op.drop_constraint(
        "ck_risk_actions_action_id_nonempty",
        "risk_actions",
        type_="check",
    )
