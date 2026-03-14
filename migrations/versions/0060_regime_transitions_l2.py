"""Layer 2: tighten regime_transitions contracts

Revision ID: 0060_regime_transitions_l2
Revises: 0059_regimes_l2
Create Date: 2025-12-16

Layer 2 contract for ``regime_transitions``:
- IDs/labels/region are non-empty
- from/to labels are in an allowed set
- from_regime_label != to_regime_label
- metadata is either NULL or a JSON object
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0060_regime_transitions_l2"
down_revision: Union[str, None] = "0059_regimes_l2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_regime_transitions_transition_id_nonempty",
        "regime_transitions",
        "btrim(transition_id) <> ''",
    )

    op.create_check_constraint(
        "ck_regime_transitions_region_nonempty",
        "regime_transitions",
        "btrim(region) <> ''",
    )

    op.create_check_constraint(
        "ck_regime_transitions_from_regime_label_nonempty",
        "regime_transitions",
        "btrim(from_regime_label) <> ''",
    )

    op.create_check_constraint(
        "ck_regime_transitions_to_regime_label_nonempty",
        "regime_transitions",
        "btrim(to_regime_label) <> ''",
    )

    op.create_check_constraint(
        "ck_regime_transitions_from_label_allowed",
        "regime_transitions",
        "from_regime_label IN ('CRISIS', 'RISK_OFF', 'CARRY', 'NEUTRAL')",
    )

    op.create_check_constraint(
        "ck_regime_transitions_to_label_allowed",
        "regime_transitions",
        "to_regime_label IN ('CRISIS', 'RISK_OFF', 'CARRY', 'NEUTRAL')",
    )

    op.create_check_constraint(
        "ck_regime_transitions_no_self_transition",
        "regime_transitions",
        "from_regime_label <> to_regime_label",
    )

    op.create_check_constraint(
        "ck_regime_transitions_metadata_object_or_null",
        "regime_transitions",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_regime_transitions_metadata_object_or_null",
        "regime_transitions",
        type_="check",
    )
    op.drop_constraint(
        "ck_regime_transitions_no_self_transition",
        "regime_transitions",
        type_="check",
    )
    op.drop_constraint(
        "ck_regime_transitions_to_label_allowed",
        "regime_transitions",
        type_="check",
    )
    op.drop_constraint(
        "ck_regime_transitions_from_label_allowed",
        "regime_transitions",
        type_="check",
    )
    op.drop_constraint(
        "ck_regime_transitions_to_regime_label_nonempty",
        "regime_transitions",
        type_="check",
    )
    op.drop_constraint(
        "ck_regime_transitions_from_regime_label_nonempty",
        "regime_transitions",
        type_="check",
    )
    op.drop_constraint(
        "ck_regime_transitions_region_nonempty",
        "regime_transitions",
        type_="check",
    )
    op.drop_constraint(
        "ck_regime_transitions_transition_id_nonempty",
        "regime_transitions",
        type_="check",
    )
