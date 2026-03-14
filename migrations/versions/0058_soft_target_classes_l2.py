"""Layer 2: tighten soft_target_classes contracts

Revision ID: 0058_soft_target_classes_l2
Revises: 0057_stability_vectors_l2
Create Date: 2025-12-16

Layer 2 contract for ``soft_target_classes``:
- IDs/types are non-empty
- soft_target_class is in an allowed set
- score/component fields are finite and within [0, 100]
- metadata is either NULL or a JSON object
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0058_soft_target_classes_l2"
down_revision: Union[str, None] = "0057_stability_vectors_l2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_soft_target_classes_soft_target_id_nonempty",
        "soft_target_classes",
        "btrim(soft_target_id) <> ''",
    )

    op.create_check_constraint(
        "ck_soft_target_classes_entity_type_nonempty",
        "soft_target_classes",
        "btrim(entity_type) <> ''",
    )

    op.create_check_constraint(
        "ck_soft_target_classes_entity_id_nonempty",
        "soft_target_classes",
        "btrim(entity_id) <> ''",
    )

    op.create_check_constraint(
        "ck_soft_target_classes_soft_target_class_nonempty",
        "soft_target_classes",
        "btrim(soft_target_class) <> ''",
    )

    op.create_check_constraint(
        "ck_soft_target_classes_soft_target_class_allowed",
        "soft_target_classes",
        "soft_target_class IN ('STABLE', 'WATCH', 'FRAGILE', 'TARGETABLE', 'BREAKER')",
    )

    op.create_check_constraint(
        "ck_soft_target_classes_soft_target_score_finite",
        "soft_target_classes",
        f"soft_target_score NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_soft_target_classes_soft_target_score_range_0_100",
        "soft_target_classes",
        "soft_target_score >= 0.0 AND soft_target_score <= 100.0",
    )

    op.create_check_constraint(
        "ck_soft_target_classes_instability_finite",
        "soft_target_classes",
        f"instability NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_soft_target_classes_instability_range_0_100",
        "soft_target_classes",
        "instability >= 0.0 AND instability <= 100.0",
    )

    op.create_check_constraint(
        "ck_soft_target_classes_high_fragility_finite",
        "soft_target_classes",
        f"high_fragility NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_soft_target_classes_high_fragility_range_0_100",
        "soft_target_classes",
        "high_fragility >= 0.0 AND high_fragility <= 100.0",
    )

    op.create_check_constraint(
        "ck_soft_target_classes_complacent_pricing_finite",
        "soft_target_classes",
        f"complacent_pricing NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_soft_target_classes_complacent_pricing_range_0_100",
        "soft_target_classes",
        "complacent_pricing >= 0.0 AND complacent_pricing <= 100.0",
    )

    op.create_check_constraint(
        "ck_soft_target_classes_metadata_object_or_null",
        "soft_target_classes",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_soft_target_classes_metadata_object_or_null",
        "soft_target_classes",
        type_="check",
    )
    op.drop_constraint(
        "ck_soft_target_classes_complacent_pricing_range_0_100",
        "soft_target_classes",
        type_="check",
    )
    op.drop_constraint(
        "ck_soft_target_classes_complacent_pricing_finite",
        "soft_target_classes",
        type_="check",
    )
    op.drop_constraint(
        "ck_soft_target_classes_high_fragility_range_0_100",
        "soft_target_classes",
        type_="check",
    )
    op.drop_constraint(
        "ck_soft_target_classes_high_fragility_finite",
        "soft_target_classes",
        type_="check",
    )
    op.drop_constraint(
        "ck_soft_target_classes_instability_range_0_100",
        "soft_target_classes",
        type_="check",
    )
    op.drop_constraint(
        "ck_soft_target_classes_instability_finite",
        "soft_target_classes",
        type_="check",
    )
    op.drop_constraint(
        "ck_soft_target_classes_soft_target_score_range_0_100",
        "soft_target_classes",
        type_="check",
    )
    op.drop_constraint(
        "ck_soft_target_classes_soft_target_score_finite",
        "soft_target_classes",
        type_="check",
    )
    op.drop_constraint(
        "ck_soft_target_classes_soft_target_class_allowed",
        "soft_target_classes",
        type_="check",
    )
    op.drop_constraint(
        "ck_soft_target_classes_soft_target_class_nonempty",
        "soft_target_classes",
        type_="check",
    )
    op.drop_constraint(
        "ck_soft_target_classes_entity_id_nonempty",
        "soft_target_classes",
        type_="check",
    )
    op.drop_constraint(
        "ck_soft_target_classes_entity_type_nonempty",
        "soft_target_classes",
        type_="check",
    )
    op.drop_constraint(
        "ck_soft_target_classes_soft_target_id_nonempty",
        "soft_target_classes",
        type_="check",
    )
