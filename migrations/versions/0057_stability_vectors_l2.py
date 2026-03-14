"""Layer 2: tighten stability_vectors contracts

Revision ID: 0057_stability_vectors_l2
Revises: 0056_joint_embeddings_l2
Create Date: 2025-12-16

Layer 2 contract for ``stability_vectors``:
- stability_id/entity_type/entity_id are non-empty
- vector_components is a JSON object
- overall_score is finite and within [0, 100]
- metadata is either NULL or a JSON object
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0057_stability_vectors_l2"
down_revision: Union[str, None] = "0056_joint_embeddings_l2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_stability_vectors_stability_id_nonempty",
        "stability_vectors",
        "btrim(stability_id) <> ''",
    )

    op.create_check_constraint(
        "ck_stability_vectors_entity_type_nonempty",
        "stability_vectors",
        "btrim(entity_type) <> ''",
    )

    op.create_check_constraint(
        "ck_stability_vectors_entity_id_nonempty",
        "stability_vectors",
        "btrim(entity_id) <> ''",
    )

    op.create_check_constraint(
        "ck_stability_vectors_vector_components_object",
        "stability_vectors",
        "jsonb_typeof(vector_components) = 'object'",
    )

    op.create_check_constraint(
        "ck_stability_vectors_overall_score_finite",
        "stability_vectors",
        f"overall_score NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_stability_vectors_overall_score_range_0_100",
        "stability_vectors",
        "overall_score >= 0.0 AND overall_score <= 100.0",
    )

    op.create_check_constraint(
        "ck_stability_vectors_metadata_object_or_null",
        "stability_vectors",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_stability_vectors_metadata_object_or_null",
        "stability_vectors",
        type_="check",
    )
    op.drop_constraint(
        "ck_stability_vectors_overall_score_range_0_100",
        "stability_vectors",
        type_="check",
    )
    op.drop_constraint(
        "ck_stability_vectors_overall_score_finite",
        "stability_vectors",
        type_="check",
    )
    op.drop_constraint(
        "ck_stability_vectors_vector_components_object",
        "stability_vectors",
        type_="check",
    )
    op.drop_constraint(
        "ck_stability_vectors_entity_id_nonempty",
        "stability_vectors",
        type_="check",
    )
    op.drop_constraint(
        "ck_stability_vectors_entity_type_nonempty",
        "stability_vectors",
        type_="check",
    )
    op.drop_constraint(
        "ck_stability_vectors_stability_id_nonempty",
        "stability_vectors",
        type_="check",
    )
