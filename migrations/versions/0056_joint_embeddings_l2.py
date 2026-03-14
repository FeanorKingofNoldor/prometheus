"""Layer 2: tighten joint_embeddings contracts

Revision ID: 0056_joint_embeddings_l2
Revises: 0055_numeric_window_embeddings_l2
Create Date: 2025-12-16

Layer 2 contract for ``joint_embeddings``:
- joint_type/model_id are non-empty
- entity_scope is a JSON object
- vector_ref is either NULL or non-empty
- at least one of (vector, vector_ref) is present
- vector bytes are non-empty when present
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0056_joint_embeddings_l2"
down_revision: Union[str, None] = "0055_numeric_window_embeddings_l2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_joint_embeddings_joint_type_nonempty",
        "joint_embeddings",
        "btrim(joint_type) <> ''",
    )

    op.create_check_constraint(
        "ck_joint_embeddings_model_id_nonempty",
        "joint_embeddings",
        "btrim(model_id) <> ''",
    )

    op.create_check_constraint(
        "ck_joint_embeddings_entity_scope_object",
        "joint_embeddings",
        "jsonb_typeof(entity_scope) = 'object'",
    )

    op.create_check_constraint(
        "ck_joint_embeddings_vector_ref_nonempty_when_present",
        "joint_embeddings",
        "vector_ref IS NULL OR btrim(vector_ref) <> ''",
    )

    op.create_check_constraint(
        "ck_joint_embeddings_vector_or_ref_present",
        "joint_embeddings",
        "vector IS NOT NULL OR vector_ref IS NOT NULL",
    )

    op.create_check_constraint(
        "ck_joint_embeddings_vector_bytes_nonempty_when_present",
        "joint_embeddings",
        "vector IS NULL OR octet_length(vector) > 0",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_joint_embeddings_vector_bytes_nonempty_when_present",
        "joint_embeddings",
        type_="check",
    )
    op.drop_constraint(
        "ck_joint_embeddings_vector_or_ref_present",
        "joint_embeddings",
        type_="check",
    )
    op.drop_constraint(
        "ck_joint_embeddings_vector_ref_nonempty_when_present",
        "joint_embeddings",
        type_="check",
    )
    op.drop_constraint(
        "ck_joint_embeddings_entity_scope_object",
        "joint_embeddings",
        type_="check",
    )
    op.drop_constraint(
        "ck_joint_embeddings_model_id_nonempty",
        "joint_embeddings",
        type_="check",
    )
    op.drop_constraint(
        "ck_joint_embeddings_joint_type_nonempty",
        "joint_embeddings",
        type_="check",
    )
