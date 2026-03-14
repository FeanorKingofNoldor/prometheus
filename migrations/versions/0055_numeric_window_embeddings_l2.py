"""Layer 2: tighten numeric_window_embeddings contracts

Revision ID: 0055_numeric_window_embeddings_l2
Revises: 0054_text_embeddings_l2
Create Date: 2025-12-16

Layer 2 contract for ``numeric_window_embeddings``:
- entity_type/entity_id/model_id are non-empty
- window_spec is a JSON object
- vector_ref is either NULL or non-empty
- at least one of (vector, vector_ref) is present
- vector bytes are non-empty when present

Note: uniqueness/versioning of embeddings is model-specific and may be
strengthened in later iterations.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0055_numeric_window_embeddings_l2"
down_revision: Union[str, None] = "0054b_alembic_version"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_numeric_window_embeddings_entity_type_nonempty",
        "numeric_window_embeddings",
        "btrim(entity_type) <> ''",
    )

    op.create_check_constraint(
        "ck_numeric_window_embeddings_entity_id_nonempty",
        "numeric_window_embeddings",
        "btrim(entity_id) <> ''",
    )

    op.create_check_constraint(
        "ck_numeric_window_embeddings_model_id_nonempty",
        "numeric_window_embeddings",
        "btrim(model_id) <> ''",
    )

    op.create_check_constraint(
        "ck_numeric_window_embeddings_window_spec_object",
        "numeric_window_embeddings",
        "jsonb_typeof(window_spec) = 'object'",
    )

    op.create_check_constraint(
        "ck_numeric_window_embeddings_vector_ref_nonempty_when_present",
        "numeric_window_embeddings",
        "vector_ref IS NULL OR btrim(vector_ref) <> ''",
    )

    op.create_check_constraint(
        "ck_numeric_window_embeddings_vector_or_ref_present",
        "numeric_window_embeddings",
        "vector IS NOT NULL OR vector_ref IS NOT NULL",
    )

    op.create_check_constraint(
        "ck_numeric_window_embeddings_vector_bytes_nonempty_when_present",
        "numeric_window_embeddings",
        "vector IS NULL OR octet_length(vector) > 0",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_numeric_window_embeddings_vector_bytes_nonempty_when_present",
        "numeric_window_embeddings",
        type_="check",
    )
    op.drop_constraint(
        "ck_numeric_window_embeddings_vector_or_ref_present",
        "numeric_window_embeddings",
        type_="check",
    )
    op.drop_constraint(
        "ck_numeric_window_embeddings_vector_ref_nonempty_when_present",
        "numeric_window_embeddings",
        type_="check",
    )
    op.drop_constraint(
        "ck_numeric_window_embeddings_window_spec_object",
        "numeric_window_embeddings",
        type_="check",
    )
    op.drop_constraint(
        "ck_numeric_window_embeddings_model_id_nonempty",
        "numeric_window_embeddings",
        type_="check",
    )
    op.drop_constraint(
        "ck_numeric_window_embeddings_entity_id_nonempty",
        "numeric_window_embeddings",
        type_="check",
    )
    op.drop_constraint(
        "ck_numeric_window_embeddings_entity_type_nonempty",
        "numeric_window_embeddings",
        type_="check",
    )
