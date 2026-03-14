"""Layer 2: tighten text_embeddings contracts

Revision ID: 0054_text_embeddings_l2
Revises: 0053_profiles_l2
Create Date: 2025-12-16

Layer 2 contract for ``text_embeddings``:
- source_type/source_id/model_id are non-empty
- vector_ref is either NULL or non-empty
- at least one of (vector, vector_ref) is present
- vector bytes are non-empty when present

Note: embedding dimensionality and model correctness are higher-level audits.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0054_text_embeddings_l2"
down_revision: Union[str, None] = "0053_profiles_l2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_text_embeddings_source_type_nonempty",
        "text_embeddings",
        "btrim(source_type) <> ''",
    )

    op.create_check_constraint(
        "ck_text_embeddings_source_id_nonempty",
        "text_embeddings",
        "btrim(source_id) <> ''",
    )

    op.create_check_constraint(
        "ck_text_embeddings_model_id_nonempty",
        "text_embeddings",
        "btrim(model_id) <> ''",
    )

    op.create_check_constraint(
        "ck_text_embeddings_vector_ref_nonempty_when_present",
        "text_embeddings",
        "vector_ref IS NULL OR btrim(vector_ref) <> ''",
    )

    op.create_check_constraint(
        "ck_text_embeddings_vector_or_ref_present",
        "text_embeddings",
        "vector IS NOT NULL OR vector_ref IS NOT NULL",
    )

    op.create_check_constraint(
        "ck_text_embeddings_vector_bytes_nonempty_when_present",
        "text_embeddings",
        "vector IS NULL OR octet_length(vector) > 0",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_text_embeddings_vector_bytes_nonempty_when_present",
        "text_embeddings",
        type_="check",
    )
    op.drop_constraint(
        "ck_text_embeddings_vector_or_ref_present",
        "text_embeddings",
        type_="check",
    )
    op.drop_constraint(
        "ck_text_embeddings_vector_ref_nonempty_when_present",
        "text_embeddings",
        type_="check",
    )
    op.drop_constraint(
        "ck_text_embeddings_model_id_nonempty",
        "text_embeddings",
        type_="check",
    )
    op.drop_constraint(
        "ck_text_embeddings_source_id_nonempty",
        "text_embeddings",
        type_="check",
    )
    op.drop_constraint(
        "ck_text_embeddings_source_type_nonempty",
        "text_embeddings",
        type_="check",
    )
