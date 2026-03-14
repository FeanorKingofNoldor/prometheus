"""Layer 2: tighten profiles contracts

Revision ID: 0053_profiles_l2
Revises: 0052_macro_events_l1
Create Date: 2025-12-16

Layer 2 contract for ``profiles``:
- issuer_id is non-empty
- structured is a JSON object
- risk_flags is a JSON object
- embedding_vector_ref is either NULL or non-empty

Note: issuer_id existence and profile coverage are higher-level audits.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0053_profiles_l2"
down_revision: Union[str, None] = "0052_macro_events_l1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_profiles_issuer_id_nonempty",
        "profiles",
        "btrim(issuer_id) <> ''",
    )

    op.create_check_constraint(
        "ck_profiles_structured_object",
        "profiles",
        "jsonb_typeof(structured) = 'object'",
    )

    op.create_check_constraint(
        "ck_profiles_risk_flags_object",
        "profiles",
        "jsonb_typeof(risk_flags) = 'object'",
    )

    op.create_check_constraint(
        "ck_profiles_embedding_vector_ref_nonempty_when_present",
        "profiles",
        "embedding_vector_ref IS NULL OR btrim(embedding_vector_ref) <> ''",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_profiles_embedding_vector_ref_nonempty_when_present",
        "profiles",
        type_="check",
    )
    op.drop_constraint(
        "ck_profiles_risk_flags_object",
        "profiles",
        type_="check",
    )
    op.drop_constraint(
        "ck_profiles_structured_object",
        "profiles",
        type_="check",
    )
    op.drop_constraint(
        "ck_profiles_issuer_id_nonempty",
        "profiles",
        type_="check",
    )
