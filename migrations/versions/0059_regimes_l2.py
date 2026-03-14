"""Layer 2: tighten regimes contracts

Revision ID: 0059_regimes_l2
Revises: 0058_soft_target_classes_l2
Create Date: 2025-12-16

Layer 2 contract for ``regimes``:
- regime_record_id/region/regime_label are non-empty
- regime_label is in an allowed set
- confidence is finite and within [0, 1]
- regime_embedding bytes are non-empty when present
- embedding_ref is either NULL or non-empty
- metadata is either NULL or a JSON object
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0059_regimes_l2"
down_revision: Union[str, None] = "0058_soft_target_classes_l2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_regimes_regime_record_id_nonempty",
        "regimes",
        "btrim(regime_record_id) <> ''",
    )

    op.create_check_constraint(
        "ck_regimes_region_nonempty",
        "regimes",
        "btrim(region) <> ''",
    )

    op.create_check_constraint(
        "ck_regimes_regime_label_nonempty",
        "regimes",
        "btrim(regime_label) <> ''",
    )

    op.create_check_constraint(
        "ck_regimes_regime_label_allowed",
        "regimes",
        "regime_label IN ('CRISIS', 'RISK_OFF', 'CARRY', 'NEUTRAL')",
    )

    op.create_check_constraint(
        "ck_regimes_confidence_finite",
        "regimes",
        f"confidence NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_regimes_confidence_range_0_1",
        "regimes",
        "confidence >= 0.0 AND confidence <= 1.0",
    )

    op.create_check_constraint(
        "ck_regimes_regime_embedding_bytes_nonempty_when_present",
        "regimes",
        "regime_embedding IS NULL OR octet_length(regime_embedding) > 0",
    )

    op.create_check_constraint(
        "ck_regimes_embedding_ref_nonempty_when_present",
        "regimes",
        "embedding_ref IS NULL OR btrim(embedding_ref) <> ''",
    )

    op.create_check_constraint(
        "ck_regimes_metadata_object_or_null",
        "regimes",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_regimes_metadata_object_or_null",
        "regimes",
        type_="check",
    )
    op.drop_constraint(
        "ck_regimes_embedding_ref_nonempty_when_present",
        "regimes",
        type_="check",
    )
    op.drop_constraint(
        "ck_regimes_regime_embedding_bytes_nonempty_when_present",
        "regimes",
        type_="check",
    )
    op.drop_constraint(
        "ck_regimes_confidence_range_0_1",
        "regimes",
        type_="check",
    )
    op.drop_constraint(
        "ck_regimes_confidence_finite",
        "regimes",
        type_="check",
    )
    op.drop_constraint(
        "ck_regimes_regime_label_allowed",
        "regimes",
        type_="check",
    )
    op.drop_constraint(
        "ck_regimes_regime_label_nonempty",
        "regimes",
        type_="check",
    )
    op.drop_constraint(
        "ck_regimes_region_nonempty",
        "regimes",
        type_="check",
    )
    op.drop_constraint(
        "ck_regimes_regime_record_id_nonempty",
        "regimes",
        type_="check",
    )
