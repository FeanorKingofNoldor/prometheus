"""Layer 3: tighten book_targets contracts

Revision ID: 0067_book_targets_l3
Revises: 0066_universe_members_l3
Create Date: 2025-12-16

Layer 3 contract for ``book_targets``:
- target_id/book_id/region/entity_type/entity_id are non-empty
- target_weight is finite
- metadata is either NULL or a JSON object

Note: portfolio-level sum rules are validated at the application layer.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0067_book_targets_l3"
down_revision: Union[str, None] = "0066_universe_members_l3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_book_targets_target_id_nonempty",
        "book_targets",
        "btrim(target_id) <> ''",
    )

    op.create_check_constraint(
        "ck_book_targets_book_id_nonempty",
        "book_targets",
        "btrim(book_id) <> ''",
    )

    op.create_check_constraint(
        "ck_book_targets_region_nonempty",
        "book_targets",
        "btrim(region) <> ''",
    )

    op.create_check_constraint(
        "ck_book_targets_entity_type_nonempty",
        "book_targets",
        "btrim(entity_type) <> ''",
    )

    op.create_check_constraint(
        "ck_book_targets_entity_id_nonempty",
        "book_targets",
        "btrim(entity_id) <> ''",
    )

    op.create_check_constraint(
        "ck_book_targets_target_weight_finite",
        "book_targets",
        f"target_weight NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_book_targets_metadata_object_or_null",
        "book_targets",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_book_targets_metadata_object_or_null",
        "book_targets",
        type_="check",
    )
    op.drop_constraint(
        "ck_book_targets_target_weight_finite",
        "book_targets",
        type_="check",
    )
    op.drop_constraint(
        "ck_book_targets_entity_id_nonempty",
        "book_targets",
        type_="check",
    )
    op.drop_constraint(
        "ck_book_targets_entity_type_nonempty",
        "book_targets",
        type_="check",
    )
    op.drop_constraint(
        "ck_book_targets_region_nonempty",
        "book_targets",
        type_="check",
    )
    op.drop_constraint(
        "ck_book_targets_book_id_nonempty",
        "book_targets",
        type_="check",
    )
    op.drop_constraint(
        "ck_book_targets_target_id_nonempty",
        "book_targets",
        type_="check",
    )
