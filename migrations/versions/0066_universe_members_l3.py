"""Layer 3: tighten universe_members contracts

Revision ID: 0066_universe_members_l3
Revises: 0065_news_factors_daily_l2
Create Date: 2025-12-16

Layer 3 contract for ``universe_members``:
- universe_member_id/universe_id/entity_type/entity_id/tier are non-empty
- tier is in an allowed set (CORE/SATELLITE/EXCLUDED)
- included/tier consistency (included implies CORE|SATELLITE; excluded implies EXCLUDED)
- score is finite
- reasons is a JSON object

Note: referential checks against instruments are validated via CLI.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0066_universe_members_l3"
down_revision: Union[str, None] = "0065_news_factors_daily_l2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_universe_members_universe_member_id_nonempty",
        "universe_members",
        "btrim(universe_member_id) <> ''",
    )

    op.create_check_constraint(
        "ck_universe_members_universe_id_nonempty",
        "universe_members",
        "btrim(universe_id) <> ''",
    )

    op.create_check_constraint(
        "ck_universe_members_entity_type_nonempty",
        "universe_members",
        "btrim(entity_type) <> ''",
    )

    op.create_check_constraint(
        "ck_universe_members_entity_id_nonempty",
        "universe_members",
        "btrim(entity_id) <> ''",
    )

    op.create_check_constraint(
        "ck_universe_members_tier_nonempty",
        "universe_members",
        "btrim(tier) <> ''",
    )

    op.create_check_constraint(
        "ck_universe_members_tier_allowed",
        "universe_members",
        "tier IN ('CORE', 'SATELLITE', 'EXCLUDED')",
    )

    op.create_check_constraint(
        "ck_universe_members_tier_included_consistent",
        "universe_members",
        "(included AND tier IN ('CORE', 'SATELLITE')) OR ((NOT included) AND tier = 'EXCLUDED')",
    )

    op.create_check_constraint(
        "ck_universe_members_score_finite",
        "universe_members",
        f"score NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_universe_members_reasons_object",
        "universe_members",
        "jsonb_typeof(reasons) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_universe_members_reasons_object",
        "universe_members",
        type_="check",
    )
    op.drop_constraint(
        "ck_universe_members_score_finite",
        "universe_members",
        type_="check",
    )
    op.drop_constraint(
        "ck_universe_members_tier_included_consistent",
        "universe_members",
        type_="check",
    )
    op.drop_constraint(
        "ck_universe_members_tier_allowed",
        "universe_members",
        type_="check",
    )
    op.drop_constraint(
        "ck_universe_members_tier_nonempty",
        "universe_members",
        type_="check",
    )
    op.drop_constraint(
        "ck_universe_members_entity_id_nonempty",
        "universe_members",
        type_="check",
    )
    op.drop_constraint(
        "ck_universe_members_entity_type_nonempty",
        "universe_members",
        type_="check",
    )
    op.drop_constraint(
        "ck_universe_members_universe_id_nonempty",
        "universe_members",
        type_="check",
    )
    op.drop_constraint(
        "ck_universe_members_universe_member_id_nonempty",
        "universe_members",
        type_="check",
    )
