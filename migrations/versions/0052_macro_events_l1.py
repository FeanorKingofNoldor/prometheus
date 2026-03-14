"""Layer 1: tighten macro_events contracts

Revision ID: 0052_macro_events_l1
Revises: 0051_earnings_calls_l1
Create Date: 2025-12-16

Layer 1 contract for ``macro_events``:
- event_type is non-empty
- description is non-empty
- country is either NULL or non-empty
- text_ref is either NULL or non-empty
- metadata is either NULL or a JSON object

Note: timestamp semantics and source normalisation are higher-level audits.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0052_macro_events_l1"
down_revision: Union[str, None] = "0051_earnings_calls_l1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_macro_events_event_type_nonempty",
        "macro_events",
        "btrim(event_type) <> ''",
    )

    op.create_check_constraint(
        "ck_macro_events_description_nonempty",
        "macro_events",
        "btrim(description) <> ''",
    )

    op.create_check_constraint(
        "ck_macro_events_country_nonempty_when_present",
        "macro_events",
        "country IS NULL OR btrim(country) <> ''",
    )

    op.create_check_constraint(
        "ck_macro_events_text_ref_nonempty_when_present",
        "macro_events",
        "text_ref IS NULL OR btrim(text_ref) <> ''",
    )

    op.create_check_constraint(
        "ck_macro_events_metadata_object_or_null",
        "macro_events",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_macro_events_metadata_object_or_null",
        "macro_events",
        type_="check",
    )
    op.drop_constraint(
        "ck_macro_events_text_ref_nonempty_when_present",
        "macro_events",
        type_="check",
    )
    op.drop_constraint(
        "ck_macro_events_country_nonempty_when_present",
        "macro_events",
        type_="check",
    )
    op.drop_constraint(
        "ck_macro_events_description_nonempty",
        "macro_events",
        type_="check",
    )
    op.drop_constraint(
        "ck_macro_events_event_type_nonempty",
        "macro_events",
        type_="check",
    )
