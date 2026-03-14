"""Tighten issuers + instruments Layer 0 contracts

Revision ID: 0032_issuers_instruments_l0
Revises: 0031_markets_holidays_fk
Create Date: 2025-12-16

This migration tightens Layer 0 contracts for the canonical entity tables:
- issuers
- instruments

Changes
-------
issuers:
- Add non-empty checks for issuer_id, issuer_type, and name.
- Add a simple format check for issuer_type (uppercase/underscore only).

instruments:
- Make market_id non-null.
- Add non-empty checks for instrument_id and symbol.
- Add simple format checks:
  - asset_class: uppercase/underscore
  - status: uppercase/underscore
  - currency: 3-letter uppercase ISO-style code
- Enforce that EQUITY instruments have a non-empty issuer_id.

Rationale
---------
Layer 0 identity tables are the root of most joins. Keeping key fields
non-empty and consistently formatted prevents silent downstream failures
and reduces UNKNOWN buckets in higher layers.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0032_issuers_instruments_l0"
down_revision: Union[str, None] = "0031_markets_holidays_fk"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # issuers
    # ------------------------------------------------------------------
    op.create_check_constraint(
        "ck_issuers_issuer_id_nonempty",
        "issuers",
        "char_length(btrim(issuer_id)) > 0",
    )
    op.create_check_constraint(
        "ck_issuers_issuer_type_nonempty",
        "issuers",
        "char_length(btrim(issuer_type)) > 0",
    )
    op.create_check_constraint(
        "ck_issuers_name_nonempty",
        "issuers",
        "char_length(btrim(name)) > 0",
    )
    op.create_check_constraint(
        "ck_issuers_issuer_type_format",
        "issuers",
        "issuer_type ~ '^[A-Z_]+$'",
    )

    # ------------------------------------------------------------------
    # instruments
    # ------------------------------------------------------------------
    # Enforce market_id presence (required for calendars, orchestration,
    # and most joins). This column was originally nullable.
    op.alter_column(
        "instruments",
        "market_id",
        existing_type=sa.String(length=50),
        nullable=False,
    )

    op.create_check_constraint(
        "ck_instruments_instrument_id_nonempty",
        "instruments",
        "char_length(btrim(instrument_id)) > 0",
    )
    op.create_check_constraint(
        "ck_instruments_symbol_nonempty",
        "instruments",
        "char_length(btrim(symbol)) > 0",
    )

    op.create_check_constraint(
        "ck_instruments_asset_class_format",
        "instruments",
        "asset_class ~ '^[A-Z_]+$'",
    )
    op.create_check_constraint(
        "ck_instruments_status_format",
        "instruments",
        "status ~ '^[A-Z_]+$'",
    )
    op.create_check_constraint(
        "ck_instruments_currency_format",
        "instruments",
        "currency ~ '^[A-Z]{3}$'",
    )

    # Require issuer_id for equities; other asset classes may be allowed
    # to omit issuer_id in future.
    op.create_check_constraint(
        "ck_instruments_equity_requires_issuer",
        "instruments",
        "asset_class <> 'EQUITY' OR (issuer_id IS NOT NULL AND char_length(btrim(issuer_id)) > 0)",
    )


def downgrade() -> None:
    # instruments
    op.drop_constraint(
        "ck_instruments_equity_requires_issuer",
        "instruments",
        type_="check",
    )
    op.drop_constraint(
        "ck_instruments_currency_format",
        "instruments",
        type_="check",
    )
    op.drop_constraint(
        "ck_instruments_status_format",
        "instruments",
        type_="check",
    )
    op.drop_constraint(
        "ck_instruments_asset_class_format",
        "instruments",
        type_="check",
    )
    op.drop_constraint(
        "ck_instruments_symbol_nonempty",
        "instruments",
        type_="check",
    )
    op.drop_constraint(
        "ck_instruments_instrument_id_nonempty",
        "instruments",
        type_="check",
    )

    op.alter_column(
        "instruments",
        "market_id",
        existing_type=sa.String(length=50),
        nullable=True,
    )

    # issuers
    op.drop_constraint(
        "ck_issuers_issuer_type_format",
        "issuers",
        type_="check",
    )
    op.drop_constraint(
        "ck_issuers_name_nonempty",
        "issuers",
        type_="check",
    )
    op.drop_constraint(
        "ck_issuers_issuer_type_nonempty",
        "issuers",
        type_="check",
    )
    op.drop_constraint(
        "ck_issuers_issuer_id_nonempty",
        "issuers",
        type_="check",
    )
