"""Tighten markets + market_holidays Layer 0 contracts

Revision ID: 0031_markets_holidays_fk
Revises: 0030_instrument_identifiers
Create Date: 2025-12-16

This migration tightens Layer 0 contracts for:
- markets
- market_holidays

Changes
-------
- Seed canonical markets (US_EQ, EU_EQ, ASIA_EQ) if missing.
- Add non-empty CHECK constraints for key string fields.
- Add a foreign key from market_holidays.market_id -> markets.market_id.

Rationale
---------
Market identity and calendars are foundational (Layer 0). We want:
- Stable market identifiers.
- Valid IANA timezones.
- Holiday rows that cannot reference unknown markets.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0031_markets_holidays_fk"
down_revision: Union[str, None] = "0030_instrument_identifiers"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ---------------------------------------------------------------------
    # Seed canonical markets
    # ---------------------------------------------------------------------
    # NOTE: Keep these aligned with core constants used across the repo
    # (e.g. prometheus.core.market_state.DEFAULT_CONFIGS).
    op.execute(
        """
        INSERT INTO markets (market_id, name, region, timezone)
        VALUES
            ('US_EQ', 'US Equity', 'US', 'America/New_York'),
            ('EU_EQ', 'EU Equity', 'EU', 'Europe/London'),
            ('ASIA_EQ', 'ASIA Equity', 'ASIA', 'Asia/Tokyo')
        ON CONFLICT (market_id) DO NOTHING
        """
    )

    # ---------------------------------------------------------------------
    # markets: basic non-empty checks
    # ---------------------------------------------------------------------
    op.create_check_constraint(
        "ck_markets_market_id_nonempty",
        "markets",
        "char_length(btrim(market_id)) > 0",
    )
    op.create_check_constraint(
        "ck_markets_name_nonempty",
        "markets",
        "char_length(btrim(name)) > 0",
    )
    op.create_check_constraint(
        "ck_markets_region_nonempty",
        "markets",
        "char_length(btrim(region)) > 0",
    )
    op.create_check_constraint(
        "ck_markets_timezone_nonempty",
        "markets",
        "char_length(btrim(timezone)) > 0",
    )

    # ---------------------------------------------------------------------
    # market_holidays: basic non-empty checks
    # ---------------------------------------------------------------------
    op.create_check_constraint(
        "ck_market_holidays_market_id_nonempty",
        "market_holidays",
        "char_length(btrim(market_id)) > 0",
    )
    op.create_check_constraint(
        "ck_market_holidays_holiday_name_nonempty",
        "market_holidays",
        "char_length(btrim(holiday_name)) > 0",
    )

    # ---------------------------------------------------------------------
    # market_holidays: enforce referential integrity to markets
    # ---------------------------------------------------------------------
    op.create_foreign_key(
        "fk_market_holidays_market_id_markets",
        "market_holidays",
        "markets",
        ["market_id"],
        ["market_id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_market_holidays_market_id_markets",
        "market_holidays",
        type_="foreignkey",
    )

    op.drop_constraint(
        "ck_market_holidays_holiday_name_nonempty",
        "market_holidays",
        type_="check",
    )
    op.drop_constraint(
        "ck_market_holidays_market_id_nonempty",
        "market_holidays",
        type_="check",
    )

    op.drop_constraint(
        "ck_markets_timezone_nonempty",
        "markets",
        type_="check",
    )
    op.drop_constraint(
        "ck_markets_region_nonempty",
        "markets",
        type_="check",
    )
    op.drop_constraint(
        "ck_markets_name_nonempty",
        "markets",
        type_="check",
    )
    op.drop_constraint(
        "ck_markets_market_id_nonempty",
        "markets",
        type_="check",
    )
