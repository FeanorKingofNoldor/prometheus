"""The backfill_markets seed list must match market_state.py DEFAULT_CONFIGS.

apatheon/core/market_state.py is the single source of truth for market
timezones (exchange_tz). The markets-table seeder must never drift from
it again (the DB briefly held Europe/Paris for EU_EQ while the state
machine used Europe/Berlin).
"""

from __future__ import annotations

from apatheon.core.market_state import DEFAULT_CONFIGS

from prometheus.scripts.backfill.backfill_markets import _CANONICAL_MARKETS

_EXPECTED_MARKETS = {"US_EQ", "UK_EQ", "EU_EQ", "HK_EQ", "KR_EQ", "AU_EQ", "JP_EQ"}


def test_seed_covers_all_seven_tradable_markets():
    assert {m.market_id for m in _CANONICAL_MARKETS} == _EXPECTED_MARKETS


def test_seed_timezones_match_market_state_exchange_tz():
    for seed in _CANONICAL_MARKETS:
        expected_tz = DEFAULT_CONFIGS[seed.market_id].session_times.exchange_tz
        assert seed.timezone == expected_tz, (
            f"{seed.market_id}: seeder says {seed.timezone}, "
            f"market_state.py says {expected_tz}"
        )


def test_eu_eq_uses_berlin_not_paris_or_london():
    eu = next(m for m in _CANONICAL_MARKETS if m.market_id == "EU_EQ")
    assert eu.timezone == "Europe/Berlin"
