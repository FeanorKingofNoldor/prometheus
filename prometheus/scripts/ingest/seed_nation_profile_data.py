"""Prometheus v2 – Seed Nation Profile Engine data.

Seeds:
1. Sovereign issuers (delegates to ingest_sovereign_issuers)
2. US Tier 1 position_occupancy entries
3. Skeleton person_profiles for US Tier 1 officials

Usage:
    python -m prometheus.scripts.ingest.seed_nation_profile_data
    python -m prometheus.scripts.ingest.seed_nation_profile_data --dry-run
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Optional, Sequence

from psycopg2.extras import Json

from apathis.core.database import get_db_manager
from apathis.core.ids import generate_uuid


# ── US Tier 1 positions ──────────────────────────────────────────────────

US_TIER1_POSITIONS = [
    {
        "position_id": "US_PRESIDENT",
        "person_name": "Donald Trump",
        "nation": "USA",
        "start_date": date(2025, 1, 20),
        "end_date": date(2029, 1, 20),
        "metadata": {
            "role_description": "President of the United States",
            "tier": 1,
        },
    },
    {
        "position_id": "US_FED_CHAIR",
        "person_name": "Jerome Powell",
        "nation": "USA",
        "start_date": date(2022, 5, 23),
        "end_date": date(2026, 5, 15),
        "metadata": {
            "role_description": "Chair of the Federal Reserve",
            "tier": 1,
        },
    },
    {
        "position_id": "US_TREASURY_SEC",
        "person_name": "Scott Bessent",
        "nation": "USA",
        "start_date": date(2025, 1, 27),
        "end_date": None,
        "metadata": {
            "role_description": "Secretary of the Treasury",
            "tier": 1,
        },
    },
    {
        "position_id": "US_SEC_STATE",
        "person_name": "Marco Rubio",
        "nation": "USA",
        "start_date": date(2025, 1, 20),
        "end_date": None,
        "metadata": {
            "role_description": "Secretary of State",
            "tier": 1,
        },
    },
    {
        "position_id": "US_SEC_DEFENSE",
        "person_name": "Pete Hegseth",
        "nation": "USA",
        "start_date": date(2025, 1, 25),
        "end_date": None,
        "metadata": {
            "role_description": "Secretary of Defense",
            "tier": 1,
        },
    },
]

# ── Skeleton person profiles ──────────────────────────────────────────────

US_TIER1_PROFILES = [
    {
        "profile_id": "US_PRESIDENT_PROFILE",
        "person_name": "Donald Trump",
        "nation": "USA",
        "role": "PRESIDENT",
        "role_tier": 1,
        "in_role_since": date(2025, 1, 20),
        "expected_term_end": date(2029, 1, 20),
        "policy_stance": {
            "monetary": 0.0,
            "fiscal": 0.6,
            "trade": -0.7,
            "regulation": -0.5,
            "geopolitical": -0.3,
        },
        "scores": {
            "credibility": 0.4,
            "influence": 0.9,
            "stability": 0.7,
            "predictability": 0.3,
            "market_sensitivity": 0.9,
            "succession_risk": 0.1,
        },
    },
    {
        "profile_id": "US_FED_CHAIR_PROFILE",
        "person_name": "Jerome Powell",
        "nation": "USA",
        "role": "FED_CHAIR",
        "role_tier": 1,
        "in_role_since": date(2022, 5, 23),
        "expected_term_end": date(2026, 5, 15),
        "policy_stance": {
            "monetary": 0.3,
            "fiscal": 0.0,
            "trade": 0.0,
            "regulation": 0.2,
            "geopolitical": 0.0,
        },
        "scores": {
            "credibility": 0.7,
            "influence": 0.9,
            "stability": 0.6,
            "predictability": 0.7,
            "market_sensitivity": 0.95,
            "succession_risk": 0.4,
        },
    },
    {
        "profile_id": "US_TREASURY_SEC_PROFILE",
        "person_name": "Scott Bessent",
        "nation": "USA",
        "role": "TREASURY_SECRETARY",
        "role_tier": 1,
        "in_role_since": date(2025, 1, 27),
        "expected_term_end": None,
        "policy_stance": {
            "monetary": 0.0,
            "fiscal": 0.4,
            "trade": -0.3,
            "regulation": -0.4,
            "geopolitical": 0.0,
        },
        "scores": {
            "credibility": 0.6,
            "influence": 0.7,
            "stability": 0.7,
            "predictability": 0.5,
            "market_sensitivity": 0.6,
            "succession_risk": 0.2,
        },
    },
    {
        "profile_id": "US_SEC_STATE_PROFILE",
        "person_name": "Marco Rubio",
        "nation": "USA",
        "role": "SECRETARY_OF_STATE",
        "role_tier": 1,
        "in_role_since": date(2025, 1, 20),
        "expected_term_end": None,
        "policy_stance": {
            "monetary": 0.0,
            "fiscal": 0.0,
            "trade": -0.4,
            "regulation": 0.0,
            "geopolitical": 0.5,
        },
        "scores": {
            "credibility": 0.6,
            "influence": 0.6,
            "stability": 0.7,
            "predictability": 0.5,
            "market_sensitivity": 0.3,
            "succession_risk": 0.2,
        },
    },
    {
        "profile_id": "US_SEC_DEFENSE_PROFILE",
        "person_name": "Pete Hegseth",
        "nation": "USA",
        "role": "SECRETARY_OF_DEFENSE",
        "role_tier": 1,
        "in_role_since": date(2025, 1, 25),
        "expected_term_end": None,
        "policy_stance": {
            "monetary": 0.0,
            "fiscal": 0.3,
            "trade": 0.0,
            "regulation": -0.3,
            "geopolitical": 0.6,
        },
        "scores": {
            "credibility": 0.4,
            "influence": 0.5,
            "stability": 0.6,
            "predictability": 0.4,
            "market_sensitivity": 0.2,
            "succession_risk": 0.3,
        },
    },
]


def _seed_positions(dry_run: bool) -> int:
    """Upsert position_occupancy entries."""

    if dry_run:
        print(json.dumps({"positions": US_TIER1_POSITIONS}, indent=2, default=str))
        return len(US_TIER1_POSITIONS)

    sql = """
        INSERT INTO position_occupancy (
            occupancy_id, position_id, person_name, nation,
            start_date, end_date, metadata
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (position_id, start_date) DO UPDATE SET
            person_name = EXCLUDED.person_name,
            nation = EXCLUDED.nation,
            end_date = EXCLUDED.end_date,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
    """

    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            for p in US_TIER1_POSITIONS:
                cur.execute(
                    sql,
                    (
                        generate_uuid(),
                        p["position_id"],
                        p["person_name"],
                        p["nation"],
                        p["start_date"],
                        p["end_date"],
                        Json(p["metadata"]),
                    ),
                )
            conn.commit()
        finally:
            cur.close()

    return len(US_TIER1_POSITIONS)


def _seed_profiles(dry_run: bool) -> int:
    """Upsert skeleton person_profiles entries."""

    if dry_run:
        print(json.dumps({"profiles": US_TIER1_PROFILES}, indent=2, default=str))
        return len(US_TIER1_PROFILES)

    sql = """
        INSERT INTO person_profiles (
            profile_id, person_name, nation, role, role_tier,
            in_role_since, expected_term_end,
            policy_stance, scores, confidence,
            metadata, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, NOW()
        )
        ON CONFLICT (profile_id) DO UPDATE SET
            person_name = EXCLUDED.person_name,
            nation = EXCLUDED.nation,
            role = EXCLUDED.role,
            role_tier = EXCLUDED.role_tier,
            in_role_since = EXCLUDED.in_role_since,
            expected_term_end = EXCLUDED.expected_term_end,
            policy_stance = EXCLUDED.policy_stance,
            scores = EXCLUDED.scores,
            confidence = EXCLUDED.confidence,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
    """

    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            for p in US_TIER1_PROFILES:
                cur.execute(
                    sql,
                    (
                        p["profile_id"],
                        p["person_name"],
                        p["nation"],
                        p["role"],
                        p["role_tier"],
                        p["in_role_since"],
                        p.get("expected_term_end"),
                        Json(p["policy_stance"]),
                        Json(p["scores"]),
                        0.5,  # initial confidence
                        Json({"source": "seed", "seed_version": 1}),
                    ),
                )
            conn.commit()
        finally:
            cur.close()

    return len(US_TIER1_PROFILES)


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Seed Nation Profile Engine data (positions + profiles)"
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    print("=== Seeding US Tier 1 position occupancy ===")
    n_pos = _seed_positions(args.dry_run)
    print(f"  → {n_pos} positions {'planned' if args.dry_run else 'upserted'}")

    print("=== Seeding US Tier 1 person profiles ===")
    n_prof = _seed_profiles(args.dry_run)
    print(f"  → {n_prof} profiles {'planned' if args.dry_run else 'upserted'}")

    print("Done.")


if __name__ == "__main__":  # pragma: no cover
    main()
