"""Prometheus v2 – Seed Global Nation Profile data (Top 10 expansion).

Seeds Tier 1 officials for 9 new nations (GBR, JPN, CHN, DEU, FRA,
CAN, AUS, CHE, KOR).  Each nation gets 3 officials: head of government,
central bank governor, and finance minister.

Seeding uses PersonProfileService.seed_profile() which, when an LLM is
configured, generates rich profiles (policy stances, scores, background,
behavioural analysis) in a single pass.

Usage:
    python -m prometheus.scripts.ingest.seed_global_nation_profiles
    python -m prometheus.scripts.ingest.seed_global_nation_profiles --dry-run
    python -m prometheus.scripts.ingest.seed_global_nation_profiles --no-llm
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from datetime import date
from typing import Optional, Sequence

from apathis.core.database import get_db_manager
from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger
from psycopg2.extras import Json

logger = get_logger(__name__)


# ── Official definitions ─────────────────────────────────────────────────


@dataclass
class OfficialDef:
    """Compact definition for a Tier 1 official to seed."""

    nation: str
    profile_id: str
    position_id: str
    person_name: str
    role: str
    role_description: str
    in_role_since: date
    expected_term_end: date | None = None


GLOBAL_TIER1: list[OfficialDef] = [
    # ── GBR ───────────────────────────────────────────────────────────
    OfficialDef(
        nation="GBR",
        profile_id="GBR_PM_PROFILE",
        position_id="GBR_PRIME_MINISTER",
        person_name="Keir Starmer",
        role="PRIME_MINISTER",
        role_description="Prime Minister of the United Kingdom",
        in_role_since=date(2024, 7, 5),
        expected_term_end=date(2029, 7, 5),
    ),
    OfficialDef(
        nation="GBR",
        profile_id="GBR_BOE_GOV_PROFILE",
        position_id="GBR_BOE_GOVERNOR",
        person_name="Andrew Bailey",
        role="BOE_GOVERNOR",
        role_description="Governor of the Bank of England",
        in_role_since=date(2020, 3, 16),
        expected_term_end=date(2028, 3, 15),
    ),
    OfficialDef(
        nation="GBR",
        profile_id="GBR_CHANCELLOR_PROFILE",
        position_id="GBR_CHANCELLOR",
        person_name="Rachel Reeves",
        role="CHANCELLOR_OF_EXCHEQUER",
        role_description="Chancellor of the Exchequer",
        in_role_since=date(2024, 7, 5),
    ),

    # ── JPN ───────────────────────────────────────────────────────────
    OfficialDef(
        nation="JPN",
        profile_id="JPN_PM_PROFILE",
        position_id="JPN_PRIME_MINISTER",
        person_name="Shigeru Ishiba",
        role="PRIME_MINISTER",
        role_description="Prime Minister of Japan",
        in_role_since=date(2024, 10, 1),
    ),
    OfficialDef(
        nation="JPN",
        profile_id="JPN_BOJ_GOV_PROFILE",
        position_id="JPN_BOJ_GOVERNOR",
        person_name="Kazuo Ueda",
        role="BOJ_GOVERNOR",
        role_description="Governor of the Bank of Japan",
        in_role_since=date(2023, 4, 9),
        expected_term_end=date(2028, 4, 8),
    ),
    OfficialDef(
        nation="JPN",
        profile_id="JPN_FM_PROFILE",
        position_id="JPN_FINANCE_MINISTER",
        person_name="Katsunobu Kato",
        role="FINANCE_MINISTER",
        role_description="Minister of Finance of Japan",
        in_role_since=date(2024, 11, 11),
    ),

    # ── CHN ───────────────────────────────────────────────────────────
    OfficialDef(
        nation="CHN",
        profile_id="CHN_PRESIDENT_PROFILE",
        position_id="CHN_PRESIDENT",
        person_name="Xi Jinping",
        role="PRESIDENT",
        role_description="President of the People's Republic of China",
        in_role_since=date(2013, 3, 14),
    ),
    OfficialDef(
        nation="CHN",
        profile_id="CHN_PBOC_GOV_PROFILE",
        position_id="CHN_PBOC_GOVERNOR",
        person_name="Pan Gongsheng",
        role="PBOC_GOVERNOR",
        role_description="Governor of the People's Bank of China",
        in_role_since=date(2023, 7, 25),
    ),
    OfficialDef(
        nation="CHN",
        profile_id="CHN_FM_PROFILE",
        position_id="CHN_FINANCE_MINISTER",
        person_name="Lan Fo'an",
        role="FINANCE_MINISTER",
        role_description="Minister of Finance of China",
        in_role_since=date(2023, 10, 24),
    ),

    # ── DEU ───────────────────────────────────────────────────────────
    OfficialDef(
        nation="DEU",
        profile_id="DEU_CHANCELLOR_PROFILE",
        position_id="DEU_CHANCELLOR",
        person_name="Friedrich Merz",
        role="CHANCELLOR",
        role_description="Chancellor of Germany",
        in_role_since=date(2025, 5, 6),
    ),
    OfficialDef(
        nation="DEU",
        profile_id="DEU_BUBA_PRES_PROFILE",
        position_id="DEU_BUNDESBANK_PRESIDENT",
        person_name="Joachim Nagel",
        role="BUNDESBANK_PRESIDENT",
        role_description="President of the Deutsche Bundesbank",
        in_role_since=date(2022, 1, 1),
        expected_term_end=date(2030, 1, 1),
    ),
    OfficialDef(
        nation="DEU",
        profile_id="DEU_FM_PROFILE",
        position_id="DEU_FINANCE_MINISTER",
        person_name="Jörg Kukies",
        role="FINANCE_MINISTER",
        role_description="Federal Minister of Finance of Germany",
        in_role_since=date(2024, 11, 7),
    ),

    # ── FRA ───────────────────────────────────────────────────────────
    OfficialDef(
        nation="FRA",
        profile_id="FRA_PRESIDENT_PROFILE",
        position_id="FRA_PRESIDENT",
        person_name="Emmanuel Macron",
        role="PRESIDENT",
        role_description="President of France",
        in_role_since=date(2017, 5, 14),
        expected_term_end=date(2027, 5, 13),
    ),
    OfficialDef(
        nation="FRA",
        profile_id="FRA_BDF_GOV_PROFILE",
        position_id="FRA_BDF_GOVERNOR",
        person_name="François Villeroy de Galhau",
        role="BDF_GOVERNOR",
        role_description="Governor of the Banque de France",
        in_role_since=date(2015, 11, 1),
    ),
    OfficialDef(
        nation="FRA",
        profile_id="FRA_FM_PROFILE",
        position_id="FRA_FINANCE_MINISTER",
        person_name="Éric Lombard",
        role="FINANCE_MINISTER",
        role_description="Minister of Economy and Finance of France",
        in_role_since=date(2024, 12, 23),
    ),

    # ── CAN ───────────────────────────────────────────────────────────
    OfficialDef(
        nation="CAN",
        profile_id="CAN_PM_PROFILE",
        position_id="CAN_PRIME_MINISTER",
        person_name="Mark Carney",
        role="PRIME_MINISTER",
        role_description="Prime Minister of Canada",
        in_role_since=date(2025, 3, 14),
    ),
    OfficialDef(
        nation="CAN",
        profile_id="CAN_BOC_GOV_PROFILE",
        position_id="CAN_BOC_GOVERNOR",
        person_name="Tiff Macklem",
        role="BOC_GOVERNOR",
        role_description="Governor of the Bank of Canada",
        in_role_since=date(2020, 6, 3),
        expected_term_end=date(2027, 6, 2),
    ),
    OfficialDef(
        nation="CAN",
        profile_id="CAN_FM_PROFILE",
        position_id="CAN_FINANCE_MINISTER",
        person_name="Dominic LeBlanc",
        role="FINANCE_MINISTER",
        role_description="Minister of Finance of Canada",
        in_role_since=date(2024, 12, 20),
    ),

    # ── AUS ───────────────────────────────────────────────────────────
    OfficialDef(
        nation="AUS",
        profile_id="AUS_PM_PROFILE",
        position_id="AUS_PRIME_MINISTER",
        person_name="Anthony Albanese",
        role="PRIME_MINISTER",
        role_description="Prime Minister of Australia",
        in_role_since=date(2022, 5, 23),
    ),
    OfficialDef(
        nation="AUS",
        profile_id="AUS_RBA_GOV_PROFILE",
        position_id="AUS_RBA_GOVERNOR",
        person_name="Michele Bullock",
        role="RBA_GOVERNOR",
        role_description="Governor of the Reserve Bank of Australia",
        in_role_since=date(2023, 9, 18),
        expected_term_end=date(2030, 9, 17),
    ),
    OfficialDef(
        nation="AUS",
        profile_id="AUS_TREASURER_PROFILE",
        position_id="AUS_TREASURER",
        person_name="Jim Chalmers",
        role="TREASURER",
        role_description="Treasurer of Australia",
        in_role_since=date(2022, 5, 23),
    ),

    # ── CHE ───────────────────────────────────────────────────────────
    OfficialDef(
        nation="CHE",
        profile_id="CHE_FM_PROFILE",
        position_id="CHE_FINANCE_MINISTER",
        person_name="Karin Keller-Sutter",
        role="FINANCE_MINISTER",
        role_description="Head of the Federal Department of Finance (Switzerland)",
        in_role_since=date(2023, 1, 1),
    ),
    OfficialDef(
        nation="CHE",
        profile_id="CHE_SNB_CHAIR_PROFILE",
        position_id="CHE_SNB_CHAIR",
        person_name="Martin Schlegel",
        role="SNB_CHAIR",
        role_description="Chairman of the Swiss National Bank",
        in_role_since=date(2024, 10, 1),
    ),
    OfficialDef(
        nation="CHE",
        profile_id="CHE_FOREIGN_PROFILE",
        position_id="CHE_FOREIGN_MINISTER",
        person_name="Ignazio Cassis",
        role="FOREIGN_MINISTER",
        role_description="Head of the Federal Department of Foreign Affairs (Switzerland)",
        in_role_since=date(2017, 12, 20),
    ),

    # ── KOR ───────────────────────────────────────────────────────────
    OfficialDef(
        nation="KOR",
        profile_id="KOR_PRESIDENT_PROFILE",
        position_id="KOR_PRESIDENT",
        person_name="Lee Jae-myung",
        role="PRESIDENT",
        role_description="President of South Korea",
        in_role_since=date(2025, 6, 12),
        expected_term_end=date(2030, 6, 11),
    ),
    OfficialDef(
        nation="KOR",
        profile_id="KOR_BOK_GOV_PROFILE",
        position_id="KOR_BOK_GOVERNOR",
        person_name="Rhee Chang-yong",
        role="BOK_GOVERNOR",
        role_description="Governor of the Bank of Korea",
        in_role_since=date(2022, 4, 21),
        expected_term_end=date(2026, 4, 20),
    ),
    OfficialDef(
        nation="KOR",
        profile_id="KOR_FM_PROFILE",
        position_id="KOR_FINANCE_MINISTER",
        person_name="Choi Sang-mok",
        role="FINANCE_MINISTER",
        role_description="Minister of Economy and Finance of South Korea",
        in_role_since=date(2024, 12, 27),
    ),
]


# ── Seed functions ───────────────────────────────────────────────────────


def _seed_positions(officials: list[OfficialDef], *, dry_run: bool) -> int:
    """Upsert position_occupancy rows for all officials."""

    if dry_run:
        for o in officials:
            print(f"  [dry] {o.position_id}: {o.person_name} ({o.nation})")
        return len(officials)

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
            for o in officials:
                cur.execute(
                    sql,
                    (
                        generate_uuid(),
                        o.position_id,
                        o.person_name,
                        o.nation,
                        o.in_role_since,
                        o.expected_term_end,
                        Json({
                            "role_description": o.role_description,
                            "tier": 1,
                        }),
                    ),
                )
            conn.commit()
        finally:
            cur.close()

    return len(officials)


def _seed_profiles(
    officials: list[OfficialDef],
    *,
    dry_run: bool,
    use_llm: bool,
) -> tuple[int, int]:
    """Seed person_profiles via PersonProfileService.

    Returns (total, llm_enriched) counts.
    """

    if dry_run:
        for o in officials:
            print(f"  [dry] {o.profile_id}: {o.person_name} – {o.role} ({o.nation})")
        return len(officials), 0

    from apathis.nation.person_service import PersonProfileService
    from apathis.nation.storage import PersonProfileStorage

    db = get_db_manager()
    storage = PersonProfileStorage(db_manager=db)

    llm = None
    tool_agent = None
    if use_llm:
        from apathis.llm.gateway import get_llm
        llm = get_llm()
        print(f"  LLM configured: {llm.__class__.__name__}")

        # Create tool agent for tool-enhanced enrichment.
        try:
            from apathis.llm.agent import ToolAgent
            tool_agent = ToolAgent(
                provider=llm,
                tool_names=[
                    "search_wikipedia", "search_web",
                    "get_nation_indicators", "get_current_date",
                ],
                max_rounds=4,
            )
            print(f"  Tool agent: enabled ({len(tool_agent.tool_names)} tools)")
        except Exception as exc:
            print(f"  Tool agent: disabled ({exc})")

    svc = PersonProfileService(storage=storage, llm=llm, tool_agent=tool_agent)

    enriched = 0
    for i, o in enumerate(officials, 1):
        t0 = time.time()
        print(f"  [{i}/{len(officials)}] {o.person_name} – {o.role} ({o.nation}) ...", end=" ", flush=True)

        profile = svc.seed_profile(
            profile_id=o.profile_id,
            person_name=o.person_name,
            nation=o.nation,
            role=o.role,
            role_tier=1,
            in_role_since=o.in_role_since,
            expected_term_end=o.expected_term_end,
        )

        elapsed = time.time() - t0
        if profile.confidence > 0.5:
            enriched += 1
            print(f"OK (conf={profile.confidence:.2f}, {elapsed:.1f}s)")
        else:
            print(f"skeleton (conf={profile.confidence:.2f}, {elapsed:.1f}s)")

    return len(officials), enriched


# ── Main ─────────────────────────────────────────────────────────────────


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Seed global Tier 1 officials (9 nations × 3 officials)"
    )
    parser.add_argument("--dry-run", action="store_true", help="Print what would be seeded")
    parser.add_argument("--no-llm", action="store_true", help="Seed skeletons only (no LLM enrichment)")
    parser.add_argument(
        "--nation",
        type=str,
        default=None,
        help="Seed a single nation only (e.g. GBR)",
    )
    args = parser.parse_args(argv)

    officials = GLOBAL_TIER1
    if args.nation:
        officials = [o for o in officials if o.nation == args.nation.upper()]
        if not officials:
            print(f"No officials defined for nation: {args.nation}")
            return

    nations = sorted(set(o.nation for o in officials))
    print(f"=== Seeding {len(officials)} Tier 1 officials across {len(nations)} nations ===")
    print(f"    Nations: {', '.join(nations)}")

    print("\n--- Position occupancy ---")
    n_pos = _seed_positions(officials, dry_run=args.dry_run)
    print(f"  → {n_pos} positions {'planned' if args.dry_run else 'upserted'}")

    llm_status = "OFF" if args.no_llm else "ON"
    print(f"\n--- Person profiles (LLM={llm_status}) ---")
    n_prof, n_enrich = _seed_profiles(
        officials,
        dry_run=args.dry_run,
        use_llm=not args.no_llm,
    )
    print(f"  → {n_prof} profiles {'planned' if args.dry_run else 'seeded'}, {n_enrich} LLM-enriched")

    print("\nDone.")


if __name__ == "__main__":  # pragma: no cover
    main()
