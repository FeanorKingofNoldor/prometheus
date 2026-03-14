"""Prometheus v2 – Seed nation_scores for 90-nation expansion.

Inserts placeholder nation_scores rows for the 90 nations that lack DB
data.  Scores are tier-based defaults with per-nation adjustments for
notable outliers.

Nations already scored (10): USA, GBR, JPN, CHN, DEU, FRA, CAN, AUS, CHE, KOR

Usage:
    python -m prometheus.scripts.ingest.seed_expansion_90_scores
    python -m prometheus.scripts.ingest.seed_expansion_90_scores --dry-run
    python -m prometheus.scripts.ingest.seed_expansion_90_scores --nation ITA
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Optional, Sequence

from psycopg2.extras import Json

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger

logger = get_logger(__name__)


# ── Tier baselines ───────────────────────────────────────────────────────
# Each tier has baseline scores; individual nations override as needed.
# Higher values = better/more stable (except contagion_risk where higher
# = more contagion exposure).
#
# Keys: econ, mkt, curr, pol, cont, lead_risk, lead_comp, opp
# Composite is computed from weights below.

TIER_BASELINES: dict[str, dict[str, float]] = {
    "A": {"econ": 0.70, "mkt": 0.73, "curr": 0.88, "pol": 0.62,
          "cont": 0.78, "lead_risk": 0.35, "lead_comp": 0.65, "opp": 0.66},
    "B": {"econ": 0.60, "mkt": 0.60, "curr": 0.72, "pol": 0.52,
          "cont": 0.65, "lead_risk": 0.40, "lead_comp": 0.58, "opp": 0.55},
    "C": {"econ": 0.65, "mkt": 0.65, "curr": 0.80, "pol": 0.60,
          "cont": 0.60, "lead_risk": 0.35, "lead_comp": 0.62, "opp": 0.60},
    "D": {"econ": 0.55, "mkt": 0.55, "curr": 0.68, "pol": 0.55,
          "cont": 0.50, "lead_risk": 0.40, "lead_comp": 0.55, "opp": 0.52},
    "E": {"econ": 0.50, "mkt": 0.50, "curr": 0.60, "pol": 0.50,
          "cont": 0.42, "lead_risk": 0.42, "lead_comp": 0.52, "opp": 0.48},
    "F": {"econ": 0.38, "mkt": 0.35, "curr": 0.42, "pol": 0.40,
          "cont": 0.32, "lead_risk": 0.50, "lead_comp": 0.42, "opp": 0.38},
    "G": {"econ": 0.33, "mkt": 0.30, "curr": 0.38, "pol": 0.35,
          "cont": 0.28, "lead_risk": 0.55, "lead_comp": 0.38, "opp": 0.32},
    "H": {"econ": 0.42, "mkt": 0.40, "curr": 0.48, "pol": 0.40,
          "cont": 0.38, "lead_risk": 0.48, "lead_comp": 0.45, "opp": 0.40},
    "I": {"econ": 0.40, "mkt": 0.38, "curr": 0.45, "pol": 0.38,
          "cont": 0.32, "lead_risk": 0.50, "lead_comp": 0.42, "opp": 0.36},
    "J": {"econ": 0.55, "mkt": 0.55, "curr": 0.72, "pol": 0.55,
          "cont": 0.38, "lead_risk": 0.38, "lead_comp": 0.55, "opp": 0.50},
}


# ── Nation → tier mapping ────────────────────────────────────────────────

NATION_TIERS: dict[str, str] = {
    # Tier A
    "ITA": "A",
    # Tier B
    "IND": "B", "BRA": "B", "RUS": "B", "MEX": "B", "ESP": "B", "IDN": "B",
    # Tier C
    "NLD": "C", "SAU": "C", "TUR": "C", "TWN": "C", "POL": "C", "SWE": "C",
    "BEL": "C", "NOR": "C", "AUT": "C", "ARE": "C", "ISR": "C",
    # Tier D
    "IRL": "D", "THA": "D", "SGP": "D", "DNK": "D", "MYS": "D", "ZAF": "D",
    "PHL": "D", "COL": "D", "CHL": "D", "FIN": "D", "EGY": "D", "KWT": "D",
    # Tier E
    "CZE": "E", "VNM": "E", "PRT": "E", "NZL": "E", "PER": "E", "ROU": "E",
    "GRC": "E", "QAT": "E", "NGA": "E", "ARG": "E",
    # Tier F
    "AGO": "F", "COD": "F", "GHA": "F", "KEN": "F", "ETH": "F", "TZA": "F",
    "CIV": "F", "MOZ": "F", "ZMB": "F", "BWA": "F",
    # Tier G
    "NAM": "G", "GAB": "G", "GIN": "G", "MAR": "G", "LBY": "G", "SEN": "G",
    "UGA": "G", "ZWE": "G", "SDN": "G", "CMR": "G",
    # Tier H
    "UKR": "H", "PAK": "H", "BGD": "H", "KAZ": "H", "IRN": "H", "IRQ": "H",
    "HUN": "H", "SRB": "H", "SVK": "H", "GEO": "H",
    # Tier I
    "ECU": "I", "VEN": "I", "BOL": "I", "URY": "I", "PAN": "I", "MMR": "I",
    "KHM": "I", "BRN": "I", "LKA": "I", "NPL": "I",
    # Tier J
    "BGR": "J", "HRV": "J", "LTU": "J", "EST": "J", "JOR": "J", "OMN": "J",
    "MNG": "J", "DOM": "J", "CRI": "J", "GTM": "J",
}


# ── Per-nation adjustments ───────────────────────────────────────────────
# Deltas applied on top of the tier baseline.  Only notable outliers need
# entries here.

NATION_DELTAS: dict[str, dict[str, float]] = {
    # Tier A – ITA: higher debt concerns
    "ITA": {"econ": -0.05, "curr": -0.02, "pol": -0.04},

    # Tier B
    "IND": {"econ": +0.05, "opp": +0.08, "pol": -0.03},
    "RUS": {"econ": -0.10, "mkt": -0.15, "curr": -0.15, "pol": -0.15,
            "cont": -0.20, "lead_risk": +0.15, "opp": -0.15},
    "ESP": {"econ": +0.03, "curr": +0.08, "pol": +0.05},
    "IDN": {"opp": +0.06, "econ": +0.03},
    "BRA": {"pol": -0.05, "curr": -0.05, "opp": +0.03},

    # Tier C
    "NLD": {"econ": +0.05, "pol": +0.05, "curr": +0.08},
    "SAU": {"curr": +0.05, "opp": +0.05, "pol": -0.05},
    "TUR": {"econ": -0.08, "curr": -0.15, "pol": -0.10, "lead_risk": +0.10},
    "TWN": {"econ": +0.08, "mkt": +0.05, "pol": -0.08, "lead_risk": +0.05},
    "NOR": {"econ": +0.10, "curr": +0.05, "pol": +0.08, "opp": +0.05},
    "SWE": {"econ": +0.05, "pol": +0.08},
    "AUT": {"econ": +0.05, "pol": +0.05},
    "ARE": {"econ": +0.05, "curr": +0.05, "opp": +0.08},
    "ISR": {"pol": -0.12, "lead_risk": +0.08, "mkt": -0.05},

    # Tier D
    "SGP": {"econ": +0.15, "mkt": +0.15, "curr": +0.15, "pol": +0.15,
            "opp": +0.12, "lead_risk": -0.10, "lead_comp": +0.10},
    "IRL": {"econ": +0.08, "curr": +0.10, "pol": +0.05},
    "DNK": {"econ": +0.10, "pol": +0.10, "curr": +0.08},
    "FIN": {"econ": +0.08, "pol": +0.10, "curr": +0.08},
    "CHL": {"econ": +0.05, "pol": +0.03},
    "KWT": {"curr": +0.08, "opp": +0.03},
    "EGY": {"econ": -0.05, "curr": -0.10, "pol": -0.08, "lead_risk": +0.05},

    # Tier E
    "NZL": {"econ": +0.10, "pol": +0.12, "curr": +0.10, "opp": +0.05},
    "CZE": {"econ": +0.05, "pol": +0.05, "curr": +0.05},
    "QAT": {"econ": +0.08, "curr": +0.10, "pol": +0.03},
    "ARG": {"econ": -0.10, "curr": -0.20, "mkt": -0.10, "pol": -0.08,
            "lead_risk": +0.10},
    "NGA": {"econ": -0.05, "curr": -0.10, "pol": -0.08},

    # Tier F
    "BWA": {"econ": +0.10, "pol": +0.12, "curr": +0.08, "lead_risk": -0.08},
    "GHA": {"econ": +0.03, "pol": +0.05},
    "KEN": {"econ": +0.05, "opp": +0.05},
    "CIV": {"econ": +0.05, "opp": +0.05},
    "COD": {"pol": -0.08, "lead_risk": +0.08},

    # Tier G
    "MAR": {"econ": +0.08, "pol": +0.05, "opp": +0.08},
    "SDN": {"econ": -0.10, "pol": -0.15, "mkt": -0.10, "lead_risk": +0.15},
    "LBY": {"pol": -0.12, "econ": -0.05, "lead_risk": +0.10},
    "ZWE": {"econ": -0.08, "curr": -0.15, "pol": -0.08},
    "NAM": {"pol": +0.05, "econ": +0.03},

    # Tier H
    "UKR": {"econ": -0.12, "mkt": -0.15, "pol": -0.12, "lead_risk": +0.10,
            "cont": -0.10},
    "IRN": {"econ": -0.10, "curr": -0.15, "pol": -0.10, "mkt": -0.10,
            "lead_risk": +0.10},
    "IRQ": {"pol": -0.10, "econ": -0.08, "lead_risk": +0.08},
    "HUN": {"econ": +0.08, "curr": +0.12, "pol": +0.05, "lead_risk": +0.05},
    "SVK": {"econ": +0.05, "curr": +0.12, "pol": +0.05},
    "KAZ": {"econ": +0.05, "opp": +0.05},

    # Tier I
    "VEN": {"econ": -0.15, "curr": -0.20, "pol": -0.15, "mkt": -0.15,
            "lead_risk": +0.15, "opp": -0.12},
    "URY": {"econ": +0.12, "pol": +0.12, "curr": +0.10, "mkt": +0.08,
            "lead_risk": -0.10},
    "PAN": {"econ": +0.08, "curr": +0.15, "opp": +0.05},
    "BRN": {"econ": +0.10, "curr": +0.10, "pol": +0.05},
    "MMR": {"pol": -0.15, "econ": -0.10, "lead_risk": +0.12},

    # Tier J
    "EST": {"econ": +0.08, "pol": +0.08, "curr": +0.08},
    "LTU": {"econ": +0.05, "pol": +0.05, "curr": +0.05},
    "HRV": {"econ": +0.03, "curr": +0.08},
    "CRI": {"pol": +0.05, "econ": +0.03},
    "OMN": {"econ": +0.05, "curr": +0.08, "pol": +0.05},
}


# ── Composite weights (match the scoring engine) ────────────────────────

COMPOSITE_WEIGHTS = {
    "economic_stability": 0.20,
    "market_stability": 0.20,
    "currency_risk": 0.15,
    "political_stability": 0.15,
    "contagion_risk_inv": 0.10,
    "leadership_composite": 0.10,
    "structural": 0.10,
}
STRUCTURAL_DEFAULT = 0.50  # placeholder for structural component


# ── Score computation ────────────────────────────────────────────────────


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _compute_scores(nation: str) -> dict:
    """Build a full nation_scores row dict for a single nation."""

    tier = NATION_TIERS[nation]
    base = TIER_BASELINES[tier].copy()
    deltas = NATION_DELTAS.get(nation, {})

    for k, d in deltas.items():
        base[k] = base.get(k, 0.0) + d

    econ = _clamp(base["econ"])
    mkt = _clamp(base["mkt"])
    curr = _clamp(base["curr"])
    pol = _clamp(base["pol"])
    cont = _clamp(base["cont"])
    lead_risk = _clamp(base["lead_risk"])
    lead_comp = _clamp(base["lead_comp"])
    opp = _clamp(base["opp"])

    # Composite risk score
    composite = (
        COMPOSITE_WEIGHTS["economic_stability"] * econ
        + COMPOSITE_WEIGHTS["market_stability"] * mkt
        + COMPOSITE_WEIGHTS["currency_risk"] * curr
        + COMPOSITE_WEIGHTS["political_stability"] * pol
        + COMPOSITE_WEIGHTS["contagion_risk_inv"] * (1.0 - cont)
        + COMPOSITE_WEIGHTS["leadership_composite"] * lead_comp
        + COMPOSITE_WEIGHTS["structural"] * STRUCTURAL_DEFAULT
    )

    # Default neutral policy direction
    policy_direction = {
        "trade": 0.0,
        "fiscal": 0.0,
        "monetary": 0.0,
        "regulatory": 0.0,
    }

    component_details = {
        "composite_weights": COMPOSITE_WEIGHTS,
        "structural_default": STRUCTURAL_DEFAULT,
        "source": "seed_expansion_90",
    }

    return {
        "nation": nation,
        "economic_stability": round(econ, 4),
        "market_stability": round(mkt, 4),
        "currency_risk": round(curr, 4),
        "political_stability": round(pol, 4),
        "contagion_risk": round(cont, 4),
        "policy_direction": policy_direction,
        "leadership_risk": round(lead_risk, 4),
        "leadership_composite": round(lead_comp, 4),
        "opportunity_score": round(opp, 4),
        "composite_risk": round(composite, 4),
        "component_details": component_details,
    }


# ── Seed function ────────────────────────────────────────────────────────


def _seed_scores(
    nations: list[str],
    *,
    dry_run: bool,
    as_of: date,
) -> int:
    """Insert nation_scores rows."""

    rows = [_compute_scores(n) for n in nations]

    if dry_run:
        for r in rows:
            print(
                f"  [dry] {r['nation']:4s}  composite={r['composite_risk']:.3f}  "
                f"econ={r['economic_stability']:.2f}  mkt={r['market_stability']:.2f}  "
                f"curr={r['currency_risk']:.2f}  pol={r['political_stability']:.2f}  "
                f"cont={r['contagion_risk']:.2f}  lead={r['leadership_composite']:.2f}  "
                f"opp={r['opportunity_score']:.2f}"
            )
        return len(rows)

    sql = """
        INSERT INTO nation_scores (
            nation, as_of_date,
            economic_stability, market_stability, currency_risk,
            political_stability, contagion_risk, policy_direction,
            leadership_risk, leadership_composite,
            opportunity_score, composite_risk,
            component_details, metadata
        ) VALUES (
            %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s
        )
        ON CONFLICT (nation, as_of_date) DO UPDATE SET
            economic_stability = EXCLUDED.economic_stability,
            market_stability = EXCLUDED.market_stability,
            currency_risk = EXCLUDED.currency_risk,
            political_stability = EXCLUDED.political_stability,
            contagion_risk = EXCLUDED.contagion_risk,
            policy_direction = EXCLUDED.policy_direction,
            leadership_risk = EXCLUDED.leadership_risk,
            leadership_composite = EXCLUDED.leadership_composite,
            opportunity_score = EXCLUDED.opportunity_score,
            composite_risk = EXCLUDED.composite_risk,
            component_details = EXCLUDED.component_details,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
    """

    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            for r in rows:
                cur.execute(
                    sql,
                    (
                        r["nation"], as_of,
                        r["economic_stability"], r["market_stability"],
                        r["currency_risk"],
                        r["political_stability"], r["contagion_risk"],
                        Json(r["policy_direction"]),
                        r["leadership_risk"], r["leadership_composite"],
                        r["opportunity_score"], r["composite_risk"],
                        Json(r["component_details"]),
                        Json({"source": "seed_expansion_90", "seed_date": str(as_of)}),
                    ),
                )
            conn.commit()
            logger.info("Seeded %d nation_scores rows (as_of=%s)", len(rows), as_of)
        finally:
            cur.close()

    return len(rows)


# ── Main ─────────────────────────────────────────────────────────────────


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Seed nation_scores for 90-nation expansion"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be seeded")
    parser.add_argument("--nation", type=str, default=None,
                        help="Seed a single nation only (e.g. ITA)")
    parser.add_argument("--as-of", type=str, default=None,
                        help="Score date (YYYY-MM-DD); defaults to today")
    args = parser.parse_args(argv)

    as_of = date.fromisoformat(args.as_of) if args.as_of else date.today()

    nations = sorted(NATION_TIERS.keys())
    if args.nation:
        n = args.nation.upper()
        if n not in NATION_TIERS:
            print(f"Unknown nation: {n}")
            return
        nations = [n]

    print(f"=== Seeding nation_scores for {len(nations)} nations (as_of={as_of}) ===")

    n_scores = _seed_scores(nations, dry_run=args.dry_run, as_of=as_of)
    print(f"\n  → {n_scores} nation_scores {'planned' if args.dry_run else 'upserted'}")
    print("Done.")


if __name__ == "__main__":  # pragma: no cover
    main()
