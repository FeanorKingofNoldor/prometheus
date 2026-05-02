"""Prometheus v2 – Backfill nation_scores with 5 years of daily data.

Generates realistic daily score time series for all 100 nations from
2021-03-06 to 2026-03-06 using:

- Tier baselines from the seed expansion framework
- Ornstein-Uhlenbeck (mean-reverting) random walk per dimension
- Correlated shocks across dimensions
- Historical macro event overlays (COVID recovery, Ukraine, rate hikes, etc.)

Usage:
    python -m prometheus.scripts.ingest.backfill_nation_scores_5y
    python -m prometheus.scripts.ingest.backfill_nation_scores_5y --dry-run
    python -m prometheus.scripts.ingest.backfill_nation_scores_5y --nation USA
    python -m prometheus.scripts.ingest.backfill_nation_scores_5y --start 2024-01-01
"""

from __future__ import annotations

import argparse
import math
import sys
from datetime import date, timedelta
from typing import Optional, Sequence

import numpy as np
from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger
from psycopg2.extras import Json

logger = get_logger(__name__)

# ── Constants ────────────────────────────────────────────────────────────

START_DATE = date(2021, 3, 6)
END_DATE = date(2026, 3, 6)

SCORE_DIMS = [
    "economic_stability",
    "market_stability",
    "currency_risk",
    "political_stability",
    "contagion_risk",
    "leadership_risk",
    "leadership_composite",
    "opportunity_score",
]

# Ornstein-Uhlenbeck parameters per dimension:
#   theta = mean-reversion speed (higher = snaps back faster)
#   sigma = daily volatility
OU_PARAMS: dict[str, dict[str, float]] = {
    "economic_stability":   {"theta": 0.02,  "sigma": 0.006},
    "market_stability":     {"theta": 0.04,  "sigma": 0.012},
    "currency_risk":        {"theta": 0.015, "sigma": 0.004},
    "political_stability":  {"theta": 0.008, "sigma": 0.003},
    "contagion_risk":       {"theta": 0.01,  "sigma": 0.005},
    "leadership_risk":      {"theta": 0.005, "sigma": 0.002},
    "leadership_composite": {"theta": 0.005, "sigma": 0.002},
    "opportunity_score":    {"theta": 0.025, "sigma": 0.008},
}

# Cross-dimension correlation (simplified: economic shocks affect market)
CORR_PAIRS = [
    ("economic_stability", "market_stability", 0.6),
    ("economic_stability", "opportunity_score", 0.5),
    ("market_stability", "opportunity_score", 0.4),
    ("political_stability", "leadership_composite", 0.3),
    ("currency_risk", "economic_stability", 0.3),
]

# ── Tier baselines (includes original 10 + expansion 90) ────────────────

TIER_BASELINES: dict[str, dict[str, float]] = {
    "S": {"econ": 0.72, "mkt": 0.78, "curr": 0.95, "pol": 0.72,
          "cont": 0.85, "lead_risk": 0.29, "lead_comp": 0.73, "opp": 0.59},
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

# All 100 nations → tier
NATION_TIERS: dict[str, str] = {
    # Original 10 (tier S – advanced economies with deep data)
    "USA": "S", "GBR": "S", "JPN": "S", "CHN": "S", "DEU": "S",
    "FRA": "S", "CAN": "S", "AUS": "S", "CHE": "S", "KOR": "S",
    # Expansion 90
    "ITA": "A",
    "IND": "B", "BRA": "B", "RUS": "B", "MEX": "B", "ESP": "B", "IDN": "B",
    "NLD": "C", "SAU": "C", "TUR": "C", "TWN": "C", "POL": "C", "SWE": "C",
    "BEL": "C", "NOR": "C", "AUT": "C", "ARE": "C", "ISR": "C",
    "IRL": "D", "THA": "D", "SGP": "D", "DNK": "D", "MYS": "D", "ZAF": "D",
    "PHL": "D", "COL": "D", "CHL": "D", "FIN": "D", "EGY": "D", "KWT": "D",
    "CZE": "E", "VNM": "E", "PRT": "E", "NZL": "E", "PER": "E", "ROU": "E",
    "GRC": "E", "QAT": "E", "NGA": "E", "ARG": "E",
    "AGO": "F", "COD": "F", "GHA": "F", "KEN": "F", "ETH": "F", "TZA": "F",
    "CIV": "F", "MOZ": "F", "ZMB": "F", "BWA": "F",
    "NAM": "G", "GAB": "G", "GIN": "G", "MAR": "G", "LBY": "G", "SEN": "G",
    "UGA": "G", "ZWE": "G", "SDN": "G", "CMR": "G",
    "UKR": "H", "PAK": "H", "BGD": "H", "KAZ": "H", "IRN": "H", "IRQ": "H",
    "HUN": "H", "SRB": "H", "SVK": "H", "GEO": "H",
    "ECU": "I", "VEN": "I", "BOL": "I", "URY": "I", "PAN": "I", "MMR": "I",
    "KHM": "I", "BRN": "I", "LKA": "I", "NPL": "I",
    "BGR": "J", "HRV": "J", "LTU": "J", "EST": "J", "JOR": "J", "OMN": "J",
    "MNG": "J", "DOM": "J", "CRI": "J", "GTM": "J",
}

# Per-nation baseline adjustments (same as seed script)
NATION_DELTAS: dict[str, dict[str, float]] = {
    "USA": {"econ": +0.02, "mkt": +0.02, "curr": +0.00, "pol": +0.00},
    "GBR": {"econ": -0.02, "curr": -0.10},
    "JPN": {"econ": -0.05, "curr": -0.15, "pol": +0.05},
    "CHN": {"econ": +0.05, "curr": -0.35, "pol": -0.12, "lead_risk": +0.10, "opp": +0.08},
    "DEU": {"econ": -0.02, "curr": -0.13},
    "FRA": {"econ": -0.03, "curr": -0.15, "pol": -0.03},
    "CAN": {"econ": +0.00, "curr": -0.13},
    "AUS": {"econ": +0.00, "curr": -0.17},
    "CHE": {"econ": +0.03, "curr": -0.05, "pol": +0.08},
    "KOR": {"econ": +0.00, "curr": -0.25, "pol": -0.02},
    "ITA": {"econ": -0.05, "curr": -0.02, "pol": -0.04},
    "IND": {"econ": +0.05, "opp": +0.08, "pol": -0.03},
    "RUS": {"econ": -0.10, "mkt": -0.15, "curr": -0.15, "pol": -0.15,
            "cont": -0.20, "lead_risk": +0.15, "opp": -0.15},
    "ESP": {"econ": +0.03, "curr": +0.08, "pol": +0.05},
    "IDN": {"opp": +0.06, "econ": +0.03},
    "BRA": {"pol": -0.05, "curr": -0.05, "opp": +0.03},
    "NLD": {"econ": +0.05, "pol": +0.05, "curr": +0.08},
    "SAU": {"curr": +0.05, "opp": +0.05, "pol": -0.05},
    "TUR": {"econ": -0.08, "curr": -0.15, "pol": -0.10, "lead_risk": +0.10},
    "TWN": {"econ": +0.08, "mkt": +0.05, "pol": -0.08, "lead_risk": +0.05},
    "NOR": {"econ": +0.10, "curr": +0.05, "pol": +0.08, "opp": +0.05},
    "SWE": {"econ": +0.05, "pol": +0.08},
    "AUT": {"econ": +0.05, "pol": +0.05},
    "ARE": {"econ": +0.05, "curr": +0.05, "opp": +0.08},
    "ISR": {"pol": -0.12, "lead_risk": +0.08, "mkt": -0.05},
    "SGP": {"econ": +0.15, "mkt": +0.15, "curr": +0.15, "pol": +0.15,
            "opp": +0.12, "lead_risk": -0.10, "lead_comp": +0.10},
    "IRL": {"econ": +0.08, "curr": +0.10, "pol": +0.05},
    "DNK": {"econ": +0.10, "pol": +0.10, "curr": +0.08},
    "FIN": {"econ": +0.08, "pol": +0.10, "curr": +0.08},
    "CHL": {"econ": +0.05, "pol": +0.03},
    "KWT": {"curr": +0.08, "opp": +0.03},
    "EGY": {"econ": -0.05, "curr": -0.10, "pol": -0.08, "lead_risk": +0.05},
    "NZL": {"econ": +0.10, "pol": +0.12, "curr": +0.10, "opp": +0.05},
    "CZE": {"econ": +0.05, "pol": +0.05, "curr": +0.05},
    "QAT": {"econ": +0.08, "curr": +0.10, "pol": +0.03},
    "ARG": {"econ": -0.10, "curr": -0.20, "mkt": -0.10, "pol": -0.08,
            "lead_risk": +0.10},
    "NGA": {"econ": -0.05, "curr": -0.10, "pol": -0.08},
    "BWA": {"econ": +0.10, "pol": +0.12, "curr": +0.08, "lead_risk": -0.08},
    "GHA": {"econ": +0.03, "pol": +0.05},
    "KEN": {"econ": +0.05, "opp": +0.05},
    "CIV": {"econ": +0.05, "opp": +0.05},
    "COD": {"pol": -0.08, "lead_risk": +0.08},
    "MAR": {"econ": +0.08, "pol": +0.05, "opp": +0.08},
    "SDN": {"econ": -0.10, "pol": -0.15, "mkt": -0.10, "lead_risk": +0.15},
    "LBY": {"pol": -0.12, "econ": -0.05, "lead_risk": +0.10},
    "ZWE": {"econ": -0.08, "curr": -0.15, "pol": -0.08},
    "NAM": {"pol": +0.05, "econ": +0.03},
    "UKR": {"econ": -0.12, "mkt": -0.15, "pol": -0.12, "lead_risk": +0.10,
            "cont": -0.10},
    "IRN": {"econ": -0.10, "curr": -0.15, "pol": -0.10, "mkt": -0.10,
            "lead_risk": +0.10},
    "IRQ": {"pol": -0.10, "econ": -0.08, "lead_risk": +0.08},
    "HUN": {"econ": +0.08, "curr": +0.12, "pol": +0.05, "lead_risk": +0.05},
    "SVK": {"econ": +0.05, "curr": +0.12, "pol": +0.05},
    "KAZ": {"econ": +0.05, "opp": +0.05},
    "VEN": {"econ": -0.15, "curr": -0.20, "pol": -0.15, "mkt": -0.15,
            "lead_risk": +0.15, "opp": -0.12},
    "URY": {"econ": +0.12, "pol": +0.12, "curr": +0.10, "mkt": +0.08,
            "lead_risk": -0.10},
    "PAN": {"econ": +0.08, "curr": +0.15, "opp": +0.05},
    "BRN": {"econ": +0.10, "curr": +0.10, "pol": +0.05},
    "MMR": {"pol": -0.15, "econ": -0.10, "lead_risk": +0.12},
    "EST": {"econ": +0.08, "pol": +0.08, "curr": +0.08},
    "LTU": {"econ": +0.05, "pol": +0.05, "curr": +0.05},
    "HRV": {"econ": +0.03, "curr": +0.08},
    "CRI": {"pol": +0.05, "econ": +0.03},
    "OMN": {"econ": +0.05, "curr": +0.08, "pol": +0.05},
}


# ── Historical events (date range → shock applied to affected nations) ───
# Each event: (start, end, affected_nations_or_"ALL", dimension_shocks)
# Shocks are additive shifts applied during the window.

HISTORICAL_EVENTS: list[dict] = [
    # COVID recovery rally (2021 H1) — markets rebounding
    {
        "start": date(2021, 3, 6), "end": date(2021, 6, 30),
        "nations": "ALL",
        "shocks": {"market_stability": +0.04, "economic_stability": +0.03,
                   "opportunity_score": +0.03},
        "label": "COVID recovery rally",
    },
    # Inflation surge (2021 H2 – 2022 H1)
    {
        "start": date(2021, 7, 1), "end": date(2022, 6, 30),
        "nations": "ALL",
        "shocks": {"economic_stability": -0.04, "market_stability": -0.02},
        "label": "Inflation surge",
    },
    # Russia-Ukraine war (Feb 2022+)
    {
        "start": date(2022, 2, 24), "end": date(2022, 12, 31),
        "nations": ["RUS", "UKR"],
        "shocks": {"economic_stability": -0.20, "market_stability": -0.25,
                   "political_stability": -0.15, "opportunity_score": -0.20,
                   "currency_risk": -0.15, "contagion_risk": -0.15},
        "label": "Ukraine war (direct)",
    },
    {
        "start": date(2022, 2, 24), "end": date(2022, 9, 30),
        "nations": ["DEU", "FRA", "ITA", "POL", "HUN", "AUT", "CZE", "SVK",
                     "ROU", "BGR", "HRV", "LTU", "EST", "FIN", "SWE", "NOR",
                     "GBR", "NLD", "BEL", "GRC"],
        "shocks": {"economic_stability": -0.06, "market_stability": -0.05,
                   "contagion_risk": -0.05},
        "label": "Ukraine war (Europe spillover)",
    },
    {
        "start": date(2022, 2, 24), "end": date(2022, 6, 30),
        "nations": "ALL",
        "shocks": {"market_stability": -0.03, "economic_stability": -0.02},
        "label": "Ukraine war (global)",
    },
    # Aggressive rate hiking cycle (2022 H2 – 2023)
    {
        "start": date(2022, 6, 1), "end": date(2023, 7, 31),
        "nations": ["USA", "GBR", "CAN", "AUS", "NZL", "KOR", "CHE"],
        "shocks": {"market_stability": -0.05, "opportunity_score": -0.04},
        "label": "Rate hiking cycle",
    },
    # UK mini-budget crisis (Sep-Oct 2022)
    {
        "start": date(2022, 9, 23), "end": date(2022, 11, 15),
        "nations": ["GBR"],
        "shocks": {"market_stability": -0.12, "currency_risk": -0.10,
                   "political_stability": -0.08, "leadership_risk": +0.10},
        "label": "UK gilt crisis",
    },
    # China property crisis
    {
        "start": date(2021, 9, 1), "end": date(2023, 6, 30),
        "nations": ["CHN"],
        "shocks": {"economic_stability": -0.08, "market_stability": -0.10,
                   "opportunity_score": -0.06},
        "label": "China property crisis",
    },
    # Turkey lira crisis
    {
        "start": date(2021, 10, 1), "end": date(2022, 6, 30),
        "nations": ["TUR"],
        "shocks": {"currency_risk": -0.18, "economic_stability": -0.12,
                   "market_stability": -0.10},
        "label": "Turkey lira crisis",
    },
    # Sri Lanka default
    {
        "start": date(2022, 4, 1), "end": date(2022, 12, 31),
        "nations": ["LKA"],
        "shocks": {"economic_stability": -0.20, "currency_risk": -0.25,
                   "political_stability": -0.20, "market_stability": -0.15},
        "label": "Sri Lanka default",
    },
    # 2023 banking stress (SVB etc.)
    {
        "start": date(2023, 3, 10), "end": date(2023, 5, 15),
        "nations": ["USA", "CHE"],
        "shocks": {"market_stability": -0.08, "contagion_risk": -0.05},
        "label": "Banking stress (SVB/CS)",
    },
    {
        "start": date(2023, 3, 10), "end": date(2023, 4, 30),
        "nations": "ALL",
        "shocks": {"market_stability": -0.03},
        "label": "Banking stress (global)",
    },
    # 2023 H2 recovery / soft landing narrative
    {
        "start": date(2023, 7, 1), "end": date(2023, 12, 31),
        "nations": "ALL",
        "shocks": {"market_stability": +0.04, "opportunity_score": +0.03,
                   "economic_stability": +0.02},
        "label": "Soft landing rally",
    },
    # Argentina crisis / Milei election
    {
        "start": date(2023, 8, 1), "end": date(2024, 3, 31),
        "nations": ["ARG"],
        "shocks": {"economic_stability": -0.10, "currency_risk": -0.12,
                   "political_stability": -0.08, "leadership_risk": +0.10},
        "label": "Argentina crisis / Milei",
    },
    # 2024 rate cut expectations rally
    {
        "start": date(2024, 1, 1), "end": date(2024, 9, 30),
        "nations": "ALL",
        "shocks": {"market_stability": +0.03, "opportunity_score": +0.02},
        "label": "Rate cut rally",
    },
    # Middle East escalation (Oct 2023+)
    {
        "start": date(2023, 10, 7), "end": date(2024, 6, 30),
        "nations": ["ISR", "EGY", "JOR", "SAU", "IRN", "IRQ"],
        "shocks": {"political_stability": -0.10, "market_stability": -0.05,
                   "contagion_risk": -0.05},
        "label": "Middle East escalation",
    },
    # Sudan civil war
    {
        "start": date(2023, 4, 15), "end": date(2025, 12, 31),
        "nations": ["SDN"],
        "shocks": {"political_stability": -0.20, "economic_stability": -0.15,
                   "leadership_risk": +0.15},
        "label": "Sudan civil war",
    },
    # Myanmar continued instability
    {
        "start": date(2021, 3, 6), "end": date(2025, 12, 31),
        "nations": ["MMR"],
        "shocks": {"political_stability": -0.12, "economic_stability": -0.08},
        "label": "Myanmar coup aftermath",
    },
    # 2025 tariff / trade war fears
    {
        "start": date(2025, 1, 15), "end": date(2026, 3, 6),
        "nations": ["CHN", "MEX", "CAN", "KOR", "TWN", "VNM", "THA", "MYS"],
        "shocks": {"market_stability": -0.04, "opportunity_score": -0.03,
                   "economic_stability": -0.03},
        "label": "2025 tariff fears",
    },
    {
        "start": date(2025, 1, 15), "end": date(2026, 3, 6),
        "nations": "ALL",
        "shocks": {"market_stability": -0.02},
        "label": "2025 global trade uncertainty",
    },
]

# Composite weights (match scoring engine)
COMPOSITE_WEIGHTS = {
    "economic_stability": 0.20,
    "market_stability": 0.20,
    "currency_risk": 0.15,
    "political_stability": 0.15,
    "contagion_risk_inv": 0.10,
    "leadership_composite": 0.10,
    "structural": 0.10,
}
STRUCTURAL_DEFAULT = 0.50


# ── Helper ───────────────────────────────────────────────────────────────

DIM_TO_KEY = {
    "economic_stability": "econ",
    "market_stability": "mkt",
    "currency_risk": "curr",
    "political_stability": "pol",
    "contagion_risk": "cont",
    "leadership_risk": "lead_risk",
    "leadership_composite": "lead_comp",
    "opportunity_score": "opp",
}


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _get_baseline(nation: str) -> dict[str, float]:
    """Get the mean-reversion target for a nation."""
    tier = NATION_TIERS[nation]
    base = TIER_BASELINES[tier].copy()
    for k, d in NATION_DELTAS.get(nation, {}).items():
        base[k] = base.get(k, 0.0) + d
    # Convert short keys to full dim names
    return {dim: _clamp(base[short]) for dim, short in DIM_TO_KEY.items()}


def _get_event_shock(nation: str, d: date) -> dict[str, float]:
    """Sum all active event shocks for a nation on a given date."""
    total: dict[str, float] = {}
    for ev in HISTORICAL_EVENTS:
        if ev["start"] <= d <= ev["end"]:
            affected = ev["nations"]
            if affected == "ALL" or nation in affected:
                for dim, shock in ev["shocks"].items():
                    # Fade in/out over first/last 20% of event window
                    span = (ev["end"] - ev["start"]).days or 1
                    elapsed = (d - ev["start"]).days
                    remaining = (ev["end"] - d).days
                    fade_in = min(1.0, elapsed / max(1, span * 0.2))
                    fade_out = min(1.0, remaining / max(1, span * 0.2))
                    fade = min(fade_in, fade_out)
                    total[dim] = total.get(dim, 0.0) + shock * fade
    return total


def _compute_composite(scores: dict[str, float]) -> float:
    composite = (
        COMPOSITE_WEIGHTS["economic_stability"] * scores["economic_stability"]
        + COMPOSITE_WEIGHTS["market_stability"] * scores["market_stability"]
        + COMPOSITE_WEIGHTS["currency_risk"] * scores["currency_risk"]
        + COMPOSITE_WEIGHTS["political_stability"] * scores["political_stability"]
        + COMPOSITE_WEIGHTS["contagion_risk_inv"] * (1.0 - scores["contagion_risk"])
        + COMPOSITE_WEIGHTS["leadership_composite"] * scores["leadership_composite"]
        + COMPOSITE_WEIGHTS["structural"] * STRUCTURAL_DEFAULT
    )
    return _clamp(composite)


# ── Time series generation ───────────────────────────────────────────────


def generate_nation_series(
    nation: str,
    start: date,
    end: date,
    rng: np.random.Generator,
) -> list[dict]:
    """Generate daily score rows for one nation."""

    baseline = _get_baseline(nation)
    n_days = (end - start).days + 1

    # Initialize at baseline with small random offset
    current = {dim: baseline[dim] + rng.normal(0, 0.01) for dim in SCORE_DIMS}
    current = {dim: _clamp(v) for dim, v in current.items()}

    rows: list[dict] = []

    for i in range(n_days):
        d = start + timedelta(days=i)

        # Event shocks shift the mean-reversion target
        event_shocks = _get_event_shock(nation, d)

        # Ornstein-Uhlenbeck step per dimension
        innovations = {dim: rng.normal(0, 1) for dim in SCORE_DIMS}

        # Apply cross-correlations
        for dim_a, dim_b, rho in CORR_PAIRS:
            if dim_a in innovations and dim_b in innovations:
                innovations[dim_b] = (
                    rho * innovations[dim_a]
                    + math.sqrt(1 - rho**2) * innovations[dim_b]
                )

        for dim in SCORE_DIMS:
            params = OU_PARAMS[dim]
            target = baseline[dim] + event_shocks.get(dim, 0.0)
            target = _clamp(target)

            # OU step: dx = theta*(mu - x)*dt + sigma*dW
            dx = (
                params["theta"] * (target - current[dim])
                + params["sigma"] * innovations[dim]
            )
            current[dim] = _clamp(current[dim] + dx)

        composite = _compute_composite(current)

        rows.append({
            "nation": nation,
            "as_of_date": d,
            "economic_stability": round(current["economic_stability"], 4),
            "market_stability": round(current["market_stability"], 4),
            "currency_risk": round(current["currency_risk"], 4),
            "political_stability": round(current["political_stability"], 4),
            "contagion_risk": round(current["contagion_risk"], 4),
            "policy_direction": {"monetary": 0.0, "fiscal": 0.0,
                                 "trade": 0.0, "regulatory": 0.0},
            "leadership_risk": round(current["leadership_risk"], 4),
            "leadership_composite": round(current["leadership_composite"], 4),
            "opportunity_score": round(current["opportunity_score"], 4),
            "composite_risk": round(composite, 4),
            "component_details": {"source": "backfill_5y"},
        })

    return rows


# ── DB insert ────────────────────────────────────────────────────────────

INSERT_SQL = """
    INSERT INTO nation_scores (
        nation, as_of_date,
        economic_stability, market_stability, currency_risk,
        political_stability, contagion_risk, policy_direction,
        leadership_risk, leadership_composite,
        opportunity_score, composite_risk,
        component_details, metadata, updated_at
    ) VALUES (
        %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s,
        %s, %s,
        %s, %s, NOW()
    )
    ON CONFLICT (nation, as_of_date)
    DO UPDATE SET
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


def _insert_rows(rows: list[dict], batch_size: int = 500) -> int:
    """Bulk insert rows into nation_scores."""
    db = get_db_manager()
    total = 0

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                for r in batch:
                    cur.execute(
                        INSERT_SQL,
                        (
                            r["nation"], r["as_of_date"],
                            r["economic_stability"], r["market_stability"],
                            r["currency_risk"],
                            r["political_stability"], r["contagion_risk"],
                            Json(r["policy_direction"]),
                            r["leadership_risk"], r["leadership_composite"],
                            r["opportunity_score"], r["composite_risk"],
                            Json(r["component_details"]),
                            Json({"source": "backfill_5y"}),
                        ),
                    )
                conn.commit()
                total += len(batch)
                if total % 5000 == 0:
                    print(f"  … inserted {total:,} / {len(rows):,} rows")
        finally:
            cur.close()

    return total


# ── Main ─────────────────────────────────────────────────────────────────


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Backfill 5 years of daily nation_scores"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Only compute, don't write to DB")
    parser.add_argument("--nation", type=str, default=None,
                        help="Backfill a single nation (e.g. USA)")
    parser.add_argument("--start", type=str, default=None,
                        help="Start date (YYYY-MM-DD; default 2021-03-06)")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD; default 2026-03-06)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility")
    args = parser.parse_args(argv)

    start = date.fromisoformat(args.start) if args.start else START_DATE
    end = date.fromisoformat(args.end) if args.end else END_DATE

    nations = sorted(NATION_TIERS.keys())
    if args.nation:
        n = args.nation.upper()
        if n not in NATION_TIERS:
            print(f"Unknown nation: {n}")
            sys.exit(1)
        nations = [n]

    n_days = (end - start).days + 1
    total_rows = len(nations) * n_days
    print(f"=== Backfilling nation_scores: {len(nations)} nations × {n_days} days = {total_rows:,} rows ===")
    print(f"    Range: {start} → {end}")

    rng = np.random.default_rng(args.seed)
    all_rows: list[dict] = []

    for i, nation in enumerate(nations, 1):
        rows = generate_nation_series(nation, start, end, rng)
        all_rows.extend(rows)
        if i % 10 == 0 or i == len(nations):
            print(f"  Generated {i}/{len(nations)} nations ({len(all_rows):,} rows)")

    if args.dry_run:
        # Print a sample
        sample_nations = ["USA", "CHN", "RUS", "ARG", "SDN"]
        for sn in sample_nations:
            if sn in [r["nation"] for r in all_rows]:
                nation_rows = [r for r in all_rows if r["nation"] == sn]
                first = nation_rows[0]
                last = nation_rows[-1]
                print(f"\n  {sn} first: composite={first['composite_risk']:.3f} "
                      f"econ={first['economic_stability']:.3f} mkt={first['market_stability']:.3f}")
                print(f"  {sn} last:  composite={last['composite_risk']:.3f} "
                      f"econ={last['economic_stability']:.3f} mkt={last['market_stability']:.3f}")
        print(f"\n  → {len(all_rows):,} rows would be inserted (dry run)")
        return

    print(f"\n  Inserting {len(all_rows):,} rows …")
    inserted = _insert_rows(all_rows)
    print(f"\n  → {inserted:,} rows upserted into nation_scores")
    print("Done.")


if __name__ == "__main__":
    main()
