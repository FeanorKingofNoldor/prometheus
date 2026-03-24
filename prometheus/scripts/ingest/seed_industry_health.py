"""Prometheus v2 – Seed nation_industry_health with realistic data.

Generates 90 days of industry health scores for every nation/industry
pair defined in industries.py.  Uses Ornstein-Uhlenbeck mean-reverting
random walks with per-industry biases for realism.

Usage:
    python -m prometheus.scripts.ingest.seed_industry_health
    python -m prometheus.scripts.ingest.seed_industry_health --dry-run
    python -m prometheus.scripts.ingest.seed_industry_health --days 180
"""

from __future__ import annotations

import argparse
import random
from datetime import date, timedelta

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from apathis.nation.industries import NATION_INDUSTRIES
from psycopg2.extras import Json

logger = get_logger(__name__)

# ── Industry baseline health biases ──────────────────────────────────────
# Higher = healthier baseline.  Some industries are structurally under
# pressure (e.g. coal mining) while others are booming (e.g. EV batteries).

INDUSTRY_HEALTH_BIAS: dict[str, float] = {
    "technology": 0.72,
    "financial_services": 0.68,
    "aerospace_defense": 0.70,
    "pharma_biotech": 0.73,
    "energy": 0.62,
    "automotive": 0.58,
    "chemicals": 0.55,
    "heavy_machinery": 0.60,
    "precision_engineering": 0.70,
    "electronics_manufacturing": 0.65,
    "steel": 0.45,
    "ev_batteries": 0.78,
    "construction_infrastructure": 0.50,
    "textiles_garments": 0.52,
    "electronics_semiconductors": 0.75,
    "robotics_automation": 0.76,
    "luxury_goods": 0.72,
    "nuclear_energy": 0.65,
    "agriculture_food": 0.60,
    "semiconductors": 0.77,
    "shipbuilding": 0.62,
    "petrochemicals": 0.55,
    "it_services": 0.74,
    "pharma_generics": 0.68,
    "mining": 0.58,
    "lng_export": 0.65,
    "oil_sands_energy": 0.52,
    "oil_gas": 0.56,
    "agriculture_agribusiness": 0.60,
    "defense_industry": 0.65,
    "metals_mining": 0.55,
    "electronics_components": 0.67,
    "precision_instruments": 0.70,
    "palm_oil": 0.58,
    "nickel_smelting": 0.62,
    "coal_mining": 0.40,
    "automotive_assembly": 0.55,
    "creative_industries": 0.65,
    "cybersecurity_defense_tech": 0.78,
    "pharma_medtech": 0.72,
    "oil_refining_trading": 0.60,
    "automotive_components": 0.60,
    "it_outsourcing": 0.72,
    "electronics_assembly": 0.65,
    "tourism": 0.58,
    "fintech_telecom": 0.70,
    "agriculture": 0.48,
    "maritime_shipping": 0.60,
    "aquaculture": 0.65,
    "logistics_trade": 0.68,
    "construction_megaprojects": 0.55,
    "construction": 0.58,
    "suez_canal_logistics": 0.50,
    "natural_gas": 0.60,
}

# Regulatory pressure biases by category
CATEGORY_REG_PRESSURE: dict[str, float] = {
    "technology": 0.45,
    "manufacturing": 0.50,
    "services": 0.35,
    "extractive": 0.55,
    "agriculture": 0.40,
}

TREND_THRESHOLDS = {"GROWING": 0.02, "CONTRACTING": -0.02}


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _ou_walk(mean: float, n: int, sigma: float = 0.015, theta: float = 0.05) -> list[float]:
    """Ornstein-Uhlenbeck mean-reverting random walk."""
    values = [mean + random.gauss(0, sigma * 2)]
    for _ in range(n - 1):
        prev = values[-1]
        drift = theta * (mean - prev)
        noise = random.gauss(0, sigma)
        values.append(_clamp(prev + drift + noise))
    return values


def generate_health_data(days: int = 90) -> list[dict]:
    """Generate industry health rows for all nations/industries."""
    today = date.today()
    start = today - timedelta(days=days - 1)
    dates = [start + timedelta(days=i) for i in range(days)]

    rows = []
    for ind in NATION_INDUSTRIES:
        # Base health from industry + small per-nation jitter
        base = INDUSTRY_HEALTH_BIAS.get(ind.industry, 0.60)
        nation_jitter = random.uniform(-0.06, 0.06)
        mean_health = _clamp(base + nation_jitter, 0.15, 0.90)

        # Generate time series
        health_series = _ou_walk(mean_health, days)
        reg_base = CATEGORY_REG_PRESSURE.get(ind.category, 0.45) + random.uniform(-0.10, 0.10)
        reg_series = _ou_walk(_clamp(reg_base), days, sigma=0.008, theta=0.03)
        sent_mean = (mean_health - 0.50) * 1.5  # map health to sentiment
        sent_series = _ou_walk(_clamp(sent_mean, -0.8, 0.8), days, sigma=0.02, theta=0.04)

        # PMI: ~50 is neutral, scale from health
        pmi_mean = 45 + mean_health * 15  # range ~45-58
        pmi_series = _ou_walk(pmi_mean, days, sigma=0.5, theta=0.03)

        for i, d in enumerate(dates):
            h = health_series[i]
            growth = (health_series[i] - health_series[max(0, i - 30)]) * 100 if i >= 30 else random.uniform(-3, 3)

            if growth > TREND_THRESHOLDS["GROWING"]:
                trend = "GROWING"
            elif growth < TREND_THRESHOLDS["CONTRACTING"]:
                trend = "CONTRACTING"
            else:
                trend = "STABLE"

            rows.append({
                "nation": ind.nation,
                "industry": ind.industry,
                "as_of_date": d,
                "health_score": round(_clamp(h), 4),
                "pmi_component": round(_clamp(pmi_series[i], 30, 70), 1),
                "output_trend": trend,
                "regulatory_pressure": round(_clamp(reg_series[i]), 4),
                "sentiment": round(_clamp(sent_series[i], -1, 1), 4),
                "growth_yoy_pct": round(growth, 2),
                "metadata": {"source": "seed", "version": "v1"},
            })

    return rows


def insert_rows(rows: list[dict], dry_run: bool = False) -> int:
    """Insert rows into nation_industry_health (upsert)."""
    if dry_run:
        logger.info("DRY RUN: would insert %d rows", len(rows))
        return len(rows)

    db = get_db_manager()
    inserted = 0

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO nation_industry_health
                        (nation, industry, as_of_date, health_score,
                         pmi_component, output_trend, regulatory_pressure,
                         sentiment, growth_yoy_pct, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (nation, industry, as_of_date) DO UPDATE SET
                        health_score = EXCLUDED.health_score,
                        pmi_component = EXCLUDED.pmi_component,
                        output_trend = EXCLUDED.output_trend,
                        regulatory_pressure = EXCLUDED.regulatory_pressure,
                        sentiment = EXCLUDED.sentiment,
                        growth_yoy_pct = EXCLUDED.growth_yoy_pct,
                        metadata = EXCLUDED.metadata,
                        updated_at = now()
                    """,
                    (
                        r["nation"], r["industry"], r["as_of_date"],
                        r["health_score"], r["pmi_component"], r["output_trend"],
                        r["regulatory_pressure"], r["sentiment"],
                        r["growth_yoy_pct"], Json(r["metadata"]),
                    ),
                )
                inserted += 1

            conn.commit()
        finally:
            cur.close()

    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed nation_industry_health")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--days", type=int, default=90, help="Days of history to generate")
    args = parser.parse_args()

    rows = generate_health_data(days=args.days)
    logger.info(
        "Generated %d rows for %d nation-industry pairs over %d days",
        len(rows),
        len(NATION_INDUSTRIES),
        args.days,
    )

    inserted = insert_rows(rows, dry_run=args.dry_run)
    logger.info("Inserted %d rows into nation_industry_health", inserted)


if __name__ == "__main__":
    main()
