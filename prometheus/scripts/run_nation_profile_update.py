"""Prometheus v2 – Nation Profile Update Pipeline.

End-to-end daily update: re-ingest FRED macro data for all 10 nations,
re-score each nation, and optionally refresh Wikipedia enrichment.

Designed to be run as a cron job or manually::

    # Full update (ingest + score all 10 nations):
    python -m prometheus.scripts.run_nation_profile_update

    # Single nation:
    python -m prometheus.scripts.run_nation_profile_update --nation USA

    # Include Wikipedia refresh:
    python -m prometheus.scripts.run_nation_profile_update --wiki

    # Dry-run (show what would happen):
    python -m prometheus.scripts.run_nation_profile_update --dry-run

Cron example (daily at 06:00 UTC)::

    0 6 * * * cd /home/feanor/coding/prometheus_v2 && .venv/bin/python -m prometheus.scripts.run_nation_profile_update >> logs/nation_update.log 2>&1
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from pathlib import Path
from typing import Optional, Sequence

from apathis.core.config import load_config
from apathis.core.database import DatabaseManager, get_db_manager
from apathis.core.logging import get_logger

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIGS_DIR = PROJECT_ROOT / "configs" / "nation"

# All 10 nations and their YAML config files.
NATION_CONFIGS: dict[str, str] = {
    "USA": "us_macro_series.yaml",
    "GBR": "gbr_macro_series.yaml",
    "JPN": "jpn_macro_series.yaml",
    "CHN": "chn_macro_series.yaml",
    "DEU": "deu_macro_series.yaml",
    "FRA": "fra_macro_series.yaml",
    "CAN": "can_macro_series.yaml",
    "AUS": "aus_macro_series.yaml",
    "CHE": "che_macro_series.yaml",
    "KOR": "kor_macro_series.yaml",
}


# ── Step 1: FRED Ingestion ──────────────────────────────────────────────


def step_ingest(
    db: DatabaseManager,
    nations: list[str],
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    """Re-ingest FRED macro data for the given nations.

    Returns {nation: total_rows_upserted}.
    """

    from apathis.data_ingestion.fred_client import FredClient
    from apathis.nation.ingestion import NationMacroIngestionService
    from apathis.nation.storage import NationMacroStorage

    results: dict[str, int] = {}

    if dry_run:
        for nation in nations:
            cfg_file = CONFIGS_DIR / NATION_CONFIGS[nation]
            print(f"  [dry] Would ingest {nation} from {cfg_file.name}")
            results[nation] = 0
        return results

    fred = FredClient()
    storage = NationMacroStorage(db)

    for nation in nations:
        cfg_file = CONFIGS_DIR / NATION_CONFIGS[nation]
        if not cfg_file.exists():
            logger.warning("Config not found for %s: %s", nation, cfg_file)
            results[nation] = 0
            continue

        t0 = time.time()
        print(f"  Ingesting {nation} ...", end=" ", flush=True)

        svc = NationMacroIngestionService(
            fred_client=fred,
            storage=storage,
            config_path=cfg_file,
        )

        try:
            series_results = svc.ingest_all()
            total = sum(series_results.values())
            failed = sum(1 for v in series_results.values() if v == 0)
            elapsed = time.time() - t0
            print(f"{total} rows ({len(series_results)} series, {failed} failed) [{elapsed:.1f}s]")
            results[nation] = total
        except Exception:
            logger.exception("Ingestion failed for %s", nation)
            print("FAILED")
            results[nation] = 0

    fred.close()
    return results


# ── Step 2: Scoring ─────────────────────────────────────────────────────


def step_score(
    db: DatabaseManager,
    nations: list[str],
    *,
    as_of: date | None = None,
    dry_run: bool = False,
) -> dict[str, float]:
    """Re-score all nations.  Returns {nation: composite_risk}."""

    from apathis.nation.engine import NationScoringEngine
    from apathis.nation.model_basic import BasicNationScoringModel
    from apathis.nation.storage import NationMacroStorage, NationScoreStorage, PersonProfileStorage

    results: dict[str, float] = {}
    score_date = as_of or date.today()

    if dry_run:
        for nation in nations:
            print(f"  [dry] Would score {nation} as of {score_date}")
            results[nation] = 0.0
        return results

    macro_storage = NationMacroStorage(db)
    profile_storage = PersonProfileStorage(db)
    score_storage = NationScoreStorage(db)

    model = BasicNationScoringModel(
        macro_storage=macro_storage,
        profile_storage=profile_storage,
    )
    engine = NationScoringEngine(model=model, storage=score_storage)

    for nation in nations:
        t0 = time.time()
        print(f"  Scoring {nation} ...", end=" ", flush=True)

        try:
            scores = engine.score_and_save(nation, score_date)
            elapsed = time.time() - t0
            print(
                f"composite={scores.composite_risk:.3f} "
                f"(econ={scores.economic_stability:.3f} mkt={scores.market_stability:.3f} "
                f"pol={scores.political_stability:.3f}) [{elapsed:.1f}s]"
            )
            results[nation] = scores.composite_risk
        except Exception:
            logger.exception("Scoring failed for %s", nation)
            print("FAILED")
            results[nation] = 0.0

    return results


# ── Step 3: Wikipedia Refresh (optional) ────────────────────────────────


def step_wiki(
    db: DatabaseManager,
    nations: list[str],
    *,
    force: bool = False,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Refresh Wikipedia photos/bios.  Returns (attempted, enriched)."""

    from apathis.nation.wikipedia_service import enrich_all_profiles

    if dry_run:
        print("  [dry] Would refresh Wikipedia for all profiles")
        return 0, 0

    for nation in nations:
        print(f"  Wikipedia: refreshing {nation} profiles ...")
        attempted, enriched = enrich_all_profiles(db, nation=nation, force=force)
        print(f"    → {enriched}/{attempted} enriched")

    # Return totals across all nations.
    total_attempted, total_enriched = enrich_all_profiles(db, force=force)
    return total_attempted, total_enriched


# ── Main ────────────────────────────────────────────────────────────────


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Nation Profile Update Pipeline — FRED ingest + scoring + optional Wikipedia"
    )
    parser.add_argument(
        "--nation",
        type=str,
        default=None,
        help="Update a single nation (e.g. USA). Default: all 10.",
    )
    parser.add_argument(
        "--wiki",
        action="store_true",
        help="Also refresh Wikipedia photos/bios",
    )
    parser.add_argument(
        "--wiki-force",
        action="store_true",
        help="Force Wikipedia refresh even if already enriched",
    )
    parser.add_argument(
        "--score-only",
        action="store_true",
        help="Skip ingestion, only re-score",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would happen without executing",
    )
    args = parser.parse_args(argv)

    # Determine which nations to process.
    if args.nation:
        nation_key = args.nation.upper()
        if nation_key not in NATION_CONFIGS:
            print(f"Unknown nation: {args.nation}. Available: {', '.join(sorted(NATION_CONFIGS))}")
            sys.exit(1)
        nations = [nation_key]
    else:
        nations = list(NATION_CONFIGS.keys())

    print(f"{'='*60}")
    print(f"  Nation Profile Update Pipeline")
    print(f"  Date: {date.today()}")
    print(f"  Nations: {', '.join(nations)}")
    print(f"  Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print(f"{'='*60}")

    db = get_db_manager()

    # Step 1: FRED ingestion.
    if not args.score_only:
        print(f"\n--- Step 1: FRED Ingestion ({len(nations)} nations) ---")
        ingest_results = step_ingest(db, nations, dry_run=args.dry_run)
        total_rows = sum(ingest_results.values())
        print(f"  Total: {total_rows} rows across {len(nations)} nations")
    else:
        print("\n--- Step 1: FRED Ingestion SKIPPED (--score-only) ---")

    # Step 2: Scoring.
    print(f"\n--- Step 2: Nation Scoring ({len(nations)} nations) ---")
    score_results = step_score(db, nations, dry_run=args.dry_run)

    if not args.dry_run:
        ranked = sorted(score_results.items(), key=lambda kv: kv[1], reverse=True)
        print("\n  Rankings:")
        for i, (nation, score) in enumerate(ranked, 1):
            print(f"    {i:2d}. {nation}  {score:.3f}")

    # Step 3: Wikipedia (optional).
    if args.wiki or args.wiki_force:
        print(f"\n--- Step 3: Wikipedia Enrichment ---")
        step_wiki(db, nations, force=args.wiki_force, dry_run=args.dry_run)
    else:
        print("\n--- Step 3: Wikipedia Enrichment SKIPPED (use --wiki) ---")

    print(f"\n{'='*60}")
    print("  Pipeline complete.")
    print(f"{'='*60}")


if __name__ == "__main__":  # pragma: no cover
    main()
