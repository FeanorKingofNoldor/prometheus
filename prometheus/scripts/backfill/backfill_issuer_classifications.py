"""Backfill issuer_classifications (time-versioned sector/industry).

This script populates the Layer 0 `issuer_classifications` table by
fetching EODHD fundamentals and extracting the `General` classification
fields (Sector/Industry/etc.).

The primary goal is to reduce the dominance of missing/UNKNOWN sectors in
cluster-based research (e.g. lambda opportunity density), while moving to
an explicit time-versioned contract.

Notes
-----
- EODHD fundamentals typically provide *current* sector/industry.
  We store it as an open-ended interval starting at `--effective-start`.
- The script is idempotent via an upsert on
  (issuer_id, taxonomy, effective_start).
- Responses are cached on disk to make the backfill resumable and to
  avoid repeated API calls.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import requests
from apathis.core.database import DatabaseManager, get_db_manager
from apathis.core.logging import get_logger
from apathis.data.classifications import DEFAULT_CLASSIFICATION_TAXONOMY
from psycopg2.extras import Json

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except Exception as exc:
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _load_sp500_issuers(db: DatabaseManager) -> List[str]:
    sql = """
        SELECT issuer_id
        FROM issuers
        WHERE metadata->>'sp500' = 'true'
        ORDER BY issuer_id
    """

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        finally:
            cur.close()

    return [str(r[0]) for r in rows if r and r[0]]


def _load_existing_classification_issuer_ids(
    db: DatabaseManager,
    *,
    taxonomy: str,
    effective_start: date,
) -> set[str]:
    """Return issuer_ids that already have the (taxonomy, effective_start) row."""

    sql = """
        SELECT issuer_id
        FROM issuer_classifications
        WHERE taxonomy = %s
          AND effective_start = %s
    """

    out: set[str] = set()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (taxonomy, effective_start))
            for (issuer_id,) in cur.fetchall():
                if issuer_id:
                    out.add(str(issuer_id))
        finally:
            cur.close()

    return out


def _load_issuer_sector_industry(db: DatabaseManager, issuer_ids: Sequence[str]) -> Dict[str, Tuple[str, str | None]]:
    """Return issuer_id -> (sector, industry) from the runtime issuers table."""

    if not issuer_ids:
        return {}

    sql = """
        SELECT issuer_id,
               NULLIF(NULLIF(sector, ''), 'UNKNOWN') AS sector,
               NULLIF(NULLIF(industry, ''), 'UNKNOWN') AS industry
        FROM issuers
        WHERE issuer_id = ANY(%s)
    """

    out: Dict[str, Tuple[str, str | None]] = {}
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (list(issuer_ids),))
            for issuer_id, sector, industry in cur.fetchall():
                if not issuer_id or not sector:
                    continue
                out[str(issuer_id)] = (str(sector), str(industry) if industry is not None else None)
        finally:
            cur.close()

    return out


@dataclass(frozen=True)
class _ClassificationPayload:
    sector: str
    industry: str | None
    sub_industry: str | None
    raw_general: dict


def _extract_classification_from_general(general: dict) -> Optional[_ClassificationPayload]:
    # EODHD commonly uses these keys.
    sector = (general.get("Sector") or general.get("GicSector") or "").strip()
    industry = (general.get("Industry") or general.get("GicIndustry") or general.get("GicIndustryGroup"))
    sub_industry = general.get("GicSubIndustry") or general.get("SubIndustry")

    if industry is not None:
        industry = str(industry).strip() or None
    if sub_industry is not None:
        sub_industry = str(sub_industry).strip() or None

    if not sector:
        return None

    return _ClassificationPayload(
        sector=sector,
        industry=industry,
        sub_industry=sub_industry,
        raw_general=general,
    )


def _cache_path_for_symbol(cache_root: Path, symbol: str) -> Path:
    safe = symbol.replace("/", "_")
    return cache_root / f"{safe}.general.json"


def _fetch_general_cached(
    symbol: str,
    *,
    api_token: str,
    cache_root: Path,
    base_url: str = "https://eodhd.com/api",
    timeout_seconds: int = 30,
    use_cache: bool = True,
) -> Optional[dict]:
    cache_root.mkdir(parents=True, exist_ok=True)
    cache_path = _cache_path_for_symbol(cache_root, symbol)

    if use_cache and cache_path.exists():
        try:
            return json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    url = f"{base_url.rstrip('/')}/fundamentals/{symbol}"
    params = {"api_token": api_token, "fmt": "json"}

    try:
        resp = requests.get(url, params=params, timeout=timeout_seconds)
    except Exception as exc:
        logger.warning("EODHD fundamentals request failed for %s: %s", symbol, exc)
        return None

    if resp.status_code != 200:
        logger.warning(
            "EODHD fundamentals request failed for %s: status=%s body=%s",
            symbol,
            resp.status_code,
            resp.text[:300],
        )
        return None

    try:
        data = resp.json()
    except Exception as exc:
        logger.warning("Failed to decode EODHD JSON for %s: %s", symbol, exc)
        return None

    general = data.get("General") or {}
    if not isinstance(general, dict):
        general = {}

    # Cache only the General section (small; avoids storing huge financial payloads).
    try:
        cache_path.write_text(json.dumps(general, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except Exception:
        pass

    return general


def _upsert_issuer_classification(
    db: DatabaseManager,
    *,
    issuer_id: str,
    taxonomy: str,
    effective_start: date,
    effective_end: date | None,
    sector: str,
    industry: str | None,
    sub_industry: str | None,
    source: str,
    metadata: dict,
    dry_run: bool,
) -> None:
    sql = """
        INSERT INTO issuer_classifications (
            issuer_id,
            taxonomy,
            effective_start,
            effective_end,
            sector,
            industry,
            sub_industry,
            source,
            ingested_at,
            metadata
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
        ON CONFLICT (issuer_id, taxonomy, effective_start)
        DO UPDATE SET
            effective_end = EXCLUDED.effective_end,
            sector = EXCLUDED.sector,
            industry = EXCLUDED.industry,
            sub_industry = EXCLUDED.sub_industry,
            source = EXCLUDED.source,
            ingested_at = NOW(),
            metadata = EXCLUDED.metadata
    """

    if dry_run:
        return

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                sql,
                (
                    issuer_id,
                    taxonomy,
                    effective_start,
                    effective_end,
                    sector,
                    industry,
                    sub_industry,
                    source,
                    Json(metadata),
                ),
            )
            conn.commit()
        finally:
            cur.close()


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Backfill issuer_classifications from EODHD fundamentals")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--issuer-id",
        dest="issuer_ids",
        action="append",
        help="Issuer ID to backfill (can be specified multiple times)",
    )
    group.add_argument(
        "--sp500",
        action="store_true",
        help="Backfill all issuers tagged as S&P 500 in runtime DB",
    )

    parser.add_argument(
        "--taxonomy",
        type=str,
        default=DEFAULT_CLASSIFICATION_TAXONOMY,
        help=f"Taxonomy label to write (default: {DEFAULT_CLASSIFICATION_TAXONOMY})",
    )

    parser.add_argument(
        "--effective-start",
        type=_parse_date,
        default=date(1997, 1, 1),
        help="Effective start date for the open-ended interval (default: 1997-01-01)",
    )

    parser.add_argument(
        "--effective-end",
        type=_parse_date,
        default=None,
        help="Optional effective end date (exclusive). If omitted, interval is open-ended.",
    )

    parser.add_argument(
        "--cache-root",
        type=str,
        default="data/cache/eodhd_fundamentals_general",
        help="Cache directory for EODHD fundamentals General payloads",
    )

    parser.add_argument(
        "--seed-from-issuers",
        action="store_true",
        help="Seed issuer_classifications from the existing issuers.sector/industry fields (no API calls)",
    )

    parser.add_argument("--no-cache", action="store_true", help="Disable cache reads/writes")
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Only process issuer_ids that do not yet have a (taxonomy, effective_start) row",
    )
    parser.add_argument("--sleep-secs", type=float, default=0.25, help="Sleep between API calls")
    parser.add_argument("--max-issuers", type=int, default=0, help="Process at most N issuers (0 = all)")

    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB")

    args = parser.parse_args(argv)

    api_token = os.getenv("EODHD_API_KEY")
    if not args.seed_from_issuers and not api_token:
        raise SystemExit("EODHD_API_KEY is not set")

    db = get_db_manager()

    if args.sp500:
        issuer_ids = _load_sp500_issuers(db)
        if not issuer_ids:
            logger.warning("No SP500 issuers found in runtime DB")
            return
    else:
        issuer_ids = list(dict.fromkeys(args.issuer_ids or []))

    taxonomy = str(args.taxonomy)

    effective_start: date = args.effective_start
    effective_end: date | None = args.effective_end

    # Optionally skip issuers that already have the target row (reduces API usage).
    if args.only_missing:
        existing = _load_existing_classification_issuer_ids(
            db,
            taxonomy=taxonomy,
            effective_start=effective_start,
        )
        issuer_ids = [iid for iid in issuer_ids if str(iid) not in existing]

    if args.max_issuers and int(args.max_issuers) > 0:
        issuer_ids = issuer_ids[: int(args.max_issuers)]

    cache_root = Path(str(args.cache_root))
    use_cache = not bool(args.no_cache)

    logger.info(
        "Backfilling issuer_classifications: issuers=%d taxonomy=%s effective_start=%s dry_run=%s cache=%s seed_from_issuers=%s",
        len(issuer_ids),
        taxonomy,
        effective_start,
        bool(args.dry_run),
        use_cache,
        bool(args.seed_from_issuers),
    )

    ok = 0
    skipped = 0
    failed = 0

    issuer_seed: Dict[str, Tuple[str, str | None]] = {}
    if args.seed_from_issuers:
        issuer_seed = _load_issuer_sector_industry(db, issuer_ids)

    for i, issuer_id in enumerate(issuer_ids):
        symbol = f"{issuer_id}.US"

        try:
            if args.seed_from_issuers:
                rec = issuer_seed.get(str(issuer_id))
                if rec is None:
                    skipped += 1
                    continue
                sector, industry = rec
                meta = {
                    "source": "issuers",
                    "seed": True,
                    "effective_assumption": "static_from_current",
                }

                _upsert_issuer_classification(
                    db,
                    issuer_id=str(issuer_id),
                    taxonomy=taxonomy,
                    effective_start=effective_start,
                    effective_end=effective_end,
                    sector=sector,
                    industry=industry,
                    sub_industry=None,
                    source="issuers",
                    metadata=meta,
                    dry_run=bool(args.dry_run),
                )
                ok += 1
                continue

            # EODHD fundamentals path.
            assert api_token is not None
            general = _fetch_general_cached(
                symbol,
                api_token=api_token,
                cache_root=cache_root,
                use_cache=use_cache,
            )
            if not general:
                skipped += 1
                continue

            payload = _extract_classification_from_general(general)
            if payload is None:
                skipped += 1
                continue

            meta = {
                "vendor": "eodhd",
                "symbol": symbol,
                "general": payload.raw_general,
                "effective_assumption": "static_from_current",
            }

            _upsert_issuer_classification(
                db,
                issuer_id=str(issuer_id),
                taxonomy=taxonomy,
                effective_start=effective_start,
                effective_end=effective_end,
                sector=payload.sector,
                industry=payload.industry,
                sub_industry=payload.sub_industry,
                source="eodhd",
                metadata=meta,
                dry_run=bool(args.dry_run),
            )
            ok += 1
        except Exception as exc:  # pragma: no cover - defensive
            failed += 1
            logger.exception("Failed for issuer_id=%s (%s): %s", issuer_id, symbol, exc)

        # Simple rate limiting.
        if i + 1 < len(issuer_ids) and float(args.sleep_secs) > 0:
            time.sleep(float(args.sleep_secs))

    logger.info(
        "issuer_classifications backfill finished: ok=%d skipped=%d failed=%d",
        ok,
        skipped,
        failed,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
