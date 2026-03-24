"""Prometheus v2 – Ingest sovereign issuers into runtime DB.

This script upserts sovereign issuers (issuer_type=SOVEREIGN) from
`configs/geo/nations.yaml` into the runtime `issuers` table.

EU modelling note
- EU is modeled as separate sovereigns only (no synthetic EU bloc issuer).

Usage
  python -m prometheus.scripts.ingest.ingest_sovereign_issuers
  python -m prometheus.scripts.ingest.ingest_sovereign_issuers --dry-run
  python -m prometheus.scripts.ingest.ingest_sovereign_issuers --config configs/geo/nations.yaml
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Optional, Sequence

import yaml
from apathis.core.database import get_db_manager
from psycopg2.extras import Json

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "geo" / "nations.yaml"


def _load_nations(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"Config not found: {path}")

    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict):
        raise SystemExit(f"Invalid YAML (expected mapping): {path}")

    nations_raw = raw.get("nations")
    if not isinstance(nations_raw, list):
        raise SystemExit(f"Invalid YAML: expected 'nations' as a list in {path}")

    nations: list[dict[str, Any]] = []
    for idx, n in enumerate(nations_raw):
        if not isinstance(n, dict):
            raise SystemExit(f"Invalid nation entry at index {idx}: expected mapping")

        iso3 = str(n.get("iso3") or "").strip().upper()
        issuer_id = str(n.get("issuer_id") or "").strip().upper()
        name = str(n.get("name") or "").strip()

        if not iso3 or len(iso3) < 3:
            raise SystemExit(f"Invalid iso3 for nation[{idx}]: {iso3!r}")
        if not issuer_id:
            raise SystemExit(f"Missing issuer_id for nation[{idx}] iso3={iso3}")
        if not name:
            raise SystemExit(f"Missing name for nation[{idx}] iso3={iso3}")

        currency = str(n.get("currency") or "").strip().upper() or None
        tier = n.get("tier")
        region = str(n.get("region") or "").strip().upper() or None
        tags = n.get("tags") if isinstance(n.get("tags"), list) else []

        nations.append(
            {
                "iso3": iso3,
                "issuer_id": issuer_id,
                "name": name,
                "currency": currency,
                "tier": int(tier) if isinstance(tier, (int, float)) else None,
                "region": region,
                "tags": [str(t).strip() for t in tags if str(t).strip()],
            }
        )

    return nations


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Upsert sovereign issuers (SOV_{ISO3}) from configs/geo/nations.yaml"
    )

    parser.add_argument(
        "--config",
        type=str,
        default=str(DEFAULT_CONFIG_PATH),
        help=f"Path to nations.yaml (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write to DB; print planned upserts",
    )

    args = parser.parse_args(argv)

    cfg_path = Path(str(args.config))
    if not cfg_path.is_absolute():
        cfg_path = PROJECT_ROOT / cfg_path

    nations = _load_nations(cfg_path)
    if not nations:
        raise SystemExit("No nations found in config")

    planned: list[dict[str, Any]] = []
    for n in nations:
        planned.append(
            {
                "issuer_id": n["issuer_id"],
                "issuer_type": "SOVEREIGN",
                "name": n["name"],
                "country": n["iso3"],
                "sector": None,
                "industry": None,
                "metadata": {
                    "source": "geo_nations_registry",
                    "iso3": n["iso3"],
                    "currency": n.get("currency"),
                    "tier": n.get("tier"),
                    "region": n.get("region"),
                    "tags": n.get("tags", []),
                },
            }
        )

    if args.dry_run:
        print(json.dumps({"planned": planned}, indent=2, sort_keys=True))
        return

    sql = """
        INSERT INTO issuers (
            issuer_id,
            issuer_type,
            name,
            country,
            sector,
            industry,
            metadata
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (issuer_id) DO UPDATE SET
            issuer_type = EXCLUDED.issuer_type,
            name = EXCLUDED.name,
            country = EXCLUDED.country,
            sector = EXCLUDED.sector,
            industry = EXCLUDED.industry,
            metadata = EXCLUDED.metadata
    """

    db = get_db_manager()

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            for p in planned:
                cur.execute(
                    sql,
                    (
                        p["issuer_id"],
                        p["issuer_type"],
                        p["name"],
                        p["country"],
                        p["sector"],
                        p["industry"],
                        Json(p["metadata"]),
                    ),
                )
            conn.commit()
        finally:
            cur.close()

    print(json.dumps({"upserted": len(planned), "config": str(cfg_path)}, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
