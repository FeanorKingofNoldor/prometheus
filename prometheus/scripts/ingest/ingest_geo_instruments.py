"""Prometheus v2 – Ingest geo series instruments into runtime DB.

This script ensures the cross-asset observable series required for the Nation
Profile Engine exist in the runtime `instruments` table.

It reads `configs/geo/series.yaml` and upserts one `instruments` row per
configured series.

Design notes
- These instruments should live in a dedicated market_id (default: GEO_SERIES)
  so they are not accidentally included in CORE_EQ universes.
- We set `metadata.eodhd_symbol` when provider.name == "eodhd".

Usage
  python -m prometheus.scripts.ingest.ingest_geo_instruments
  python -m prometheus.scripts.ingest.ingest_geo_instruments --dry-run
  python -m prometheus.scripts.ingest.ingest_geo_instruments --include-disabled
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
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "geo" / "series.yaml"


def _load_series_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Config not found: {path}")

    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict):
        raise SystemExit(f"Invalid YAML (expected mapping): {path}")

    series = raw.get("series")
    if not isinstance(series, dict):
        raise SystemExit(f"Invalid YAML: expected 'series' mapping in {path}")

    default_market_id = str(raw.get("default_market_id") or "GEO_SERIES").strip().upper() or "GEO_SERIES"

    return {
        "default_market_id": default_market_id,
        "series": series,
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Upsert geo series instruments from configs/geo/series.yaml"
    )

    parser.add_argument(
        "--config",
        type=str,
        default=str(DEFAULT_CONFIG_PATH),
        help=f"Path to series.yaml (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--market-id",
        type=str,
        default=None,
        help="Override market_id for inserted instruments (default from YAML: GEO_SERIES)",
    )
    parser.add_argument(
        "--include-disabled",
        action="store_true",
        help="Also upsert series entries with enabled=false",
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

    cfg = _load_series_config(cfg_path)
    market_id = str(args.market_id).strip().upper() if isinstance(args.market_id, str) and args.market_id else str(cfg["default_market_id"])

    series_map: dict[str, Any] = cfg["series"]

    planned: list[dict[str, Any]] = []

    for series_id, entry in series_map.items():
        if not isinstance(series_id, str) or not series_id.strip():
            continue
        if not isinstance(entry, dict):
            continue

        enabled = bool(entry.get("enabled", True))
        if not enabled and not bool(args.include_disabled):
            continue

        asset_class = str(entry.get("asset_class") or "INDEX").strip().upper() or "INDEX"
        currency = str(entry.get("currency") or "USD").strip().upper() or "USD"

        provider = entry.get("provider") if isinstance(entry.get("provider"), dict) else {}
        provider_name = str(provider.get("name") or "").strip().lower() or None
        provider_symbol = str(provider.get("symbol") or "").strip() or None

        instrument_id = str(series_id).strip()
        symbol = instrument_id.split(".", 1)[0].strip() or instrument_id

        meta: dict[str, Any] = {
            "source": "geo_series_registry",
            "enabled": bool(enabled),
            "role": entry.get("role"),
            "tags": entry.get("tags") if isinstance(entry.get("tags"), list) else [],
            "provider": {
                "name": provider_name,
                "symbol": provider_symbol,
            },
        }

        if provider_name == "eodhd" and provider_symbol:
            meta["eodhd_symbol"] = provider_symbol

        planned.append(
            {
                "instrument_id": instrument_id,
                "issuer_id": None,
                "market_id": market_id,
                "asset_class": asset_class,
                "symbol": symbol,
                "exchange": (provider_name or "").upper() or None,
                "currency": currency,
                "status": "ACTIVE",
                "metadata": meta,
            }
        )

    if args.dry_run:
        print(json.dumps({"planned": planned, "market_id": market_id}, indent=2, sort_keys=True))
        return

    if not planned:
        raise SystemExit("No series selected for upsert")

    db = get_db_manager()

    # Ensure market exists.
    sql_market = """
        INSERT INTO markets (market_id, name, region, timezone)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (market_id) DO NOTHING
    """

    # Upsert instruments.
    sql_inst = """
        INSERT INTO instruments (
            instrument_id,
            issuer_id,
            market_id,
            asset_class,
            symbol,
            exchange,
            currency,
            status,
            metadata
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (instrument_id) DO UPDATE SET
            issuer_id = EXCLUDED.issuer_id,
            market_id = EXCLUDED.market_id,
            asset_class = EXCLUDED.asset_class,
            symbol = EXCLUDED.symbol,
            exchange = EXCLUDED.exchange,
            currency = EXCLUDED.currency,
            status = EXCLUDED.status,
            metadata = EXCLUDED.metadata
    """

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_market, (market_id, "Geo Series", "GLOBAL", "UTC"))

            for p in planned:
                cur.execute(
                    sql_inst,
                    (
                        p["instrument_id"],
                        p["issuer_id"],
                        p["market_id"],
                        p["asset_class"],
                        p["symbol"],
                        p["exchange"],
                        p["currency"],
                        p["status"],
                        Json(p["metadata"]),
                    ),
                )

            conn.commit()
        finally:
            cur.close()

    print(json.dumps({"upserted": len(planned), "config": str(cfg_path), "market_id": market_id}, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
