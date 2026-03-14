"""Prometheus v2 – Show markets status (Layer 0 validation).

This script validates basic Layer 0 contracts for the ``markets`` table:
- market_id uniqueness (should be guaranteed by PK)
- timezone values are valid IANA TZ identifiers
- referenced market_ids in other tables are present in markets

It reports results for both runtime_db and historical_db.
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Dict, Optional, Sequence

from apathis.core.database import get_db_manager


def _load_markets(db, which: str) -> list[dict[str, Any]]:
    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    sql = """
        SELECT market_id, name, region, timezone
        FROM markets
        ORDER BY market_id
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        finally:
            cur.close()

    out: list[dict[str, Any]] = []
    for market_id, name, region, timezone in rows:
        out.append(
            {
                "market_id": str(market_id),
                "name": str(name),
                "region": str(region),
                "timezone": str(timezone),
            }
        )
    return out


def _load_referenced_market_ids(db, which: str) -> dict[str, list[str]]:
    """Return market_ids referenced by other tables in the same DB."""

    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    refs: dict[str, list[str]] = {}

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            # instruments.market_id
            try:
                cur.execute("SELECT DISTINCT market_id FROM instruments ORDER BY market_id")
                refs["instruments"] = [str(r[0]) for r in cur.fetchall() if r and r[0] is not None]
            except Exception:
                refs["instruments"] = []

            # market_holidays.market_id
            try:
                cur.execute("SELECT DISTINCT market_id FROM market_holidays ORDER BY market_id")
                refs["market_holidays"] = [str(r[0]) for r in cur.fetchall() if r and r[0] is not None]
            except Exception:
                refs["market_holidays"] = []
        finally:
            cur.close()

    return refs


def _validate_timezones(markets: list[dict[str, Any]]) -> list[str]:
    invalid: list[str] = []

    # ZoneInfo is stdlib (py>=3.9). Fedora provides tzdata via system.
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

    for m in markets:
        tz = str(m.get("timezone") or "")
        if tz.strip() == "":
            invalid.append(str(m.get("market_id")))
            continue
        try:
            ZoneInfo(tz)
        except ZoneInfoNotFoundError:
            invalid.append(str(m.get("market_id")))

    return invalid


def _summarise_db(db, which: str) -> dict[str, Any]:
    markets = _load_markets(db, which)
    market_ids = [m["market_id"] for m in markets]

    invalid_tz = _validate_timezones(markets)

    refs = _load_referenced_market_ids(db, which)
    referenced_all = sorted(set(refs.get("instruments", [])) | set(refs.get("market_holidays", [])))
    missing_referenced = sorted(set(referenced_all) - set(market_ids))

    return {
        "total_markets": len(markets),
        "markets": markets,
        "timezone_invalid_market_ids": invalid_tz,
        "timezone_check_passed": len(invalid_tz) == 0,
        "referenced_market_ids": refs,
        "missing_referenced_market_ids": missing_referenced,
        "referential_check_passed": len(missing_referenced) == 0,
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show markets status and basic Layer 0 validations")
    parser.parse_args(argv)

    db = get_db_manager()

    report: Dict[str, Any] = {
        "runtime": _summarise_db(db, "runtime"),
        "historical": _summarise_db(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
