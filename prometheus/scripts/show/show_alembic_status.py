"""Prometheus v2 – Show Alembic revision status.

This script is a Layer 0 validation tool.

It reports:
- the current alembic_version revision in runtime_db and historical_db
- the codebase head revision(s)
- whether each DB is at head

The goal is to keep migrations deterministic and ensure both DBs stay on
known schema versions.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional, Sequence

from alembic.config import Config
from alembic.script import ScriptDirectory
from apatheon.core.database import get_db_manager


def _get_alembic_heads(project_root: Path) -> list[str]:
    cfg = Config(str(project_root / "alembic.ini"))
    cfg.set_main_option("script_location", str(project_root / "migrations"))
    script_dir = ScriptDirectory.from_config(cfg)
    return list(script_dir.get_heads())


def _read_db_version(db, which: str) -> str | None:
    sql = "SELECT version_num FROM alembic_version"

    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    try:
        with conn_cm as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql)
                row = cur.fetchone()
            finally:
                cur.close()
    except Exception:
        # If the table is missing or the DB is unreachable, surface as None.
        return None

    if row is None or row[0] is None:
        return None

    return str(row[0])


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show Alembic version status for runtime and historical DBs")
    parser.parse_args(argv)

    project_root = Path(__file__).resolve().parents[3]
    heads = _get_alembic_heads(project_root)

    db = get_db_manager()

    runtime_version = _read_db_version(db, "runtime")
    historical_version = _read_db_version(db, "historical")

    report = {
        "alembic_heads": heads,
        "runtime_db_version": runtime_version,
        "historical_db_version": historical_version,
        "runtime_at_head": runtime_version in set(heads) if runtime_version is not None else False,
        "historical_at_head": historical_version in set(heads) if historical_version is not None else False,
        "db_versions_match": (
            runtime_version == historical_version
            if runtime_version is not None and historical_version is not None
            else False
        ),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
