"""Prometheus v2 – Layer 0 validation gate (deploy/CI).

This entrypoint is intended to be used as a *deployment/preflight* gate:
- Verify DB schema is at the expected Alembic head (runtime + historical)
- Run fast Layer 0 validators and exit non-zero on any failure

Deep audits (coverage-style checks across large tables) are available behind
an explicit flag so they can be scheduled (CI/nightly) instead of blocking
service start.

Typical usage:

  # Preflight (fast): fail if either DB is not at Alembic head or any L0 validator fails
  python -m prometheus.scripts.validate.validate_layer0

  # Controlled schema upgrade before validation
  python -m prometheus.scripts.validate.validate_layer0 --upgrade

  # Deep audits (scheduled): requires an explicit as-of date
  python -m prometheus.scripts.validate.validate_layer0 --deep --as-of 2024-12-31
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Optional, Sequence

from alembic import command as alembic_command
from alembic.config import Config

from apathis.core.database import get_db_manager
from apathis.core.time import US_EQ
from apathis.data.classifications import DEFAULT_CLASSIFICATION_TAXONOMY

from prometheus.scripts.show import (
    show_alembic_status,
    show_config_change_log_status,
    show_data_ingestion_status_status,
    show_engine_runs_status,
    show_instruments_status,
    show_issuers_status,
    show_job_executions_status,
    show_market_holidays_status,
    show_markets_status,
    show_meta_config_proposals_status,
    show_portfolios_status,
    show_strategies_status,
    show_strategy_configs_status,
)


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    details: dict[str, Any]


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except Exception as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _project_root() -> Path:
    # prometheus/scripts/validate/validate_layer0.py -> project root is 4 parents up.
    return Path(__file__).resolve().parents[3]


def _alembic_upgrade(project_root: Path, *, db_selector: str) -> None:
    cfg = Config(str(project_root / "alembic.ini"))

    old = os.environ.get("ALEMBIC_DB")
    os.environ["ALEMBIC_DB"] = db_selector
    try:
        alembic_command.upgrade(cfg, "head")
    finally:
        if old is None:
            os.environ.pop("ALEMBIC_DB", None)
        else:
            os.environ["ALEMBIC_DB"] = old


def _check_alembic_heads(db, project_root: Path) -> CheckResult:
    heads = show_alembic_status._get_alembic_heads(project_root)
    runtime_version = show_alembic_status._read_db_version(db, "runtime")
    historical_version = show_alembic_status._read_db_version(db, "historical")

    runtime_at_head = runtime_version in set(heads) if runtime_version is not None else False
    historical_at_head = historical_version in set(heads) if historical_version is not None else False
    db_versions_match = (
        runtime_version == historical_version
        if runtime_version is not None and historical_version is not None
        else False
    )

    passed = bool(runtime_at_head and historical_at_head and db_versions_match)

    return CheckResult(
        name="alembic_heads",
        passed=passed,
        details={
            "alembic_heads": heads,
            "runtime_db_version": runtime_version,
            "historical_db_version": historical_version,
            "runtime_at_head": runtime_at_head,
            "historical_at_head": historical_at_head,
            "db_versions_match": db_versions_match,
        },
    )


def _check_markets(db) -> CheckResult:
    details = {
        "runtime": show_markets_status._summarise_db(db, "runtime"),
        "historical": show_markets_status._summarise_db(db, "historical"),
    }

    def _passed(which: str) -> bool:
        return bool(
            details[which].get("timezone_check_passed")
            and details[which].get("referential_check_passed")
        )

    return CheckResult(
        name="markets",
        passed=_passed("runtime") and _passed("historical"),
        details=details,
    )


def _check_market_holidays(db) -> CheckResult:
    details = {
        "runtime": show_market_holidays_status._summarise(db, "runtime"),
        "historical": show_market_holidays_status._summarise(db, "historical"),
    }

    def _passed(which: str) -> bool:
        r = details[which]
        return bool(
            r.get("duplicates_check_passed")
            and r.get("holiday_name_nonempty_check_passed")
            and r.get("orphan_check_passed")
        )

    return CheckResult(
        name="market_holidays",
        passed=_passed("runtime") and _passed("historical"),
        details=details,
    )


def _check_simple_checks_passed(name: str, db, module) -> CheckResult:
    details = {
        "runtime": module._summarise(db, "runtime"),
        "historical": module._summarise(db, "historical"),
    }
    passed = bool(details["runtime"].get("checks_passed") and details["historical"].get("checks_passed"))
    return CheckResult(name=name, passed=passed, details=details)


def _check_trigger_enabled(
    *,
    db,
    which: str,
    table: str,
    trigger_name: str,
) -> dict[str, Any]:
    """Fast schema sanity: ensure a trigger exists and is not disabled."""

    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    sql = """
        SELECT t.tgenabled
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        WHERE c.relname = %s
          AND t.tgname = %s
          AND NOT t.tgisinternal
        LIMIT 1
    """

    try:
        with conn_cm as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (table, trigger_name))
                row = cur.fetchone()
            finally:
                cur.close()

        found = row is not None and row[0] is not None
        tgenabled = str(row[0]) if found else None
        # Postgres tgenabled: O=enabled, D=disabled, A=always, R=replica
        passed = bool(found and tgenabled != "D")

        return {
            "passed": passed,
            "found": bool(found),
            "tgenabled": tgenabled,
        }
    except Exception as exc:
        return {
            "passed": False,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _check_interval_overlap_guards(db) -> CheckResult:
    """Preflight: verify overlap-prevention triggers exist and are enabled.

    Deep audits are responsible for scanning for historical overlaps.
    """

    details = {
        "issuer_classifications": {
            "runtime": _check_trigger_enabled(
                db=db,
                which="runtime",
                table="issuer_classifications",
                trigger_name="trg_issuer_classifications_prevent_overlap",
            ),
            "historical": _check_trigger_enabled(
                db=db,
                which="historical",
                table="issuer_classifications",
                trigger_name="trg_issuer_classifications_prevent_overlap",
            ),
        },
        "instrument_identifiers": {
            "runtime": _check_trigger_enabled(
                db=db,
                which="runtime",
                table="instrument_identifiers",
                trigger_name="trg_instrument_identifiers_prevent_overlap",
            ),
            "historical": _check_trigger_enabled(
                db=db,
                which="historical",
                table="instrument_identifiers",
                trigger_name="trg_instrument_identifiers_prevent_overlap",
            ),
        },
    }

    passed = bool(
        details["issuer_classifications"]["runtime"].get("passed")
        and details["issuer_classifications"]["historical"].get("passed")
        and details["instrument_identifiers"]["runtime"].get("passed")
        and details["instrument_identifiers"]["historical"].get("passed")
    )

    return CheckResult(
        name="interval_overlap_guards",
        passed=passed,
        details=details,
    )


def _check_no_overlap(
    *,
    db,
    table: str,
    pk_left: str,
    group_cols: tuple[str, str],
    start_col: str,
    end_col: str,
) -> dict[str, Any]:
    """Deep audit: scan for any overlapping effective intervals (runtime DB)."""

    conn_cm = db.get_runtime_connection()
    group_a, group_b = group_cols

    sql = f"""
        SELECT 1
        FROM {table} AS a
        JOIN {table} AS b
          ON a.{group_a} = b.{group_a}
         AND a.{group_b} = b.{group_b}
         AND a.{pk_left} < b.{pk_left}
         AND daterange(a.{start_col}, COALESCE(a.{end_col}, 'infinity'::date), '[)')
             && daterange(b.{start_col}, COALESCE(b.{end_col}, 'infinity'::date), '[)')
        LIMIT 1
    """

    try:
        with conn_cm as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql)
                overlap_exists = cur.fetchone() is not None
            finally:
                cur.close()

        return {
            "passed": not overlap_exists,
            "overlap_exists": bool(overlap_exists),
        }
    except Exception as exc:
        return {
            "passed": False,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _audit_interval_overlaps(db) -> CheckResult:
    details = {
        "issuer_classifications": _check_no_overlap(
            db=db,
            table="issuer_classifications",
            pk_left="classification_id",
            group_cols=("issuer_id", "taxonomy"),
            start_col="effective_start",
            end_col="effective_end",
        ),
        "instrument_identifiers": _check_no_overlap(
            db=db,
            table="instrument_identifiers",
            pk_left="instrument_identifier_id",
            group_cols=("instrument_id", "identifier_type"),
            start_col="effective_start",
            end_col="effective_end",
        ),
    }

    passed = bool(details["issuer_classifications"].get("passed") and details["instrument_identifiers"].get("passed"))

    return CheckResult(
        name="audit_interval_overlaps",
        passed=passed,
        details=details,
    )


def _audit_instrument_identifier_coverage(
    db,
    *,
    as_of: date,
    market_ids: list[str],
    identifier_type: str,
    asset_class: str,
    status: str,
    max_missing_frac: float,
) -> CheckResult:
    sql_summary = """
        WITH base AS (
            SELECT
                i.instrument_id,
                NULLIF(NULLIF(ii.identifier_value, ''), 'UNKNOWN') AS identifier_value
            FROM instruments AS i
            LEFT JOIN LATERAL (
                SELECT ii.identifier_value
                FROM instrument_identifiers AS ii
                WHERE ii.instrument_id = i.instrument_id
                  AND ii.identifier_type = %s
                  AND ii.effective_start <= %s
                  AND (ii.effective_end IS NULL OR %s < ii.effective_end)
                ORDER BY ii.effective_start DESC
                LIMIT 1
            ) AS ii ON TRUE
            WHERE i.market_id = ANY(%s)
              AND i.asset_class = %s
              AND i.status = %s
        )
        SELECT
            COUNT(*) AS total_instruments,
            SUM(CASE WHEN identifier_value IS NOT NULL THEN 1 ELSE 0 END) AS with_identifier,
            SUM(CASE WHEN identifier_value IS NULL THEN 1 ELSE 0 END) AS missing_identifier
        FROM base
    """

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                sql_summary,
                (identifier_type, as_of, as_of, list(market_ids), asset_class, status),
            )
            total, with_identifier, missing = cur.fetchone()
        finally:
            cur.close()

    total_i = int(total or 0)
    with_i = int(with_identifier or 0)
    missing_i = int(missing or 0)
    missing_frac = (missing_i / total_i) if total_i else None

    if total_i == 0:
        passed = True
    else:
        passed = bool(
            missing_frac is not None
            and missing_frac <= float(max_missing_frac)
        )

    return CheckResult(
        name="audit_instrument_identifier_coverage",
        passed=passed,
        details={
            "as_of_date": as_of.isoformat(),
            "market_ids": list(market_ids),
            "identifier_type": identifier_type,
            "asset_class": asset_class,
            "status": status,
            "total_instruments": total_i,
            "with_identifier": with_i,
            "missing_identifier": missing_i,
            "missing_identifier_frac": missing_frac,
            "max_missing_identifier_frac": float(max_missing_frac),
        },
    )


def _audit_issuer_classification_coverage(
    db,
    *,
    as_of: date,
    market_ids: list[str],
    taxonomy: str,
    asset_class: str,
    status: str,
    sp500_members_asof: bool,
    max_missing_frac: float,
) -> CheckResult:
    sp500_filter_sql = ""
    if sp500_members_asof:
        sp500_filter_sql = """
          AND u.metadata->>'sp500' = 'true'
          AND (NULLIF(u.metadata->>'start_date', '')::date IS NULL OR NULLIF(u.metadata->>'start_date', '')::date <= %s)
          AND (NULLIF(u.metadata->>'end_date', '')::date IS NULL OR %s <= NULLIF(u.metadata->>'end_date', '')::date)
        """

    sql_summary = f"""
        WITH base AS (
            SELECT
                i.instrument_id,
                i.issuer_id,
                i.market_id,
                NULLIF(NULLIF(ic.sector, ''), 'UNKNOWN') AS sector_class,
                NULLIF(NULLIF(u.sector, ''), 'UNKNOWN') AS sector_issuer
            FROM instruments AS i
            LEFT JOIN LATERAL (
                SELECT ic.sector
                FROM issuer_classifications AS ic
                WHERE ic.issuer_id = i.issuer_id
                  AND ic.taxonomy = %s
                  AND ic.effective_start <= %s
                  AND (ic.effective_end IS NULL OR %s < ic.effective_end)
                ORDER BY ic.effective_start DESC
                LIMIT 1
            ) AS ic ON TRUE
            LEFT JOIN issuers AS u
              ON u.issuer_id = i.issuer_id
            WHERE i.market_id = ANY(%s)
              AND i.asset_class = %s
              AND i.status = %s
              {sp500_filter_sql}
        )
        SELECT
            COUNT(*) AS total_instruments,
            SUM(CASE WHEN sector_class IS NOT NULL THEN 1 ELSE 0 END) AS with_classification,
            SUM(CASE WHEN sector_class IS NULL AND sector_issuer IS NOT NULL THEN 1 ELSE 0 END) AS with_issuer_fallback,
            SUM(CASE WHEN COALESCE(sector_class, sector_issuer) IS NULL THEN 1 ELSE 0 END) AS missing_sector
        FROM base
    """

    params: list[object] = [taxonomy, as_of, as_of, list(market_ids), asset_class, status]
    if sp500_members_asof:
        params.extend([as_of, as_of])

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_summary, tuple(params))
            total, with_class, with_fallback, missing = cur.fetchone()
        finally:
            cur.close()

    total_i = int(total or 0)
    with_c = int(with_class or 0)
    with_f = int(with_fallback or 0)
    missing_i = int(missing or 0)
    missing_frac = (missing_i / total_i) if total_i else None

    if total_i == 0:
        passed = True
    else:
        passed = bool(
            missing_frac is not None
            and missing_frac <= float(max_missing_frac)
        )

    return CheckResult(
        name="audit_issuer_classification_coverage",
        passed=passed,
        details={
            "as_of_date": as_of.isoformat(),
            "market_ids": list(market_ids),
            "taxonomy": taxonomy,
            "asset_class": asset_class,
            "status": status,
            "sp500_members_asof": bool(sp500_members_asof),
            "total_instruments": total_i,
            "with_classification": with_c,
            "with_issuer_fallback": with_f,
            "missing_sector": missing_i,
            "missing_sector_frac": missing_frac,
            "max_missing_sector_frac": float(max_missing_frac),
        },
    )


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Layer 0 contracts (preflight deploy gate; optional deep audits)",
    )

    parser.add_argument(
        "--upgrade",
        action="store_true",
        help="If set, run alembic upgrade head (runtime + historical) before validation",
    )

    parser.add_argument(
        "--deep",
        action="store_true",
        help="If set, run deep L0 coverage audits (requires --as-of)",
    )
    parser.add_argument(
        "--as-of",
        type=_parse_date,
        default=None,
        help="As-of date (YYYY-MM-DD) used by deep audits",
    )
    parser.add_argument(
        "--market-id",
        dest="market_ids",
        action="append",
        default=None,
        help=f"Market ID for deep audits (repeatable; default: {US_EQ})",
    )
    parser.add_argument(
        "--taxonomy",
        type=str,
        default=DEFAULT_CLASSIFICATION_TAXONOMY,
        help=f"Issuer classification taxonomy for deep audit (default: {DEFAULT_CLASSIFICATION_TAXONOMY})",
    )
    parser.add_argument(
        "--sp500-members-asof",
        action="store_true",
        help="Deep audit: restrict issuer classification coverage to SP500 membership window",
    )
    parser.add_argument(
        "--identifier-type",
        type=str,
        default="SYMBOL",
        help="Instrument identifier type for deep audit (default: SYMBOL)",
    )
    parser.add_argument("--asset-class", type=str, default="EQUITY")
    parser.add_argument("--status", type=str, default="ACTIVE")

    parser.add_argument(
        "--max-missing-sector-frac",
        type=float,
        default=0.01,
        help="Deep audit: max allowed missing sector fraction (default: 0.01)",
    )
    parser.add_argument(
        "--max-missing-identifier-frac",
        type=float,
        default=0.01,
        help="Deep audit: max allowed missing identifier fraction (default: 0.01)",
    )

    args = parser.parse_args(argv)

    if args.deep and args.as_of is None:
        parser.error("--deep requires --as-of")

    if args.max_missing_sector_frac < 0.0 or args.max_missing_sector_frac > 1.0:
        parser.error("--max-missing-sector-frac must be between 0 and 1")
    if args.max_missing_identifier_frac < 0.0 or args.max_missing_identifier_frac > 1.0:
        parser.error("--max-missing-identifier-frac must be between 0 and 1")

    return args


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)

    project_root = _project_root()

    if args.upgrade:
        _alembic_upgrade(project_root, db_selector="runtime")
        _alembic_upgrade(project_root, db_selector="historical")

    db = get_db_manager()

    checks: list[CheckResult] = []

    # Schema at head.
    checks.append(_check_alembic_heads(db, project_root))

    # Fast L0 validators.
    checks.append(_check_markets(db))
    checks.append(_check_market_holidays(db))
    checks.append(_check_simple_checks_passed("issuers", db, show_issuers_status))
    checks.append(_check_interval_overlap_guards(db))
    checks.append(_check_simple_checks_passed("instruments", db, show_instruments_status))
    checks.append(_check_simple_checks_passed("strategies", db, show_strategies_status))
    checks.append(_check_simple_checks_passed("portfolios", db, show_portfolios_status))
    checks.append(_check_simple_checks_passed("strategy_configs", db, show_strategy_configs_status))
    checks.append(_check_simple_checks_passed("config_change_log", db, show_config_change_log_status))
    checks.append(_check_simple_checks_passed("meta_config_proposals", db, show_meta_config_proposals_status))
    checks.append(_check_simple_checks_passed("data_ingestion_status", db, show_data_ingestion_status_status))
    checks.append(_check_simple_checks_passed("job_executions", db, show_job_executions_status))
    checks.append(_check_simple_checks_passed("engine_runs", db, show_engine_runs_status))

    # Optional deep audits.
    deep_results: list[CheckResult] = []
    if args.deep:
        market_ids = args.market_ids if args.market_ids else [US_EQ]
        deep_results.append(
            _audit_instrument_identifier_coverage(
                db,
                as_of=args.as_of,
                market_ids=list(market_ids),
                identifier_type=str(args.identifier_type),
                asset_class=str(args.asset_class),
                status=str(args.status),
                max_missing_frac=float(args.max_missing_identifier_frac),
            )
        )
        deep_results.append(
            _audit_issuer_classification_coverage(
                db,
                as_of=args.as_of,
                market_ids=list(market_ids),
                taxonomy=str(args.taxonomy),
                asset_class=str(args.asset_class),
                status=str(args.status),
                sp500_members_asof=bool(args.sp500_members_asof),
                max_missing_frac=float(args.max_missing_sector_frac),
            )
        )
        deep_results.append(_audit_interval_overlaps(db))

    overall_passed = all(c.passed for c in checks) and all(c.passed for c in deep_results)

    report = {
        "mode": "deep" if args.deep else "preflight",
        "overall_passed": bool(overall_passed),
        "checks": {c.name: {"passed": bool(c.passed), **c.details} for c in checks},
        "deep_audits": {c.name: {"passed": bool(c.passed), **c.details} for c in deep_results},
    }

    print(json.dumps(report, indent=2, sort_keys=True))

    raise SystemExit(0 if overall_passed else 1)


if __name__ == "__main__":  # pragma: no cover
    main()
