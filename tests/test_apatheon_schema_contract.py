"""Cross-repo schema contract: tables/columns Prometheus reads from Apatheon.

Apatheon owns the shared Postgres schema. Prometheus reads those tables via
raw SQL with no foreign keys, and its unit tests mock the DB — so an Apatheon
column rename breaks Prometheus silently at runtime. This test connects to the
real database and asserts that every table + specific column Prometheus depends
on still exists (queried from information_schema.columns).

The column lists below are derived from the actual SELECTs in the Prometheus
code, not guessed:
  - regimes              -> prometheus/monitoring/{iris_service,trading_report_service}.py
  - fragility_measures   -> prometheus/monitoring/{iris_service,entities_api}.py
  - sector_health_daily  -> prometheus/monitoring/iris_service.py
  - prices_daily         -> prometheus/{execution/risk_broker,decisions/*,monitoring/*}.py
  - returns_daily        -> assessment/backtest readers
  - nation_scores        -> prometheus/monitoring/nation_api.py
  - nation_macro_indicators / nation_industry_health -> nation_api.py
  - person_profiles      -> nation_api.py (leader-per-nation join)

The test SKIPS cleanly if the DB is unreachable so CI without a database does
not hard-fail; it runs and must pass when the DB is up.
"""

from __future__ import annotations

import pytest

# Tables (and the specific columns Prometheus selects) that Apatheon owns.
APATHEON_CONTRACT: dict[str, set[str]] = {
    "regimes": {"as_of_date", "region", "regime_label", "confidence"},
    "fragility_measures": {"fragility_id", "entity_type", "entity_id", "as_of_date", "fragility_score"},
    "sector_health_daily": {"sector_name", "as_of_date", "score"},
    "prices_daily": {"instrument_id", "trade_date", "close", "adjusted_close", "volume"},
    "returns_daily": {"instrument_id", "trade_date", "ret_1d", "ret_5d", "ret_21d"},
    "nation_scores": {
        "nation", "as_of_date", "composite_stability", "economic_stability",
        "market_stability", "political_stability", "contagion_risk",
        "currency_stability", "opportunity_score", "leadership_risk",
    },
    "nation_macro_indicators": {"nation", "series_id", "observation_date", "value"},
    "nation_industry_health": {"nation", "industry", "as_of_date", "health_score"},
    "person_profiles": {"profile_id", "person_name", "nation", "role", "role_tier"},
}


def _get_db_manager():
    """Return the real Apatheon DB manager, or skip if unavailable."""
    try:
        from apatheon.core.database import get_db_manager
    except Exception as exc:  # apatheon not installed / import error
        pytest.skip(f"apatheon.core.database not importable: {exc}")

    mgr = get_db_manager()
    # The Prometheus test conftest stubs apatheon when the real package is
    # missing; the stub manager has no connection methods. Skip in that case.
    if not hasattr(mgr, "get_historical_connection") or not hasattr(mgr, "get_runtime_connection"):
        pytest.skip("apatheon DB manager is stubbed (real package unavailable)")
    return mgr


def _columns_for(mgr, table: str) -> set[str]:
    """Return the column set for ``table``, looking in both shared DBs.

    Returns an empty set if the table is absent from both databases.
    Raises (to trigger a skip) if neither DB is reachable.
    """
    reachable = False
    cols: set[str] = set()
    for getter in (mgr.get_historical_connection, mgr.get_runtime_connection):
        try:
            with getter() as conn:
                reachable = True
                cur = conn.cursor()
                try:
                    cur.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = %s",
                        (table,),
                    )
                    cols |= {r[0] for r in cur.fetchall()}
                finally:
                    cur.close()
        except Exception:
            continue
    if not reachable:
        raise RuntimeError("no shared database reachable")
    return cols


@pytest.fixture(scope="module")
def db_manager():
    mgr = _get_db_manager()
    try:
        _columns_for(mgr, "regimes")
    except Exception as exc:
        pytest.skip(f"shared database unreachable: {exc}")
    return mgr


@pytest.mark.parametrize("table,expected_cols", sorted(APATHEON_CONTRACT.items()))
def test_apatheon_table_and_columns_exist(db_manager, table, expected_cols):
    actual = _columns_for(db_manager, table)
    assert actual, f"Apatheon table '{table}' missing from both shared databases"
    missing = expected_cols - actual
    assert not missing, (
        f"Apatheon table '{table}' is missing columns Prometheus depends on: "
        f"{sorted(missing)} (present: {sorted(actual)})"
    )
