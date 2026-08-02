"""EU-retail (PRIIPs) purchase-eligibility layer tests.

Covers ``prometheus.execution.eligibility`` (point check, DB loader,
professional override, fail-safe fallback) and the universe-engine
integration (blocked instruments never enter a book; exclusion reason
``retail_ineligible_priips`` is recorded; a DB error does not silently
re-admit SPY.US).
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest

import prometheus.execution.eligibility as elig
from prometheus.execution.eligibility import (
    KNOWN_US_PACKAGED_PRODUCT_IDS,
    PROFESSIONAL_ACCOUNT_ENV,
    RETAIL_BLOCKED_ASSET_CLASSES,
    is_retail_purchase_eligible,
    load_ineligible_instrument_ids,
    static_fallback_ineligible_ids,
)
from prometheus.universe.engine import BasicUniverseModel


@pytest.fixture(autouse=True)
def _retail_account(monkeypatch):
    """Tests run as EU retail unless a test opts into the override."""
    monkeypatch.delenv(PROFESSIONAL_ACCOUNT_ENV, raising=False)


# ---------------------------------------------------------------------------
# is_retail_purchase_eligible — point checks
# ---------------------------------------------------------------------------


def test_us_etf_blocked_by_asset_class() -> None:
    assert is_retail_purchase_eligible("SPY.US", "ETF", "US_EQ") is False


@pytest.mark.parametrize("asset_class", sorted(RETAIL_BLOCKED_ASSET_CLASSES))
def test_all_blocked_asset_classes_blocked_on_us(asset_class: str) -> None:
    assert is_retail_purchase_eligible("XXX.US", asset_class, "US_EQ") is False


def test_us_etf_blocked_despite_equity_mislabel() -> None:
    # DB reality: SPY/RSP/KRE are stored as asset_class='EQUITY'; the
    # static snapshot must still catch them.
    for inst in ("SPY.US", "RSP.US", "KRE.US", "QQQ.US", "XLE.US"):
        assert is_retail_purchase_eligible(inst, "EQUITY", "US_EQ") is False


def test_us_suffix_blocks_even_without_market_id() -> None:
    assert is_retail_purchase_eligible("SPY.US", "ETF", None) is False


def test_metadata_markers_block_unknown_us_etfs() -> None:
    # New ETFs unknown to the snapshot are caught via metadata markers.
    assert is_retail_purchase_eligible(
        "NEWETF.US", "EQUITY", "US_EQ", metadata={"index": "GLOBAL_ETF"}
    ) is False
    assert is_retail_purchase_eligible(
        "NEWETF.US", "EQUITY", "US_EQ", metadata={"etf_category": "Robotics"}
    ) is False
    assert is_retail_purchase_eligible(
        "NEWETF.US", "EQUITY", "US_EQ", metadata={"source": "sector_etf_ingest"}
    ) is False


def test_us_single_stocks_eligible() -> None:
    assert is_retail_purchase_eligible("AAPL.US", "EQUITY", "US_EQ") is True
    assert is_retail_purchase_eligible(
        "MSFT.US", "EQUITY", "US_EQ", metadata={"index": "S&P 500"}
    ) is True


def test_non_us_stocks_eligible_everywhere() -> None:
    assert is_retail_purchase_eligible("SAP.XETRA", "EQUITY", "EU_EQ") is True
    assert is_retail_purchase_eligible("7203.TSE", "EQUITY", "JP_EQ") is True


def test_ucits_etf_on_non_us_venue_eligible() -> None:
    # UCITS ETFs on LSE/XETRA publish KIDs and remain eligible even with
    # a blocked asset_class label.
    assert is_retail_purchase_eligible("VUSA.LSE", "ETF", "UK_EQ") is True
    assert is_retail_purchase_eligible("EUNL.XETRA", "ETF", "EU_EQ") is True


def test_none_asset_class_defaults_to_eligible_stock() -> None:
    # Direct shares are out of PRIIPs scope (Reg. 1286/2014 recital 7);
    # unknown asset_class is treated as a stock.
    assert is_retail_purchase_eligible("SOMESTOCK.US", None, "US_EQ") is True


def test_none_asset_class_debug_logged_once() -> None:
    elig._NONE_ASSET_CLASS_LOGGED[0] = False
    is_retail_purchase_eligible("A.US", None, "US_EQ")
    assert elig._NONE_ASSET_CLASS_LOGGED[0] is True


def test_professional_override_makes_everything_eligible(monkeypatch) -> None:
    monkeypatch.setenv(PROFESSIONAL_ACCOUNT_ENV, "1")
    assert is_retail_purchase_eligible("SPY.US", "ETF", "US_EQ") is True
    assert is_retail_purchase_eligible("SDS.US", "ETF", "US_EQ") is True
    assert static_fallback_ineligible_ids() == set()


# ---------------------------------------------------------------------------
# load_ineligible_instrument_ids — stub DB
# ---------------------------------------------------------------------------


class _Cursor:
    def __init__(self, sink: list, rows, raise_on_execute: bool = False) -> None:
        self._sink = sink
        self._rows = rows
        self._raise = raise_on_execute

    def execute(self, sql, params=None):
        self._sink.append(str(sql))
        if self._raise:
            raise RuntimeError("db down")

    def fetchall(self):
        return self._rows

    def close(self):
        pass


def _db(sink: list, rows, raise_on_execute: bool = False):
    class _Conn:
        def cursor(self):
            return _Cursor(sink, rows, raise_on_execute)

    class _DB:
        @contextmanager
        def get_runtime_connection(self):
            yield _Conn()

    return _DB()


def test_load_ineligible_returns_db_ids_union_snapshot() -> None:
    sink: list = []
    rows = [("SPY.US",), ("SDS.US",), ("NEWETF.US",)]
    ids = load_ineligible_instrument_ids(_db(sink, rows))

    # DB-reported ids present, including ones unknown to the snapshot.
    assert {"SPY.US", "SDS.US", "NEWETF.US"} <= ids
    # Snapshot union: mislabeled ETFs stay blocked even if the DB query
    # ever under-reports.
    assert KNOWN_US_PACKAGED_PRODUCT_IDS <= ids

    sql = "\n".join(sink)
    # Query must combine the asset-class set with the metadata markers
    # (100/101 US ETFs are mislabeled asset_class='EQUITY' in the DB).
    assert "asset_class = ANY" in sql
    assert "GLOBAL_ETF" not in sql  # passed as a parameter, not inlined
    assert "metadata->>'index'" in sql
    assert "etf_category" in sql
    assert "metadata->>'source'" in sql


def test_load_ineligible_professional_override_empty(monkeypatch) -> None:
    monkeypatch.setenv(PROFESSIONAL_ACCOUNT_ENV, "1")
    sink: list = []
    assert load_ineligible_instrument_ids(_db(sink, [("SPY.US",)])) == set()
    assert sink == []  # short-circuits before touching the DB


def test_load_ineligible_db_error_falls_back_loudly(caplog) -> None:
    # A DB error must NOT silently re-admit SPY: fall back to the static
    # snapshot and log at ERROR level.
    sink: list = []
    with caplog.at_level("ERROR"):
        ids = load_ineligible_instrument_ids(_db(sink, [], raise_on_execute=True))

    assert "SPY.US" in ids
    assert ids == KNOWN_US_PACKAGED_PRODUCT_IDS
    assert any(
        "fall" in r.message.lower() and r.levelname == "ERROR" for r in caplog.records
    )


# ---------------------------------------------------------------------------
# Universe integration — build_universe excludes ineligible instruments
# ---------------------------------------------------------------------------

AS_OF = date(2026, 6, 15)


class _StubCalendar:
    def trading_days_between(self, start: date, end: date) -> list[date]:
        days: list[date] = []
        d = start
        while d <= end:
            if d.weekday() < 5:
                days.append(d)
            d += timedelta(days=1)
        return days


class _StubReader:
    """Full price history + close-on-as_of for every requested instrument."""

    def read_prices(self, instrument_ids, start: date, end: date) -> pd.DataFrame:
        days = _StubCalendar().trading_days_between(start, end)
        return pd.DataFrame(
            {
                "instrument_id": [instrument_ids[0]] * len(days),
                "trade_date": days,
                "close": [100.0] * len(days),
                "volume": [5_000_000.0] * len(days),
            }
        )

    def read_prices_close(self, instrument_ids, start: date, end: date) -> pd.DataFrame:
        return pd.DataFrame(
            {"instrument_id": list(instrument_ids), "close": [100.0] * len(instrument_ids)}
        )


class _StubStability:
    def get_latest_state(self, entity_type, entity_id, as_of_date=None):
        return SimpleNamespace(
            soft_target_score=10.0,
            soft_target_class=SimpleNamespace(value="STABLE"),
            weak_profile=False,
        )


def _universe_model(db_manager) -> BasicUniverseModel:
    model = BasicUniverseModel(
        db_manager=db_manager,
        calendar=_StubCalendar(),
        data_reader=_StubReader(),
        profile_service=None,
        stability_storage=_StubStability(),
        market_ids=["US_EQ"],
    )
    # Enumeration is not under test; the db_manager stub serves the
    # eligibility query inside build_universe.
    model._enumerate_instruments = lambda as_of_date: [  # type: ignore[method-assign]
        ("SPY.US", "SPY", "UNKNOWN", "US_EQ", "UNKNOWN"),
        ("KRE.US", "KRE", "Financials", "US_EQ", "issuer_classifications"),
        ("AAPL.US", "AAPL", "Technology", "US_EQ", "issuer_classifications"),
    ]
    return model


def _by_id(members):
    return {m.entity_id: m for m in members}


def test_build_universe_excludes_retail_ineligible_with_reason() -> None:
    sink: list = []
    db = _db(sink, [("SPY.US",), ("KRE.US",)])
    members = _by_id(_universe_model(db).build_universe(AS_OF, "CORE_EQ"))

    for blocked in ("SPY.US", "KRE.US"):
        assert members[blocked].included is False
        assert members[blocked].tier == "EXCLUDED"
        assert members[blocked].reasons.get("retail_ineligible_priips") is True

    assert members["AAPL.US"].included is True
    assert "retail_ineligible_priips" not in members["AAPL.US"].reasons


def test_build_universe_db_error_still_excludes_known_etfs(caplog) -> None:
    # Infrastructure failure: the loader logs loudly and falls back to the
    # static snapshot — SPY/KRE stay out; the stock stays in (fail-open
    # only for instruments unknown to the snapshot).
    db = _db([], [], raise_on_execute=True)
    with caplog.at_level("ERROR"):
        members = _by_id(_universe_model(db).build_universe(AS_OF, "CORE_EQ"))

    assert members["SPY.US"].included is False
    assert members["SPY.US"].reasons.get("retail_ineligible_priips") is True
    assert members["KRE.US"].included is False
    assert members["AAPL.US"].included is True
    assert any(r.levelname == "ERROR" for r in caplog.records)


def test_build_universe_professional_override_admits_etfs(monkeypatch) -> None:
    monkeypatch.setenv(PROFESSIONAL_ACCOUNT_ENV, "1")
    db = _db([], [("SPY.US",), ("KRE.US",)])
    members = _by_id(_universe_model(db).build_universe(AS_OF, "CORE_EQ"))

    assert members["SPY.US"].included is True
    assert members["KRE.US"].included is True
    assert members["AAPL.US"].included is True
