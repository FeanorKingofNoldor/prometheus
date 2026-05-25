"""Tests for prometheus.derivatives.historical_signals."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from typing import Any

import pytest

from prometheus.derivatives import historical_signals as hs

# ── Fake DB ──────────────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, db: "_FakeDb") -> None:
        self._db = db
        self._result: list[tuple] = []

    def execute(self, sql: str, args: Any = ()) -> None:
        sql_norm = " ".join(sql.split()).upper()

        if "FROM PRICES_DAILY" in sql_norm and "LIMIT 6" in sql_norm:
            # VIX.INDX is hardcoded in the SQL — only one arg (as_of)
            as_of = args[0]
            rows = sorted(
                [
                    r for r in self._db.prices
                    if r["instrument_id"] == "VIX.INDX" and r["trade_date"] <= as_of
                ],
                key=lambda r: r["trade_date"], reverse=True,
            )[:6]
            self._result = [(r["close"],) for r in rows]
        elif "FROM PRICES_DAILY" in sql_norm:
            iid, as_of = args[0], args[1]
            rows = sorted(
                [
                    r for r in self._db.prices
                    if r["instrument_id"] == iid and r["trade_date"] <= as_of
                ],
                key=lambda r: r["trade_date"], reverse=True,
            )
            self._result = [(rows[0]["close"],)] if rows else []
        elif "FROM SECTOR_HEALTH_DAILY" in sql_norm:
            as_of = args[0]
            on_or_before = [
                r for r in self._db.sector_shi
                if r["as_of_date"] <= as_of
            ]
            if not on_or_before:
                self._result = []
            else:
                latest_date = max(r["as_of_date"] for r in on_or_before)
                self._result = [
                    (r["sector_name"], r["score"])
                    for r in on_or_before if r["as_of_date"] == latest_date
                ]
        elif "FROM DIVERGENCE_SIGNALS" in sql_norm:
            self._result = []
        elif "FROM CONVERGENCE_SIGNALS" in sql_norm:
            self._result = []
        elif "FROM COMPOUND_PRESSURE_ALERTS" in sql_norm:
            self._result = []
        elif "FROM PORTFOLIO_GEO_RISK" in sql_norm:
            self._result = []
        else:
            raise AssertionError(f"unhandled SQL: {sql_norm[:80]}")

    def fetchone(self) -> tuple | None:
        return self._result[0] if self._result else None

    def fetchall(self) -> list[tuple]:
        return list(self._result)

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None


class _FakeConnection:
    def __init__(self, db: "_FakeDb") -> None:
        self._db = db

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._db)

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None


class _FakeDb:
    def __init__(self) -> None:
        self.prices: list[dict[str, Any]] = []
        self.sector_shi: list[dict[str, Any]] = []

    @contextmanager
    def get_connection(self):
        yield _FakeConnection(self)


# ── VIX → state / proxy mapping ──────────────────────────────────────


@pytest.mark.parametrize("vix,expected", [
    (10.0, "RISK_ON"),
    (17.0, "NEUTRAL"),
    (22.0, "RECOVERY"),
    (28.0, "RISK_OFF"),
    (40.0, "CRISIS"),
])
def test_vix_to_market_state(vix, expected):
    assert hs._vix_to_market_state(vix) == expected


def test_proxy_mhi_decreases_as_vix_rises():
    assert hs._vix_to_proxy_mhi(10) > hs._vix_to_proxy_mhi(20)
    assert hs._vix_to_proxy_mhi(20) > hs._vix_to_proxy_mhi(30)
    assert hs._vix_to_proxy_mhi(30) > hs._vix_to_proxy_mhi(50)


def test_proxy_frag_increases_as_vix_rises():
    assert hs._vix_to_proxy_frag(10) < hs._vix_to_proxy_frag(20)
    assert hs._vix_to_proxy_frag(20) < hs._vix_to_proxy_frag(30)


# ── Signal provider integration ─────────────────────────────────────


def test_db_signal_provider_returns_vix_and_state():
    db = _FakeDb()
    today = date(2026, 5, 22)
    db.prices.append({
        "instrument_id": "VIX.INDX", "trade_date": today, "close": 28.0,
    })
    db.prices.append({
        "instrument_id": "SPY.US", "trade_date": today, "close": 500.0,
    })
    provider = hs.make_db_signal_provider(db)
    signals = provider(today)
    assert signals["vix_level"] == 28.0
    assert signals["spy_price"] == 500.0
    assert signals["market_state"] == "RISK_OFF"
    assert "mhi" in signals
    assert "frag" in signals


def test_db_signal_provider_uses_latest_sector_shi():
    db = _FakeDb()
    today = date(2026, 5, 22)
    db.prices.append({
        "instrument_id": "VIX.INDX", "trade_date": today, "close": 18.0,
    })
    # Today's SHI and yesterday's SHI; provider should pick today's
    db.sector_shi.extend([
        {"sector_name": "ENERGY", "as_of_date": today, "score": 0.40},
        {"sector_name": "TECHNOLOGY", "as_of_date": today, "score": 0.65},
        {"sector_name": "ENERGY", "as_of_date": date(2026, 5, 21), "score": 0.50},
    ])
    provider = hs.make_db_signal_provider(db)
    signals = provider(today)
    shi = signals["sector_shi"]
    assert shi["ENERGY"] == 0.40
    assert shi["TECHNOLOGY"] == 0.65


def test_db_signal_provider_vix_5d_change_pct():
    db = _FakeDb()
    today = date(2026, 5, 22)
    # 6 days of VIX: 18, 19, 20, 21, 22, 24
    for i, close in enumerate([18, 19, 20, 21, 22, 24]):
        d = date(2026, 5, 17 + i)
        db.prices.append({
            "instrument_id": "VIX.INDX", "trade_date": d, "close": float(close),
        })
    provider = hs.make_db_signal_provider(db)
    signals = provider(today)
    # Recent=24, oldest=18 → (24-18)/18 = 0.333
    assert signals["vix_5d_change_pct"] == pytest.approx(0.333, abs=0.01)


def test_db_signal_provider_extra_keys_win_on_conflict():
    db = _FakeDb()
    today = date(2026, 5, 22)
    db.prices.append({
        "instrument_id": "VIX.INDX", "trade_date": today, "close": 18.0,
    })
    provider = hs.make_db_signal_provider(
        db, extra={"nav": 200_000.0, "vix_level": 99.0},
    )
    signals = provider(today)
    assert signals["nav"] == 200_000.0
    assert signals["vix_level"] == 99.0   # extra overrides loaded value


def test_db_signal_provider_handles_missing_prices_gracefully():
    db = _FakeDb()
    today = date(2026, 5, 22)
    provider = hs.make_db_signal_provider(db)
    signals = provider(today)
    assert signals["vix_level"] == 0.0
    assert signals["spy_price"] == 0.0
    # Default market_state when VIX is 0
    assert signals["market_state"] == "RISK_ON"


# ── Underlying price provider ───────────────────────────────────────


def test_db_underlying_price_provider_resolves_spy():
    db = _FakeDb()
    today = date(2026, 5, 22)
    db.prices.append({
        "instrument_id": "SPY.US", "trade_date": today, "close": 502.5,
    })
    provider = hs.make_db_underlying_price_provider(db)
    assert provider(today, "SPY") == 502.5


def test_db_underlying_price_provider_resolves_vix_with_indx_suffix():
    db = _FakeDb()
    today = date(2026, 5, 22)
    db.prices.append({
        "instrument_id": "VIX.INDX", "trade_date": today, "close": 22.0,
    })
    provider = hs.make_db_underlying_price_provider(db)
    assert provider(today, "VIX") == 22.0


def test_db_underlying_price_provider_resolves_sector_etf():
    db = _FakeDb()
    today = date(2026, 5, 22)
    db.prices.append({
        "instrument_id": "XLE.US", "trade_date": today, "close": 91.5,
    })
    provider = hs.make_db_underlying_price_provider(db)
    assert provider(today, "XLE") == 91.5
