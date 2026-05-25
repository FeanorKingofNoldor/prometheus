"""Historical signal provider for the backtest harness.

The Phase 2.1 backtest harness takes a ``signal_provider:
Callable[[date], dict]`` so it can drive the new sleeve pipeline on
arbitrary days. The unit tests use stubs; this module is the
production-side provider that loads signals from the live database
for any date Apatheon has data for.

It composes:

* VIX / SPY price from ``prices_daily``
* Sector SHI from ``sector_health_daily``
* Intel signals (divergence / convergence / compound pressure /
  portfolio geo risk) via ``load_intel_signals`` + ``merge_into_signals``
* A coarse ``market_state`` proxy derived from VIX (good enough for
  regime gating in backtest; production uses Apatheon's regime
  classifier directly)
* Proxy ``mhi`` / ``frag`` values derived from VIX so the existing
  template triggers still fire (the live signals dict carries these
  populated by Apatheon — backtest reuses VIX as a stand-in).

The result is a function the user can hand to
``replay_sleeve_pipeline`` to validate template behaviour against a
real year of history without waiting for shadow data to accumulate.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date
from typing import Any

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger

from prometheus.derivatives.intel_signals import (
    load_intel_signals,
    merge_into_signals,
)

logger = get_logger(__name__)


# ── VIX → coarse market state and proxy MHI / frag ──────────────────


def _vix_to_market_state(vix: float) -> str:
    if vix >= 35:
        return "CRISIS"
    if vix >= 25:
        return "RISK_OFF"
    if vix >= 20:
        return "RECOVERY"
    if vix >= 15:
        return "NEUTRAL"
    return "RISK_ON"


def _vix_to_proxy_mhi(vix: float) -> float:
    """Map VIX → a proxy MHI in [0, 1] (low MHI = unhealthy market)."""
    if vix <= 0:
        return 0.50
    if vix <= 15:
        return 0.90
    if vix <= 20:
        return 0.70
    if vix <= 25:
        return 0.50
    if vix <= 35:
        return 0.30
    return 0.10


def _vix_to_proxy_frag(vix: float) -> float:
    if vix <= 0:
        return 0.20
    if vix <= 18:
        return 0.10
    if vix <= 25:
        return 0.25
    if vix <= 35:
        return 0.50
    return 0.75


# ── Signal loaders ──────────────────────────────────────────────────


def _load_price(db: DatabaseManager, as_of: date, instrument_id: str) -> float:
    """Prices live in the historical DB."""
    with db.get_historical_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT close
                FROM prices_daily
                WHERE instrument_id = %s AND trade_date <= %s
                ORDER BY trade_date DESC LIMIT 1
                """,
                (instrument_id, as_of),
            )
            row = cur.fetchone()
            return float(row[0]) if row and row[0] is not None else 0.0


def _load_sector_shi(db: DatabaseManager, as_of: date) -> dict[str, float]:
    """sector_health_daily lives in the runtime DB."""
    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sector_name, score
                FROM sector_health_daily
                WHERE as_of_date = (
                    SELECT MAX(as_of_date) FROM sector_health_daily
                    WHERE as_of_date <= %s
                )
                """,
                (as_of,),
            )
            return {
                str(name).upper(): float(score)
                for name, score in cur.fetchall()
            }


def _load_vix_5d_change_pct(db: DatabaseManager, as_of: date) -> float:
    """Percent change in VIX over the trailing 5 trading days."""
    with db.get_historical_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT close
                FROM prices_daily
                WHERE instrument_id = 'VIX.INDX' AND trade_date <= %s
                ORDER BY trade_date DESC LIMIT 6
                """,
                (as_of,),
            )
            rows = [float(r[0]) for r in cur.fetchall() if r[0] is not None]
            if len(rows) < 2:
                return 0.0
            recent, prior = rows[0], rows[-1]
            return (recent - prior) / prior if prior else 0.0


# ── Provider factory ────────────────────────────────────────────────


def make_db_signal_provider(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> Callable[[date], dict[str, Any]]:
    """Return a ``Callable[[date], dict]`` the backtest harness can use.

    Each call loads VIX + SPY prices + sector SHI + intel signals for
    the given date from the live database and folds them into a
    signals dict matching what ``_load_signals`` would have built in
    production.

    ``extra`` is merged in last and wins on key conflict — useful for
    pinning ``nav`` or ``equity_positions`` for a backtest scenario.
    """
    base_extra = dict(extra or {})

    def _provider(as_of: date) -> dict[str, Any]:
        vix = _load_price(db_manager, as_of, "VIX.INDX")
        spy = _load_price(db_manager, as_of, "SPY.US")
        sector_shi = _load_sector_shi(db_manager, as_of)
        vix_5d = _load_vix_5d_change_pct(db_manager, as_of)

        signals: dict[str, Any] = {
            "vix_level": vix,
            "spy_price": spy,
            "sector_shi": sector_shi,
            "market_state": _vix_to_market_state(vix),
            "mhi": _vix_to_proxy_mhi(vix),
            "frag": _vix_to_proxy_frag(vix),
            "vix_5d_change_pct": vix_5d,
        }

        try:
            intel = load_intel_signals(
                db_manager, as_of_date=as_of, portfolio_id=portfolio_id,
            )
            signals = dict(merge_into_signals(signals, intel))
        except Exception as exc:
            logger.debug(
                "historical signal provider: intel load failed for %s: %s",
                as_of, exc,
            )

        signals.update(base_extra)
        return signals

    return _provider


def make_db_underlying_price_provider(
    db_manager: DatabaseManager,
) -> Callable[[date, str], float]:
    """Return a price provider that loads spot from ``prices_daily``."""

    # Symbol → instrument_id mapping (mirrors the production loader's logic)
    def _to_iid(symbol: str) -> str:
        s = symbol.upper()
        if s == "VIX":
            return "VIX.INDX"
        if s.endswith(".INDX") or s.endswith(".US"):
            return s
        return f"{s}.US"

    def _lookup(as_of: date, symbol: str) -> float:
        return _load_price(db_manager, as_of, _to_iid(symbol))

    return _lookup


__all__ = [
    "make_db_signal_provider",
    "make_db_underlying_price_provider",
]
