"""Daily ATM IV snapshot — persists today's at-the-money implied
volatility per tracked underlying into ``daily_atm_iv``, the Phase 5.5
IV-percentile foundation.

Per underlying we:
  1. Resolve spot price (Stock / Index / front-month Future)
  2. Pick the option contract with strike closest to spot, ~30 DTE
  3. Read IV from IBKR market data
  4. Upsert (underlying, snapshot_date, atm_iv, atm_strike, atm_expiry)

The 12 underlyings tracked match the COMMODITY + CONVEX + HEDGE
templates' instrument universe:

  Index / VIX: SPY, QQQ, VIX
  Sector ETFs: XLE, XLK, XLF
  Commodity FOPs: CL, BZ, NG, ZW, GC, HG

Run from ``scripts/run/run_atm_iv_snapshot.py`` once per trading day.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable

from prometheus.execution.futures_option_specs import (
    get_fop_spec,
    is_commodity_fop_symbol,
)

_INDEX_SYMBOLS: frozenset[str] = frozenset({"VIX"})


# Default list — the COMMODITY + CONVEX + HEDGE universe.
DEFAULT_UNDERLYINGS: tuple[str, ...] = (
    "SPY", "QQQ", "VIX",
    "XLE", "XLK", "XLF",
    "CL", "BZ", "NG", "ZW", "GC", "HG",
)


_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS daily_atm_iv (
    underlying    TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    atm_iv        REAL NOT NULL,
    atm_strike    REAL NOT NULL,
    atm_expiry    DATE NOT NULL,
    source        TEXT NOT NULL DEFAULT 'ibkr_paper',
    PRIMARY KEY (underlying, snapshot_date)
);
CREATE INDEX IF NOT EXISTS idx_daily_atm_iv_underlying
    ON daily_atm_iv (underlying, snapshot_date);
"""


def ensure_daily_atm_iv_table() -> None:
    """Idempotent table create. Called by the snapshot runner."""
    from apatheon.core.database import get_db_manager
    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_CREATE_TABLE_SQL)
        conn.commit()


@dataclass(frozen=True)
class AtmIvSnapshot:
    underlying: str
    snapshot_date: date
    atm_iv: float
    atm_strike: float
    atm_expiry: date
    source: str = "ibkr_paper"


def upsert_snapshots(rows: Iterable[AtmIvSnapshot]) -> int:
    from apatheon.core.database import get_db_manager
    db = get_db_manager()
    payload = list(rows)
    if not payload:
        return 0
    with db.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO daily_atm_iv
                  (underlying, snapshot_date, atm_iv, atm_strike, atm_expiry, source)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (underlying, snapshot_date) DO UPDATE
                SET atm_iv = EXCLUDED.atm_iv,
                    atm_strike = EXCLUDED.atm_strike,
                    atm_expiry = EXCLUDED.atm_expiry,
                    source = EXCLUDED.source
                """,
                [
                    (r.underlying, r.snapshot_date, r.atm_iv,
                     r.atm_strike, r.atm_expiry, r.source)
                    for r in payload
                ],
            )
        conn.commit()
    return len(payload)


# ── IBKR snapshot logic ────────────────────────────────────────────


def _pick_atm_strike(strikes: Iterable[float], spot: float) -> float | None:
    candidates = sorted(strikes)
    if not candidates:
        return None
    return min(candidates, key=lambda s: abs(s - spot))


def _pick_expiry_near_30dte(
    expirations: Iterable[str],
    today: date,
    *,
    target_dte: int = 30,
) -> str | None:
    """Pick the expiry closest to ``target_dte`` ahead of ``today``."""
    best: tuple[int, str] | None = None
    for s in expirations:
        try:
            d = datetime.strptime(s, "%Y%m%d").date()
        except ValueError:
            continue
        dte = (d - today).days
        if dte < 7:
            continue
        diff = abs(dte - target_dte)
        if best is None or diff < best[0]:
            best = (diff, s)
    return best[1] if best else None


def _fetch_with_fallback(
    ib,
    contract,
    *,
    generic_ticks: str = "",
    wait_sec: float = 2.5,
    extract: str = "spot",
) -> tuple[float | None, int | None]:
    """Snapshot market data with live→delayed fallback.

    Tries ``reqMarketDataType(1)`` (live) first. If the value comes
    back NaN/missing (usually error 10089 for products without API
    streaming entitlement), retries with ``reqMarketDataType(3)``
    (delayed). Returns ``(value, market_data_type_that_worked)`` or
    ``(None, None)`` if both modes failed.

    extract:
      'spot' — marketPrice() | last | close
      'iv'   — modelGreeks.impliedVol | impliedVolatility
    """
    for mode in (1, 3):
        try:
            ib.reqMarketDataType(mode)
        except Exception:
            pass
        ticker = ib.reqMktData(contract, generic_ticks, True, False)
        ib.sleep(wait_sec)
        if extract == "iv":
            val: float | None = None
            mg = getattr(ticker, "modelGreeks", None)
            if mg is not None and getattr(mg, "impliedVol", None) is not None:
                val = mg.impliedVol
            elif getattr(ticker, "impliedVolatility", None) is not None:
                val = ticker.impliedVolatility
        else:
            mp = ticker.marketPrice()
            val = mp if mp is not None else None
            if val is None or (isinstance(val, float) and math.isnan(val)) or val <= 0:
                val = ticker.last
            if val is None or (isinstance(val, float) and math.isnan(val)) or val <= 0:
                val = ticker.close
        ib.cancelMktData(contract)
        if val is not None and not (isinstance(val, float) and math.isnan(val)) and val > 0:
            return float(val), mode
    return None, None


def snapshot_underlying(
    ib,                       # ib_async / ib_insync IB instance
    underlying: str,
    *,
    today: date,
) -> AtmIvSnapshot | None:
    """Read today's ATM IV for ``underlying`` and return a snapshot,
    or ``None`` if no data could be qualified.
    """
    from prometheus.execution.ib_compat import (
        FuturesOption,
        Index,
        Option,
        Stock,
    )

    symbol = underlying.upper()

    # 1. Resolve underlying + spot price
    if is_commodity_fop_symbol(symbol):
        spec = get_fop_spec(symbol)
        if spec is None:
            return None
        # Use the front-month future as the spot reference.
        from prometheus.execution.contract_discovery import ContractDiscoveryService
        discovery = ContractDiscoveryService(ib)
        chain_futs = discovery.discover_futures_chain(symbol, spec.exchange)
        valid_futs = [f for f in chain_futs if f.last_trade_date and f.dte >= 3]
        if not valid_futs:
            return None
        front = sorted(valid_futs, key=lambda f: f.last_trade_date)[0]
        # Price-stream the future for a moment to get last/mid.
        from prometheus.execution.ib_compat import Future
        fut = Future(symbol=spec.symbol, lastTradeDateOrContractMonth=front.last_trade_date,
                     exchange=spec.exchange, currency=spec.currency)
        qualified = ib.qualifyContracts(fut)
        if not qualified:
            return None
        fut = qualified[0]
        spot, _ = _fetch_with_fallback(ib, fut, wait_sec=2.5, extract="spot")
        if not spot:
            return None

        # FOP chain for this future
        fop_chains = discovery.discover_fop_chain(symbol, spec.exchange,
                                                  fut_con_id=front.con_id)
        if not fop_chains:
            return None
        chain = fop_chains[0]
        atm_strike = _pick_atm_strike(chain.strikes, spot)
        target_expiry = _pick_expiry_near_30dte(chain.expirations, today)
        if atm_strike is None or target_expiry is None:
            return None
        opt = FuturesOption(
            symbol=spec.symbol,
            lastTradeDateOrContractMonth=target_expiry,
            strike=atm_strike,
            right="C",
            exchange=spec.exchange,
            currency=spec.currency,
            multiplier=spec.multiplier,
            tradingClass=spec.trading_class,
        )
    elif symbol in _INDEX_SYMBOLS:
        underlying_contract = Index(symbol=symbol, exchange="CBOE", currency="USD")
        qualified = ib.qualifyContracts(underlying_contract)
        if not qualified:
            return None
        underlying_contract = qualified[0]
        spot, _ = _fetch_with_fallback(ib, underlying_contract, wait_sec=2.5, extract="spot")
        if not spot:
            return None
        from prometheus.execution.contract_discovery import ContractDiscoveryService
        discovery = ContractDiscoveryService(ib)
        opt_chains = discovery.discover_option_chain(symbol, sec_type="IND",
                                                    exchange="CBOE")
        if not opt_chains:
            return None
        chain = opt_chains[0]
        atm_strike = _pick_atm_strike(chain.strikes, spot)
        target_expiry = _pick_expiry_near_30dte(chain.expirations, today)
        if atm_strike is None or target_expiry is None:
            return None
        opt = Option(
            symbol=symbol,
            lastTradeDateOrContractMonth=target_expiry,
            strike=atm_strike,
            right="C",
            exchange="CBOE",
            currency="USD",
            multiplier="100",
        )
    else:
        # Stock / ETF
        underlying_contract = Stock(symbol=symbol, exchange="SMART", currency="USD")
        qualified = ib.qualifyContracts(underlying_contract)
        if not qualified:
            return None
        underlying_contract = qualified[0]
        spot, _ = _fetch_with_fallback(ib, underlying_contract, wait_sec=2.5, extract="spot")
        if not spot:
            return None
        from prometheus.execution.contract_discovery import ContractDiscoveryService
        discovery = ContractDiscoveryService(ib)
        opt_chains = discovery.discover_option_chain(symbol, sec_type="STK")
        if not opt_chains:
            return None
        chain = opt_chains[0]
        atm_strike = _pick_atm_strike(chain.strikes, spot)
        target_expiry = _pick_expiry_near_30dte(chain.expirations, today)
        if atm_strike is None or target_expiry is None:
            return None
        opt = Option(
            symbol=symbol,
            lastTradeDateOrContractMonth=target_expiry,
            strike=atm_strike,
            right="C",
            exchange="SMART",
            currency="USD",
        )

    # Qualify the option + read IV
    qualified = ib.qualifyContracts(opt)
    if not qualified or not getattr(qualified[0], "conId", 0):
        return None
    opt = qualified[0]
    # genericTickList 106 = optionImpliedVol. Fallback to delayed mode
    # handles products without live API entitlement (e.g. SPY/QQQ on
    # the US Securities Snapshot tier).
    iv, _ = _fetch_with_fallback(ib, opt, generic_ticks="106", wait_sec=3.0, extract="iv")
    if iv is None:
        return None

    return AtmIvSnapshot(
        underlying=symbol,
        snapshot_date=today,
        atm_iv=float(iv),
        atm_strike=float(atm_strike),
        atm_expiry=datetime.strptime(target_expiry, "%Y%m%d").date(),
    )


def snapshot_all(
    ib,
    *,
    today: date | None = None,
    underlyings: Iterable[str] = DEFAULT_UNDERLYINGS,
    market_data_type: int = 1,
) -> list[AtmIvSnapshot]:
    """Snapshot ATM IV across the full underlying list. Best-effort —
    failures per underlying are logged and skipped.

    ``market_data_type`` is passed to ``ib.reqMarketDataType``:
      1 = live (default; right when subscriptions are in place)
      2 = frozen, 3 = delayed, 4 = delayed-frozen.
    """
    today = today or date.today()
    ensure_daily_atm_iv_table()
    # Per-product live→delayed fallback is handled in _fetch_with_fallback;
    # no global reqMarketDataType needed here.
    out: list[AtmIvSnapshot] = []
    for sym in underlyings:
        try:
            snap = snapshot_underlying(ib, sym, today=today)
            if snap is not None:
                out.append(snap)
        except Exception:
            # Don't let one product kill the whole run
            continue
    if out:
        upsert_snapshots(out)
    return out
