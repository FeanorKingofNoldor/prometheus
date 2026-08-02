"""PortfolioModel — owns a read-only IBKR connection in a background thread and
exposes an immutable snapshot the UI polls.

ib_insync needs its own asyncio loop and fires events on it; Textual runs its
own loop. We isolate ib_insync entirely in a daemon thread (its own loop), let
streaming events mutate live `Ticker`/`PnL`/`PnLSingle` objects, and rebuild a
plain-dataclass `Snapshot` on a periodic sampler. The UI thread only ever reads
the latest snapshot under a lock — no shared loop, no call_from_thread.

Read-only: connects with readonly=True and never calls an order method.
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field

# ib_compat sets up the event loop so importing ib_insync on py3.14 doesn't blow
# up; it also transparently uses ib_async if that's what's installed.
from prometheus.execution.ib_compat import IB  # noqa: E402
from prometheus.execution.ibkr_config import (  # noqa: E402
    create_live_config,
    create_paper_config,
)

from .prices import latest_close
from .sectors import asset_class_for_sectype, sector_for_symbol

# Distinct from the prometheus daemon (clientId=1). Override via env if needed.
_CLIENT_IDS = {"paper": int(os.getenv("FOLIO_CLIENT_ID_PAPER", "2")),
               "live": int(os.getenv("FOLIO_CLIENT_ID_LIVE", "3"))}
# 1=live, 2=frozen, 3=delayed, 4=delayed-frozen. Delayed (3) streams without a
# live market-data entitlement, so it's the safe default; set FOLIO_MKT_DATA=1
# on an entitled (live) account for real-time ticks.
_MKT_DATA_TYPE = int(os.getenv("FOLIO_MKT_DATA", "3"))
_STREAM_CAP = 100        # IBKR hard cap on simultaneous streaming lines
_NAV_HISTORY = 600       # session NAV samples (ring buffer)
_PRICE_HISTORY = 60      # per-position price samples for sparklines


def _num(x) -> float:
    """Coerce to float, mapping NaN/None/garbage to 0.0."""
    try:
        v = float(x)
        return v if v == v else 0.0  # NaN != NaN
    except (TypeError, ValueError):
        return 0.0


@dataclass
class PositionRow:
    symbol: str
    sec_type: str
    asset_class: str
    sector: str
    qty: float
    avg_cost: float
    con_id: int
    currency: str = "USD"
    last: float = 0.0
    market_value: float = 0.0
    unrealized_pnl: float = 0.0
    daily_pnl: float = 0.0
    day_pct: float = 0.0
    weight: float = 0.0
    streaming: bool = False
    eod: bool = False
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    iv: float | None = None
    price_hist: list[float] = field(default_factory=list)


@dataclass
class AccountSnap:
    net_liq: float = 0.0
    day_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    buying_power: float = 0.0
    excess_liquidity: float = 0.0
    maint_margin: float = 0.0
    gross_position: float = 0.0
    cash: float = 0.0

    @property
    def leverage(self) -> float:
        return (self.gross_position / self.net_liq) if self.net_liq else 0.0

    @property
    def day_pct(self) -> float:
        base = self.net_liq - self.day_pnl
        return (self.day_pnl / base * 100.0) if base else 0.0


@dataclass
class Snapshot:
    mode: str = "paper"
    account: str = ""
    connected: bool = False
    market_data: str = "delayed"
    positions: list[PositionRow] = field(default_factory=list)
    acct: AccountSnap = field(default_factory=AccountSnap)
    nav_hist: list[float] = field(default_factory=list)
    streamed: int = 0
    total: int = 0
    last_update: float = 0.0
    error: str | None = None


_MKT_LABEL = {1: "live", 2: "frozen", 3: "delayed", 4: "delayed-frozen"}


class PortfolioModel:
    """Background-thread IBKR feed + a poll-able snapshot."""

    def __init__(self, mode: str = "paper") -> None:
        self.mode = mode
        self._ib: IB | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._snapshot = Snapshot(mode=mode, market_data=_MKT_LABEL.get(_MKT_DATA_TYPE, "?"))
        self._nav: deque[float] = deque(maxlen=_NAV_HISTORY)
        self._price_hist: dict[int, deque[float]] = {}
        self._stop = threading.Event()
        # live ib objects, keyed by conId, read by the sampler
        self._positions: list = []
        self._tickers: dict[int, object] = {}
        self._pnl_single: dict[int, object] = {}
        self._acct_pnl = None
        self._account = ""

    # ----- lifecycle ----------------------------------------------------

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="folio-ib", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        loop = self._loop
        if loop and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)

    def switch(self, mode: str) -> None:
        """Tear down and reconnect to the other account."""
        if mode == self.mode:
            return
        self.stop()
        if self._thread:
            self._thread.join(timeout=5)
        self.mode = mode
        with self._lock:
            self._snapshot = Snapshot(mode=mode, market_data=_MKT_LABEL.get(_MKT_DATA_TYPE, "?"))
        self._nav.clear()
        self._price_hist.clear()
        self.start()

    def get_snapshot(self) -> Snapshot:
        with self._lock:
            return self._snapshot

    # ----- background thread --------------------------------------------

    def _run(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._ib = IB()
        try:
            self._loop.create_task(self._sampler())
            self._loop.run_forever()
        finally:
            try:
                if self._ib and self._ib.isConnected():
                    self._ib.disconnect()
            except Exception:
                pass
            try:
                self._loop.close()
            except Exception:
                pass

    async def _connect_and_subscribe(self) -> None:
        self._tickers.clear()
        self._pnl_single.clear()
        cfg = create_live_config(client_id=_CLIENT_IDS["live"], readonly=True) if self.mode == "live" \
            else create_paper_config(client_id=_CLIENT_IDS["paper"], readonly=True)
        host = getattr(cfg, "host", "127.0.0.1")
        port = getattr(cfg, "port")
        client_id = getattr(cfg, "client_id", None) or getattr(cfg, "clientId", _CLIENT_IDS[self.mode])
        creds = getattr(cfg, "credentials", None)
        cfg_account = getattr(cfg, "account", None) or (getattr(creds, "account", None) if creds else None)

        assert client_id != 1, "folio must not use the daemon's clientId=1"
        await self._ib.connectAsync(host, port, clientId=client_id, readonly=True, timeout=15)
        self._ib.reqMarketDataType(_MKT_DATA_TYPE)
        # the gateway is the source of truth for the account id
        managed = list(self._ib.managedAccounts() or [])
        env_acct = os.getenv("IBKR_LIVE_ACCOUNT" if self.mode == "live" else "IBKR_PAPER_ACCOUNT")
        account = (managed[0] if managed else None) or cfg_account or env_acct or ""
        self._account = account

        with self._lock:
            self._snapshot.connected = True
            self._snapshot.account = account

        # account-level daily PnL (push)
        try:
            self._acct_pnl = self._ib.reqPnL(account)
        except Exception:
            self._acct_pnl = None

        positions = await self._ib.reqPositionsAsync()
        self._positions = [p for p in positions if getattr(p, "account", account) == account] or positions

        # stream the top-N positions by notional; snapshot the tail
        ranked = sorted(
            self._positions,
            key=lambda p: abs(getattr(p, "position", 0) * getattr(p, "avgCost", 0)),
            reverse=True,
        )
        for pos in ranked[:_STREAM_CAP]:
            con = pos.contract
            con_id = getattr(con, "conId", 0)
            if not con_id:
                continue
            try:
                self._tickers[con_id] = self._ib.reqMktData(con, "", False, False)
                self._pnl_single[con_id] = self._ib.reqPnLSingle(account, "", con_id)
            except Exception:
                pass
        with self._lock:
            self._snapshot.streamed = len(self._tickers)
            self._snapshot.total = len(self._positions)

    async def _sampler(self) -> None:
        """Connect (with retry) + rebuild the snapshot. Resilient to a gateway
        that's down or recycles mid-session — keeps retrying every few seconds
        instead of dying on the first failure (which left LIVE stuck)."""
        while not self._stop.is_set():
            connected = bool(self._ib and self._ib.isConnected())
            try:
                if not connected:
                    await self._connect_and_subscribe()
                    connected = True
                self._rebuild()
            except Exception as exc:
                with self._lock:
                    self._snapshot.error = f"{type(exc).__name__}: {exc}"[:160]
                    self._snapshot.connected = False
                try:
                    if self._ib and self._ib.isConnected():
                        self._ib.disconnect()
                except Exception:
                    pass
                connected = False
            await asyncio.sleep(0.5 if connected else 4.0)
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)

    def _rebuild(self) -> None:
        acct = AccountSnap()
        # account summary tags
        vals = self._ib.accountValues(self._account) if self._ib else []
        tag = {}
        for v in vals:
            if getattr(v, "currency", "") in ("", "USD", "BASE"):
                tag[v.tag] = v.value
        def f(k: str) -> float:
            return _num(tag.get(k, 0))
        acct.net_liq = f("NetLiquidation")
        acct.buying_power = f("BuyingPower")
        acct.excess_liquidity = f("ExcessLiquidity")
        acct.maint_margin = f("MaintMarginReq")
        acct.gross_position = f("GrossPositionValue")
        acct.cash = f("TotalCashValue")
        if self._acct_pnl is not None:
            acct.day_pnl = _num(getattr(self._acct_pnl, "dailyPnL", 0))
            acct.unrealized_pnl = _num(getattr(self._acct_pnl, "unrealizedPnL", 0))
            acct.realized_pnl = _num(getattr(self._acct_pnl, "realizedPnL", 0))

        # IBKR-computed portfolio (marketPrice/marketValue/unrealizedPNL) — reliable
        # even with delayed data / market closed; this is what the daemon uses.
        port = {}
        try:
            for it in (self._ib.portfolio() if self._ib else []):
                cid = getattr(getattr(it, "contract", None), "conId", 0)
                if cid:
                    port[cid] = it
        except Exception:
            pass

        rows: list[PositionRow] = []
        total_mv = 0.0
        for pos in self._positions:
            con = pos.contract
            con_id = getattr(con, "conId", 0)
            sym = getattr(con, "symbol", "?")
            sec = getattr(con, "secType", "")
            mult = float(getattr(con, "multiplier", "") or (100 if sec in ("OPT", "FOP") else 1) or 1)
            qty = _num(getattr(pos, "position", 0))
            if qty == 0:
                continue  # closed position
            avg = _num(getattr(pos, "avgCost", 0))
            tk = self._tickers.get(con_id)
            ps = self._pnl_single.get(con_id)
            pi = port.get(con_id)
            last = _num(getattr(pi, "marketPrice", 0)) if pi is not None else 0.0
            mv = _num(getattr(pi, "marketValue", 0)) if pi is not None else 0.0
            upl = _num(getattr(pi, "unrealizedPNL", 0)) if pi is not None else 0.0
            if pi is not None:
                avg = _num(getattr(pi, "averageCost", 0)) or avg
            dpl = _num(getattr(ps, "dailyPnL", 0)) if ps is not None else 0.0
            # prefer a fresh live tick for last if streaming has one
            if tk is not None:
                for cand in (getattr(tk, "last", None), getattr(tk, "markPrice", None)):
                    if cand and cand == cand and cand > 0:
                        last = float(cand)
                        break
            eod = False
            if mv == 0:  # no IBKR price (live: daemon holds the data session) -> EOD close
                close = latest_close(sym, getattr(con, "currency", "USD"))
                if close and qty:
                    avg_unit = avg / mult if mult else avg
                    last = close
                    mv = close * qty * mult
                    upl = (close - avg_unit) * qty * mult
                    eod = True
            if not last and qty and mult:  # derive price from value when no tick
                last = mv / (qty * mult)
            row = PositionRow(
                symbol=sym, sec_type=sec, asset_class=asset_class_for_sectype(sec),
                sector=sector_for_symbol(sym) if sec == "STK" else asset_class_for_sectype(sec),
                qty=qty, avg_cost=avg / mult if mult else avg, con_id=con_id,
                currency=getattr(con, "currency", "USD"),
                last=last, market_value=mv, unrealized_pnl=upl, daily_pnl=dpl,
                streaming=con_id in self._tickers, eod=eod,
            )
            if tk is not None and getattr(tk, "modelGreeks", None):
                g = tk.modelGreeks
                row.delta, row.gamma = getattr(g, "delta", None), getattr(g, "gamma", None)
                row.theta, row.vega = getattr(g, "theta", None), getattr(g, "vega", None)
                row.iv = getattr(g, "impliedVol", None)
            ph = self._price_hist.setdefault(con_id, deque(maxlen=_PRICE_HISTORY))
            if last > 0:
                ph.append(last)
            row.price_hist = list(ph)
            total_mv += abs(mv)
            rows.append(row)

        for r in rows:
            r.weight = (abs(r.market_value) / total_mv * 100.0) if total_mv else 0.0
            r.day_pct = (r.daily_pnl / abs(r.market_value) * 100.0) if r.market_value else 0.0
        rows.sort(key=lambda r: abs(r.market_value), reverse=True)

        if acct.net_liq:
            self._nav.append(acct.net_liq)

        md_label = "EOD close" if any(r.eod for r in rows) else _MKT_LABEL.get(_MKT_DATA_TYPE, "?")
        snap = Snapshot(
            mode=self.mode, account=self._account, connected=bool(self._ib and self._ib.isConnected()),
            market_data=md_label,
            positions=rows, acct=acct, nav_hist=list(self._nav),
            streamed=len(self._tickers), total=len(self._positions), last_update=time.time(),
        )
        with self._lock:
            self._snapshot = snap
