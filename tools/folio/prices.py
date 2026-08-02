"""Last-close fallback from the historical DB.

On the LIVE account the prometheus daemon (clientId 1) holds IBKR's account-
update + market-data session, so folio's portfolio()/reqMktData come back empty
(error 10197). Rather than show an empty book, fall back to the last close from
`prices_daily` (EODHD, keyed by instrument_id like "NVDA.US"). Account-level
P&L still comes live from IBKR; only per-position prices use this fallback.
"""

from __future__ import annotations

import threading

_lock = threading.Lock()
_cache: dict[str, float | None] = {}

# symbol -> EODHD instrument_id, for the handful of names where ".US" is wrong
try:
    from apatheon.graph.company_tickers import TICKER_MAP

    _EODHD_BY_SYMBOL: dict[str, str] = {}
    for _cid, _maps in (TICKER_MAP or {}).items():
        for _m in _maps:
            _es = getattr(_m, "eodhd_symbol", "") or ""
            _base = _es.split(".")[0].upper()
            if _base and getattr(_m, "is_primary", True):
                _EODHD_BY_SYMBOL.setdefault(_base, _es)
except Exception:  # pragma: no cover
    _EODHD_BY_SYMBOL = {}


def latest_close(symbol: str | None, currency: str = "USD") -> float | None:
    """Most recent close for an equity symbol, or None if unknown. Cached for
    the session (EOD close doesn't move intraday)."""
    sym = (symbol or "").upper().strip()
    if not sym:
        return None
    with _lock:
        if sym in _cache:
            return _cache[sym]

    candidates: list[str] = []
    if sym in _EODHD_BY_SYMBOL:
        candidates.append(_EODHD_BY_SYMBOL[sym])
    if currency in ("USD", "", None):
        candidates.append(f"{sym}.US")
    candidates.append(f"{sym}.US")
    candidates = list(dict.fromkeys(candidates))  # dedupe, preserve order

    val: float | None = None
    try:
        from apatheon.core.database import get_db_manager

        m = get_db_manager()
        with m.get_historical_connection() as c:
            cur = c.cursor()
            cur.execute(
                "SELECT close FROM prices_daily WHERE instrument_id = ANY(%s) "
                "ORDER BY trade_date DESC LIMIT 1",
                (candidates,),
            )
            r = cur.fetchone()
            if r and r[0]:
                val = float(r[0])
    except Exception:
        val = None

    with _lock:
        _cache[sym] = val
    return val
