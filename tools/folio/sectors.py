"""Symbol -> sector mapping + IBKR secType -> asset-class classifier.

Sector data is reused from apatheon's company graph (partial coverage of the
investable universe — geopolitically significant names). Anything unmapped
falls back to "Unclassified". Asset class is always exact (from IBKR secType).
"""

from __future__ import annotations

try:  # apatheon is a sibling package on PYTHONPATH
    from apatheon.graph.companies import COMPANIES
    from apatheon.graph.company_tickers import TICKER_MAP
except Exception:  # pragma: no cover - apatheon not importable
    COMPANIES = []
    TICKER_MAP = {}


def _build_sector_index() -> dict[str, str]:
    sector_by_company = {getattr(c, "id", None): getattr(c, "sector", None) for c in COMPANIES}
    out: dict[str, str] = {}
    for company_id, mappings in (TICKER_MAP or {}).items():
        sector = sector_by_company.get(company_id)
        if not sector:
            continue
        for m in mappings:
            raw = getattr(m, "eodhd_symbol", None) or ""
            base = raw.split(".")[0].strip().upper()  # "AAPL.US" -> "AAPL"
            if base:
                out.setdefault(base, sector)
    return out


_SECTOR_BY_TICKER: dict[str, str] = _build_sector_index()

# Curated supplement so common ETFs + large-caps aren't all "Unclassified".
# Consulted after the apatheon graph map, before the "Unclassified" fallback.
_SUPPLEMENT: dict[str, str] = {
    # --- broad / index ETFs ---
    "SPY": "US Broad", "VOO": "US Broad", "IVV": "US Broad", "VTI": "US Broad",
    "SPLG": "US Broad", "RSP": "US Broad", "DIA": "US Broad", "IWM": "US Small Cap",
    "IJR": "US Small Cap", "IWB": "US Broad", "VT": "Global Equity", "ACWI": "Global Equity",
    "VEA": "Developed Intl", "EFA": "Developed Intl", "VWO": "Emerging Mkts",
    "EEM": "Emerging Mkts", "VXUS": "Developed Intl", "IEFA": "Developed Intl",
    "QQQ": "US Tech", "QQQM": "US Tech", "VGT": "Technology", "XLK": "Technology",
    "SOXX": "Semiconductors", "SMH": "Semiconductors",
    # --- sector ETFs ---
    "XLE": "Energy", "XOP": "Energy", "OIH": "Energy", "VDE": "Energy",
    "XLF": "Financials", "VFH": "Financials", "KRE": "Financials",
    "XLV": "Healthcare", "VHT": "Healthcare", "XBI": "Healthcare",
    "XLI": "Industrials", "VIS": "Industrials",
    "XLY": "Consumer Discretionary", "XLP": "Consumer Staples",
    "XLU": "Utilities", "XLB": "Materials", "XLRE": "Real Estate",
    "VNQ": "Real Estate", "XLC": "Communications",
    # --- bonds / rates ---
    "TLT": "Bonds", "IEF": "Bonds", "SHY": "Bonds", "BND": "Bonds", "AGG": "Bonds",
    "LQD": "Bonds", "HYG": "Bonds", "JNK": "Bonds", "TIP": "Bonds", "MUB": "Bonds",
    "BIL": "Bonds", "GOVT": "Bonds", "EMB": "Bonds", "VCIT": "Bonds", "VCSH": "Bonds",
    # --- commodities / metals ---
    "GLD": "Gold", "IAU": "Gold", "SLV": "Silver", "GDX": "Gold Miners",
    "USO": "Energy", "UNG": "Energy", "DBC": "Commodities", "PDBC": "Commodities",
    "GLDM": "Gold",
    # --- volatility / crypto ---
    "VIXY": "Volatility", "UVXY": "Volatility", "VXX": "Volatility",
    "BITO": "Crypto", "IBIT": "Crypto", "GBTC": "Crypto", "ETHE": "Crypto",
    # --- large-cap single names by sector ---
    "AAPL": "Technology", "MSFT": "Technology", "NVDA": "Technology",
    "AVGO": "Technology", "ORCL": "Technology", "CRM": "Technology",
    "ADBE": "Technology", "CSCO": "Technology", "AMD": "Technology",
    "INTC": "Technology", "TXN": "Technology", "QCOM": "Technology", "IBM": "Technology",
    "GOOGL": "Communications", "GOOG": "Communications", "META": "Communications",
    "NFLX": "Communications", "DIS": "Communications", "T": "Communications",
    "VZ": "Communications", "TMUS": "Communications",
    "AMZN": "Consumer Discretionary", "TSLA": "Consumer Discretionary",
    "HD": "Consumer Discretionary", "MCD": "Consumer Discretionary",
    "NKE": "Consumer Discretionary", "LOW": "Consumer Discretionary",
    "SBUX": "Consumer Discretionary", "BKNG": "Consumer Discretionary",
    "WMT": "Consumer Staples", "PG": "Consumer Staples", "KO": "Consumer Staples",
    "PEP": "Consumer Staples", "COST": "Consumer Staples", "PM": "Consumer Staples",
    "JPM": "Financials", "BAC": "Financials", "WFC": "Financials", "GS": "Financials",
    "MS": "Financials", "C": "Financials", "BRK.B": "Financials", "BRK B": "Financials",
    "AXP": "Financials", "BLK": "Financials", "SCHW": "Financials", "V": "Financials",
    "MA": "Financials",
    "UNH": "Healthcare", "JNJ": "Healthcare", "LLY": "Healthcare", "PFE": "Healthcare",
    "MRK": "Healthcare", "ABBV": "Healthcare", "TMO": "Healthcare", "ABT": "Healthcare",
    "XOM": "Energy", "CVX": "Energy", "COP": "Energy", "SLB": "Energy",
    "CAT": "Industrials", "BA": "Industrials", "GE": "Industrials", "HON": "Industrials",
    "UPS": "Industrials", "RTX": "Industrials", "LMT": "Industrials", "DE": "Industrials",
    "NEE": "Utilities", "DUK": "Utilities", "SO": "Utilities",
    "LIN": "Materials", "SHW": "Materials", "FCX": "Materials",
    "PLD": "Real Estate", "AMT": "Real Estate",
}

# IBKR secType -> human asset class. IBKR does not distinguish ETFs from stocks
# (both are STK), so equities + ETFs land in "Equity".
_ASSET_CLASS = {
    "STK": "Equity",
    "OPT": "Option",
    "FOP": "Option",
    "FUT": "Future",
    "CONTFUT": "Future",
    "CASH": "FX",
    "IND": "Index",
    "BOND": "Bond",
    "BILL": "Bond",
    "FUND": "Fund",
    "CMDTY": "Commodity",
    "WAR": "Warrant",
    "BAG": "Combo",
}


def sector_for_symbol(symbol: str | None) -> str:
    key = (symbol or "").upper()
    return _SECTOR_BY_TICKER.get(key) or _SUPPLEMENT.get(key, "Unclassified")


def asset_class_for_sectype(sec_type: str | None) -> str:
    return _ASSET_CLASS.get((sec_type or "").upper(), (sec_type or "Other"))


def coverage() -> int:
    """Number of tickers with a known sector (for honesty in the UI)."""
    return len(set(_SECTOR_BY_TICKER) | set(_SUPPLEMENT))
