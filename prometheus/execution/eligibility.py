"""EU-retail (PRIIPs) instrument purchase-eligibility layer.

Regulatory background
---------------------
The account is EU **retail** at IBKR Ireland. Under the PRIIPs Regulation
(EU) No 1286/2014, a "packaged" product (ETF/ETN/fund/CEF) may only be
sold to an EU retail investor if the manufacturer provides a PRIIPs KID.
US-domiciled ETFs/ETNs/funds do not publish KIDs, so IBKR blocks *opening*
purchases of US-domiciled packaged products on the live entity.

Scope of the block (verified 3x):

- Only **purchases** are blocked. Selling / closing an existing holding is
  always allowed.
- **Direct shares are out of scope** of PRIIPs — Reg. (EU) 1286/2014
  recital 7 explicitly excludes assets "held directly, such as corporate
  shares or sovereign bonds", so individual stocks are unaffected on every
  market.
- **UCITS ETFs** listed on non-US venues (LSE, XETRA, ...) publish KIDs and
  remain fully eligible.
- **US-listed options** are allowed (OCC publishes KIDs), so the
  derivatives sleeves are unaffected.

Paper-vs-live divergence
------------------------
The IBKR *paper* entity does NOT enforce the PRIIPs gate, so a paper book
can happily hold SPY/RSP/KRE while the identical live order would be
rejected. Prometheus must therefore be live-parity on paper: an instrument
the live account cannot buy must never enter a book. This module is the
single source of truth for that gate; the universe engine applies it at
candidate-enumeration time.

Data reality (prometheus_runtime.instruments, audited 2026-07-03)
-----------------------------------------------------------------
``asset_class`` is NOT a reliable ETF marker: 100 of the 101 US-listed
ETFs are stored with ``asset_class = 'EQUITY'`` (only SDS.US carries
``'ETF'``). The reliable markers are in ``metadata``:

- ``metadata->>'index' = 'GLOBAL_ETF'`` (100 rows, incl. SPY/QQQ/RSP/KRE/XL*)
- ``metadata ? 'etf_category'`` (same rows)
- ``metadata->>'source' = 'sector_etf_ingest'`` (SDS.US)

Detection therefore combines the asset-class set with the metadata
markers, plus a static snapshot fallback so a DB error can never silently
re-admit SPY.

Professional-account override
-----------------------------
``PROMETHEUS_PROFESSIONAL_ACCOUNT=1`` (MiFID II elective professional
status removes the PRIIPs retail gate) makes everything eligible — one
switch to flip if the account is ever upgraded.
"""

from __future__ import annotations

from apatheon.core.logging import get_logger

from prometheus.env_utils import env_flag

logger = get_logger(__name__)

# Env var: MiFID elective professional status — removes the PRIIPs gate.
PROFESSIONAL_ACCOUNT_ENV = "PROMETHEUS_PROFESSIONAL_ACCOUNT"

# Packaged-product asset classes blocked for EU retail purchase on US
# venues. NOTE (DB audit 2026-07-03): the live instruments table only ever
# uses 'ETF' from this set (SDS.US); everything else is mislabeled
# 'EQUITY' and is caught via the metadata markers below. The full set is
# kept so correctly-labeled rows are blocked the day ingestion improves.
RETAIL_BLOCKED_ASSET_CLASSES = frozenset({"ETF", "ETN", "FUND", "CEF"})

# Metadata markers that identify a US-listed packaged product even when
# asset_class says 'EQUITY' (the dominant case in the current DB).
_ETF_METADATA_INDEX = "GLOBAL_ETF"
_ETF_METADATA_CATEGORY_KEY = "etf_category"
_ETF_METADATA_SOURCES = frozenset({"sector_etf_ingest"})

# Static snapshot of every US-domiciled packaged product in the
# instruments table (audited 2026-07-03; 101 ids). Used two ways:
#  1. belt-and-braces in the point check (SPY.US is blocked even when the
#     caller passes the DB's wrong asset_class and no metadata), and
#  2. fail-safe fallback when the DB query in
#     ``load_ineligible_instrument_ids`` errors — a DB outage must not
#     silently re-admit SPY into a book.
KNOWN_US_PACKAGED_PRODUCT_IDS: frozenset[str] = frozenset(
    {
        "AGG.US", "ARKK.US", "BDRY.US", "BND.US", "BNDX.US", "BOAT.US",
        "CORN.US", "DBC.US", "DIA.US", "EEM.US", "EFA.US", "EIDO.US",
        "EIS.US", "EMB.US", "EWA.US", "EWC.US", "EWG.US", "EWH.US",
        "EWI.US", "EWJ.US", "EWL.US", "EWM.US", "EWN.US", "EWP.US",
        "EWQ.US", "EWS.US", "EWT.US", "EWU.US", "EWY.US", "EWZ.US",
        "FXB.US", "FXE.US", "FXI.US", "FXY.US", "GLD.US", "HACK.US",
        "HYG.US", "IAU.US", "ICLN.US", "IEF.US", "IEFA.US", "IEMG.US",
        "IGV.US", "INDA.US", "ITA.US", "IWM.US", "IYR.US", "KBE.US",
        "KRE.US", "KSA.US", "KWEB.US", "LIT.US", "LQD.US", "MDY.US",
        "MSOS.US", "MUB.US", "PALL.US", "PPLT.US", "QQQ.US", "RSP.US",
        "SDS.US", "SH.US", "SHY.US", "SLV.US", "SMH.US", "SOXX.US",
        "SPY.US", "SQQQ.US", "TAN.US", "THD.US", "TIP.US", "TLT.US",
        "TQQQ.US", "TUR.US", "UNG.US", "URA.US", "USO.US", "UUP.US",
        "UVXY.US", "VIXY.US", "VNM.US", "VNQ.US", "VNQI.US", "VOO.US",
        "VTI.US", "VWO.US", "VXUS.US", "VXX.US", "WEAT.US", "XBI.US",
        "XLB.US", "XLC.US", "XLE.US", "XLF.US", "XLI.US", "XLK.US",
        "XLP.US", "XLRE.US", "XLU.US", "XLV.US", "XLY.US",
    }
)

# One-shot guard so the "None asset_class treated as stock" debug message
# is logged once per process, not once per instrument per day.
_NONE_ASSET_CLASS_LOGGED = [False]


def _is_professional_account() -> bool:
    """True when the MiFID elective-professional override is set."""
    return env_flag(PROFESSIONAL_ACCOUNT_ENV, default=False)


def _is_us_market(instrument_id: str, market_id: str | None) -> bool:
    """True when the instrument trades on a US venue (PRIIPs-relevant)."""
    if market_id == "US_EQ":
        return True
    return instrument_id.upper().endswith(".US")


def _metadata_flags_packaged_product(metadata: dict | None) -> bool:
    """True when instrument metadata identifies a packaged product.

    Needed because the instruments table mislabels almost every US ETF as
    asset_class='EQUITY'; the metadata markers are the reliable signal.
    """
    if not metadata:
        return False
    if metadata.get("index") == _ETF_METADATA_INDEX:
        return True
    if _ETF_METADATA_CATEGORY_KEY in metadata:
        return True
    if metadata.get("source") in _ETF_METADATA_SOURCES:
        return True
    return False


def is_retail_purchase_eligible(
    instrument_id: str,
    asset_class: str | None,
    market_id: str | None,
    metadata: dict | None = None,
) -> bool:
    """Return False iff an EU retail account cannot BUY this instrument live.

    Ineligible only when the instrument is US-listed (market_id == 'US_EQ'
    or the instrument id carries a ``.US`` suffix) AND it is a packaged
    product (asset_class in :data:`RETAIL_BLOCKED_ASSET_CLASSES`, ETF
    metadata markers, or the static known-ETF snapshot). Everything else —
    individual stocks everywhere, UCITS ETFs on LSE/XETRA, non-US markets
    — is eligible. Selling/closing is always allowed and is NOT gated here.

    ``asset_class=None`` is treated as a direct share (stocks-by-default;
    PRIIPs recital 7 keeps direct shares out of scope) with a log-once
    debug note.

    The :data:`PROFESSIONAL_ACCOUNT_ENV` env override makes everything
    eligible.
    """
    if _is_professional_account():
        return True

    if not _is_us_market(instrument_id, market_id):
        # UCITS ETFs on LSE/XETRA and all non-US instruments: eligible.
        return True

    if asset_class in RETAIL_BLOCKED_ASSET_CLASSES:
        return False

    if _metadata_flags_packaged_product(metadata):
        return False

    if instrument_id in KNOWN_US_PACKAGED_PRODUCT_IDS:
        # DB mislabels these as EQUITY; the static snapshot catches them
        # even when the caller has no metadata in hand.
        return False

    if asset_class is None and not _NONE_ASSET_CLASS_LOGGED[0]:
        _NONE_ASSET_CLASS_LOGGED[0] = True
        logger.debug(
            "is_retail_purchase_eligible: asset_class=None for %s — treating as a "
            "direct share (eligible; PRIIPs recital 7). Logged once per process.",
            instrument_id,
        )

    return True


def static_fallback_ineligible_ids() -> set[str]:
    """Fail-safe ineligible set used when the DB query is unavailable.

    Respects the professional-account override (empty set when set).
    """
    if _is_professional_account():
        return set()
    return set(KNOWN_US_PACKAGED_PRODUCT_IDS)


def load_ineligible_instrument_ids(db_manager) -> set[str]:
    """Return every instrument_id an EU retail account cannot BUY live.

    Queries the instruments table for US-listed packaged products using
    both the blocked asset classes and the metadata ETF markers (the DB
    mislabels most US ETFs as EQUITY), unioned with the static snapshot.

    Returns an empty set when :data:`PROFESSIONAL_ACCOUNT_ENV` is set.

    Never raises on DB/infrastructure errors: it logs loudly and returns
    the static snapshot fallback so a DB outage cannot silently re-admit
    SPY into a book (fail-open applies only to instruments unknown to the
    snapshot).
    """
    if _is_professional_account():
        return set()

    sql = """
        SELECT instrument_id
        FROM instruments
        WHERE (market_id = 'US_EQ' OR UPPER(instrument_id) LIKE '%%.US')
          AND (
                asset_class = ANY(%s)
                OR metadata->>'index' = %s
                OR metadata ? 'etf_category'
                OR metadata->>'source' = ANY(%s)
              )
    """

    try:
        with db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (
                        sorted(RETAIL_BLOCKED_ASSET_CLASSES),
                        _ETF_METADATA_INDEX,
                        sorted(_ETF_METADATA_SOURCES),
                    ),
                )
                rows = cursor.fetchall()
            finally:
                cursor.close()
    except Exception:
        logger.exception(
            "load_ineligible_instrument_ids: DB query FAILED — falling back to the "
            "static known-US-ETF snapshot (%d ids) so PRIIPs-blocked instruments "
            "(SPY.US et al.) are NOT silently re-admitted. Universe builds continue "
            "fail-open only for instruments unknown to the snapshot.",
            len(KNOWN_US_PACKAGED_PRODUCT_IDS),
        )
        return static_fallback_ineligible_ids()

    ids = {str(row[0]) for row in rows}
    # Union with the snapshot: harmless when the DB agrees (it should be a
    # superset) and a cheap invariant if rows are ever reclassified.
    return ids | set(KNOWN_US_PACKAGED_PRODUCT_IDS)


__all__ = [
    "PROFESSIONAL_ACCOUNT_ENV",
    "RETAIL_BLOCKED_ASSET_CLASSES",
    "KNOWN_US_PACKAGED_PRODUCT_IDS",
    "is_retail_purchase_eligible",
    "load_ineligible_instrument_ids",
    "static_fallback_ineligible_ids",
]
