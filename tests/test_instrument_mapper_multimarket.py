"""Tests for EODHD -> IBKR contract mapping for non-US markets.

Covers:
- The ``EODHD_TO_IBKR`` translation table applied in
  ``InstrumentMapper._metadata_to_contract`` (SMART routing +
  ``primaryExchange`` + local currency).
- HK leading-zero symbol normalization; KO symbols unchanged.
- Fallback parser raising on unknown suffixes instead of guessing USD.
- US behavior unchanged (SMART, USD, no primaryExchange).
- Loud qualification failure in ``IbkrClientImpl._create_contract`` for
  non-US contracts, lenient fallback preserved for US contracts.

Pure unit tests — no IBKR gateway, no database.
"""

from __future__ import annotations

import pytest

from prometheus.execution.instrument_mapper import (
    EODHD_TO_IBKR,
    ContractQualificationError,
    InstrumentMapper,
    InstrumentMetadata,
)


def _make_mapper() -> InstrumentMapper:
    """Build a mapper that never touches the database."""
    mapper = InstrumentMapper(db_manager=object())
    mapper._loaded = True
    mapper._instruments = {}
    return mapper


def _equity_metadata(instrument_id: str, symbol: str, exchange: str, currency: str) -> InstrumentMetadata:
    return InstrumentMetadata(
        instrument_id=instrument_id,
        symbol=symbol,
        exchange=exchange,
        currency=currency,
        asset_class="EQUITY",
    )


# ---------------------------------------------------------------------------
# Translation table -> contract construction (metadata path)
# ---------------------------------------------------------------------------

NON_US_CASES = [
    # (instrument_id, symbol, eodhd_exchange, row_currency, expected_symbol,
    #  expected_primary, expected_currency)
    ("AAL.LSE", "AAL", "LSE", "GBP", "AAL", "LSE", "GBP"),
    ("BMW.XETRA", "BMW", "XETRA", "EUR", "BMW", "IBIS", "EUR"),
    ("MC.PA", "MC", "PA", "EUR", "MC", "SBF", "EUR"),
    ("ASML.AS", "ASML", "AS", "EUR", "ASML", "AEB", "EUR"),
    ("ABI.BR", "ABI", "BR", "EUR", "ABI", "ENEXT.BE", "EUR"),
    ("ROG.SW", "ROG", "SW", "CHF", "ROG", "EBS", "CHF"),
    ("IBE.MC", "IBE", "MC", "EUR", "IBE", "BM", "EUR"),
    ("NOKIA.HE", "NOKIA", "HE", "EUR", "NOKIA", "HEX", "EUR"),
    ("0005.HK", "0005", "HK", "HKD", "5", "SEHK", "HKD"),
    ("000660.KO", "000660", "KO", "KRW", "000660", "KSE", "KRW"),
    ("BHP.AU", "BHP", "AU", "AUD", "BHP", "ASX", "AUD"),
]


@pytest.mark.parametrize(
    "instrument_id, symbol, exchange, currency, exp_symbol, exp_primary, exp_currency",
    NON_US_CASES,
)
def test_metadata_non_us_maps_to_smart_with_primary_exchange(
    instrument_id, symbol, exchange, currency, exp_symbol, exp_primary, exp_currency,
):
    mapper = _make_mapper()
    metadata = _equity_metadata(instrument_id, symbol, exchange, currency)

    contract = mapper._metadata_to_contract(metadata)

    assert contract.secType == "STK"
    assert contract.symbol == exp_symbol
    assert contract.exchange == "SMART"
    assert contract.currency == exp_currency
    assert contract.primaryExchange == exp_primary


def test_translation_table_covers_expected_suffixes():
    assert EODHD_TO_IBKR == {
        "LSE": ("LSE", "GBP"),
        "XETRA": ("IBIS", "EUR"),
        "PA": ("SBF", "EUR"),
        "AS": ("AEB", "EUR"),
        "BR": ("ENEXT.BE", "EUR"),
        "SW": ("EBS", "CHF"),
        "MC": ("BM", "EUR"),
        "HE": ("HEX", "EUR"),
        "HK": ("SEHK", "HKD"),
        "KO": ("KSE", "KRW"),
        "AU": ("ASX", "AUD"),
        "US": ("SMART", "USD"),
    }


def test_metadata_currency_from_row_wins_over_table():
    """The instruments row currency is authoritative; table is a fallback."""
    mapper = _make_mapper()
    # Row says GBX (pence) — must not be overwritten by the table's GBP.
    metadata = _equity_metadata("AAL.LSE", "AAL", "LSE", "GBX")
    contract = mapper._metadata_to_contract(metadata)
    assert contract.currency == "GBX"


def test_metadata_currency_table_fallback_when_row_empty():
    mapper = _make_mapper()
    metadata = _equity_metadata("AAL.LSE", "AAL", "LSE", "")
    contract = mapper._metadata_to_contract(metadata)
    assert contract.currency == "GBP"


def test_hk_zero_strip_only_for_numeric_symbols():
    mapper = _make_mapper()
    # Numeric: leading zeros stripped
    c1 = mapper._metadata_to_contract(_equity_metadata("0005.HK", "0005", "HK", "HKD"))
    assert c1.symbol == "5"
    c2 = mapper._metadata_to_contract(_equity_metadata("0700.HK", "0700", "HK", "HKD"))
    assert c2.symbol == "700"
    # KO numeric symbols keep their digits as-is
    c3 = mapper._metadata_to_contract(_equity_metadata("000660.KO", "000660", "KO", "KRW"))
    assert c3.symbol == "000660"


def test_metadata_us_behavior_unchanged():
    mapper = _make_mapper()
    for exchange in ("US", "NYSE_ARCA", "ARCA", "NASDAQ", "NYSE", "BATS", "IEX"):
        metadata = _equity_metadata("AAPL.US", "AAPL", exchange, "USD")
        contract = mapper._metadata_to_contract(metadata)
        assert contract.symbol == "AAPL"
        assert contract.exchange == "SMART"
        assert contract.currency == "USD"
        # US contracts must NOT carry a primaryExchange (keeps SMART lenient path)
        assert not getattr(contract, "primaryExchange", "")


def test_metadata_unknown_exchange_keeps_legacy_verbatim_behavior():
    """Codes outside the table (e.g. TSE for JP_EQ) pass through unchanged."""
    mapper = _make_mapper()
    metadata = _equity_metadata("7203.TSE", "7203", "TSE", "JPY")
    contract = mapper._metadata_to_contract(metadata)
    assert contract.exchange == "TSE"
    assert contract.currency == "JPY"
    assert not getattr(contract, "primaryExchange", "")


# ---------------------------------------------------------------------------
# Fallback parser (instrument not in DB)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "instrument_id, exp_symbol, exp_primary, exp_currency",
    [
        ("AAL.LSE", "AAL", "LSE", "GBP"),
        ("BMW.XETRA", "BMW", "IBIS", "EUR"),
        ("ROG.SW", "ROG", "EBS", "CHF"),
        ("0005.HK", "5", "SEHK", "HKD"),
        ("000660.KO", "000660", "KSE", "KRW"),
        ("BHP.AU", "BHP", "ASX", "AUD"),
    ],
)
def test_fallback_uses_suffix_table(instrument_id, exp_symbol, exp_primary, exp_currency):
    mapper = _make_mapper()
    contract = mapper.get_contract(instrument_id)  # not in DB -> fallback
    assert contract.symbol == exp_symbol
    assert contract.exchange == "SMART"
    assert contract.currency == exp_currency
    assert contract.primaryExchange == exp_primary


def test_fallback_us_unchanged():
    mapper = _make_mapper()
    contract = mapper.get_contract("AAPL.US")
    assert contract.symbol == "AAPL"
    assert contract.exchange == "SMART"
    assert contract.currency == "USD"
    assert not getattr(contract, "primaryExchange", "")


def test_fallback_unknown_suffix_raises_instead_of_guessing_usd():
    mapper = _make_mapper()
    with pytest.raises(ValueError, match="Unknown EODHD exchange suffix"):
        mapper.get_contract("SHOP.TO")
    with pytest.raises(ValueError, match="EODHD_TO_IBKR"):
        mapper._parse_instrument_id_fallback("7203.TSE")


def test_fallback_bare_symbol_still_treated_as_us():
    mapper = _make_mapper()
    contract = mapper.get_contract("AAPL")
    assert contract.symbol == "AAPL"
    assert contract.exchange == "SMART"
    assert contract.currency == "USD"


# ---------------------------------------------------------------------------
# Loud qualification in IbkrClientImpl._create_contract
# ---------------------------------------------------------------------------

class _FakeIB:
    """Minimal qualifyContracts stub."""

    def __init__(self, result=None, exc: Exception | None = None):
        self.result = result if result is not None else []
        self.exc = exc
        self.calls: list = []

    def qualifyContracts(self, *contracts):
        self.calls.append(contracts)
        if self.exc is not None:
            raise self.exc
        return list(self.result)

    def isConnected(self) -> bool:  # keeps IbkrClientImpl.__del__ quiet
        return False


def _make_client(instruments: dict[str, InstrumentMetadata]):
    # Reload fresh: other suites (test_ibkr_client_impl_account_fallback,
    # test_third_pass_audit) re-import ibkr_client_impl against STUB
    # instrument_mapper modules and leave the stub-bound module object in
    # sys.modules — its ContractQualificationError would then be a
    # different class than the real one we assert against.
    import importlib
    import sys

    sys.modules.pop("prometheus.execution.ibkr_client_impl", None)
    impl = importlib.import_module("prometheus.execution.ibkr_client_impl")

    from prometheus.execution.ibkr_client import IbkrConnectionConfig

    mapper = _make_mapper()
    mapper._instruments = dict(instruments)
    config = IbkrConnectionConfig(host="127.0.0.1", port=4002, client_id=99, readonly=True)
    return impl.IbkrClientImpl(config, mapper=mapper)


_LSE_META = _equity_metadata("AAL.LSE", "AAL", "LSE", "GBP")
_US_META = _equity_metadata("AAPL.US", "AAPL", "US", "USD")


def test_create_contract_non_us_raises_on_empty_qualification():
    client = _make_client({"AAL.LSE": _LSE_META})
    client._ib = _FakeIB(result=[])
    with pytest.raises(ContractQualificationError, match="AAL.LSE"):
        client._create_contract("AAL.LSE")


def test_create_contract_non_us_raises_when_qualify_throws():
    client = _make_client({"AAL.LSE": _LSE_META})
    client._ib = _FakeIB(exc=RuntimeError("gateway says no"))
    with pytest.raises(ContractQualificationError, match="gateway says no"):
        client._create_contract("AAL.LSE")


def test_create_contract_non_us_returns_qualified_contract():
    client = _make_client({"AAL.LSE": _LSE_META})

    class _Qualified:
        conId = 123456
        symbol = "AAL"
        exchange = "SMART"
        primaryExchange = "LSE"
        currency = "GBP"

    client._ib = _FakeIB(result=[_Qualified()])
    contract = client._create_contract("AAL.LSE")
    assert contract.conId == 123456


def test_create_contract_us_keeps_lenient_fallback_on_empty():
    client = _make_client({"AAPL.US": _US_META})
    client._ib = _FakeIB(result=[])
    contract = client._create_contract("AAPL.US")
    # Falls back to the unqualified contract instead of raising
    assert contract.symbol == "AAPL"
    assert contract.exchange == "SMART"
    assert getattr(contract, "conId", 0) in (0, None)


def test_create_contract_us_keeps_lenient_fallback_on_exception():
    client = _make_client({"AAPL.US": _US_META})
    client._ib = _FakeIB(exc=RuntimeError("timeout"))
    contract = client._create_contract("AAPL.US")
    assert contract.symbol == "AAPL"
    assert contract.exchange == "SMART"
