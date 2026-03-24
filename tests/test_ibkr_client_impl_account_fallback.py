from __future__ import annotations

import importlib
import sys
import types
from types import SimpleNamespace


class _StubLogger:
    def debug(self, *_args, **_kwargs) -> None:
        return None

    def info(self, *_args, **_kwargs) -> None:
        return None

    def warning(self, *_args, **_kwargs) -> None:
        return None

    def error(self, *_args, **_kwargs) -> None:
        return None

    def exception(self, *_args, **_kwargs) -> None:
        return None


def _load_ibkr_client_impl_module(monkeypatch):
    # Minimal apathis logging stub.
    apathis_mod = types.ModuleType("apathis")
    apathis_core_mod = types.ModuleType("apathis.core")
    apathis_logging_mod = types.ModuleType("apathis.core.logging")
    apathis_logging_mod.get_logger = lambda _name: _StubLogger()  # type: ignore[attr-defined]

    # Minimal ib_compat stub so importing ibkr_client_impl does not require
    # external IB libraries in the test environment.
    ib_compat_mod = types.ModuleType("prometheus.execution.ib_compat")

    class _Event:
        def __iadd__(self, _cb):
            return self

    class _IB:
        def __init__(self) -> None:
            self.orderStatusEvent = _Event()
            self.execDetailsEvent = _Event()
            self.errorEvent = _Event()
            self.connectedEvent = _Event()
            self.disconnectedEvent = _Event()

        def isConnected(self) -> bool:
            return False

    class _Contract:
        def __init__(self, symbol: str = "") -> None:
            self.symbol = symbol

    class _NoopOrder:
        def __init__(self, *_args, **_kwargs) -> None:
            self.orderRef = ""

    ib_compat_mod.IB = _IB  # type: ignore[attr-defined]
    ib_compat_mod.Contract = _Contract  # type: ignore[attr-defined]
    ib_compat_mod.LimitOrder = _NoopOrder  # type: ignore[attr-defined]
    ib_compat_mod.MarketOrder = _NoopOrder  # type: ignore[attr-defined]
    ib_compat_mod.StopOrder = _NoopOrder  # type: ignore[attr-defined]
    ib_compat_mod.StopLimitOrder = _NoopOrder  # type: ignore[attr-defined]
    ib_compat_mod.Trade = object  # type: ignore[attr-defined]
    ib_compat_mod.Fill = object  # type: ignore[attr-defined]
    ib_compat_mod.Order = object  # type: ignore[attr-defined]

    # Minimal InstrumentMapper stub.
    mapper_mod = types.ModuleType("prometheus.execution.instrument_mapper")

    class _StubInstrumentMapper:
        def __init__(self, *_args, **_kwargs) -> None:
            return None

        def load_instruments(self) -> None:
            return None

        def get_contract(self, instrument_id: str):
            return _Contract(symbol=instrument_id)

        @staticmethod
        def contract_to_instrument_id(contract) -> str:
            return getattr(contract, "symbol", "UNKNOWN")

    mapper_mod.InstrumentMapper = _StubInstrumentMapper  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "apathis", apathis_mod)
    monkeypatch.setitem(sys.modules, "apathis.core", apathis_core_mod)
    monkeypatch.setitem(sys.modules, "apathis.core.logging", apathis_logging_mod)
    monkeypatch.setitem(sys.modules, "prometheus.execution.ib_compat", ib_compat_mod)
    monkeypatch.setitem(sys.modules, "prometheus.execution.instrument_mapper", mapper_mod)

    for name in (
        "prometheus.execution.connection_manager",
        "prometheus.execution.ibkr_client_impl",
    ):
        sys.modules.pop(name, None)

    return importlib.import_module("prometheus.execution.ibkr_client_impl")


class _FakeIB:
    def __init__(self, *, portfolio_items: list, account_values_by_account: dict) -> None:
        self._portfolio_items = list(portfolio_items)
        self._account_values_by_account = dict(account_values_by_account)
        self.account_values_calls: list[str | None] = []

    def portfolio(self):
        return list(self._portfolio_items)

    def accountValues(self, account=None):
        self.account_values_calls.append(account)
        return list(self._account_values_by_account.get(account, []))

    def isConnected(self) -> bool:
        return False

    def disconnect(self) -> None:
        return None


def _mk_portfolio_item(account: str, symbol: str):
    return SimpleNamespace(
        account=account,
        contract=SimpleNamespace(symbol=symbol),
        position=10.0,
        averageCost=100.0,
        marketValue=1000.0,
        unrealizedPNL=5.0,
    )


def _mk_account_value(account: str, tag: str, value: str):
    return SimpleNamespace(account=account, tag=tag, value=value)


def test_sync_positions_falls_back_when_configured_account_missing(monkeypatch) -> None:
    module = _load_ibkr_client_impl_module(monkeypatch)
    mapper = module.InstrumentMapper()
    client = module.IbkrClientImpl(
        config=module.IbkrConnectionConfig(account_id="DUN807925"),
        mapper=mapper,
    )
    client._ib = _FakeIB(
        portfolio_items=[
            _mk_portfolio_item("DUN188994", "SH.US"),
            _mk_portfolio_item("DUN188994", "SPY.US"),
        ],
        account_values_by_account={},
    )

    client._sync_positions()

    positions = client.get_positions()
    assert set(positions.keys()) == {"SH.US", "SPY.US"}
    assert all(p.quantity == 10.0 for p in positions.values())


def test_sync_account_values_retries_unfiltered_when_configured_account_empty(monkeypatch) -> None:
    module = _load_ibkr_client_impl_module(monkeypatch)
    mapper = module.InstrumentMapper()
    client = module.IbkrClientImpl(
        config=module.IbkrConnectionConfig(account_id="DUN807925"),
        mapper=mapper,
    )
    client._ib = _FakeIB(
        portfolio_items=[],
        account_values_by_account={
            "DUN807925": [],
            None: [
                _mk_account_value("DUN188994", "NetLiquidation", "250000"),
                _mk_account_value("DUN188994", "TotalCashValue", "15000"),
                _mk_account_value("DUN188994", "BuyingPower", "500000"),
            ],
        },
    )

    client._sync_account_values()

    state = client.get_account_state()
    assert state["NetLiquidation"] == 250000.0
    assert state["TotalCashValue"] == 15000.0
    assert state["equity"] == 250000.0
    assert state["cash"] == 15000.0
    assert client._ib.account_values_calls == ["DUN807925", None]
