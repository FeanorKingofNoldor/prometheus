"""Tests for the paper/live defense-in-depth guard (audit fix #5).

The only thing separating paper from live is the port number, so the
PaperBroker construction path hard-stops if it is wired to a live port or
a live account.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from prometheus.execution.paper_broker import (
    LIVE_PORTS,
    PaperBroker,
    PaperLiveMisconfiguration,
    assert_not_live,
)


class _FakeClient:
    """Minimal client exposing a ``_config`` with a port, like IbkrClientImpl."""

    def __init__(self, port: int):
        self._config = SimpleNamespace(port=port)


def test_assert_not_live_rejects_live_gateway_port():
    with pytest.raises(PaperLiveMisconfiguration, match="LIVE IBKR port"):
        assert_not_live(port=4001, account_id="DU123")


def test_assert_not_live_rejects_live_tws_port():
    with pytest.raises(PaperLiveMisconfiguration, match="LIVE IBKR port"):
        assert_not_live(port=7496, account_id="DU123")


def test_assert_not_live_allows_paper_port():
    # 4002 (paper gateway) with a DU paper account is fine.
    assert_not_live(port=4002, account_id="DU188994")


def test_assert_not_live_rejects_live_account_env(monkeypatch):
    monkeypatch.setenv("IBKR_LIVE_ACCOUNT", "U22014992")
    with pytest.raises(PaperLiveMisconfiguration, match="LIVE account"):
        assert_not_live(port=4002, account_id="U22014992")


def test_assert_not_live_rejects_bare_U_account_heuristic(monkeypatch):
    monkeypatch.delenv("IBKR_LIVE_ACCOUNT", raising=False)
    # Live accounts start with 'U', paper with 'DU'.
    with pytest.raises(PaperLiveMisconfiguration, match="looks"):
        assert_not_live(port=4002, account_id="U9999999")


def test_paper_broker_construction_rejects_live_port():
    """Constructing a PaperBroker against a live port is a hard stop."""
    client = _FakeClient(port=4001)
    with pytest.raises(PaperLiveMisconfiguration):
        PaperBroker(account_id="DU188994", client=client)


def test_paper_broker_construction_allows_paper_port():
    client = _FakeClient(port=4002)
    broker = PaperBroker(account_id="DU188994", client=client)
    assert broker.account_id == "DU188994"


def test_live_ports_constant():
    assert 4001 in LIVE_PORTS
    assert 7496 in LIVE_PORTS
    assert 4002 not in LIVE_PORTS
