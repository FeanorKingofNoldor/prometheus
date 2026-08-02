"""Tests for the canonical boolean env parser (prometheus.env_utils)."""

from __future__ import annotations

import pytest

from prometheus.env_utils import env_flag

_VAR = "PROMETHEUS_TEST_ENV_FLAG"


@pytest.mark.parametrize("raw", ["1", "true", "TRUE", "True", "yes", "YES", "on", "On", " true "])
def test_truthy_tokens(monkeypatch, raw):
    monkeypatch.setenv(_VAR, raw)
    assert env_flag(_VAR) is True
    assert env_flag(_VAR, default=False) is True


@pytest.mark.parametrize("raw", ["0", "false", "FALSE", "no", "No", "off", "OFF", "", "  "])
def test_falsy_tokens(monkeypatch, raw):
    monkeypatch.setenv(_VAR, raw)
    assert env_flag(_VAR) is False
    assert env_flag(_VAR, default=True) is False


def test_unset_returns_default(monkeypatch):
    monkeypatch.delenv(_VAR, raising=False)
    assert env_flag(_VAR) is False
    assert env_flag(_VAR, default=True) is True


def test_unrecognized_token_returns_default(monkeypatch):
    monkeypatch.setenv(_VAR, "ture")  # typo must not silently flip a flag
    assert env_flag(_VAR, default=False) is False
    assert env_flag(_VAR, default=True) is True
