from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path

import pytest


def _load_show_alembic_status_module(monkeypatch: pytest.MonkeyPatch):
    # Minimal stubs so module import does not require installing cross-repo deps.
    apathis_mod = types.ModuleType("apathis")
    apathis_core_mod = types.ModuleType("apathis.core")
    apathis_db_mod = types.ModuleType("apathis.core.database")

    def _dummy_get_db_manager():
        return None

    apathis_db_mod.get_db_manager = _dummy_get_db_manager  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "apathis", apathis_mod)
    monkeypatch.setitem(sys.modules, "apathis.core", apathis_core_mod)
    monkeypatch.setitem(sys.modules, "apathis.core.database", apathis_db_mod)

    sys.modules.pop("prometheus.scripts.show.show_alembic_status", None)
    return importlib.import_module("prometheus.scripts.show.show_alembic_status")


def test_get_alembic_heads_from_project_root(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_show_alembic_status_module(monkeypatch)
    project_root = Path(__file__).resolve().parents[1]
    heads = module._get_alembic_heads(project_root)

    assert heads
    assert all(isinstance(head, str) and head for head in heads)


def test_read_db_version_rejects_unknown_selector(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_show_alembic_status_module(monkeypatch)

    with pytest.raises(ValueError):
        module._read_db_version(object(), "unknown")
