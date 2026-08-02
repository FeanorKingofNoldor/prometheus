"""Tests for prometheus.meta.config_resolver (active config overlay)."""

from __future__ import annotations

from typing import Any, List

from prometheus.meta.config_resolver import apply_overlay, load_active_config_overlay
from prometheus.portfolio.config import PortfolioConfig

# ── Fake DB plumbing ─────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, rows: List[tuple], raise_on_execute: bool = False) -> None:
        self._rows = rows
        self._raise = raise_on_execute
        self.executed: List[tuple] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))
        if self._raise:
            raise RuntimeError("boom")

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self) -> None:
        pass


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *a: Any) -> bool:
        return False


class _FakeDB:
    def __init__(self, rows: List[tuple], raise_on_execute: bool = False) -> None:
        self.cursor = _FakeCursor(rows, raise_on_execute)

    def get_runtime_connection(self) -> _FakeConn:
        return _FakeConn(self.cursor)


def _base_config(**overrides: Any) -> PortfolioConfig:
    kwargs: dict[str, Any] = dict(
        portfolio_id="US_CORE_LONG_EQ",
        strategies=["US_CORE_LONG_EQ"],
        markets=["US_EQ"],
        base_currency="USD",
        risk_model_id="basic-longonly-v1",
        optimizer_type="SIMPLE_LONG_ONLY",
        risk_aversion_lambda=0.0,
        leverage_limit=1.0,
        gross_exposure_limit=1.0,
        per_instrument_max_weight=0.05,
        max_names=25,
        sector_limits={},
        country_limits={},
        factor_limits={},
        fragility_exposure_limit=0.5,
        turnover_limit=0.5,
        cost_model_id="none",
    )
    kwargs.update(overrides)
    return PortfolioConfig(**kwargs)


# ── load_active_config_overlay ───────────────────────────────────────


def test_load_overlay_returns_config_json():
    db = _FakeDB([({"max_names": 30, "score_concentration_power": 2.0},)])
    overlay = load_active_config_overlay(db, "US_CORE_LONG_EQ")
    assert overlay == {"max_names": 30, "score_concentration_power": 2.0}
    sql, params = db.cursor.executed[0]
    assert "active_strategy_config_id" in sql
    assert params == ("US_CORE_LONG_EQ",)


def test_load_overlay_empty_when_no_active_config():
    db = _FakeDB([])
    assert load_active_config_overlay(db, "US_CORE_LONG_EQ") == {}


def test_load_overlay_empty_when_config_json_null():
    db = _FakeDB([(None,)])
    assert load_active_config_overlay(db, "US_CORE_LONG_EQ") == {}


def test_load_overlay_empty_on_db_error():
    db = _FakeDB([], raise_on_execute=True)
    assert load_active_config_overlay(db, "US_CORE_LONG_EQ") == {}


def test_load_overlay_rejects_non_dict_config():
    db = _FakeDB([(["not", "a", "dict"],)])
    assert load_active_config_overlay(db, "US_CORE_LONG_EQ") == {}


# ── apply_overlay ────────────────────────────────────────────────────


def test_apply_overlay_whitelisted_keys():
    cfg = _base_config()
    out = apply_overlay(cfg, {
        "max_names": 40,
        "score_concentration_power": 2.0,
        "conviction_decay_rate": 3.0,
    })
    assert out.max_names == 40
    assert out.score_concentration_power == 2.0
    assert out.conviction_decay_rate == 3.0
    # Input not mutated
    assert cfg.max_names == 25


def test_apply_overlay_alias_max_weight_per_name():
    cfg = _base_config()
    out = apply_overlay(cfg, {"max_weight_per_name": 0.08})
    assert out.per_instrument_max_weight == 0.08


def test_apply_overlay_ignores_non_whitelisted_keys():
    cfg = _base_config()
    out = apply_overlay(cfg, {
        "portfolio_id": "EVIL",           # not whitelisted — must not apply
        "risk_model_id": "EVIL",          # not whitelisted
        "leverage_limit": 99.0,           # not whitelisted
        "max_names": 10,                  # whitelisted
    })
    assert out.portfolio_id == "US_CORE_LONG_EQ"
    assert out.risk_model_id == "basic-longonly-v1"
    assert out.leverage_limit == 1.0
    assert out.max_names == 10


def test_apply_overlay_skips_invalid_types():
    cfg = _base_config()
    out = apply_overlay(cfg, {
        "max_names": "lots",                     # not an int → skipped
        "score_concentration_power": "2.5",      # numeric string → coerced
        "conviction_enabled": "true",            # bool encoding → coerced
    })
    assert out.max_names == 25
    assert out.score_concentration_power == 2.5
    assert out.conviction_enabled is True


def test_apply_overlay_int_rejects_fractional_float():
    cfg = _base_config()
    out = apply_overlay(cfg, {"max_names": 12.5})
    assert out.max_names == 25  # skipped


def test_apply_overlay_budget_floor_keys_skipped_until_model_supports_them():
    # meta_budget_min is whitelisted for forward-compat but PortfolioConfig
    # has no such field yet — it must be skipped, not blindly set.
    cfg = _base_config()
    out = apply_overlay(cfg, {"meta_budget_min": 0.4})
    assert not hasattr(out, "meta_budget_min") or out is cfg


def test_apply_overlay_empty_returns_same_instance():
    cfg = _base_config()
    assert apply_overlay(cfg, {}) is cfg


def test_apply_overlay_none_clears_optional_max_names():
    cfg = _base_config()
    out = apply_overlay(cfg, {"max_names": None})
    assert out.max_names is None
