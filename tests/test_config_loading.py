"""Tests for YAML/env config loading in sector allocator, crisis alpha,
market situation, conviction mechanics, book registry sleeve consolidation,
allocator risk controls, and the Iris dynamic system prompt.

Each config loader follows the same pattern:
1. Dataclass defaults are the baseline
2. YAML file overrides individual fields
3. Environment variables override the most critical parameters
"""

from __future__ import annotations

import os
import textwrap
from unittest.mock import MagicMock, patch

import pytest
import yaml


# ---------------------------------------------------------------------------
# Issue 1: SectorAllocatorConfig
# ---------------------------------------------------------------------------


class TestSectorAllocatorConfigDefaults:
    """Regression tests: default values must match the original hardcoded values."""

    def test_default_sector_kill_threshold(self):
        from prometheus.sector.allocator import SectorAllocatorConfig

        cfg = SectorAllocatorConfig()
        assert cfg.sector_kill_threshold == 0.25

    def test_default_sector_reduce_threshold(self):
        from prometheus.sector.allocator import SectorAllocatorConfig

        cfg = SectorAllocatorConfig()
        assert cfg.sector_reduce_threshold == 0.40

    def test_default_sector_max_concentration(self):
        from prometheus.sector.allocator import SectorAllocatorConfig

        cfg = SectorAllocatorConfig()
        assert cfg.sector_max_concentration == 0.30

    def test_default_stress_counts(self):
        from prometheus.sector.allocator import SectorAllocatorConfig

        cfg = SectorAllocatorConfig()
        assert cfg.sector_stress_count == 1
        assert cfg.broad_stress_count == 3
        assert cfg.systemic_crisis_count == 6

    def test_default_mhi_thresholds(self):
        from prometheus.sector.allocator import SectorAllocatorConfig

        cfg = SectorAllocatorConfig()
        assert cfg.mhi_broad_stress_threshold == -0.20
        assert cfg.mhi_systemic_crisis_threshold == -0.50

    def test_default_equity_multipliers(self):
        from prometheus.sector.allocator import SectorAllocatorConfig

        cfg = SectorAllocatorConfig()
        assert cfg.equity_multiplier_normal == 1.0
        assert cfg.equity_multiplier_sector_stress == 0.85
        assert cfg.equity_multiplier_broad_stress == 0.50
        assert cfg.equity_multiplier_systemic_crisis == 0.0

    def test_default_hedge_allocations(self):
        from prometheus.sector.allocator import SectorAllocatorConfig

        cfg = SectorAllocatorConfig()
        assert cfg.hedge_allocation_normal == 0.0
        assert cfg.hedge_allocation_sector_stress == 0.10
        assert cfg.hedge_allocation_broad_stress == 0.40
        assert cfg.hedge_allocation_systemic_crisis == 1.0

    def test_default_redistribution(self):
        from prometheus.sector.allocator import SectorAllocatorConfig

        cfg = SectorAllocatorConfig()
        assert cfg.redistribute_killed_weight is True
        assert cfg.healthy_sector_threshold == 0.55


class TestSectorAllocatorConfigYAML:
    """Test YAML loading for the sector allocator config."""

    def test_load_from_yaml_overrides_fields(self, tmp_path):
        from prometheus.sector.allocator import load_allocator_config

        yaml_file = tmp_path / "allocator.yaml"
        yaml_file.write_text(
            yaml.dump({
                "sector_kill_threshold": 0.15,
                "sector_reduce_threshold": 0.35,
                "systemic_crisis_count": 8,
            })
        )

        cfg = load_allocator_config(path=yaml_file)
        assert cfg.sector_kill_threshold == 0.15
        assert cfg.sector_reduce_threshold == 0.35
        assert cfg.systemic_crisis_count == 8
        # Non-overridden fields keep defaults.
        assert cfg.sector_max_concentration == 0.30
        assert cfg.equity_multiplier_normal == 1.0

    def test_load_from_missing_yaml_returns_defaults(self, tmp_path):
        from prometheus.sector.allocator import load_allocator_config

        cfg = load_allocator_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.sector_kill_threshold == 0.25
        assert cfg.sector_reduce_threshold == 0.40

    def test_load_from_empty_yaml_returns_defaults(self, tmp_path):
        from prometheus.sector.allocator import load_allocator_config

        yaml_file = tmp_path / "empty.yaml"
        yaml_file.write_text("")

        cfg = load_allocator_config(path=yaml_file)
        assert cfg.sector_kill_threshold == 0.25

    def test_load_from_malformed_yaml_returns_defaults(self, tmp_path):
        from prometheus.sector.allocator import load_allocator_config

        yaml_file = tmp_path / "bad.yaml"
        yaml_file.write_text("this is not valid: [yaml: {")

        # Should not raise, just fall back to defaults.
        cfg = load_allocator_config(path=yaml_file)
        assert cfg.sector_kill_threshold == 0.25

    def test_unknown_yaml_keys_are_ignored(self, tmp_path):
        from prometheus.sector.allocator import load_allocator_config

        yaml_file = tmp_path / "allocator.yaml"
        yaml_file.write_text(
            yaml.dump({
                "sector_kill_threshold": 0.20,
                "unknown_future_field": 42,
            })
        )

        cfg = load_allocator_config(path=yaml_file)
        assert cfg.sector_kill_threshold == 0.20
        assert not hasattr(cfg, "unknown_future_field")

    def test_yaml_none_values_are_ignored(self, tmp_path):
        from prometheus.sector.allocator import load_allocator_config

        yaml_file = tmp_path / "allocator.yaml"
        yaml_file.write_text("sector_kill_threshold: null\n")

        cfg = load_allocator_config(path=yaml_file)
        assert cfg.sector_kill_threshold == 0.25  # default, not None


class TestSectorAllocatorConfigEnv:
    """Test environment variable overrides for the sector allocator config."""

    def test_env_override_sector_kill_threshold(self, monkeypatch, tmp_path):
        from prometheus.sector.allocator import load_allocator_config

        monkeypatch.setenv("PROMETHEUS_SECTOR_KILL_THRESHOLD", "0.18")

        cfg = load_allocator_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.sector_kill_threshold == 0.18

    def test_env_overrides_yaml(self, monkeypatch, tmp_path):
        """Environment variables take precedence over YAML values."""
        from prometheus.sector.allocator import load_allocator_config

        yaml_file = tmp_path / "allocator.yaml"
        yaml_file.write_text(yaml.dump({"sector_kill_threshold": 0.15}))

        monkeypatch.setenv("PROMETHEUS_SECTOR_KILL_THRESHOLD", "0.10")

        cfg = load_allocator_config(path=yaml_file)
        # Env wins over YAML.
        assert cfg.sector_kill_threshold == 0.10

    def test_env_override_mhi_systemic_crisis(self, monkeypatch, tmp_path):
        from prometheus.sector.allocator import load_allocator_config

        monkeypatch.setenv("PROMETHEUS_MHI_SYSTEMIC_CRISIS_THRESHOLD", "-0.60")

        cfg = load_allocator_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.mhi_systemic_crisis_threshold == -0.60

    def test_env_override_equity_multiplier_systemic_crisis(self, monkeypatch, tmp_path):
        from prometheus.sector.allocator import load_allocator_config

        monkeypatch.setenv("PROMETHEUS_EQUITY_MULT_SYSTEMIC_CRISIS", "0.05")

        cfg = load_allocator_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.equity_multiplier_systemic_crisis == 0.05

    def test_invalid_env_value_is_ignored(self, monkeypatch, tmp_path):
        from prometheus.sector.allocator import load_allocator_config

        monkeypatch.setenv("PROMETHEUS_SECTOR_KILL_THRESHOLD", "not_a_number")

        cfg = load_allocator_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.sector_kill_threshold == 0.25  # falls back to default


class TestSectorAllocatorConfigCanonicalYAML:
    """Test that the shipped YAML file at configs/sector/allocator.yaml
    produces a config identical to the dataclass defaults."""

    def test_canonical_yaml_matches_defaults(self):
        from prometheus.sector.allocator import (
            DEFAULT_ALLOCATOR_CONFIG_PATH,
            SectorAllocatorConfig,
            load_allocator_config,
        )

        assert DEFAULT_ALLOCATOR_CONFIG_PATH.exists(), (
            f"Canonical YAML not found at {DEFAULT_ALLOCATOR_CONFIG_PATH}"
        )
        from_yaml = load_allocator_config(path=DEFAULT_ALLOCATOR_CONFIG_PATH)
        from_defaults = SectorAllocatorConfig()
        assert from_yaml == from_defaults


# ---------------------------------------------------------------------------
# Issue 2: CrisisAlphaConfig
# ---------------------------------------------------------------------------


class TestCrisisAlphaConfigDefaults:
    """Regression tests: default values must match the original hardcoded values."""

    def test_default_shi_threshold(self):
        from prometheus.sector.crisis_alpha import CrisisAlphaConfig

        cfg = CrisisAlphaConfig()
        assert cfg.shi_threshold == 0.25

    def test_default_sustained_params(self):
        from prometheus.sector.crisis_alpha import CrisisAlphaConfig

        cfg = CrisisAlphaConfig()
        assert cfg.sustained_engage_count == 5
        assert cfg.sustained_days == 3
        assert cfg.sustained_nav_pct == 0.07

    def test_default_flash_params(self):
        from prometheus.sector.crisis_alpha import CrisisAlphaConfig

        cfg = CrisisAlphaConfig()
        assert cfg.flash_sector_count == 5
        assert cfg.flash_drop_threshold == 0.10
        assert cfg.flash_min_sick == 3
        assert cfg.flash_nav_pct == 0.10

    def test_default_put_params(self):
        from prometheus.sector.crisis_alpha import CrisisAlphaConfig

        cfg = CrisisAlphaConfig()
        assert cfg.target_dte_min == 45
        assert cfg.target_dte_max == 60
        assert cfg.otm_pct == 0.05
        assert cfg.profit_target_multiple == 2.5
        assert cfg.min_hold_days == 10

    def test_default_exit_and_risk(self):
        from prometheus.sector.crisis_alpha import CrisisAlphaConfig

        cfg = CrisisAlphaConfig()
        assert cfg.exit_sick_count == 2
        assert cfg.cooldown_days == 30
        assert cfg.underlying == "SPY"


class TestCrisisAlphaConfigYAML:
    """Test YAML loading for the crisis alpha config."""

    def test_load_from_yaml_overrides_fields(self, tmp_path):
        from prometheus.sector.crisis_alpha import load_crisis_alpha_config

        yaml_file = tmp_path / "crisis_alpha.yaml"
        yaml_file.write_text(
            yaml.dump({
                "shi_threshold": 0.30,
                "sustained_nav_pct": 0.10,
                "cooldown_days": 45,
            })
        )

        cfg = load_crisis_alpha_config(path=yaml_file)
        assert cfg.shi_threshold == 0.30
        assert cfg.sustained_nav_pct == 0.10
        assert cfg.cooldown_days == 45
        # Non-overridden keep defaults.
        assert cfg.flash_nav_pct == 0.10
        assert cfg.underlying == "SPY"

    def test_load_from_missing_yaml_returns_defaults(self, tmp_path):
        from prometheus.sector.crisis_alpha import load_crisis_alpha_config

        cfg = load_crisis_alpha_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.shi_threshold == 0.25

    def test_unknown_yaml_keys_are_ignored(self, tmp_path):
        from prometheus.sector.crisis_alpha import load_crisis_alpha_config

        yaml_file = tmp_path / "crisis_alpha.yaml"
        yaml_file.write_text(yaml.dump({"shi_threshold": 0.20, "bogus_key": 99}))

        cfg = load_crisis_alpha_config(path=yaml_file)
        assert cfg.shi_threshold == 0.20


class TestCrisisAlphaConfigEnv:
    """Test environment variable overrides for crisis alpha config."""

    def test_env_override_shi_threshold(self, monkeypatch, tmp_path):
        from prometheus.sector.crisis_alpha import load_crisis_alpha_config

        monkeypatch.setenv("PROMETHEUS_CRISIS_SHI_THRESHOLD", "0.30")

        cfg = load_crisis_alpha_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.shi_threshold == 0.30

    def test_env_override_cooldown_days(self, monkeypatch, tmp_path):
        from prometheus.sector.crisis_alpha import load_crisis_alpha_config

        monkeypatch.setenv("PROMETHEUS_CRISIS_COOLDOWN_DAYS", "60")

        cfg = load_crisis_alpha_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.cooldown_days == 60

    def test_env_overrides_yaml(self, monkeypatch, tmp_path):
        from prometheus.sector.crisis_alpha import load_crisis_alpha_config

        yaml_file = tmp_path / "crisis_alpha.yaml"
        yaml_file.write_text(yaml.dump({"sustained_nav_pct": 0.05}))
        monkeypatch.setenv("PROMETHEUS_CRISIS_SUSTAINED_NAV_PCT", "0.12")

        cfg = load_crisis_alpha_config(path=yaml_file)
        assert cfg.sustained_nav_pct == 0.12  # env wins

    def test_invalid_env_value_is_ignored(self, monkeypatch, tmp_path):
        from prometheus.sector.crisis_alpha import load_crisis_alpha_config

        monkeypatch.setenv("PROMETHEUS_CRISIS_COOLDOWN_DAYS", "not_int")

        cfg = load_crisis_alpha_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.cooldown_days == 30  # default


class TestCrisisAlphaConfigCanonicalYAML:
    """Test that the shipped YAML matches defaults."""

    def test_canonical_yaml_matches_defaults(self):
        from prometheus.sector.crisis_alpha import (
            CrisisAlphaConfig,
            DEFAULT_CRISIS_ALPHA_CONFIG_PATH,
            load_crisis_alpha_config,
        )

        assert DEFAULT_CRISIS_ALPHA_CONFIG_PATH.exists()
        from_yaml = load_crisis_alpha_config(path=DEFAULT_CRISIS_ALPHA_CONFIG_PATH)
        from_defaults = CrisisAlphaConfig()
        assert from_yaml == from_defaults


# ---------------------------------------------------------------------------
# Issue 3: Iris system prompt dynamic assembly
# ---------------------------------------------------------------------------


class TestIrisSystemPrompt:
    """Test that the Iris system prompt is assembled at call time."""

    def test_build_system_prompt_returns_string(self):
        from prometheus.monitoring.iris_service import build_system_prompt

        # Mock get_db_manager so we don't need a real DB.
        with patch("prometheus.monitoring.iris_service.get_db_manager") as mock_db:
            mock_db.side_effect = Exception("no DB")
            prompt = build_system_prompt()

        assert isinstance(prompt, str)
        assert "Iris" in prompt
        assert "meta-orchestrator" in prompt

    def test_fallback_when_no_db(self):
        """When DB is unavailable, the fallback description is used."""
        from prometheus.monitoring.iris_service import (
            _FALLBACK_ALPHA_DESCRIPTION,
            build_system_prompt,
        )

        with patch("prometheus.monitoring.iris_service.get_db_manager") as mock_db:
            mock_db.side_effect = Exception("connection refused")
            prompt = build_system_prompt()

        assert _FALLBACK_ALPHA_DESCRIPTION in prompt

    def test_prompt_does_not_contain_hardcoded_performance_claims(self):
        """The old hardcoded '12-17% CAGR, 0.9 Sharpe' must NOT appear."""
        from prometheus.monitoring.iris_service import build_system_prompt

        with patch("prometheus.monitoring.iris_service.get_db_manager") as mock_db:
            mock_db.side_effect = Exception("no DB")
            prompt = build_system_prompt()

        assert "12-17% CAGR" not in prompt
        assert "0.9 Sharpe" not in prompt

    def test_prompt_uses_live_data_when_available(self):
        """When DB returns scorecard data, the prompt includes it."""
        from prometheus.monitoring.iris_service import build_system_prompt

        mock_db = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (
            "PORTFOLIO",  # engine_name
            25,           # n_names
            100,          # n_decisions
            0.0045,       # avg_return
            0.620,        # hit_rate
            "2026-04-10", # latest_date
        )
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_db.get_runtime_connection.return_value = mock_conn

        with patch("prometheus.monitoring.iris_service.get_db_manager", return_value=mock_db):
            prompt = build_system_prompt()

        assert "25 names" in prompt
        assert "62%" in prompt  # hit rate
        assert "2026-04-10" in prompt

    def test_build_system_prompt_is_not_the_static_constant(self):
        """Prove the prompt is assembled per-call, not a static import."""
        from prometheus.monitoring.iris_service import (
            SYSTEM_PROMPT,
            build_system_prompt,
        )

        # With live data, built prompt differs from the static fallback.
        mock_db = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (
            "PORTFOLIO", 20, 50, 0.003, 0.550, "2026-04-01",
        )
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_db.get_runtime_connection.return_value = mock_conn

        with patch("prometheus.monitoring.iris_service.get_db_manager", return_value=mock_db):
            dynamic = build_system_prompt()

        # The dynamic prompt should contain live data not in the static one.
        assert "20 names" in dynamic
        assert "20 names" not in SYSTEM_PROMPT

    def test_template_has_no_hardcoded_claims(self):
        """The template itself should not contain specific CAGR/Sharpe numbers."""
        from prometheus.monitoring.iris_service import _SYSTEM_PROMPT_TEMPLATE

        assert "12-17%" not in _SYSTEM_PROMPT_TEMPLATE
        assert "0.9 Sharpe" not in _SYSTEM_PROMPT_TEMPLATE


# ---------------------------------------------------------------------------
# Issue 4: MarketSituationConfig
# ---------------------------------------------------------------------------


class TestMarketSituationConfigDefaults:
    """Regression tests: default values must match the original hardcoded values."""

    def test_default_recovery_fragility_threshold(self):
        from prometheus.meta.market_situation import MarketSituationConfig

        cfg = MarketSituationConfig()
        assert cfg.recovery_fragility_threshold == 0.30

    def test_default_crisis_fragility_override_threshold(self):
        from prometheus.meta.market_situation import MarketSituationConfig

        cfg = MarketSituationConfig()
        assert cfg.crisis_fragility_override_threshold == 0.75

    def test_default_recovery_requires_stress_transition(self):
        from prometheus.meta.market_situation import MarketSituationConfig

        cfg = MarketSituationConfig()
        assert cfg.recovery_requires_stress_transition is False


class TestMarketSituationConfigYAML:
    """Test YAML loading for market situation config."""

    def test_load_from_yaml_overrides_fields(self, tmp_path):
        from prometheus.meta.market_situation import load_market_situation_config

        yaml_file = tmp_path / "market_situation.yaml"
        yaml_file.write_text(
            yaml.dump({
                "recovery_fragility_threshold": 0.40,
                "crisis_fragility_override_threshold": 0.80,
            })
        )

        cfg = load_market_situation_config(path=yaml_file)
        assert cfg.recovery_fragility_threshold == 0.40
        assert cfg.crisis_fragility_override_threshold == 0.80
        # Non-overridden keeps default.
        assert cfg.recovery_requires_stress_transition is False

    def test_load_from_missing_yaml_returns_defaults(self, tmp_path):
        from prometheus.meta.market_situation import load_market_situation_config

        cfg = load_market_situation_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.recovery_fragility_threshold == 0.30

    def test_load_recovery_requires_stress_transition(self, tmp_path):
        from prometheus.meta.market_situation import load_market_situation_config

        yaml_file = tmp_path / "market_situation.yaml"
        yaml_file.write_text(yaml.dump({"recovery_requires_stress_transition": True}))

        cfg = load_market_situation_config(path=yaml_file)
        assert cfg.recovery_requires_stress_transition is True


class TestMarketSituationConfigEnv:
    """Test environment variable overrides for market situation config."""

    def test_env_override_recovery_threshold(self, monkeypatch, tmp_path):
        from prometheus.meta.market_situation import load_market_situation_config

        monkeypatch.setenv("PROMETHEUS_RECOVERY_FRAGILITY_THRESHOLD", "0.35")

        cfg = load_market_situation_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.recovery_fragility_threshold == 0.35

    def test_env_override_crisis_override_threshold(self, monkeypatch, tmp_path):
        from prometheus.meta.market_situation import load_market_situation_config

        monkeypatch.setenv("PROMETHEUS_CRISIS_FRAGILITY_OVERRIDE_THRESHOLD", "0.85")

        cfg = load_market_situation_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.crisis_fragility_override_threshold == 0.85

    def test_env_overrides_yaml(self, monkeypatch, tmp_path):
        from prometheus.meta.market_situation import load_market_situation_config

        yaml_file = tmp_path / "market_situation.yaml"
        yaml_file.write_text(yaml.dump({"recovery_fragility_threshold": 0.40}))
        monkeypatch.setenv("PROMETHEUS_RECOVERY_FRAGILITY_THRESHOLD", "0.50")

        cfg = load_market_situation_config(path=yaml_file)
        assert cfg.recovery_fragility_threshold == 0.50  # env wins

    def test_invalid_env_value_is_ignored(self, monkeypatch, tmp_path):
        from prometheus.meta.market_situation import load_market_situation_config

        monkeypatch.setenv("PROMETHEUS_RECOVERY_FRAGILITY_THRESHOLD", "abc")

        cfg = load_market_situation_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.recovery_fragility_threshold == 0.30  # default


class TestMarketSituationConfigCanonicalYAML:
    """Test that the shipped YAML matches defaults."""

    def test_canonical_yaml_matches_defaults(self):
        from prometheus.meta.market_situation import (
            DEFAULT_MARKET_SITUATION_CONFIG_PATH,
            MarketSituationConfig,
            load_market_situation_config,
        )

        assert DEFAULT_MARKET_SITUATION_CONFIG_PATH.exists()
        from_yaml = load_market_situation_config(path=DEFAULT_MARKET_SITUATION_CONFIG_PATH)
        from_defaults = MarketSituationConfig()
        assert from_yaml == from_defaults


# ---------------------------------------------------------------------------
# Issue 5: ConvictionDefaults config
# ---------------------------------------------------------------------------


class TestConvictionConfigDefaults:
    """Regression tests: default values must match the original hardcoded values."""

    def test_default_entry_credit(self):
        from prometheus.portfolio.config import ConvictionDefaults

        cfg = ConvictionDefaults()
        assert cfg.entry_credit == 5.0

    def test_default_build_rate(self):
        from prometheus.portfolio.config import ConvictionDefaults

        cfg = ConvictionDefaults()
        assert cfg.build_rate == 1.0

    def test_default_decay_rate(self):
        from prometheus.portfolio.config import ConvictionDefaults

        cfg = ConvictionDefaults()
        assert cfg.decay_rate == 2.0

    def test_default_score_cap(self):
        from prometheus.portfolio.config import ConvictionDefaults

        cfg = ConvictionDefaults()
        assert cfg.score_cap == 20.0

    def test_default_sell_threshold(self):
        from prometheus.portfolio.config import ConvictionDefaults

        cfg = ConvictionDefaults()
        assert cfg.sell_threshold == 0.0

    def test_default_hard_stop_pct(self):
        from prometheus.portfolio.config import ConvictionDefaults

        cfg = ConvictionDefaults()
        assert cfg.hard_stop_pct == 0.20

    def test_default_scale_up_days(self):
        from prometheus.portfolio.config import ConvictionDefaults

        cfg = ConvictionDefaults()
        assert cfg.scale_up_days == 3

    def test_default_entry_weight_fraction(self):
        from prometheus.portfolio.config import ConvictionDefaults

        cfg = ConvictionDefaults()
        assert cfg.entry_weight_fraction == 0.50


class TestConvictionConfigYAML:
    """Test YAML loading for conviction config."""

    def test_load_from_yaml_overrides_fields(self, tmp_path):
        from prometheus.portfolio.config import load_conviction_config

        yaml_file = tmp_path / "conviction.yaml"
        yaml_file.write_text(
            yaml.dump({
                "entry_credit": 10.0,
                "decay_rate": 3.0,
                "hard_stop_pct": 0.15,
            })
        )

        cfg = load_conviction_config(path=yaml_file)
        assert cfg.entry_credit == 10.0
        assert cfg.decay_rate == 3.0
        assert cfg.hard_stop_pct == 0.15
        # Non-overridden keep defaults.
        assert cfg.build_rate == 1.0
        assert cfg.score_cap == 20.0
        assert cfg.scale_up_days == 3

    def test_load_from_missing_yaml_returns_defaults(self, tmp_path):
        from prometheus.portfolio.config import load_conviction_config

        cfg = load_conviction_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.entry_credit == 5.0
        assert cfg.decay_rate == 2.0

    def test_load_from_empty_yaml_returns_defaults(self, tmp_path):
        from prometheus.portfolio.config import load_conviction_config

        yaml_file = tmp_path / "empty.yaml"
        yaml_file.write_text("")

        cfg = load_conviction_config(path=yaml_file)
        assert cfg.entry_credit == 5.0

    def test_load_from_malformed_yaml_returns_defaults(self, tmp_path):
        from prometheus.portfolio.config import load_conviction_config

        yaml_file = tmp_path / "bad.yaml"
        yaml_file.write_text("this is not valid: [yaml: {")

        cfg = load_conviction_config(path=yaml_file)
        assert cfg.entry_credit == 5.0

    def test_unknown_yaml_keys_are_ignored(self, tmp_path):
        from prometheus.portfolio.config import load_conviction_config

        yaml_file = tmp_path / "conviction.yaml"
        yaml_file.write_text(
            yaml.dump({
                "entry_credit": 8.0,
                "unknown_future_field": 42,
            })
        )

        cfg = load_conviction_config(path=yaml_file)
        assert cfg.entry_credit == 8.0
        assert not hasattr(cfg, "unknown_future_field")

    def test_yaml_none_values_are_ignored(self, tmp_path):
        from prometheus.portfolio.config import load_conviction_config

        yaml_file = tmp_path / "conviction.yaml"
        yaml_file.write_text("entry_credit: null\n")

        cfg = load_conviction_config(path=yaml_file)
        assert cfg.entry_credit == 5.0  # default, not None


class TestConvictionConfigEnv:
    """Test environment variable overrides for conviction config."""

    def test_env_override_decay_rate(self, monkeypatch, tmp_path):
        from prometheus.portfolio.config import load_conviction_config

        monkeypatch.setenv("PROMETHEUS_CONVICTION_DECAY_RATE", "4.0")

        cfg = load_conviction_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.decay_rate == 4.0

    def test_env_override_hard_stop_pct(self, monkeypatch, tmp_path):
        from prometheus.portfolio.config import load_conviction_config

        monkeypatch.setenv("PROMETHEUS_CONVICTION_HARD_STOP_PCT", "0.15")

        cfg = load_conviction_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.hard_stop_pct == 0.15

    def test_env_override_scale_up_days(self, monkeypatch, tmp_path):
        from prometheus.portfolio.config import load_conviction_config

        monkeypatch.setenv("PROMETHEUS_CONVICTION_SCALE_UP_DAYS", "5")

        cfg = load_conviction_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.scale_up_days == 5

    def test_env_overrides_yaml(self, monkeypatch, tmp_path):
        """Environment variables take precedence over YAML values."""
        from prometheus.portfolio.config import load_conviction_config

        yaml_file = tmp_path / "conviction.yaml"
        yaml_file.write_text(yaml.dump({"decay_rate": 3.0}))

        monkeypatch.setenv("PROMETHEUS_CONVICTION_DECAY_RATE", "5.0")

        cfg = load_conviction_config(path=yaml_file)
        # Env wins over YAML.
        assert cfg.decay_rate == 5.0

    def test_invalid_env_value_is_ignored(self, monkeypatch, tmp_path):
        from prometheus.portfolio.config import load_conviction_config

        monkeypatch.setenv("PROMETHEUS_CONVICTION_DECAY_RATE", "not_a_number")

        cfg = load_conviction_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.decay_rate == 2.0  # falls back to default

    def test_all_env_overrides_work(self, monkeypatch, tmp_path):
        """Every env override in the mapping should apply correctly."""
        from prometheus.portfolio.config import load_conviction_config

        monkeypatch.setenv("PROMETHEUS_CONVICTION_ENTRY_CREDIT", "7.0")
        monkeypatch.setenv("PROMETHEUS_CONVICTION_BUILD_RATE", "2.0")
        monkeypatch.setenv("PROMETHEUS_CONVICTION_DECAY_RATE", "3.5")
        monkeypatch.setenv("PROMETHEUS_CONVICTION_SCORE_CAP", "25.0")
        monkeypatch.setenv("PROMETHEUS_CONVICTION_SELL_THRESHOLD", "1.0")
        monkeypatch.setenv("PROMETHEUS_CONVICTION_HARD_STOP_PCT", "0.10")
        monkeypatch.setenv("PROMETHEUS_CONVICTION_SCALE_UP_DAYS", "5")
        monkeypatch.setenv("PROMETHEUS_CONVICTION_ENTRY_WEIGHT_FRACTION", "0.30")

        cfg = load_conviction_config(path=tmp_path / "nonexistent.yaml")
        assert cfg.entry_credit == 7.0
        assert cfg.build_rate == 2.0
        assert cfg.decay_rate == 3.5
        assert cfg.score_cap == 25.0
        assert cfg.sell_threshold == 1.0
        assert cfg.hard_stop_pct == 0.10
        assert cfg.scale_up_days == 5
        assert cfg.entry_weight_fraction == 0.30


class TestConvictionConfigCanonicalYAML:
    """Test that the shipped YAML matches defaults."""

    def test_canonical_yaml_matches_defaults(self):
        from prometheus.portfolio.config import (
            ConvictionDefaults,
            DEFAULT_CONVICTION_CONFIG_PATH,
            load_conviction_config,
        )

        assert DEFAULT_CONVICTION_CONFIG_PATH.exists(), (
            f"Canonical YAML not found at {DEFAULT_CONVICTION_CONFIG_PATH}"
        )
        from_yaml = load_conviction_config(path=DEFAULT_CONVICTION_CONFIG_PATH)
        from_defaults = ConvictionDefaults()
        assert from_yaml == from_defaults


# ---------------------------------------------------------------------------
# Issue 6: Book registry - sleeve conviction params from YAML
# ---------------------------------------------------------------------------


class TestBookRegistryConvictionParams:
    """Test that conviction params are parsed from YAML into LongEquitySleeveSpec."""

    def test_conviction_params_parsed_from_yaml(self, tmp_path):
        from prometheus.books.registry import load_book_registry

        yaml_file = tmp_path / "books.yaml"
        yaml_file.write_text(yaml.dump({
            "books": {
                "TEST_BOOK": {
                    "kind": "LONG_EQUITY",
                    "region": "US",
                    "market_id": "US_EQ",
                    "default_sleeve_id": "TEST_SLEEVE",
                    "sleeves": {
                        "TEST_SLEEVE": {
                            "portfolio_max_names": 20,
                            "conviction_enabled": True,
                            "conviction_entry_credit": 8.0,
                            "conviction_build_rate": 1.5,
                            "conviction_decay_rate": 3.0,
                            "conviction_score_cap": 25.0,
                            "conviction_sell_threshold": 1.0,
                            "conviction_hard_stop_pct": 0.15,
                            "conviction_scale_up_days": 5,
                            "conviction_entry_weight_fraction": 0.40,
                        },
                    },
                },
            },
        }))

        registry = load_book_registry(path=yaml_file)
        book = registry["TEST_BOOK"]
        sleeve = book.sleeves["TEST_SLEEVE"]

        assert sleeve.conviction_enabled is True
        assert sleeve.conviction_entry_credit == 8.0
        assert sleeve.conviction_build_rate == 1.5
        assert sleeve.conviction_decay_rate == 3.0
        assert sleeve.conviction_score_cap == 25.0
        assert sleeve.conviction_sell_threshold == 1.0
        assert sleeve.conviction_hard_stop_pct == 0.15
        assert sleeve.conviction_scale_up_days == 5
        assert sleeve.conviction_entry_weight_fraction == 0.40

    def test_conviction_params_default_to_none_when_absent(self, tmp_path):
        from prometheus.books.registry import load_book_registry

        yaml_file = tmp_path / "books.yaml"
        yaml_file.write_text(yaml.dump({
            "books": {
                "TEST_BOOK": {
                    "kind": "LONG_EQUITY",
                    "region": "US",
                    "market_id": "US_EQ",
                    "default_sleeve_id": "TEST_SLEEVE",
                    "sleeves": {
                        "TEST_SLEEVE": {
                            "portfolio_max_names": 10,
                        },
                    },
                },
            },
        }))

        registry = load_book_registry(path=yaml_file)
        sleeve = registry["TEST_BOOK"].sleeves["TEST_SLEEVE"]

        assert sleeve.conviction_enabled is False
        assert sleeve.conviction_entry_credit is None
        assert sleeve.conviction_decay_rate is None
        assert sleeve.conviction_hard_stop_pct is None
        assert sleeve.conviction_scale_up_days is None

    def test_shipped_yaml_v12_has_conviction_params(self):
        """The shipped books.yaml V12 sleeve has conviction params that the registry parses."""
        from prometheus.books.registry import DEFAULT_REGISTRY_PATH, load_book_registry

        assert DEFAULT_REGISTRY_PATH.exists()
        registry = load_book_registry(path=DEFAULT_REGISTRY_PATH)

        v12 = registry.get("US_EQ_LONG_V12")
        assert v12 is not None, "US_EQ_LONG_V12 not found in registry"

        sleeve = v12.sleeves.get("US_EQ_LONG_V12_K20")
        assert sleeve is not None, "US_EQ_LONG_V12_K20 sleeve not found"

        assert sleeve.conviction_enabled is True
        assert sleeve.conviction_entry_credit == 5.0
        assert sleeve.conviction_build_rate == 1.0
        assert sleeve.conviction_decay_rate == 2.0
        assert sleeve.conviction_score_cap == 20.0
        assert sleeve.conviction_sell_threshold == 0.0
        assert sleeve.conviction_hard_stop_pct == 0.20
        assert sleeve.conviction_entry_weight_fraction == 0.50


# ---------------------------------------------------------------------------
# Issue 7: Allocator risk controls env var overrides
# ---------------------------------------------------------------------------


class TestAllocatorRiskControlDefaults:
    """Regression: shipped YAML allocator risk controls match expected values."""

    def test_shipped_yaml_allocator_risk_controls(self):
        from prometheus.books.registry import DEFAULT_REGISTRY_PATH, load_book_registry

        assert DEFAULT_REGISTRY_PATH.exists()
        registry = load_book_registry(path=DEFAULT_REGISTRY_PATH)

        alloc = registry.get("US_EQ_ALLOCATOR")
        assert alloc is not None, "US_EQ_ALLOCATOR not found in registry"

        assert alloc.max_turnover_one_way == 0.25
        assert alloc.crisis_force_hedge_allocation == 1.0
        assert alloc.drawdown_brake_threshold == -0.06
        assert alloc.vol_target_annual == 0.22


class TestAllocatorRiskControlYAML:
    """Test that allocator risk controls are read from YAML."""

    def test_risk_controls_from_yaml(self, tmp_path):
        from prometheus.books.registry import load_book_registry

        yaml_file = tmp_path / "books.yaml"
        yaml_file.write_text(yaml.dump({
            "books": {
                "TEST_ALLOC": {
                    "kind": "ALLOCATOR",
                    "region": "US",
                    "market_id": "US_EQ",
                    "max_turnover_one_way": 0.30,
                    "drawdown_brake_threshold": -0.08,
                    "vol_target_annual": 0.18,
                    "crisis_force_hedge_allocation": 0.9,
                    "sleeves": {
                        "TEST_SLEEVE": {
                            "hedge_instrument_ids": ["SH.US"],
                            "portfolio_max_names": 20,
                        },
                    },
                },
            },
        }))

        registry = load_book_registry(path=yaml_file)
        alloc = registry["TEST_ALLOC"]

        assert alloc.max_turnover_one_way == 0.30
        assert alloc.drawdown_brake_threshold == -0.08
        assert alloc.vol_target_annual == 0.18
        assert alloc.crisis_force_hedge_allocation == 0.9


class TestAllocatorRiskControlEnv:
    """Test environment variable overrides for allocator risk controls."""

    def test_env_override_max_turnover(self, monkeypatch, tmp_path):
        from prometheus.books.registry import load_book_registry

        yaml_file = tmp_path / "books.yaml"
        yaml_file.write_text(yaml.dump({
            "books": {
                "TEST_ALLOC": {
                    "kind": "ALLOCATOR",
                    "region": "US",
                    "market_id": "US_EQ",
                    "max_turnover_one_way": 0.25,
                    "sleeves": {
                        "S": {"hedge_instrument_ids": ["SH.US"]},
                    },
                },
            },
        }))

        monkeypatch.setenv("PROMETHEUS_MAX_TURNOVER", "0.15")

        registry = load_book_registry(path=yaml_file)
        assert registry["TEST_ALLOC"].max_turnover_one_way == 0.15

    def test_env_override_drawdown_brake(self, monkeypatch, tmp_path):
        from prometheus.books.registry import load_book_registry

        yaml_file = tmp_path / "books.yaml"
        yaml_file.write_text(yaml.dump({
            "books": {
                "TEST_ALLOC": {
                    "kind": "ALLOCATOR",
                    "region": "US",
                    "market_id": "US_EQ",
                    "drawdown_brake_threshold": -0.06,
                    "sleeves": {
                        "S": {"hedge_instrument_ids": ["SH.US"]},
                    },
                },
            },
        }))

        monkeypatch.setenv("PROMETHEUS_DRAWDOWN_BRAKE_THRESHOLD", "-0.10")

        registry = load_book_registry(path=yaml_file)
        assert registry["TEST_ALLOC"].drawdown_brake_threshold == -0.10

    def test_env_override_vol_target(self, monkeypatch, tmp_path):
        from prometheus.books.registry import load_book_registry

        yaml_file = tmp_path / "books.yaml"
        yaml_file.write_text(yaml.dump({
            "books": {
                "TEST_ALLOC": {
                    "kind": "ALLOCATOR",
                    "region": "US",
                    "market_id": "US_EQ",
                    "vol_target_annual": 0.22,
                    "sleeves": {
                        "S": {"hedge_instrument_ids": ["SH.US"]},
                    },
                },
            },
        }))

        monkeypatch.setenv("PROMETHEUS_VOL_TARGET_ANNUAL", "0.18")

        registry = load_book_registry(path=yaml_file)
        assert registry["TEST_ALLOC"].vol_target_annual == 0.18

    def test_env_overrides_yaml_values(self, monkeypatch, tmp_path):
        """Environment variables take precedence over YAML values."""
        from prometheus.books.registry import load_book_registry

        yaml_file = tmp_path / "books.yaml"
        yaml_file.write_text(yaml.dump({
            "books": {
                "TEST_ALLOC": {
                    "kind": "ALLOCATOR",
                    "region": "US",
                    "market_id": "US_EQ",
                    "max_turnover_one_way": 0.25,
                    "drawdown_brake_threshold": -0.06,
                    "vol_target_annual": 0.22,
                    "sleeves": {
                        "S": {"hedge_instrument_ids": ["SH.US"]},
                    },
                },
            },
        }))

        monkeypatch.setenv("PROMETHEUS_MAX_TURNOVER", "0.10")
        monkeypatch.setenv("PROMETHEUS_DRAWDOWN_BRAKE_THRESHOLD", "-0.12")
        monkeypatch.setenv("PROMETHEUS_VOL_TARGET_ANNUAL", "0.15")

        registry = load_book_registry(path=yaml_file)
        alloc = registry["TEST_ALLOC"]
        # All env vars win.
        assert alloc.max_turnover_one_way == 0.10
        assert alloc.drawdown_brake_threshold == -0.12
        assert alloc.vol_target_annual == 0.15

    def test_env_does_not_affect_non_allocator_books(self, monkeypatch, tmp_path):
        """Env var overrides only apply to ALLOCATOR books."""
        from prometheus.books.registry import load_book_registry

        yaml_file = tmp_path / "books.yaml"
        yaml_file.write_text(yaml.dump({
            "books": {
                "LONG_BOOK": {
                    "kind": "LONG_EQUITY",
                    "region": "US",
                    "market_id": "US_EQ",
                    "sleeves": {
                        "S": {"portfolio_max_names": 10},
                    },
                },
                "ALLOC_BOOK": {
                    "kind": "ALLOCATOR",
                    "region": "US",
                    "market_id": "US_EQ",
                    "max_turnover_one_way": 0.25,
                    "sleeves": {
                        "S": {"hedge_instrument_ids": ["SH.US"]},
                    },
                },
            },
        }))

        monkeypatch.setenv("PROMETHEUS_MAX_TURNOVER", "0.10")

        registry = load_book_registry(path=yaml_file)
        # ALLOCATOR book gets the override.
        assert registry["ALLOC_BOOK"].max_turnover_one_way == 0.10
        # LONG_EQUITY book is unaffected.
        assert registry["LONG_BOOK"].max_turnover_one_way is None

    def test_invalid_env_value_is_ignored(self, monkeypatch, tmp_path):
        from prometheus.books.registry import load_book_registry

        yaml_file = tmp_path / "books.yaml"
        yaml_file.write_text(yaml.dump({
            "books": {
                "TEST_ALLOC": {
                    "kind": "ALLOCATOR",
                    "region": "US",
                    "market_id": "US_EQ",
                    "max_turnover_one_way": 0.25,
                    "sleeves": {
                        "S": {"hedge_instrument_ids": ["SH.US"]},
                    },
                },
            },
        }))

        monkeypatch.setenv("PROMETHEUS_MAX_TURNOVER", "not_a_number")

        registry = load_book_registry(path=yaml_file)
        assert registry["TEST_ALLOC"].max_turnover_one_way == 0.25  # YAML value, not overridden


# ---------------------------------------------------------------------------
# Issue 8: Assessment horizon days env var override
# ---------------------------------------------------------------------------


class TestAssessmentHorizonDays:
    """Test PROMETHEUS_ASSESSMENT_HORIZON_DAYS env var override for
    backtest/config.py SleeveConfig and universe/engine.py BasicUniverseModel.
    """

    def test_sleeve_config_default_is_21(self):
        from prometheus.backtest.config import SleeveConfig

        cfg = SleeveConfig(
            sleeve_id="T", strategy_id="T", market_id="T",
            universe_id="T", portfolio_id="T", assessment_strategy_id="T",
        )
        assert cfg.assessment_horizon_days == 21

    def test_sleeve_config_env_override(self, monkeypatch):
        from prometheus.backtest.config import SleeveConfig

        monkeypatch.setenv("PROMETHEUS_ASSESSMENT_HORIZON_DAYS", "42")

        cfg = SleeveConfig(
            sleeve_id="T", strategy_id="T", market_id="T",
            universe_id="T", portfolio_id="T", assessment_strategy_id="T",
        )
        assert cfg.assessment_horizon_days == 42

    def test_sleeve_config_invalid_env_ignored(self, monkeypatch):
        from prometheus.backtest.config import SleeveConfig

        monkeypatch.setenv("PROMETHEUS_ASSESSMENT_HORIZON_DAYS", "not_int")

        cfg = SleeveConfig(
            sleeve_id="T", strategy_id="T", market_id="T",
            universe_id="T", portfolio_id="T", assessment_strategy_id="T",
        )
        assert cfg.assessment_horizon_days == 21

    def test_universe_model_default_is_21(self):
        from prometheus.universe.engine import _default_assessment_horizon_days

        # Without env var set, should return 21.
        assert _default_assessment_horizon_days() == 21

    def test_universe_model_env_override(self, monkeypatch):
        from prometheus.universe.engine import _default_assessment_horizon_days

        monkeypatch.setenv("PROMETHEUS_ASSESSMENT_HORIZON_DAYS", "30")
        assert _default_assessment_horizon_days() == 30

    def test_universe_model_invalid_env_ignored(self, monkeypatch):
        from prometheus.universe.engine import _default_assessment_horizon_days

        monkeypatch.setenv("PROMETHEUS_ASSESSMENT_HORIZON_DAYS", "abc")
        assert _default_assessment_horizon_days() == 21

    def test_sleeve_config_explicit_value_overrides_env(self, monkeypatch):
        """When a caller sets assessment_horizon_days explicitly, env is ignored."""
        from prometheus.backtest.config import SleeveConfig

        monkeypatch.setenv("PROMETHEUS_ASSESSMENT_HORIZON_DAYS", "42")

        cfg = SleeveConfig(
            sleeve_id="T", strategy_id="T", market_id="T",
            universe_id="T", portfolio_id="T", assessment_strategy_id="T",
            assessment_horizon_days=10,
        )
        assert cfg.assessment_horizon_days == 10


# ---------------------------------------------------------------------------
# Issue 9: Risk constraints per-name cap env var override
# ---------------------------------------------------------------------------


class TestRiskConstraintsEnvOverride:
    """Test PROMETHEUS_MAX_WEIGHT_PER_NAME env var override."""

    def test_default_core_long_eq(self):
        from prometheus.risk.constraints import get_strategy_risk_config

        cfg = get_strategy_risk_config("US_EQ_CORE_LONG_EQ")
        assert cfg.max_abs_weight_per_name == 0.05

    def test_default_allocator(self):
        from prometheus.risk.constraints import get_strategy_risk_config

        cfg = get_strategy_risk_config("US_EQ_ALLOCATOR")
        assert cfg.max_abs_weight_per_name == 1.0

    def test_default_fallback_strategy(self):
        from prometheus.risk.constraints import get_strategy_risk_config

        cfg = get_strategy_risk_config("UNKNOWN_STRATEGY")
        assert cfg.max_abs_weight_per_name == 0.05  # dataclass default

    def test_env_override_applies_to_all_strategies(self, monkeypatch):
        from prometheus.risk.constraints import get_strategy_risk_config

        monkeypatch.setenv("PROMETHEUS_MAX_WEIGHT_PER_NAME", "0.03")

        cfg = get_strategy_risk_config("US_EQ_CORE_LONG_EQ")
        assert cfg.max_abs_weight_per_name == 0.03

        cfg2 = get_strategy_risk_config("US_EQ_ALLOCATOR")
        assert cfg2.max_abs_weight_per_name == 0.03

    def test_env_override_applies_to_unknown_strategy(self, monkeypatch):
        from prometheus.risk.constraints import get_strategy_risk_config

        monkeypatch.setenv("PROMETHEUS_MAX_WEIGHT_PER_NAME", "0.08")

        cfg = get_strategy_risk_config("UNKNOWN_STRATEGY")
        assert cfg.max_abs_weight_per_name == 0.08

    def test_invalid_env_ignored(self, monkeypatch):
        from prometheus.risk.constraints import get_strategy_risk_config

        monkeypatch.setenv("PROMETHEUS_MAX_WEIGHT_PER_NAME", "not_float")

        cfg = get_strategy_risk_config("US_EQ_CORE_LONG_EQ")
        assert cfg.max_abs_weight_per_name == 0.05

    def test_apply_per_name_limit_unchanged(self):
        """Functional behaviour of apply_per_name_limit is unchanged."""
        from prometheus.risk.constraints import StrategyRiskConfig, apply_per_name_limit

        cfg = StrategyRiskConfig(strategy_id="T", max_abs_weight_per_name=0.05)
        adj, reason = apply_per_name_limit(0.03, cfg)
        assert adj == 0.03
        assert reason is None

        adj, reason = apply_per_name_limit(0.10, cfg)
        assert adj == 0.05
        assert reason == "CAPPED_PER_NAME"


# ---------------------------------------------------------------------------
# Issue 10: Order execution params env var overrides
# ---------------------------------------------------------------------------


class TestOrderPlannerEnvOverride:
    """Test PROMETHEUS_MIN_REBALANCE_PCT and PROMETHEUS_LIMIT_BUFFER_PCT."""

    def test_compiled_defaults(self):
        from prometheus.execution.order_planner import (
            _COMPILED_LIMIT_BUFFER_PCT,
            _COMPILED_MIN_REBALANCE_PCT,
        )

        assert _COMPILED_MIN_REBALANCE_PCT == 0.02
        assert _COMPILED_LIMIT_BUFFER_PCT == 0.001

    def test_resolve_min_rebalance_default(self):
        from prometheus.execution.order_planner import _resolve_min_rebalance_pct

        assert _resolve_min_rebalance_pct() == 0.02

    def test_resolve_min_rebalance_env_override(self, monkeypatch):
        from prometheus.execution.order_planner import _resolve_min_rebalance_pct

        monkeypatch.setenv("PROMETHEUS_MIN_REBALANCE_PCT", "0.05")
        assert _resolve_min_rebalance_pct() == 0.05

    def test_resolve_min_rebalance_invalid_env(self, monkeypatch):
        from prometheus.execution.order_planner import _resolve_min_rebalance_pct

        monkeypatch.setenv("PROMETHEUS_MIN_REBALANCE_PCT", "bad")
        assert _resolve_min_rebalance_pct() == 0.02

    def test_resolve_limit_buffer_default(self):
        from prometheus.execution.order_planner import _resolve_limit_buffer_pct

        assert _resolve_limit_buffer_pct() == 0.001

    def test_resolve_limit_buffer_env_override(self, monkeypatch):
        from prometheus.execution.order_planner import _resolve_limit_buffer_pct

        monkeypatch.setenv("PROMETHEUS_LIMIT_BUFFER_PCT", "0.005")
        assert _resolve_limit_buffer_pct() == 0.005

    def test_resolve_limit_buffer_invalid_env(self, monkeypatch):
        from prometheus.execution.order_planner import _resolve_limit_buffer_pct

        monkeypatch.setenv("PROMETHEUS_LIMIT_BUFFER_PCT", "nope")
        assert _resolve_limit_buffer_pct() == 0.001

    def test_plan_orders_uses_env_default(self, monkeypatch):
        """plan_orders reads env var at call time when no explicit value passed."""
        from prometheus.execution.broker_interface import Position
        from prometheus.execution.order_planner import plan_orders

        monkeypatch.setenv("PROMETHEUS_MIN_REBALANCE_PCT", "0.50")

        # With 50% rebalance threshold, a 1% delta should be suppressed.
        current = {"AAPL": Position(
            instrument_id="AAPL", quantity=100.0, avg_cost=150.0,
            market_value=15000.0, unrealized_pnl=0.0,
        )}
        target = {"AAPL": 101.0}  # 1% change
        orders = plan_orders(current, target)
        assert len(orders) == 0  # suppressed by high threshold

    def test_plan_orders_explicit_value_overrides_env(self, monkeypatch):
        """Explicitly passed min_rebalance_pct overrides env var."""
        from prometheus.execution.broker_interface import Position
        from prometheus.execution.order_planner import plan_orders

        monkeypatch.setenv("PROMETHEUS_MIN_REBALANCE_PCT", "0.50")

        current = {"AAPL": Position(
            instrument_id="AAPL", quantity=100.0, avg_cost=150.0,
            market_value=15000.0, unrealized_pnl=0.0,
        )}
        target = {"AAPL": 101.0}  # 1% change
        orders = plan_orders(current, target, min_rebalance_pct=0.005)
        assert len(orders) == 1  # not suppressed because explicit threshold is low


# ---------------------------------------------------------------------------
# Issue 11: Lambda feature windows env var overrides
# ---------------------------------------------------------------------------


class TestLambdaFeatureWindowsEnvOverride:
    """Test PROMETHEUS_STAB_LOOKBACK_DAYS and PROMETHEUS_LAMBDA_LOOKBACK_DAYS."""

    def test_compiled_defaults(self):
        from prometheus.opportunity.lambda_daily import (
            _COMPILED_LAMBDA_LOOKBACK_DAYS,
            _COMPILED_STAB_LOOKBACK_DAYS,
        )

        assert _COMPILED_STAB_LOOKBACK_DAYS == 10
        assert _COMPILED_LAMBDA_LOOKBACK_DAYS == 20

    def test_resolve_stab_lookback_default(self):
        from prometheus.opportunity.lambda_daily import _resolve_stab_lookback_days

        assert _resolve_stab_lookback_days() == 10

    def test_resolve_stab_lookback_env_override(self, monkeypatch):
        from prometheus.opportunity.lambda_daily import _resolve_stab_lookback_days

        monkeypatch.setenv("PROMETHEUS_STAB_LOOKBACK_DAYS", "15")
        assert _resolve_stab_lookback_days() == 15

    def test_resolve_stab_lookback_invalid_env(self, monkeypatch):
        from prometheus.opportunity.lambda_daily import _resolve_stab_lookback_days

        monkeypatch.setenv("PROMETHEUS_STAB_LOOKBACK_DAYS", "bad")
        assert _resolve_stab_lookback_days() == 10

    def test_resolve_lambda_lookback_default(self):
        from prometheus.opportunity.lambda_daily import _resolve_lambda_lookback_days

        assert _resolve_lambda_lookback_days() == 20

    def test_resolve_lambda_lookback_env_override(self, monkeypatch):
        from prometheus.opportunity.lambda_daily import _resolve_lambda_lookback_days

        monkeypatch.setenv("PROMETHEUS_LAMBDA_LOOKBACK_DAYS", "30")
        assert _resolve_lambda_lookback_days() == 30

    def test_resolve_lambda_lookback_invalid_env(self, monkeypatch):
        from prometheus.opportunity.lambda_daily import _resolve_lambda_lookback_days

        monkeypatch.setenv("PROMETHEUS_LAMBDA_LOOKBACK_DAYS", "nope")
        assert _resolve_lambda_lookback_days() == 20


# ---------------------------------------------------------------------------
# Issue 12: Iris tools list env var override
# ---------------------------------------------------------------------------


class TestIrisToolsEnvOverride:
    """Test PROMETHEUS_IRIS_TOOLS env var override."""

    def test_default_tools_list(self):
        from prometheus.monitoring.iris_service import _IRIS_TOOLS_DEFAULT, _resolve_iris_tools

        tools = _resolve_iris_tools()
        assert tools == _IRIS_TOOLS_DEFAULT
        assert "get_current_date" in tools
        assert "search_web" in tools
        assert "query_fred_data" in tools
        assert "get_nation_indicators" in tools
        assert "search_wikipedia" in tools

    def test_env_override_replaces_tools(self, monkeypatch):
        from prometheus.monitoring.iris_service import _resolve_iris_tools

        monkeypatch.setenv("PROMETHEUS_IRIS_TOOLS", "tool_a,tool_b,tool_c")
        tools = _resolve_iris_tools()
        assert tools == ["tool_a", "tool_b", "tool_c"]

    def test_env_override_strips_whitespace(self, monkeypatch):
        from prometheus.monitoring.iris_service import _resolve_iris_tools

        monkeypatch.setenv("PROMETHEUS_IRIS_TOOLS", " tool_a , tool_b , tool_c ")
        tools = _resolve_iris_tools()
        assert tools == ["tool_a", "tool_b", "tool_c"]

    def test_env_override_ignores_empty_entries(self, monkeypatch):
        from prometheus.monitoring.iris_service import _resolve_iris_tools

        monkeypatch.setenv("PROMETHEUS_IRIS_TOOLS", "tool_a,,tool_b,")
        tools = _resolve_iris_tools()
        assert tools == ["tool_a", "tool_b"]

    def test_env_override_empty_string_falls_back_to_default(self, monkeypatch):
        from prometheus.monitoring.iris_service import _IRIS_TOOLS_DEFAULT, _resolve_iris_tools

        monkeypatch.setenv("PROMETHEUS_IRIS_TOOLS", "")
        tools = _resolve_iris_tools()
        assert tools == _IRIS_TOOLS_DEFAULT

    def test_module_level_constant_preserved(self):
        """_IRIS_TOOLS module-level constant still matches defaults for backward compat."""
        from prometheus.monitoring.iris_service import _IRIS_TOOLS, _IRIS_TOOLS_DEFAULT

        assert _IRIS_TOOLS == _IRIS_TOOLS_DEFAULT

    def test_resolve_returns_copy(self):
        """Mutating the returned list should not affect the default."""
        from prometheus.monitoring.iris_service import _IRIS_TOOLS_DEFAULT, _resolve_iris_tools

        tools = _resolve_iris_tools()
        tools.append("extra")
        assert "extra" not in _IRIS_TOOLS_DEFAULT
