"""Tests for prometheus.research.combiner.SignalCombiner.

Pure unit tests on synthetic data: weight normalization, regime-conditional
selection, IC tilt, and graceful fallback when regime/IC/component is missing.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from prometheus.research.combiner import (
    CARRY,
    CRISIS,
    NEUTRAL,
    CombinerConfig,
    SignalCombiner,
    _normalize_l1,
    trailing_rank_ic_by_date,
)

# ---------------------------------------------------------------------------
# Weight normalization
# ---------------------------------------------------------------------------


def test_normalize_l1_sums_to_one_abs():
    w = _normalize_l1({"a": 2.0, "b": -2.0})
    assert pytest.approx(sum(abs(v) for v in w.values())) == 1.0
    # sign preserved
    assert w["a"] > 0 and w["b"] < 0
    assert pytest.approx(w["a"]) == 0.5
    assert pytest.approx(w["b"]) == -0.5


def test_normalize_l1_all_zero_is_noop():
    w = _normalize_l1({"a": 0.0, "b": 0.0})
    assert w == {"a": 0.0, "b": 0.0}


def test_resolved_weights_normalized_by_default():
    c = SignalCombiner()  # normalize defaults to True
    resolved = c.resolve_weights()
    s = sum(abs(v) for v in resolved.effective.values())
    assert pytest.approx(s) == 1.0


def test_resolved_weights_unnormalized_when_disabled():
    cfg = CombinerConfig(
        weights={"momentum_z": 1.0, "base_z": 0.5}, normalize=False, regime_weights={}
    )
    c = SignalCombiner(cfg)
    resolved = c.resolve_weights()
    assert resolved.effective == {"momentum_z": 1.0, "base_z": 0.5}


# ---------------------------------------------------------------------------
# Regime-conditional selection
# ---------------------------------------------------------------------------


def test_regime_selects_correct_weight_set():
    cfg = CombinerConfig(
        weights={"momentum_z": 1.0, "base_z": 0.0},
        regime_weights={
            CRISIS: {"momentum_z": 0.0, "base_z": 1.0},
            CARRY: {"momentum_z": 1.0, "base_z": 0.0},
        },
        normalize=False,
    )
    c = SignalCombiner(cfg)

    crisis = c.resolve_weights(regime_label=CRISIS)
    assert crisis.regime == CRISIS
    assert crisis.effective == {"momentum_z": 0.0, "base_z": 1.0}

    carry = c.resolve_weights(regime_label=CARRY)
    assert carry.regime == CARRY
    assert carry.effective == {"momentum_z": 1.0, "base_z": 0.0}


def test_regime_accepts_enum_with_value_attr():
    class _Label:
        value = CRISIS

    cfg = CombinerConfig(
        weights={"momentum_z": 1.0},
        regime_weights={CRISIS: {"momentum_z": 0.3}},
        normalize=False,
    )
    c = SignalCombiner(cfg)
    resolved = c.resolve_weights(regime_label=_Label())
    assert resolved.regime == CRISIS
    assert resolved.effective == {"momentum_z": 0.3}


def test_unknown_regime_falls_back_to_default():
    cfg = CombinerConfig(
        weights={"momentum_z": 1.0, "base_z": 0.25},
        regime_weights={CRISIS: {"momentum_z": 0.0, "base_z": 1.0}},
        normalize=False,
    )
    c = SignalCombiner(cfg)
    # NEUTRAL has no override -> default weights, regime resolves to None.
    resolved = c.resolve_weights(regime_label=NEUTRAL)
    assert resolved.regime is None
    assert resolved.effective == {"momentum_z": 1.0, "base_z": 0.25}


def test_none_regime_falls_back_to_default():
    c = SignalCombiner()
    resolved = c.resolve_weights(regime_label=None)
    assert resolved.regime is None


def test_use_regime_false_ignores_regime():
    cfg = CombinerConfig(
        weights={"momentum_z": 1.0},
        regime_weights={CRISIS: {"momentum_z": 0.0}},
        use_regime=False,
        normalize=False,
    )
    c = SignalCombiner(cfg)
    resolved = c.resolve_weights(regime_label=CRISIS)
    assert resolved.regime is None
    assert resolved.effective == {"momentum_z": 1.0}


# ---------------------------------------------------------------------------
# IC tilt
# ---------------------------------------------------------------------------


def test_ic_tilt_noop_when_disabled():
    cfg = CombinerConfig(
        weights={"momentum_z": 1.0}, use_ic_tilt=False, normalize=False,
        regime_weights={},
    )
    c = SignalCombiner(cfg)
    resolved = c.resolve_weights(trailing_ic={"momentum_z": 0.3})
    assert resolved.ic_factors == {"momentum_z": 1.0}
    assert resolved.effective == {"momentum_z": 1.0}


def test_ic_tilt_noop_when_no_history():
    cfg = CombinerConfig(
        weights={"momentum_z": 1.0}, use_ic_tilt=True, normalize=False,
        regime_weights={},
    )
    c = SignalCombiner(cfg)
    resolved = c.resolve_weights(trailing_ic=None)
    assert resolved.ic_factors == {"momentum_z": 1.0}
    assert resolved.effective == {"momentum_z": 1.0}


def test_ic_tilt_flips_negative_ic():
    cfg = CombinerConfig(
        weights={"momentum_z": 1.0}, use_ic_tilt=True, ic_tilt_strength=1.0,
        normalize=False, regime_weights={},
    )
    c = SignalCombiner(cfg)
    # strongly negative trailing IC -> factor goes negative -> weight flips sign
    resolved = c.resolve_weights(trailing_ic={"momentum_z": -0.3})
    assert resolved.effective["momentum_z"] < 0


def test_ic_floor_gates_noise():
    cfg = CombinerConfig(
        weights={"momentum_z": 1.0}, use_ic_tilt=True, ic_floor=0.05,
        normalize=False, regime_weights={},
    )
    c = SignalCombiner(cfg)
    resolved = c.resolve_weights(trailing_ic={"momentum_z": 0.01})
    # below floor -> identity factor
    assert resolved.ic_factors["momentum_z"] == 1.0


# ---------------------------------------------------------------------------
# Cross-section combine + graceful component fallback
# ---------------------------------------------------------------------------


def test_combine_cross_section_basic():
    cfg = CombinerConfig(
        weights={"momentum_z": 1.0, "base_z": 0.5}, normalize=False, regime_weights={}
    )
    c = SignalCombiner(cfg)
    out = c.combine_cross_section(
        {
            "momentum_z": {"A": 1.0, "B": -1.0},
            "base_z": {"A": 2.0, "B": 0.0},
        }
    )
    assert pytest.approx(out["A"]) == 1.0 * 1.0 + 0.5 * 2.0
    assert pytest.approx(out["B"]) == 1.0 * -1.0 + 0.5 * 0.0


def test_combine_missing_component_for_name_is_neutral():
    cfg = CombinerConfig(
        weights={"momentum_z": 1.0, "base_z": 1.0}, normalize=False, regime_weights={}
    )
    c = SignalCombiner(cfg)
    out = c.combine_cross_section(
        {
            "momentum_z": {"A": 1.0, "B": 2.0},
            "base_z": {"A": 3.0},  # B missing base_z -> treated as 0
        }
    )
    assert pytest.approx(out["A"]) == 1.0 + 3.0
    assert pytest.approx(out["B"]) == 2.0  # base contributes 0


def test_combine_ignores_component_absent_from_weights():
    cfg = CombinerConfig(
        weights={"momentum_z": 1.0}, normalize=False, regime_weights={}
    )
    c = SignalCombiner(cfg)
    out = c.combine_cross_section(
        {
            "momentum_z": {"A": 1.0},
            "unused_z": {"A": 99.0},  # not in weights -> ignored
        }
    )
    assert pytest.approx(out["A"]) == 1.0


def test_combine_handles_nan_component_value():
    cfg = CombinerConfig(
        weights={"momentum_z": 1.0, "base_z": 1.0}, normalize=False, regime_weights={}
    )
    c = SignalCombiner(cfg)
    out = c.combine_cross_section(
        {"momentum_z": {"A": 2.0}, "base_z": {"A": float("nan")}}
    )
    assert pytest.approx(out["A"]) == 2.0


# ---------------------------------------------------------------------------
# Panel combine
# ---------------------------------------------------------------------------


def test_combine_panel_uses_regime_by_date():
    cfg = CombinerConfig(
        weights={"momentum_z": 1.0, "base_z": 0.0},
        regime_weights={CRISIS: {"momentum_z": 0.0, "base_z": 1.0}},
        normalize=False,
    )
    c = SignalCombiner(cfg)
    d1 = pd.Timestamp("2022-01-03")
    d2 = pd.Timestamp("2022-01-10")
    panel = pd.DataFrame(
        {
            "instrument_id": ["A", "B", "A", "B"],
            "as_of_date": [d1, d1, d2, d2],
            "momentum_z": [1.0, -1.0, 1.0, -1.0],
            "base_z": [-1.0, 1.0, -1.0, 1.0],
        }
    )
    out = c.combine_panel(
        panel, regime_by_date={d1: NEUTRAL, d2: CRISIS}
    )
    # d1 NEUTRAL/default -> follows momentum_z
    a1 = out[(out.instrument_id == "A") & (out.as_of_date == d1)]["score"].iloc[0]
    assert pytest.approx(a1) == 1.0
    # d2 CRISIS -> follows base_z
    a2 = out[(out.instrument_id == "A") & (out.as_of_date == d2)]["score"].iloc[0]
    assert pytest.approx(a2) == -1.0


def test_combine_panel_missing_regime_uses_default():
    c = SignalCombiner(
        CombinerConfig(
            weights={"momentum_z": 1.0}, regime_weights={}, normalize=False
        )
    )
    d1 = pd.Timestamp("2022-01-03")
    panel = pd.DataFrame(
        {"instrument_id": ["A"], "as_of_date": [d1], "momentum_z": [2.0]}
    )
    # no regime_by_date at all -> default weights, no crash
    out = c.combine_panel(panel)
    assert pytest.approx(out["score"].iloc[0]) == 2.0


def test_combine_panel_empty_returns_empty():
    c = SignalCombiner()
    out = c.combine_panel(pd.DataFrame(columns=["instrument_id", "as_of_date"]))
    assert out.empty


def test_combine_panel_no_matching_columns_raises():
    c = SignalCombiner(CombinerConfig(weights={"momentum_z": 1.0}, regime_weights={}))
    panel = pd.DataFrame(
        {"instrument_id": ["A"], "as_of_date": [pd.Timestamp("2022-01-03")],
         "irrelevant": [1.0]}
    )
    with pytest.raises(ValueError):
        c.combine_panel(panel)


# ---------------------------------------------------------------------------
# Trailing IC estimation (no look-ahead)
# ---------------------------------------------------------------------------


def test_trailing_rank_ic_no_lookahead_and_signs():
    # Construct a component perfectly correlated with forward return so IC ~ +1.
    rng = np.random.default_rng(0)
    dates = pd.to_datetime([f"2022-01-{d:02d}" for d in range(3, 21)])
    insts = [f"I{i}" for i in range(10)]
    rows_c = []
    rows_r = []
    for d in dates:
        vals = rng.normal(size=len(insts))
        for inst, v in zip(insts, vals):
            rows_c.append({"instrument_id": inst, "as_of_date": d, "momentum_z": v})
            # forward return == component (perfect rank corr)
            rows_r.append({"instrument_id": inst, "as_of_date": d, "fwd_ret_5d": v})
    comps = pd.DataFrame(rows_c)
    rets = pd.DataFrame(rows_r)

    ic_by_date = trailing_rank_ic_by_date(
        comps, rets, component_cols=["momentum_z"], horizon=5, window=5,
        min_cross_section=5,
    )
    # first date has no prior history -> absent
    assert dates[0] not in ic_by_date
    # later dates present with strongly positive trailing IC
    later = [d for d in dates if d in ic_by_date]
    assert later, "expected some dates with trailing IC"
    for d in later:
        assert ic_by_date[d]["momentum_z"] > 0.9


def test_universe_engine_call_shape_default_config():
    """Contract test for the universe-engine wiring (engine.py blend pass).

    The engine calls ``combine_cross_section`` with the component keys
    {"base_z", "momentum_z", "alpha_z"}. The default config weights only
    momentum_z + base_z (+ a fragility_z that is absent). This asserts the
    default combiner produces a finite blend for that exact call shape and that
    the absent fragility component degrades gracefully.
    """
    c = SignalCombiner()  # default config
    base_z = {"A": 1.0, "B": -1.0}
    alpha_z = {"A": 0.5, "B": -0.5}
    out = c.combine_cross_section(
        {"base_z": base_z, "momentum_z": alpha_z, "alpha_z": alpha_z},
        regime_label="NEUTRAL",
    )
    assert set(out) == {"A", "B"}
    assert all(np.isfinite(v) for v in out.values())
    # higher base+momentum name (A) ranks above B
    assert out["A"] > out["B"]


def test_trailing_rank_ic_missing_return_col_returns_empty():
    comps = pd.DataFrame(
        {"instrument_id": ["A"], "as_of_date": [pd.Timestamp("2022-01-03")],
         "momentum_z": [1.0]}
    )
    rets = pd.DataFrame(
        {"instrument_id": ["A"], "as_of_date": [pd.Timestamp("2022-01-03")]}
    )
    out = trailing_rank_ic_by_date(
        comps, rets, component_cols=["momentum_z"], horizon=5
    )
    assert out == {}
