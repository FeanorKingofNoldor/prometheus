"""Prometheus v2 – Backtesting configuration models.

This module defines configuration structures for sleeve-level backtests.
For this iteration only a single :class:`SleeveConfig` model is
implemented; it can be extended later with richer constraint and
analytics options as described in the backtesting design docs.
"""

from __future__ import annotations

from pydantic import BaseModel


class SleeveConfig(BaseModel):
    """Configuration for a single backtest sleeve/book.

    Attributes:
        sleeve_id: Logical identifier for the sleeve/book (e.g.
            ``"US_EQ_CORE_LONG"``).
        strategy_id: Strategy/alpha identifier associated with this
            sleeve; stored into ``backtest_runs.strategy_id``.
        market_id: Market identifier traded by the sleeve (e.g.
            ``"US_EQ"``).
        universe_id: Universe identifier whose members the sleeve trades.
        portfolio_id: Portfolio/book identifier whose targets the sleeve
            executes.
        assessment_strategy_id: Strategy identifier used when reading
            Assessment Engine scores from ``instrument_scores``.
        assessment_horizon_days: Assessment horizon in trading days.
        assessment_backend: Assessment backend used inside the sleeve
            pipeline. ``"basic"`` selects the price/STAB-based
            :class:`BasicAssessmentModel`; ``"context"`` selects the
            joint-space :class:`ContextAssessmentModel`.
        assessment_model_id: Optional assessment model identifier used
            for persistence/tracing. If omitted, a reasonable default is
            chosen based on :attr:`assessment_backend`.
        assessment_use_joint_context: When using the ``"basic"`` backend,
            enable or disable joint Assessment context diagnostics inside
            :class:`BasicAssessmentModel`.
        assessment_context_model_id: Joint Assessment context
            ``model_id`` in ``joint_embeddings`` to use when
            ``assessment_use_joint_context`` is True or when using the
            ``"context"`` backend.
        stability_risk_alpha: Strength of the per-instrument STAB
            state-change risk modifier applied in the sleeve's universe
            model. A value of 0.0 disables STAB risk integration for this
            sleeve.
        stability_risk_horizon_steps: Horizon in soft-target transition
            steps for the STAB risk forecast.
        regime_risk_alpha: Strength of the global regime risk modifier
            applied in the sleeve's universe model. A value of 0.0
            disables regime risk integration.
        hazard_profile: Optional hazard profile name used by the
            market-proxy regime detector. This selects which cached hazard
            signal configuration to use when classifying the 4-state
            regime.
        scenario_risk_set_id: Optional ``scenario_set_id`` used when
            computing scenario-based portfolio risk for this sleeve's
            portfolios. If ``None``, scenario risk is disabled for the
            sleeve.
        universe_max_size: Optional cap on the number of included names in
            the sleeve's universe. If ``None`` or non-positive, no global
            cap is applied.
        universe_sector_max_names: Optional per-sector cap applied after
            ranking. If ``None`` or non-positive, no per-sector cap is
            applied.
        portfolio_max_names: Optional cap on the number of names that may
            receive non-zero weights in the portfolio model (top-K culling
            at the portfolio stage). If ``None`` or non-positive, no
            portfolio-level top-K is applied.
        portfolio_per_instrument_max_weight: Optional override for
            :class:`PortfolioConfig.per_instrument_max_weight` used inside
            the sleeve's portfolio model. If ``None``, the sleeve pipeline
            uses its default.
        portfolio_hysteresis_buffer: Optional rank buffer ("top-K with
            hysteresis") used to reduce churn when
            :attr:`portfolio_max_names` is set.
        lambda_score_weight: Backwards-compatible single weight applied to
            lambda-based opportunity scores. When used, this weight affects
            both (a) universe inclusion ranking and (b) the score used for
            portfolio sizing.
        lambda_score_weight_selection: Optional lambda weight used ONLY for
            universe selection/inclusion ranking.
        lambda_score_weight_portfolio: Optional lambda weight used ONLY for
            the score consumed by the portfolio model for sizing.

        If the *_selection/_portfolio fields are set, they take precedence
        over ``lambda_score_weight``.
    """

    sleeve_id: str
    strategy_id: str
    market_id: str
    universe_id: str
    portfolio_id: str
    assessment_strategy_id: str
    assessment_horizon_days: int = 21

    # Assessment engine configuration for this sleeve.
    assessment_backend: str = "basic"
    assessment_model_id: str | None = None
    assessment_use_joint_context: bool = False
    assessment_context_model_id: str = "joint-assessment-context-v1"

    # STAB state-change risk integration for the sleeve's universe. When
    # ``stability_risk_alpha`` is non-zero, the universe model applies a
    # multiplicative penalty based on STAB state-change risk, mirroring
    # the behaviour in the live pipeline.
    stability_risk_alpha: float = 0.5
    stability_risk_horizon_steps: int = 1
    regime_risk_alpha: float = 0.0


    # Optional scenario and lambda configuration for this sleeve.
    scenario_risk_set_id: str | None = None

    # Optional universe capacity cap (enables universe-selection experiments).
    universe_max_size: int | None = None

    # Optional per-sector cap applied during universe selection.
    universe_sector_max_names: int | None = None

    # Optional portfolio concentration controls.
    portfolio_max_names: int | None = None
    portfolio_per_instrument_max_weight: float | None = None
    portfolio_hysteresis_buffer: int | None = None

    # Lambda opportunity integration weights.
    lambda_score_weight: float = 0.0
    lambda_score_weight_selection: float | None = None
    lambda_score_weight_portfolio: float | None = None

    # ------------------------------------------------------------------
    # Meta budget allocation (optional)
    # ------------------------------------------------------------------

    # When enabled, the sleeve pipeline will apply a Meta budget multiplier
    # (capital allocation scalar) derived from regime state-change risk.
    meta_budget_enabled: bool = False
    meta_budget_alpha: float = 1.0
    meta_budget_min: float = 0.35
    meta_budget_horizon_steps: int = 21
    meta_budget_region: str | None = None

    # When enabled, apply a market fragility overlay that scales the
    # budget multiplier (cash lives in the remainder).
    apply_fragility_overlay: bool = False

    # Fragility overlay configuration (defaults match the legacy step rule).
    fragility_overlay_mode: str = "step"  # step | ema_hysteresis | circuit_breaker

    # Step mode params.
    fragility_overlay_t1: float = 0.30
    fragility_overlay_t2: float = 0.50
    fragility_overlay_mid_mult: float = 0.5
    fragility_overlay_high_mult: float = 0.0

    # EMA+hysteresis params.
    fragility_overlay_ema_span: int = 20
    fragility_overlay_trim_on: float = 0.42
    fragility_overlay_trim_off: float = 0.38
    fragility_overlay_off_on: float = 0.52
    fragility_overlay_off_off: float = 0.48

    # Optional start date used when reconstructing EMA from history.
    # (YYYY-MM-DD)
    fragility_overlay_ema_history_start_date: str | None = None

    # Optional hazard profile used by the market-proxy regime detector.
    hazard_profile: str | None = None

    # ── Conviction-based position lifecycle ────────────────────────────
    # When enabled, the backtester wraps BasicLongOnlyPortfolioModel with
    # ConvictionPortfolioModel, mirroring the live pipeline.
    conviction_enabled: bool = False
    conviction_entry_credit: float = 5.0
    conviction_build_rate: float = 1.0
    conviction_decay_rate: float = 2.0
    conviction_score_cap: float = 20.0
    conviction_sell_threshold: float = 0.0
    conviction_hard_stop_pct: float = 0.20
    conviction_scale_up_days: int = 3
    conviction_entry_weight_fraction: float = 0.50

    # ── Sector Allocator overlay ──────────────────────────────────────
    # When enabled, sector health scores are computed for the backtest
    # range and SectorAllocator adjusts portfolio weights per date.
    apply_sector_allocator: bool = False
    sector_allocator_kill_threshold: float = 0.25
    sector_allocator_reduce_threshold: float = 0.40
