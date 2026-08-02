"""Prometheus – Active strategy-config overlay resolver.

Closes the loop between the meta layer's applied config proposals and the
daily pipeline: the ProposalApplicator writes approved changes into
``strategy_configs`` and points ``strategies.active_strategy_config_id``
at the new version, but until now nothing ever read that pointer back at
book-construction time — applied proposals were dead letters.

Usage (intended call site: ``run_books_for_run`` in
``prometheus/pipeline/tasks.py``, immediately after the ``PortfolioConfig``
is constructed)::

    from prometheus.meta.config_resolver import (
        apply_overlay, load_active_config_overlay,
    )
    portfolio_config = apply_overlay(
        portfolio_config, load_active_config_overlay(db_manager, book_id),
    )

Safety model: the overlay is *whitelisted* — only explicitly enumerated
tuning keys can override the pipeline's config, each with type validation
and per-key logging. Anything else in config_json is ignored so a bad
proposal can never rewire portfolio_id, markets, or risk-model identity.
"""

from __future__ import annotations

from typing import Any, Dict

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger

from prometheus.portfolio.config import PortfolioConfig

logger = get_logger(__name__)


# Whitelist: overlay key → (PortfolioConfig attribute, expected type).
# ``max_weight_per_name`` is the proposal-side alias for
# ``per_instrument_max_weight``. Budget floor keys are included for
# forward-compatibility; they only apply if/when the target config model
# grows those fields (unknown attributes are logged and skipped, never
# silently set).
_OVERLAY_WHITELIST: Dict[str, tuple[str, type]] = {
    # Concentration / sizing
    "max_weight_per_name": ("per_instrument_max_weight", float),
    "per_instrument_max_weight": ("per_instrument_max_weight", float),
    "max_names": ("max_names", int),
    "score_concentration_power": ("score_concentration_power", float),
    # Conviction lifecycle parameters
    "conviction_enabled": ("conviction_enabled", bool),
    "conviction_entry_credit": ("conviction_entry_credit", float),
    "conviction_build_rate": ("conviction_build_rate", float),
    "conviction_decay_rate": ("conviction_decay_rate", float),
    "conviction_score_cap": ("conviction_score_cap", float),
    "conviction_sell_threshold": ("conviction_sell_threshold", float),
    "conviction_hard_stop_pct": ("conviction_hard_stop_pct", float),
    "conviction_scale_up_days": ("conviction_scale_up_days", int),
    "conviction_entry_weight_fraction": ("conviction_entry_weight_fraction", float),
    "conviction_scaling_enabled": ("conviction_scaling_enabled", bool),
    "conviction_scaling_min": ("conviction_scaling_min", float),
    "conviction_scaling_max": ("conviction_scaling_max", float),
    # Budget floor keys (forward-compat: applied only if the model has them)
    "meta_budget_min": ("meta_budget_min", float),
    "meta_budget_floor": ("meta_budget_floor", float),
}


def load_active_config_overlay(
    db_manager: DatabaseManager,
    strategy_id: str,
) -> Dict[str, Any]:
    """Load the active strategy config overlay for ``strategy_id``.

    Reads ``strategies.active_strategy_config_id`` and returns the pointed
    ``strategy_configs.config_json``.

    Returns:
        The config_json dict, or ``{}`` when the strategy is unknown, the
        active pointer is unset, or the lookup fails (never raises).
    """
    sql = """
        SELECT sc.config_json
        FROM strategies s
        JOIN strategy_configs sc
          ON sc.strategy_config_id = s.active_strategy_config_id
        WHERE s.strategy_id = %s
    """
    try:
        with db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (strategy_id,))
                row = cur.fetchone()
            finally:
                cur.close()
    except Exception as exc:
        logger.warning(
            "[config_resolver] overlay lookup failed for strategy=%s: %s",
            strategy_id, exc,
        )
        return {}

    if row is None or not row[0]:
        logger.debug(
            "[config_resolver] no active config overlay for strategy=%s",
            strategy_id,
        )
        return {}
    overlay = row[0]
    if not isinstance(overlay, dict):
        logger.warning(
            "[config_resolver] active config for strategy=%s is not a dict "
            "(%s) — ignoring", strategy_id, type(overlay).__name__,
        )
        return {}
    return overlay


def apply_overlay(
    portfolio_config: PortfolioConfig,
    overlay: Dict[str, Any],
) -> PortfolioConfig:
    """Apply a whitelisted config overlay onto a :class:`PortfolioConfig`.

    Only keys in the overlay whitelist are applied, each with type
    validation; every applied key is logged with old → new values. Unknown
    keys, keys whose target attribute doesn't exist on the model, and
    values that fail type coercion are skipped with a log line.

    Args:
        portfolio_config: Base config built by the pipeline.
        overlay: config_json from :func:`load_active_config_overlay`.

    Returns:
        A new PortfolioConfig with the overlay applied (the input is not
        mutated); the input instance itself when nothing applies.
    """
    if not overlay:
        return portfolio_config

    model_fields = type(portfolio_config).model_fields
    updates: Dict[str, Any] = {}

    for key, raw in overlay.items():
        spec = _OVERLAY_WHITELIST.get(key)
        if spec is None:
            logger.debug(
                "[config_resolver] overlay key %r not whitelisted — skipped", key,
            )
            continue
        attr, expected_type = spec
        if attr not in model_fields:
            logger.info(
                "[config_resolver] overlay key %r targets %r which does not "
                "exist on %s — skipped",
                key, attr, type(portfolio_config).__name__,
            )
            continue
        try:
            value = _coerce(raw, expected_type)
        except (TypeError, ValueError) as exc:
            logger.warning(
                "[config_resolver] overlay key %r has invalid value %r "
                "(expected %s): %s — skipped",
                key, raw, expected_type.__name__, exc,
            )
            continue
        old = getattr(portfolio_config, attr)
        updates[attr] = value
        logger.info(
            "[config_resolver] overlay applied: %s.%s %r -> %r (overlay key %r)",
            type(portfolio_config).__name__, attr, old, value, key,
        )

    if not updates:
        return portfolio_config
    return portfolio_config.model_copy(update=updates)


def _coerce(raw: Any, expected_type: type) -> Any:
    """Coerce ``raw`` to ``expected_type`` strictly enough to catch garbage.

    ``None`` passes through (Optional fields like max_names accept it).
    Booleans accept bool plus the usual string/int encodings; ints reject
    fractional floats; floats accept int/float/numeric strings.
    """
    if raw is None:
        return None
    if expected_type is bool:
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)) and raw in (0, 1):
            return bool(raw)
        if isinstance(raw, str) and raw.strip().lower() in ("true", "false", "0", "1"):
            return raw.strip().lower() in ("true", "1")
        raise ValueError(f"not a boolean: {raw!r}")
    if expected_type is int:
        if isinstance(raw, bool):
            raise ValueError("boolean is not an int")
        if isinstance(raw, int):
            return raw
        if isinstance(raw, float) and raw.is_integer():
            return int(raw)
        if isinstance(raw, str):
            return int(raw.strip())
        raise ValueError(f"not an int: {raw!r}")
    if expected_type is float:
        if isinstance(raw, bool):
            raise ValueError("boolean is not a float")
        if isinstance(raw, (int, float)):
            return float(raw)
        if isinstance(raw, str):
            return float(raw.strip())
        raise ValueError(f"not a float: {raw!r}")
    if isinstance(raw, expected_type):
        return raw
    raise ValueError(f"cannot coerce {raw!r} to {expected_type.__name__}")


__all__ = [
    "apply_overlay",
    "load_active_config_overlay",
]
