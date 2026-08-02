"""Shadow-mode logging + orchestrator.

During Phases 2-4 the legacy ``options_strategy.py`` classes continue
to drive real execution while the new sleeve runner runs in parallel
and logs what it *would* have done into ``derivatives_shadow_decisions``.
The daily diff between the two surfaces the behavioural changes that
need to be reconciled before each sleeve's cutover.

This module:

* persists ``SleeveDirective`` and ``SleeveSkip`` rows;
* exposes ``run_shadow_pass`` that walks the default sleeve set in one
  call and persists the result;
* keeps both calls dependency-injected so tests can use an in-memory
  fake DB.

No order submission happens here — the runner is read-only and the
shadow table is append-only.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict
from datetime import date
from typing import Any

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from psycopg2.extras import Json

from prometheus.derivatives.iv_lookup import IvLookupLike
from prometheus.derivatives.liquidity_filter import LiquidityLike
from prometheus.derivatives.runner import (
    SleeveDirective,
    SleeveRunResult,
    SleeveSkip,
    run_sleeve,
)
from prometheus.derivatives.sleeves import SleeveConfig, default_sleeves
from prometheus.execution.contract_discovery import ContractDiscoveryService

logger = get_logger(__name__)


KIND_DIRECTIVE = "DIRECTIVE"
KIND_SKIP = "SKIP"


# ── Persistence ──────────────────────────────────────────────────────


def record_shadow_result(
    db_manager: DatabaseManager,
    *,
    run_id: str,
    as_of_date: date,
    nav: float,
    signals: Mapping[str, Any],
    sleeve_results: Iterable[SleeveRunResult],
) -> int:
    """Insert every directive + skip row for a shadow pass.

    Returns the total number of rows inserted.
    """
    vix = _maybe_float(signals.get("vix_level"))
    mhi = _maybe_float(signals.get("mhi"))

    rows = 0
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            for sleeve_result in sleeve_results:
                for d in sleeve_result.directives:
                    _insert_directive(
                        cur, run_id=run_id, as_of_date=as_of_date,
                        nav=nav, vix=vix, mhi=mhi, directive=d,
                    )
                    rows += 1
                for s in sleeve_result.skips:
                    _insert_skip(
                        cur, run_id=run_id, as_of_date=as_of_date,
                        nav=nav, vix=vix, mhi=mhi, skip=s,
                    )
                    rows += 1
        conn.commit()

    logger.info(
        "shadow pass %s: persisted %d rows across sleeves", run_id, rows,
    )
    return rows


def _insert_directive(
    cur: Any, *,
    run_id: str, as_of_date: date, nav: float,
    vix: float | None, mhi: float | None,
    directive: SleeveDirective,
) -> None:
    cur.execute(
        """
        INSERT INTO derivatives_shadow_decisions (
            run_id, as_of_date, sleeve, template_name, kind,
            nav, vix_level, mhi,
            underlying, "right", expiry, strike, quantity, limit_price,
            iv_used, iv_source, delta, estimated_premium_per_contract,
            sizing_contracts, sizing_capacity_bound, sizing_budget_bound,
            trigger_reason, trigger_metadata_json, selection_trace_json,
            reason, skip_reason, skip_detail
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s
        )
        """,
        (
            run_id, as_of_date, directive.sleeve.value,
            directive.template_name, KIND_DIRECTIVE,
            float(nav), vix, mhi,
            directive.underlying, directive.right, directive.expiry,
            float(directive.strike), int(directive.quantity),
            float(directive.limit_price),
            float(directive.iv_used), directive.iv_source,
            float(directive.delta),
            float(directive.estimated_premium_per_contract),
            int(directive.sizing.contracts),
            bool(directive.sizing.capacity_bound),
            bool(directive.sizing.budget_bound),
            directive.trigger_reason,
            Json(dict(directive.trigger_metadata)) if directive.trigger_metadata else None,
            Json(_trace_to_dict(directive.selection_trace)),
            directive.reason,
            None, None,
        ),
    )


def _insert_skip(
    cur: Any, *,
    run_id: str, as_of_date: date, nav: float,
    vix: float | None, mhi: float | None,
    skip: SleeveSkip,
) -> None:
    trigger_meta_json = None
    if skip.trigger is not None and skip.trigger.metadata:
        trigger_meta_json = Json(dict(skip.trigger.metadata))
    trace_json = None
    if skip.selection is not None:
        trace_json = Json(_trace_to_dict(skip.selection.trace))
    trigger_reason = skip.trigger.reason if skip.trigger is not None else None

    cur.execute(
        """
        INSERT INTO derivatives_shadow_decisions (
            run_id, as_of_date, sleeve, template_name, kind,
            nav, vix_level, mhi,
            trigger_reason, trigger_metadata_json, selection_trace_json,
            reason, skip_reason, skip_detail
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s
        )
        """,
        (
            run_id, as_of_date, skip.sleeve.value, skip.template_name, KIND_SKIP,
            float(nav), vix, mhi,
            trigger_reason, trigger_meta_json, trace_json,
            f"SKIP[{skip.reason}] {skip.template_name}: {skip.detail}",
            skip.reason, skip.detail,
        ),
    )


# ── Orchestrator ─────────────────────────────────────────────────────


def run_shadow_pass(
    *,
    db_manager: DatabaseManager,
    run_id: str,
    as_of_date: date,
    nav: float,
    signals: Mapping[str, Any],
    open_contracts_by_template: Mapping[str, int],
    underlying_price_fn: Callable[[str], float],
    discovery: ContractDiscoveryService,
    iv_lookup: IvLookupLike,
    liquidity: LiquidityLike,
    sleeves_cfg: Mapping[Any, SleeveConfig] | None = None,
) -> tuple[list[SleeveRunResult], int]:
    """Run every default sleeve and persist the outcome.

    Returns ``(sleeve_results, rows_written)`` so callers can also
    inspect what the runner would have done in-memory (useful for the
    daily diff against the legacy strategies' output).
    """
    sleeves_cfg = sleeves_cfg or default_sleeves()
    results: list[SleeveRunResult] = []
    for cfg in sleeves_cfg.values():
        results.append(
            run_sleeve(
                cfg,
                signals=signals, nav=nav,
                open_contracts_by_template=open_contracts_by_template,
                underlying_price_fn=underlying_price_fn,
                discovery=discovery, iv_lookup=iv_lookup, liquidity=liquidity,
                today=as_of_date,
            )
        )

    rows = record_shadow_result(
        db_manager, run_id=run_id, as_of_date=as_of_date,
        nav=nav, signals=signals, sleeve_results=results,
    )
    return results, rows


# ── Helpers ──────────────────────────────────────────────────────────


def _maybe_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _trace_to_dict(trace: Any) -> dict[str, Any]:
    """asdict() recursively converts the SelectionTrace + nested
    LiquidityQuote / StrikeCandidate dataclasses into plain dicts,
    which the psycopg2 Json adapter then serialises. Falls back to
    str() for anything unexpected so a future non-serialisable field
    can't break the shadow log."""
    d = asdict(trace)
    return json.loads(json.dumps(d, default=str))


__all__ = [
    "KIND_DIRECTIVE",
    "KIND_SKIP",
    "record_shadow_result",
    "run_shadow_pass",
]
