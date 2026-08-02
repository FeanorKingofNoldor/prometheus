"""Prometheus v2 – Daily Derivatives Orchestrator.

Connects to IBKR, syncs equity + option + futures positions, refreshes
market signals, runs all derivative strategies, applies risk checks,
and submits orders.

Usage (paper trading)::

    python -m prometheus.scripts.run.run_derivatives_daily \
        --paper --dry-run

Usage (live)::

    python -m prometheus.scripts.run.run_derivatives_daily \
        --port 4001 --account U1234567

Steps
-----
1. Connect to IBKR (paper or live).
2. Sync positions: equities, options, futures.
3. Refresh market signals: FRAG, STAB, MHI, lambda, VIX, ES price.
4. Check futures rolls.
5. Run all derivative strategies → collect ``OptionTradeDirective`` list.
6. Apply risk checks (margin, position limits, greeks limits).
7. Submit orders (or log in dry-run mode).
8. Log activity.
"""

from __future__ import annotations

import argparse
import math
import re
import sys
import uuid
from datetime import date
from typing import Any, Dict, Optional, Sequence

from apatheon.core.logging import get_logger

logger = get_logger(__name__)


# ── Error taxonomy ────────────────────────────────────────────────────
#
# The daemon treats ANY entry in the returned ``errors`` list as job
# failure and retries the whole script (up to 3 times). A retry re-runs
# strategy evaluation and re-submits orders — safe only if nothing was
# submitted on the failed attempt. So the contract is:
#
#   summary["errors"]   — FATAL, pre-submission only (IBKR connect,
#                         position sync, signal loading). Non-empty
#                         errors ⇒ the daemon may retry safely.
#   summary["warnings"] — everything that can fail at/after order
#                         submission (lifecycle reconcile, shadow pass,
#                         diff report, cutover bookkeeping, futures roll
#                         detection, decision logging, status readout).
#                         Logged loudly but NEVER triggers a retry.
#
# Anything appended to ``errors`` must be provably impossible after an
# order has been placed; when in doubt, classify as a warning.


def _record_warning(summary: Dict[str, Any], label: str, exc: BaseException) -> None:
    """Record a non-fatal failure loudly without triggering a daemon retry."""
    logger.error(
        "derivatives daily [%s] failed (non-fatal — will NOT retry): %s",
        label, exc, exc_info=True,
    )
    summary.setdefault("warnings", []).append(f"{label}: {exc}")


def _derive_trading_context(*, port: int) -> "tuple[str, str]":
    """Map the IBKR gateway port to ``(portfolio_id, mode)``.

    4001 is the live gateway; anything else (4002, the paper default) is
    paper. The daemon maps ``--options-mode`` to the port
    (``live → 4001``, ``paper``/``dry_run → 4002``), so the port is the
    single source of truth for which ACCOUNT we are talking to.

    ``dry_run`` deliberately does NOT feed this: it only controls
    whether orders are submitted. Positions synced from the gateway
    still belong to the account the port points at, so persisting them
    under a dry/paper/live label derived from ``dry_run`` (the
    pre-2026-07 behaviour) mislabeled every paper run as
    US_OPTIONS_LIVE/mode=LIVE.
    """
    live = port == 4001
    if live:
        return "US_OPTIONS_LIVE", "LIVE"
    return "US_OPTIONS_PAPER", "PAPER"


def _make_submission_recorder(
    *,
    portfolio_id: str,
    mode: str,
    as_of_date: date,
    summary: Dict[str, Any],
) -> Any:
    """Build the callback ``_submit_directives`` invokes after each
    successfully submitted order.

    Persists strategy provenance (contract signature + strategy name)
    into ``options_position_events`` (event_type=SUBMIT) so the next
    sync can restore tags onto positions coming back from IBKR — the
    fix for the position-blind re-entry defect. Runs AFTER the order
    went out, so any failure here is a post-submission warning (never
    retried).
    """

    def _record(directive: Any, instrument_id: str, order_id: Optional[str]) -> None:
        try:
            from apatheon.core.database import get_db_manager

            from prometheus.execution.options_storage import record_order_submission

            md = dict(directive.metadata or {})
            record_order_submission(
                get_db_manager(),
                portfolio_id=portfolio_id,
                mode=mode,
                instrument_id=instrument_id,
                symbol=directive.symbol,
                right=directive.right,
                expiry=directive.expiry,
                strike=float(directive.strike),
                quantity=int(directive.quantity),
                strategy=str(directive.strategy or ""),
                order_id=order_id,
                limit_price=directive.limit_price,
                sleeve=md.get("sleeve"),
                template=md.get("template"),
                as_of_date=as_of_date,
                metadata={
                    "action": directive.action.value,
                    "reason": directive.reason,
                },
            )
        except Exception as exc:
            _record_warning(summary, "submission_record", exc)

    return _record


# ── Shadow-mode helper ───────────────────────────────────────────────
#
# Runs the new sleeve-based derivatives pipeline alongside the legacy
# strategies and persists what it *would* trade into
# ``derivatives_shadow_decisions``. Read-only — no orders submitted.
#
# Gated entirely by the ``PROMETHEUS_DERIVATIVES_SHADOW`` env var. Any
# failure in this path is logged + swallowed so the live pipeline keeps
# running. This is the safety contract for Phase 1d → Phase 2 bridging.


def _shadow_enabled() -> bool:
    # Shared parser: the tracker reads the SAME flag — divergent token
    # sets previously let a value like "y" enable one half of shadow
    # mode and not the other.
    from prometheus.env_utils import env_flag

    return env_flag("PROMETHEUS_DERIVATIVES_SHADOW", default=False)


def _make_underlying_price_fn(
    ib: Any,
    signals: Dict[str, Any],
) -> Any:
    """Return a callable that resolves an underlying's spot price.

    First-pass lookup uses ``signals`` (SPY/VIX already loaded, plus
    equity_prices populated from positions). Falls back to a one-shot
    IBKR snapshot for symbols not in the signals dict — the shadow
    runner only needs a price for whichever underlying a fired
    template targets, so this is bounded to a few calls.
    """
    cache: Dict[str, float] = {}

    def _lookup(symbol: str) -> float:
        sym = (symbol or "").upper()
        if sym in cache:
            return cache[sym]

        # Try signals first (already loaded for SPY / VIX / portfolio).
        if sym == "SPY":
            px = float(signals.get("spy_price", 0.0) or 0.0)
        elif sym == "VIX":
            px = float(signals.get("vix_level", 0.0) or 0.0)
        else:
            px = float(signals.get("equity_prices", {}).get(sym, 0.0) or 0.0)
            if px <= 0:
                px = float(signals.get("etf_prices", {}).get(sym, 0.0) or 0.0)

        # On-demand IBKR snapshot for missing symbols (sector ETFs etc).
        if px <= 0 and ib is not None:
            try:
                from prometheus.execution.ib_compat import Stock
                contract = Stock(sym, "SMART", "USD")
                qualified = ib.qualifyContracts(contract)
                if qualified:
                    ticker = ib.reqMktData(qualified[0], snapshot=True)
                    ib.sleep(1.5)
                    last = float(getattr(ticker, "last", 0) or 0)
                    close = float(getattr(ticker, "close", 0) or 0)
                    bid = float(getattr(ticker, "bid", 0) or 0)
                    ask = float(getattr(ticker, "ask", 0) or 0)
                    mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0
                    px = last or close or mid or 0.0
                    try:
                        ib.cancelMktData(qualified[0])
                    except Exception:
                        pass
            except Exception as exc:
                logger.debug("shadow: spot lookup failed for %s: %s", sym, exc)

        cache[sym] = px
        return px

    return _lookup


_LEGACY_STRATEGY_TO_TEMPLATE: Dict[str, str] = {
    "protective_put": "hedge.spy_protective_put",
    "sector_put_spread": "hedge.sector_put_spread",
    "vix_tail_hedge": "hedge.vix_tail_call",
    "iron_butterfly": "income.spy_iron_butterfly",
    "iron_condor": "income.spy_iron_condor",
    "covered_call": "income.covered_call",
    "crisis_alpha": "convex.thematic_sector_put",
}

# Legs per *spread* directive emitted by the new pipeline. Legacy
# strategies that produce N legs (e.g. sector_put_spread = 2 legs)
# show up as N positions; we divide by this to recover spread count.
_TEMPLATE_LEG_COUNT: Dict[str, int] = {
    "hedge.spy_protective_put": 1,
    "hedge.sector_put_spread": 2,
    "hedge.vix_tail_call": 1,
    "hedge.collar": 2,
    "income.spy_short_put": 1,
    "income.spy_iron_butterfly": 4,
    "income.spy_iron_condor": 4,
    "income.covered_call": 1,
    "convex.thematic_sector_put": 1,
    "convex.vix_escalation_call": 1,
    "convex.convergence_straddle": 2,
    # COMMODITY sleeve — all four are single-leg long FOP calls.
    "commodity.crude_chokepoint_call": 1,
    "commodity.natgas_supply_call": 1,
    "commodity.gold_sanctions_call": 1,
    "commodity.wheat_blacksea_call": 1,
}


def _open_contracts_by_template(existing_options: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    """Count open *spreads* per template for the shadow capacity check.

    The legacy ``existing_options`` rows are tagged with ``strategy``
    (the old per-strategy class name). We map each legacy strategy to
    its replacement template and divide by the template's leg count to
    recover the spread count (so a single sector_put_spread with 2
    legs becomes 1, not 2).

    This is what stops the shadow / cut-over runner from double-firing
    when legacy already holds the position the new template would
    open. Unmapped legacy strategies contribute nothing — they aren't
    replaced by any current template.

    Two tagging conventions are recognised:

    * legacy strategy names (``protective_put`` …) mapped to their
      replacement template via ``_LEGACY_STRATEGY_TO_TEMPLATE``;
    * positions already tagged with the *template* name directly
      (``commodity.crude_chokepoint_call`` …). The COMMODITY sleeve is
      shadow-only with no legacy equivalent, so it has no
      ``_LEGACY_STRATEGY_TO_TEMPLATE`` row — its positions carry the
      template name, which we recognise via ``_TEMPLATE_LEG_COUNT``.
    """
    raw: Dict[str, int] = {}
    for opt in existing_options:
        strategy = str(opt.get("strategy", "") or "")
        # Position tagged with the template name itself (e.g. COMMODITY
        # FOP positions, or any cut-over sleeve directive whose strategy
        # tag is the template name) — count it directly.
        if strategy in _TEMPLATE_LEG_COUNT:
            template = strategy
        else:
            template = _LEGACY_STRATEGY_TO_TEMPLATE.get(strategy)
        if template is None:
            continue
        raw[template] = raw.get(template, 0) + 1

    out: Dict[str, int] = {}
    for template, legs_count in raw.items():
        per_spread = _TEMPLATE_LEG_COUNT.get(template, 1)
        out[template] = max(legs_count // per_spread, 0)
    return out


def _run_shadow_pass(
    *,
    ib: Any,
    discovery: Any,
    signals: Dict[str, Any],
    existing_options: Sequence[Dict[str, Any]],
    as_of_date: date,
    summary: Dict[str, Any],
    portfolio_id: str = "US_OPTIONS_LIVE",
) -> list:
    """Run the new sleeve pipeline in read-only shadow mode.

    Returns the per-sleeve ``SleeveRunResult`` list so callers can
    convert directives from cut-over sleeves into legacy
    ``OptionTradeDirective`` instances for live submission.
    """
    from apatheon.core.database import get_db_manager

    from prometheus.derivatives.intel_signals import (
        load_intel_signals,
        merge_into_signals,
    )
    from prometheus.derivatives.iv_lookup import IvLookupService
    from prometheus.derivatives.liquidity_filter import LiquidityFilter
    from prometheus.derivatives.shadow import run_shadow_pass

    nav = float(signals.get("nav", 0.0) or 0.0)
    if nav <= 0:
        logger.info("shadow: NAV=0 — skipping shadow pass")
        return []

    iv_svc = IvLookupService(ib=ib)
    liq_svc = LiquidityFilter(ib=ib)
    price_fn = _make_underlying_price_fn(ib, signals)
    db_manager = get_db_manager()
    run_id = f"shadow-{as_of_date.isoformat()}-{uuid.uuid4().hex[:8]}"

    # Phase 4a: load Apatheon intel signals and fold them into the
    # signals dict the sleeve runner will consume. Failure here is
    # non-fatal — the convex sleeve will skip without intel inputs
    # but the hedge + income sleeves run unimpaired.
    try:
        intel = load_intel_signals(
            db_manager, as_of_date=as_of_date,
            portfolio_id=portfolio_id,
        )
        enriched_signals: Dict[str, Any] = dict(merge_into_signals(signals, intel))
        summary["intel_signals"] = {
            "divergence_count": len(intel.divergence),
            "convergence_count": len(intel.convergence),
            "compound_pressure_count": len(intel.compound_pressure),
            "geo_risk_score": intel.overall_geo_risk_score(),
        }
    except Exception as exc:
        logger.debug("intel signal load failed (continuing): %s", exc)
        enriched_signals = signals

    results, rows = run_shadow_pass(
        db_manager=db_manager,
        run_id=run_id,
        as_of_date=as_of_date,
        nav=nav,
        signals=enriched_signals,
        open_contracts_by_template=_open_contracts_by_template(existing_options),
        underlying_price_fn=price_fn,
        discovery=discovery,
        iv_lookup=iv_svc,
        liquidity=liq_svc,
    )

    shadow_summary = {
        "run_id": run_id,
        "rows_persisted": rows,
        "per_sleeve": {
            r.sleeve.value: {"fired": r.fired, "skipped": r.skipped}
            for r in results
        },
    }
    summary["shadow_derivatives"] = shadow_summary
    logger.info(
        "shadow pass complete: run_id=%s rows=%d sleeves=%s",
        run_id, rows, shadow_summary["per_sleeve"],
    )
    return results


# ── Cutover wiring (Phase 2.7) ────────────────────────────────────────
#
# When a sleeve is in cutover (env-gated), two things happen:
#   1. Legacy strategies that the sleeve replaces are silenced — their
#      directives are dropped before risk checks.
#   2. Directives produced by the new sleeve runner for that sleeve
#      are converted to OptionTradeDirective and added to the
#      submission list so they flow through the same risk + submit
#      path as the legacy directives.


def _filter_silenced_directives(
    all_directives: list,
    silenced_strategies: "frozenset[str]",
) -> tuple[list, list]:
    """Split directives into (kept, dropped) based on the silenced set."""
    if not silenced_strategies:
        return all_directives, []
    kept: list = []
    dropped: list = []
    for d in all_directives:
        strat = getattr(d, "strategy", "")
        if strat in silenced_strategies:
            dropped.append(d)
        else:
            kept.append(d)
    return kept, dropped


def _sleeve_directives_to_legacy(sleeve_results: list) -> list:
    """Convert SleeveDirective objects from cut-over sleeves into
    OptionTradeDirective so they flow through the existing submission
    path.

    The ``strategy`` field is set to the legacy strategy name (via
    inverse template mapping) when one exists — this keeps the daily
    diff report's pairing logic working post-cutover. The full
    template name lives in ``metadata.template``.
    """
    from prometheus.execution.options_strategy import (
        OptionTradeDirective,
        TradeAction,
    )

    # Inverse of _LEGACY_STRATEGY_TO_TEMPLATE — used for outbound tag.
    template_to_legacy = {v: k for k, v in _LEGACY_STRATEGY_TO_TEMPLATE.items()}

    out: list = []
    for sleeve_result in sleeve_results:
        for d in sleeve_result.directives:
            legacy_strategy = template_to_legacy.get(d.template_name, d.template_name)
            out.append(OptionTradeDirective(
                strategy=legacy_strategy,
                action=TradeAction.OPEN,
                symbol=d.underlying,
                right=d.right,
                expiry=d.expiry,
                strike=float(d.strike),
                quantity=int(d.quantity),
                limit_price=float(d.limit_price),
                reason=d.reason,
                metadata={
                    "sleeve": d.sleeve.value,
                    "template": d.template_name,
                    "source": "cutover_pipeline",
                    **dict(d.trigger_metadata),
                },
            ))
    return out


def _reconcile_options_storage(
    *,
    ib_unused: Any,
    options_portfolio: Any,
    portfolio_id: str,
    mode: str,
    summary: Dict[str, Any],
) -> None:
    """Write through the in-memory OptionsPortfolio snapshot to the
    ``options_positions`` table (Phase 2.5). Best-effort: never
    breaks the live pipeline."""
    try:
        from apatheon.core.database import get_db_manager

        from prometheus.execution.options_storage import reconcile_positions
        db = get_db_manager()
        snapshot = {
            entry.instrument_id: entry
            for entry in options_portfolio.get_all_positions()
        }
        result = reconcile_positions(
            db, portfolio_id=portfolio_id, mode=mode,
            snapshot=snapshot,
        )
        summary["options_storage_reconcile"] = {
            "opened": result.opened,
            "updated": result.updated,
            "closed": result.closed,
        }
    except Exception as exc:
        _record_warning(summary, "reconcile", exc)


def _write_daily_diff_report(*, as_of_date: date, summary: Dict[str, Any]) -> None:
    """Run the shadow-vs-legacy diff and persist the markdown report.
    Best-effort; never breaks the live pipeline."""
    try:
        import os.path

        from apatheon.core.database import get_db_manager

        from prometheus.derivatives.diff_report import run_daily_diff
        db = get_db_manager()
        md = run_daily_diff(db, as_of_date)
        brief_path = f"/app/briefs/derivatives_diff_{as_of_date.isoformat()}.md"
        if os.path.isdir(os.path.dirname(brief_path)):
            with open(brief_path, "w") as fh:
                fh.write(md)
            summary["diff_report_path"] = brief_path
            logger.info("diff report written to %s", brief_path)
        else:
            summary["diff_report_skipped"] = "briefs directory missing"
    except Exception as exc:
        _record_warning(summary, "diff_report", exc)


# ── Signal loader (stub — wired to real pipelines in production) ──────

def _load_signals(
    ib: Any,
    account_state: Dict[str, Any],
    *,
    positions: Dict[str, Any],
) -> Dict[str, Any]:
    """Assemble the signals dict consumed by all strategies.

    Fetching priority for live prices:
      1. IBKR streaming delayed data (reqMarketDataType=3, non-competing)
      2. DB ``prices_daily`` table (SPY.US / VIX.INDX) as reliable fallback
      3. Hardcoded defaults if both sources fail

    Using streaming instead of reqTickers (snapshot) avoids Error 10197
    "competing live session" that occurs when another TWS/Gateway session
    holds the market data subscription lock.
    """
    nav = float(account_state.get("NetLiquidation", 0))

    signals: Dict[str, Any] = {
        # Portfolio
        "nav": nav,
        "buying_power": float(account_state.get("AvailableFunds", nav)),
        "market_state": "NEUTRAL",
        # Health indices (real pipeline fills these)
        "mhi": 1.0,
        "frag": 0.0,
        # VIX
        "vix_level": 20.0,
        # Equity-index prices
        "spy_price": 0.0,
        "es_price": 0.0,
        # Lambda / STAB scores (keyed by symbol)
        "lambda_scores": {},
        "lambda_aggregate": 0.0,
        "stab_scores": {},
        # Sector
        "sector_shi": {},
        "sector_exposures": {},
        "etf_prices": {},
        # Futures positions
        "futures_positions": {},
        # Equity prices (single-name option strategies)
        "equity_prices": {},
    }

    def _valid_price(v: Any) -> Optional[float]:
        """Return float if v is a real positive price, else None."""
        try:
            fv = float(v)
            if fv > 0 and not math.isnan(fv):
                return fv
        except (TypeError, ValueError):
            pass
        return None

    def _fetch_streaming(contract: Any, timeout: float = 5.0) -> Optional[float]:
        """Subscribe to delayed streaming data and wait *timeout* seconds.

        Uses ``reqMarketDataType(3)`` (delayed) which does not compete with
        a live session, so avoids Error 10197.
        """
        try:
            qualified = ib.qualifyContracts(contract)
            if not qualified:
                return None
            ticker = ib.reqMktData(qualified[0], "", False, False)
            ib.sleep(timeout)
            for attr in ("last", "close", "bid", "ask"):
                p = _valid_price(getattr(ticker, attr, None))
                if p is not None:
                    return p
            ib.cancelMktData(qualified[0])
        except Exception as exc:
            logger.debug("IBKR streaming fetch error for %s: %s",
                         getattr(contract, "symbol", str(contract)), exc)
        return None

    def _db_price(instrument_id: str, max_age_days: int = 7) -> Optional[float]:
        """Fetch the most recent close from ``prices_daily`` in the historical DB.

        Bounded staleness: a row older than ``max_age_days`` calendar days is
        REFUSED (returns None) rather than silently used — gating short-vol
        strategies on a months-old VIX print is how positions get sized on a
        market that no longer exists. Callers fall back to their conservative
        defaults when this returns None.
        """
        try:
            from apatheon.core.database import get_db_manager
            db = get_db_manager()
            with db.get_historical_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT close, trade_date FROM prices_daily "
                        "WHERE instrument_id=%s ORDER BY trade_date DESC LIMIT 1",
                        (instrument_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        p = _valid_price(row[0])
                        if p is not None:
                            age_days = (date.today() - row[1]).days
                            if age_days > max_age_days:
                                logger.warning(
                                    "DB price for %s is %d days stale (as of %s, bound %dd) — "
                                    "refusing to use it; caller falls back to safe defaults",
                                    instrument_id, age_days, row[1], max_age_days,
                                )
                                return None
                            logger.info(
                                "DB price for %s: %.4f (as of %s)",
                                instrument_id, p, row[1],
                            )
                            return p
        except Exception as exc:
            logger.debug("DB price lookup failed for %s: %s", instrument_id, exc)
        return None

    # Request delayed data type once — applies to all subsequent reqMktData calls.
    # Type 3 = delayed (15-20 min) which is independent of live session.
    try:
        ib.reqMarketDataType(3)
    except Exception:
        pass

    # ── VIX ────────────────────────────────────────────────────────────
    from prometheus.execution.ib_compat import Index
    vix_ibkr = _fetch_streaming(Index("VIX", "CBOE", "USD"))
    if vix_ibkr is not None:
        signals["vix_level"] = vix_ibkr
        logger.info("VIX from IBKR: %.2f", vix_ibkr)
    else:
        vix_db = _db_price("VIX.INDX")
        if vix_db is not None:
            signals["vix_level"] = vix_db
        else:
            logger.warning("VIX unavailable from IBKR and DB — using default %.1f",
                           signals["vix_level"])

    # ── SPY price ───────────────────────────────────────────────────────
    from prometheus.execution.ib_compat import Stock
    spy_ibkr = _fetch_streaming(Stock("SPY", "ARCA", "USD"))
    if spy_ibkr is not None:
        signals["spy_price"] = spy_ibkr
        logger.info("SPY from IBKR: %.2f", spy_ibkr)
    else:
        spy_db = _db_price("SPY.US")
        if spy_db is not None:
            signals["spy_price"] = spy_db
        else:
            logger.warning("SPY price unavailable from IBKR and DB")

    # ── ES price ────────────────────────────────────────────────────────
    try:
        from prometheus.execution.futures_manager import PRODUCTS
        from prometheus.execution.ib_compat import Future
        if PRODUCTS.get("ES"):
            es_contract = Future("ES", exchange="CME", currency="USD")
            es_contract.secType = "CONTFUT"
            es_ibkr = _fetch_streaming(es_contract)
            if es_ibkr is not None:
                signals["es_price"] = es_ibkr
                logger.info("ES from IBKR: %.2f", es_ibkr)
                # Use ES/10 as secondary SPY proxy only if SPY is still unset
                if signals["spy_price"] == 0.0:
                    signals["spy_price"] = es_ibkr / 10
    except Exception as exc:
        logger.debug("Could not fetch live ES price: %s", exc)

    # Derive ES from SPY if ES is still missing
    if signals["es_price"] == 0.0 and signals["spy_price"] > 0:
        signals["es_price"] = signals["spy_price"] * 10
        logger.debug("ES estimated from SPY×10: %.1f", signals["es_price"])

    # ── Equity prices from live portfolio positions ─────────────────────
    for iid, pos in positions.items():
        if iid.endswith(".US") or (not iid.endswith(".FUT") and "_" not in iid):
            symbol = iid.replace(".US", "").split(".")[0]
            qty = getattr(pos, "quantity", 0) or 0
            mv = getattr(pos, "market_value", 0) or 0
            if qty > 0 and mv > 0:
                signals["equity_prices"][symbol] = mv / qty

    # ── Sector Health Index (SHI) from runtime DB ────────────────────
    # Used by SectorPutSpreadStrategy and CrisisAlphaStrategy.
    try:
        from apatheon.core.database import get_db_manager as _get_db
        _db = _get_db()
        with _db.get_runtime_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT sector_name, score
                    FROM sector_health_daily
                    WHERE as_of_date = (SELECT MAX(as_of_date) FROM sector_health_daily)
                """)
                for sector_name, score in cur.fetchall():
                    signals["sector_shi"][str(sector_name)] = float(score)
        if signals["sector_shi"]:
            logger.info("Loaded SHI for %d sectors", len(signals["sector_shi"]))
    except Exception as exc:
        logger.warning("Failed to load sector SHI: %s", exc)

    return signals


# ── Risk checks ───────────────────────────────────────────────────────

def _apply_risk_checks(
    directives: list,
    margin_snapshot: Any,
    portfolio_greeks: Any,
    *,
    max_margin_util: float = 0.60,
    max_total_delta: float = 500_000.0,
    max_total_theta: float = -5_000.0,
) -> list:
    """Filter directives through risk checks.

    Returns only directives that pass all checks.
    """
    approved: list = []

    current_margin_util = 0.0
    if margin_snapshot and margin_snapshot.net_liquidation > 0:
        current_margin_util = margin_snapshot.init_margin_utilisation

    for d in directives:
        # Skip margin-intensive trades when utilisation is high
        if current_margin_util > max_margin_util:
            if d.action.value in ("OPEN", "HEDGE"):
                logger.warning(
                    "Risk: blocking %s %s — margin utilisation %.1f%% > %.1f%%",
                    d.action.value, d.symbol,
                    current_margin_util * 100, max_margin_util * 100,
                )
                continue

        # Greeks limits
        if portfolio_greeks:
            if abs(portfolio_greeks.total_delta) > max_total_delta:
                if d.action.value == "OPEN":
                    logger.warning(
                        "Risk: blocking OPEN %s — portfolio delta %.0f > %.0f limit",
                        d.symbol, abs(portfolio_greeks.total_delta), max_total_delta,
                    )
                    continue

            if portfolio_greeks.total_theta < max_total_theta:
                if d.quantity < 0:  # Selling options adds negative theta
                    logger.warning(
                        "Risk: blocking short %s — portfolio theta $%.0f < $%.0f limit",
                        d.symbol, portfolio_greeks.total_theta, max_total_theta,
                    )
                    continue

        approved.append(d)

    blocked = len(directives) - len(approved)
    if blocked > 0:
        logger.info("Risk: approved %d / %d directives (%d blocked)",
                     len(approved), len(directives), blocked)

    return approved


# ── Main orchestrator ─────────────────────────────────────────────────

def run_derivatives_daily(
    *,
    host: str = "127.0.0.1",
    port: int = 4002,
    client_id: int = 10,
    account: str = "",
    dry_run: bool = True,
    max_margin_util: float = 0.60,
) -> Dict[str, Any]:
    """Run the full daily derivatives pipeline.

    Returns a summary dict with counts and diagnostics.
    """
    from prometheus.execution.broker_interface import BrokerInterface
    from prometheus.execution.contract_discovery import ContractDiscoveryService
    from prometheus.execution.futures_manager import FuturesManager
    from prometheus.execution.ib_compat import IB
    from prometheus.execution.instrument_mapper import InstrumentMapper
    from prometheus.execution.options_portfolio import OptionsPortfolio
    from prometheus.execution.options_strategy import OptionsStrategyManager

    # Which account (and hence persistence namespace) this run talks to
    # is derived from the gateway port — NOT from dry_run (see
    # _derive_trading_context).
    options_portfolio_id, trading_mode = _derive_trading_context(port=port)

    summary: Dict[str, Any] = {
        "date": date.today().isoformat(),
        "dry_run": dry_run,
        "trading_mode": trading_mode,
        "options_portfolio_id": options_portfolio_id,
        "steps_completed": [],
        # FATAL pre-submission failures only — the daemon retries on these.
        "errors": [],
        # Non-fatal failures (anything at/after order submission) — logged
        # loudly, never retried. See the error-taxonomy note at module top.
        "warnings": [],
    }

    # Flipped just before real orders go to IBKR. Once True, any later
    # exception must be classified as a warning: a daemon retry would
    # re-evaluate strategies and re-submit the already-placed orders.
    orders_submitted = False

    ib = IB()

    # ── Step 1: Connect ───────────────────────────────────────────────
    try:
        logger.info("Connecting to IBKR at %s:%d (client_id=%d)", host, port, client_id)
        # ib_insync requires an asyncio event loop. Daemon threads don't have one.
        import asyncio
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

        ib.connect(host=host, port=port, clientId=client_id, timeout=30)
        summary["steps_completed"].append("connect")
        logger.info("Connected to IBKR")
    except Exception as exc:
        logger.error("Failed to connect to IBKR: %s", exc)
        summary["errors"].append(f"connect: {exc}")
        return summary

    try:
        # ── Step 2: Sync positions & account ──────────────────────────
        logger.info("Syncing positions and account state...")

        # Brief pause so IBKR streams account values before we read them.
        # Without this, accountValues() often returns an empty list immediately
        # after connect() and NAV comes out as $0.
        ib.sleep(2)

        account_values = ib.accountValues()
        account_state: Dict[str, Any] = {}
        for av in account_values:
            # Accept USD and empty-currency tags (always); also accept BASE
            # currency which is present when the account base currency is not
            # USD (e.g. CHF paper accounts).
            if av.currency in ("USD", "BASE", ""):
                account_state[av.tag] = av.value
            # For key financial metrics, accept any currency as a last resort
            # (covers e.g. NetLiquidation [CHF] on CHF-base accounts).
            elif av.tag in ("NetLiquidation", "TotalCashValue",
                            "AvailableFunds", "BuyingPower") \
                    and av.tag not in account_state:
                account_state[av.tag] = av.value
        # NetLiquidationByCurrency [BASE] is the canonical cross-currency NAV;
        # alias it to NetLiquidation if the direct tag wasn't found.
        if "NetLiquidation" not in account_state \
                and "NetLiquidationByCurrency" in account_state:
            account_state["NetLiquidation"] = account_state["NetLiquidationByCurrency"]

        # Fallback: if NetLiquidation still missing, sum portfolio market values
        if not account_state.get("NetLiquidation"):
            portfolio_items = ib.portfolio()
            if portfolio_items:
                total_mv = sum(abs(float(getattr(item, "marketValue", 0) or 0))
                               for item in portfolio_items)
                if total_mv > 0:
                    account_state["NetLiquidation"] = str(total_mv)
                    logger.info("NAV computed from portfolio market values: $%.0f", total_mv)

        # Get all positions and convert to internal Position dataclass.
        # Strategies expect Position.quantity / .market_value / etc. (broker_interface.py),
        # not the raw ib_insync Position namedtuple (.position / .avgCost).
        # Market values come from ib.portfolio() which streams per-position MV.
        raw_positions = ib.positions()
        portfolio_mv: Dict[int, float] = {}  # conId → market_value
        portfolio_unreal: Dict[int, float] = {}
        for item in ib.portfolio():
            con_id = getattr(item.contract, "conId", None)
            if con_id:
                portfolio_mv[con_id] = float(getattr(item, "marketValue", 0) or 0)
                portfolio_unreal[con_id] = float(getattr(item, "unrealizedPNL", 0) or 0)

        from prometheus.execution.broker_interface import Position as InternalPosition
        positions: Dict[str, Any] = {}
        for p in raw_positions:
            contract = p.contract
            iid = InstrumentMapper.contract_to_instrument_id(contract)
            con_id = getattr(contract, "conId", None)
            positions[iid] = InternalPosition(
                instrument_id=iid,
                quantity=float(p.position),
                avg_cost=float(p.avgCost),
                market_value=portfolio_mv.get(con_id, 0.0),
                unrealized_pnl=portfolio_unreal.get(con_id, 0.0),
            )

        summary["position_count"] = len(positions)
        summary["steps_completed"].append("sync_positions")

        # ── Step 3: Initialize services ───────────────────────────────
        discovery = ContractDiscoveryService(ib)
        mapper = InstrumentMapper()

        futures_mgr = FuturesManager(discovery, max_margin_utilisation=max_margin_util)
        futures_mgr.sync_positions(positions)
        futures_mgr.sync_margin(account_state)

        options_portfolio = OptionsPortfolio(ib)
        options_portfolio.sync(broker_positions=positions)

        # Restore strategy provenance BEFORE anything reads the synced
        # positions. IBKR returns positions untagged; without this every
        # tag-filtered check (vix_tail_hedge re-entry guard,
        # condor/butterfly max_positions + margin-used, profit targets,
        # rolls, regime exits) saw zero owned positions and re-entered
        # nightly. Tags come from options_positions.strategy plus SUBMIT
        # events written at order time (options_storage.load_strategy_tags).
        #
        # A failure here is FATAL (pre-submission, so the daemon may
        # retry safely): trading position-blind is exactly the defect
        # this guards against. Skipped when the account holds no option
        # positions — nothing to restore.
        if options_portfolio.get_all_positions():
            try:
                from apatheon.core.database import get_db_manager as _get_db

                from prometheus.execution.options_storage import load_strategy_tags

                _tags = load_strategy_tags(
                    _get_db(),
                    portfolio_id=options_portfolio_id,
                    mode=trading_mode,
                )
                summary["strategy_tags_restored"] = (
                    options_portfolio.apply_strategy_tags(_tags)
                )
            except Exception as exc:
                raise RuntimeError(
                    f"strategy_tag_restore failed — refusing to run "
                    f"position-blind: {exc}"
                ) from exc

        # Phase 2.5: write through to options_positions for queryable
        # state across the diff report + audit log. Gated by shadow
        # mode so we don't toggle persistence independently.
        if _shadow_enabled():
            _reconcile_options_storage(
                ib_unused=ib,
                options_portfolio=options_portfolio,
                portfolio_id=options_portfolio_id,
                mode=trading_mode,
                summary=summary,
            )

        summary["steps_completed"].append("init_services")

        # ── Step 4: Load signals ──────────────────────────────────────
        signals = _load_signals(ib, account_state, positions=positions)
        summary["nav"] = signals["nav"]
        summary["vix"] = signals["vix_level"]
        summary["es_price"] = signals["es_price"]
        summary["steps_completed"].append("load_signals")

        # ── Step 5: Check futures rolls ───────────────────────────────
        # Non-fatal: roll detection failing must not abort (or retry)
        # the options pipeline — classified as a warning.
        roll_directives: list = []
        roll_orders: list = []
        try:
            roll_directives = futures_mgr.check_rolls()
            for rd in roll_directives:
                orders = futures_mgr.create_roll_orders(rd)
                roll_orders.extend(orders)
        except Exception as exc:
            _record_warning(summary, "futures_rolls", exc)

        summary["roll_directives"] = len(roll_directives)
        summary["roll_orders"] = len(roll_orders)
        summary["steps_completed"].append("check_rolls")

        if roll_directives:
            logger.info(
                "Futures rolls needed: %d positions, %d orders",
                len(roll_directives), len(roll_orders),
            )
            if dry_run:
                for ro in roll_orders:
                    logger.info("[DRY RUN] Roll order: %s", ro)
            # In live mode, roll orders are submitted separately
            # (they bypass the strategy manager)

        # ── Step 5.5: Compute market situation & allocations ─────────
        logger.info("Computing market situation and strategy allocations...")

        from prometheus.execution.position_lifecycle import PositionLifecycleManager
        from prometheus.execution.strategy_allocator import StrategyAllocator

        # Determine market situation from signals
        market_state = signals.get("market_state", "NEUTRAL")

        allocator = StrategyAllocator()
        portfolio_greeks = options_portfolio.compute_portfolio_greeks()
        existing_options = options_portfolio.get_positions_as_dicts()

        # Inject derivatives-budget cap signals — mirrors options_backtest.py
        # _build_signals() exactly.  Without this, butterfly/condor margin cap
        # defaults to AvailableFunds (full account BP) and margin_used = 0,
        # making the book-level cap a no-op.
        _spread_strats = {"iron_butterfly", "iron_condor"}
        signals["butterfly_condor_margin_used"] = sum(
            abs(opt.get("entry_price", 0)) * abs(opt.get("quantity", 0)) * 100
            for opt in existing_options
            if opt.get("strategy") in _spread_strats and opt.get("quantity", 0) < 0
        )
        # Override buying_power to mean the legacy derivatives budget
        # (NAV × 30%), not the raw IBKR AvailableFunds figure. The new
        # sleeve runner enforces its own per-sleeve budgets independently
        # (HEDGE 10% + INCOME 15% + CONVEX 5% + COMMODITY 5% = 35% total
        # when all sleeves are in cutover).
        signals["buying_power"] = signals["nav"] * 0.30

        allocations = allocator.allocate(
            market_situation=market_state,
            signals=signals,
            portfolio_greeks=portfolio_greeks,
            existing_positions=existing_options,
        )

        enabled_count = sum(1 for a in allocations.values() if a.enabled)
        summary["market_situation"] = market_state
        summary["strategies_enabled"] = enabled_count
        summary["steps_completed"].append("strategy_allocation")

        # ── Step 6: Run derivative strategies ───────────────────────
        logger.info("Running derivative strategies...")

        # Broker implementations used by the strategy manager.
        # _StubBroker: logs orders (always used inside evaluate_all, which runs
        #              with dry_run=True so it never actually submits).
        # _IbkrDirectBroker: submits real orders via the already-connected `ib`
        #              instance; used in Step 8 when not dry_run.
        class _StubBroker(BrokerInterface):
            """Minimal stub — logs orders instead of submitting."""
            def submit_order(self, order):
                logger.info("[SUBMIT] %s %s x%d", order.side.value, order.instrument_id, order.quantity)
            def cancel_order(self, order_id):
                return False
            def get_positions(self):
                return positions
            def get_order_status(self, order_id):
                return None
            def get_account_state(self):
                return account_state
            def get_fills(self, since=None):
                return []
            def sync(self):
                pass

        class _IbkrDirectBroker(BrokerInterface):
            """Submit option orders via the already-connected ib_insync/ib_async instance.

            Retry safety: every order gets a deterministic ``orderRef``
            (sha1 over portfolio/strategy/contract/side/date — see
            ``prometheus.derivatives.order_refs``), and submission is
            skipped when a non-terminal trade with the same ref is
            already working on the connection. A daemon retry of the
            same cycle therefore cannot double-submit.
            """

            # Instrument-id patterns:
            #   Equity option: SYMBOL_YYMMDD_STRIKEC/P.US
            #     e.g.  VIX_260417_32C.US   SPY_260418_560P.US
            #   Futures option (FOP): SYMBOL_YYMMDD_STRIKEC/P.FOP
            #     e.g.  CL_260622_75C.FOP   ZW_260626_410P.FOP
            _OPT_RE = re.compile(r'^([A-Z0-9]+)_(\d{6}|\d{8})_([\d.]+)([CP])\.US$')
            _FOP_RE = re.compile(r'^([A-Z0-9]+)_(\d{6}|\d{8})_([\d.]+)([CP])\.FOP$')

            # IBKR statuses that mean the order is finished. Anything
            # else (PendingSubmit, PreSubmitted, Submitted, ...) is a
            # working order from a previous attempt.
            _TERMINAL_STATUSES = frozenset(
                {"Filled", "Cancelled", "ApiCancelled", "Inactive"}
            )

            def __init__(self, *, ib, portfolio_id: str, as_of_date: date) -> None:
                self._ib = ib
                self._portfolio_id = portfolio_id
                self._as_of_date = as_of_date

            def _order_ref(self, order, *, underlying, right, expiry, strike) -> str:
                from prometheus.derivatives.order_refs import (
                    deterministic_option_order_ref,
                )
                strategy = str((order.metadata or {}).get("strategy", "") or "")
                return deterministic_option_order_ref(
                    portfolio_id=self._portfolio_id,
                    strategy=strategy,
                    underlying=underlying,
                    right=right,
                    expiry=expiry,
                    strike=strike,
                    side=order.side.value,
                    as_of_date=self._as_of_date,
                )

            def _find_working_trade(self, order_ref: str):
                """Return a non-terminal trade already carrying this
                orderRef, or None. Best-effort: a scan failure logs and
                falls through to submission (``trades()`` is a local
                list in ib_insync/ib_async, so this should not happen)."""
                try:
                    trades = self._ib.trades()
                except Exception as exc:
                    logger.warning(
                        "open-order scan failed (submitting anyway): %s", exc,
                    )
                    return None
                for t in trades:
                    ref = getattr(getattr(t, "order", None), "orderRef", None)
                    if ref != order_ref:
                        continue
                    status = str(getattr(
                        getattr(t, "orderStatus", None), "status", "",
                    ) or "")
                    if status not in self._TERMINAL_STATUSES:
                        return t
                return None

            def submit_order(self, order) -> str:
                from prometheus.execution.broker_interface import OrderSide, OrderType
                from prometheus.execution.futures_option_specs import get_fop_spec
                from prometheus.execution.ib_compat import (
                    FuturesOption,
                    LimitOrder,
                    MarketOrder,
                    Option,
                )

                fop_match = self._FOP_RE.match(order.instrument_id)
                m = self._OPT_RE.match(order.instrument_id)
                if not m and not fop_match:
                    raise ValueError(
                        f"_IbkrDirectBroker cannot parse instrument_id: "
                        f"{order.instrument_id!r}  (expected SYMBOL_YYMMDD_STRIKE[CP].US "
                        f"or SYMBOL_YYMMDD_STRIKE[CP].FOP)"
                    )

                # Deterministic ref + duplicate check, shared by both
                # paths. Computed from the *directive-level* contract
                # fields (before any qualification fallback) so retries
                # of the same cycle always reproduce the same ref.
                _match = fop_match or m
                _exp_raw = _match.group(2)
                order_ref = self._order_ref(
                    order,
                    underlying=_match.group(1),
                    right=_match.group(4),
                    expiry="20" + _exp_raw if len(_exp_raw) == 6 else _exp_raw,
                    strike=float(_match.group(3)),
                )
                working = self._find_working_trade(order_ref)
                if working is not None:
                    logger.warning(
                        "[IBKR] SKIP %s %s x%d — order with ref=%s already "
                        "working from a previous attempt (status=%s)",
                        order.side.value, order.instrument_id,
                        int(order.quantity), order_ref,
                        getattr(getattr(working, "orderStatus", None), "status", "?"),
                    )
                    return order_ref

                # ── FOP path (commodity futures options) ──────────────
                if fop_match:
                    fop_symbol = fop_match.group(1)
                    fop_exp_raw = fop_match.group(2)
                    fop_expiry = "20" + fop_exp_raw if len(fop_exp_raw) == 6 else fop_exp_raw
                    fop_strike = float(fop_match.group(3))
                    fop_right = fop_match.group(4)

                    spec = get_fop_spec(fop_symbol)
                    if spec is None:
                        raise RuntimeError(
                            f"No FOP spec registered for {fop_symbol} "
                            f"(see prometheus.execution.futures_option_specs)"
                        )

                    contract = FuturesOption(
                        symbol=spec.symbol,
                        lastTradeDateOrContractMonth=fop_expiry,
                        strike=fop_strike,
                        right=fop_right,
                        exchange=spec.exchange,
                        currency=spec.currency,
                        multiplier=spec.multiplier,
                        tradingClass=spec.trading_class,
                    )
                    qualified = self._ib.qualifyContracts(contract)
                    contract = qualified[0] if qualified else None
                    if not contract or not getattr(contract, "conId", 0):
                        raise RuntimeError(
                            f"Could not qualify FOP contract for {order.instrument_id} "
                            f"(spec exchange={spec.exchange} tc={spec.trading_class} "
                            f"mult={spec.multiplier})"
                        )

                    action = "BUY" if order.side == OrderSide.BUY else "SELL"
                    qty = int(order.quantity)
                    if order.order_type == OrderType.LIMIT and order.limit_price is not None:
                        ib_order = LimitOrder(action, qty, round(order.limit_price, 2))
                    else:
                        ib_order = MarketOrder(action, qty)
                    ib_order.tif = "DAY"
                    ib_order.orderRef = order_ref
                    trade = self._ib.placeOrder(contract, ib_order)
                    logger.info(
                        "[IBKR] Placed FOP %s %s x%d (orderId=%s ref=%s)",
                        action, order.instrument_id, qty,
                        trade.order.orderId, order_ref,
                    )
                    return str(trade.order.orderId)

                # ── Equity option path (legacy) ───────────────────────
                symbol = m.group(1)
                exp_raw = m.group(2)
                # Accept both YYMMDD (6 digits) and YYYYMMDD (8 digits)
                expiry = "20" + exp_raw if len(exp_raw) == 6 else exp_raw
                strike = float(m.group(3))
                right = m.group(4)  # 'C' or 'P'

                # VIX index options trade on CBOE (not CFE which is for VX futures).
                # Everything else routes through SMART.
                exchange = "CBOE" if symbol == "VIX" else "SMART"

                # VIX options on CBOE require multiplier=100 to uniquely identify
                # the contract (avoids ambiguity with VIX mini-options).
                # IBKR lastTradeDateOrContractMonth = settlement_wednesday - 1 day.
                # Our formula may compute the settlement date rather than the
                # last-trade date, so we try the given expiry and also expiry-1
                # as a fallback (handles the off-by-one seen in some months).
                if symbol == "VIX":
                    from datetime import datetime as _dt
                    from datetime import timedelta as _td
                    _expiry_attempts = [
                        expiry,
                        (_dt.strptime(expiry, "%Y%m%d").date() - _td(days=1)).strftime("%Y%m%d"),
                    ]
                    contract = None
                    for _try_expiry in _expiry_attempts:
                        _c = Option(
                            symbol=symbol,
                            lastTradeDateOrContractMonth=_try_expiry,
                            strike=strike,
                            right=right,
                            exchange="CBOE",
                            currency="USD",
                            multiplier="100",
                        )
                        _q = self._ib.qualifyContracts(_c)
                        _qualified = _q[0] if _q else None
                        if _qualified and getattr(_qualified, "conId", 0):
                            contract = _qualified
                            logger.debug("VIX contract qualified with expiry=%s", _try_expiry)
                            break
                    if not contract:
                        raise RuntimeError(
                            f"Could not qualify VIX contract for {order.instrument_id} "
                            f"(tried expiries: {_expiry_attempts})"
                        )
                else:
                    contract = Option(
                        symbol=symbol,
                        lastTradeDateOrContractMonth=expiry,
                        strike=strike,
                        right=right,
                        exchange=exchange,
                        currency="USD",
                    )
                    qualified = self._ib.qualifyContracts(contract)
                    # ib_async may return [None] (not []) when Error 200 fires;
                    # guard against both empty list and None element.
                    contract = qualified[0] if qualified else None
                    if not contract or not getattr(contract, "conId", 0):
                        raise RuntimeError(
                            f"Could not qualify IBKR contract for {order.instrument_id} "
                            f"(no conId — check symbol, expiry, strike, exchange)"
                        )

                action = "BUY" if order.side == OrderSide.BUY else "SELL"
                qty = int(order.quantity)

                if order.order_type == OrderType.LIMIT and order.limit_price is not None:
                    ib_order = LimitOrder(action, qty, round(order.limit_price, 2))
                else:
                    ib_order = MarketOrder(action, qty)

                ib_order.tif = "DAY"
                ib_order.orderRef = order_ref

                # ib_async placeOrder accesses contract.secIdType; if the
                # field is None (contract qualified but field not set),
                # it raises AttributeError.  Guard against that here.
                if getattr(contract, "secIdType", None) is None:
                    contract.secIdType = ""

                trade = self._ib.placeOrder(contract, ib_order)
                logger.info(
                    "[IBKR] Placed %s %s x%d @ %s (orderId=%s ref=%s)",
                    action, order.instrument_id, qty,
                    order.limit_price, trade.order.orderId, order_ref,
                )
                return str(trade.order.orderId)

            def cancel_order(self, order_id):
                """Cancel a working order by orderId (or orderRef, for the
                skip-path where submit_order returned the ref of an
                already-working order). Returns True when a cancel was
                issued — used by the naked-leg guard when a spread leg
                fails after its sibling was submitted."""
                try:
                    wanted = str(order_id)
                    for t in self._ib.trades():
                        o = getattr(t, "order", None)
                        if o is None:
                            continue
                        if str(getattr(o, "orderId", "")) != wanted and \
                                str(getattr(o, "orderRef", "")) != wanted:
                            continue
                        status = str(getattr(
                            getattr(t, "orderStatus", None), "status", "",
                        ) or "")
                        if status in self._TERMINAL_STATUSES:
                            logger.warning(
                                "[IBKR] cancel_order(%s): order already "
                                "terminal (status=%s) — nothing to cancel",
                                order_id, status,
                            )
                            return False
                        self._ib.cancelOrder(o)
                        logger.warning(
                            "[IBKR] Cancel issued for orderId=%s ref=%s",
                            getattr(o, "orderId", "?"),
                            getattr(o, "orderRef", ""),
                        )
                        return True
                    logger.warning(
                        "[IBKR] cancel_order(%s): no matching trade found",
                        order_id,
                    )
                except Exception as exc:
                    logger.error(
                        "[IBKR] cancel_order(%s) failed: %s",
                        order_id, exc, exc_info=True,
                    )
                return False

            def get_positions(self):
                return positions

            def get_order_status(self, order_id):
                return None

            def get_account_state(self):
                return account_state

            def get_fills(self, since=None):
                return []

            def sync(self):
                pass

        # evaluate_all always runs with dry_run=True so it never submits.
        # Actual submission is done below in Step 8 after risk checks.
        broker = _StubBroker()

        strategy_mgr = OptionsStrategyManager(
            broker=broker,
            mapper=mapper,
            discovery=discovery,
            dry_run=True,  # always — real submission handled in Step 8 after risk checks
        )

        all_directives = strategy_mgr.evaluate_all(
            portfolio=positions,
            signals=signals,
            existing_options=existing_options,
            allocations=allocations,
        )

        summary["strategy_directives"] = len(all_directives)
        summary["steps_completed"].append("run_strategies")

        # Phase 2.7: silence legacy strategies for any sleeves that
        # have been cut over to the new pipeline. The new sleeve's
        # directives get re-added below from the shadow pass results.
        _cutover_state = None
        try:
            from prometheus.derivatives.allocator import (
                SleeveCutoverState,
                silenced_strategies,
            )
            from prometheus.derivatives.sleeves import Sleeve

            _cutover_state = SleeveCutoverState.from_env()
            _silenced = silenced_strategies(_cutover_state)
            if _silenced:
                kept, dropped = _filter_silenced_directives(all_directives, _silenced)
                if dropped:
                    logger.info(
                        "cutover: silenced %d directives from legacy strategies %s",
                        len(dropped),
                        sorted({getattr(d, "strategy", "") for d in dropped}),
                    )
                all_directives = kept
                active = [
                    s.value for s in Sleeve if _cutover_state.is_active(s)
                ]
                summary["cutover"] = {
                    "active_sleeves": sorted(active),
                    "silenced_legacy_strategies": sorted(_silenced),
                    "dropped_directives": len(dropped),
                }
        except Exception as exc:
            _record_warning(summary, "cutover_silence", exc)

        # ── Step 6.5: Position lifecycle management ─────────────────
        lifecycle = PositionLifecycleManager()
        lifecycle_directives = lifecycle.evaluate(
            positions=existing_options,
            signals=signals,
        )
        all_directives.extend(lifecycle_directives)

        summary["lifecycle_directives"] = len(lifecycle_directives)
        summary["steps_completed"].append("lifecycle_management")

        # ── Step 6.6: Shadow-mode derivatives runner ─────────────────
        # Gated by PROMETHEUS_DERIVATIVES_SHADOW. Read-only by default;
        # when a sleeve is in cutover, its directives are converted and
        # injected into all_directives so they go through the same
        # risk-check + submission path as the legacy directives.
        _shadow_results: list = []
        if _shadow_enabled():
            try:
                _shadow_results = _run_shadow_pass(
                    ib=ib,
                    discovery=discovery,
                    signals=signals,
                    existing_options=existing_options,
                    as_of_date=date.today(),
                    summary=summary,
                    portfolio_id=options_portfolio_id,
                )
                summary["steps_completed"].append("shadow_pass")
            except Exception as exc:
                _record_warning(summary, "shadow_pass", exc)

        # Phase 2.7 (b): inject directives from cut-over sleeves into
        # all_directives so they're submitted alongside any surviving
        # legacy directives. Only sleeves marked active in the cutover
        # state contribute; the rest stay shadow-only.
        if _cutover_state is not None and _shadow_results:
            try:
                cutover_sleeve_results = [
                    r for r in _shadow_results
                    if _cutover_state.is_active(r.sleeve)
                ]
                if cutover_sleeve_results:
                    new_legacy_directives = _sleeve_directives_to_legacy(
                        cutover_sleeve_results,
                    )
                    all_directives.extend(new_legacy_directives)
                    summary.setdefault("cutover", {})["new_pipeline_directives"] = \
                        len(new_legacy_directives)
                    logger.info(
                        "cutover: injected %d new-pipeline directives into submission",
                        len(new_legacy_directives),
                    )
            except Exception as exc:
                _record_warning(summary, "cutover_inject", exc)

        # ── Step 7: Risk checks (with greeks budget) ─────────────────
        portfolio_greeks = options_portfolio.compute_portfolio_greeks()
        margin_snapshot = futures_mgr.margin

        # Check greeks budget
        greeks_util = options_portfolio.check_greeks_budget(
            nav=signals.get("nav", 0.0),
        )
        summary["greeks_within_budget"] = greeks_util.within_budget

        approved = _apply_risk_checks(
            all_directives,
            margin_snapshot,
            portfolio_greeks,
            max_margin_util=max_margin_util,
        )

        summary["approved_directives"] = len(approved)
        summary["blocked_directives"] = len(all_directives) - len(approved)
        summary["steps_completed"].append("risk_checks")

        # ── Step 8: Submit (or log) ───────────────────────────────────
        if dry_run:
            logger.info("=== DRY RUN — %d directives would be submitted ===", len(approved))
            for d in approved:
                logger.info(
                    "  [DRY] %s %s %s %s %.1f x%d — %s",
                    d.strategy, d.action.value, d.symbol, d.right,
                    d.strike, d.quantity, d.reason,
                )
        else:
            logger.info("Submitting %d approved directives via IBKR...", len(approved))
            # Swap in the real broker so _submit_directives routes to IBKR.
            strategy_mgr._broker = _IbkrDirectBroker(
                ib=ib,
                portfolio_id=account or options_portfolio_id,
                as_of_date=date.today(),
            )
            # Persist strategy provenance per submitted order so the
            # next sync can restore tags (see _make_submission_recorder).
            strategy_mgr._submission_recorder = _make_submission_recorder(
                portfolio_id=options_portfolio_id,
                mode=trading_mode,
                as_of_date=date.today(),
                summary=summary,
            )
            # From here on a retry could double-submit — every later
            # failure must be a warning, not an error (see taxonomy note).
            orders_submitted = bool(approved)
            submission_failures = strategy_mgr._submit_directives(approved)
            # Leg failures (including naked-leg cancels) surface in the
            # run summary — at/after submission, so warnings by taxonomy.
            for msg in submission_failures:
                summary.setdefault("warnings", []).append(f"submission: {msg}")
            summary["submission_failures"] = len(submission_failures)

            # ── Log options decisions to DecisionTracker ──────────────────
            if approved:
                try:
                    from apatheon.core.database import get_db_manager

                    from prometheus.decisions.tracker import DecisionTracker

                    # Map underlying symbol → canonical instrument ID for price lookups
                    _UNDERLYING_MAP: Dict[str, str] = {
                        "VIX": "VIX.INDX",
                        "SPY": "SPY.US",
                        "QQQ": "QQQ.US",
                        "IWM": "IWM.US",
                        "EFA": "EFA.US",
                        "TLT": "TLT.US",
                        "GLD": "GLD.US",
                        "ES": "ES.CME",
                    }

                    orders_for_log = []
                    for d in approved:
                        underlying_id = _UNDERLYING_MAP.get(d.symbol, f"{d.symbol}.US")
                        # Instrument ID: SYMBOL_YYMMDD_STRIKEC/P.US
                        exp_short = d.expiry[2:] if len(d.expiry) == 8 else d.expiry
                        instrument_id = f"{d.symbol}_{exp_short}_{d.strike:.0f}{d.right}.US"
                        # Skip directives without a real limit price.  Logging
                        # entry_price=0 here corrupts the decision-outcomes
                        # P&L attribution — the evaluator treats it as "sold
                        # for zero credit" and synthesises catastrophic
                        # losses on every SELL leg.  See May 2026 audit:
                        # 58 legs with entry_price=0 produced -$856K of
                        # fictitious P&L.  Fix the upstream strategy to
                        # always set d.limit_price (market orders should
                        # capture the chain mid at decision time).
                        entry_px = d.limit_price
                        if not entry_px or entry_px <= 0:
                            logger.warning(
                                "[options.log] skipping decision log for "
                                "%s %s %s strike=%s qty=%s: missing limit_price "
                                "(fix the directive emitter so this never happens)",
                                d.symbol, d.right, d.action.value, d.strike, d.quantity,
                            )
                            continue
                        orders_for_log.append({
                            "symbol": d.symbol,
                            "underlying_id": underlying_id,
                            "instrument_id": instrument_id,
                            "right": d.right,
                            "expiry": d.expiry,
                            "strike": d.strike,
                            "action": "BUY" if d.quantity > 0 else "SELL",
                            "quantity": abs(d.quantity),
                            "entry_price": float(entry_px),
                            "strategy": d.strategy,
                            "reason": d.reason,
                            "trade_action": d.action.value,
                        })

                    signals_snap = {
                        "vix_level": signals.get("vix_level"),
                        "nav": signals.get("nav"),
                        "mhi": signals.get("mhi"),
                        "frag": signals.get("frag"),
                        "market_state": signals.get("market_state"),
                    }

                    tracker = DecisionTracker(db_manager=get_db_manager())
                    tracker.record_options_decision(
                        strategy_id="US_OPTIONS",
                        market_id="US_EQ",
                        as_of_date=date.today(),
                        orders=orders_for_log,
                        signals_snapshot=signals_snap,
                    )
                except Exception as exc:
                    _record_warning(summary, "decision_tracker", exc)

        summary["steps_completed"].append("submit_orders")

        # ── Step 8.5: Daily diff report (Phase 2.6) ──────────────────
        # Compares what the new sleeve pipeline would have done with
        # what the legacy strategies actually did, writes to briefs.
        # Best-effort — never breaks the live pipeline.
        if _shadow_enabled():
            _write_daily_diff_report(as_of_date=date.today(), summary=summary)

        # ── Step 9: Portfolio status ──────────────────────────────────
        status = options_portfolio.get_status()
        summary["portfolio_status"] = status
        summary["futures_positions"] = len(futures_mgr.get_all_positions())
        summary["futures_notional"] = futures_mgr.get_total_notional()

        logger.info(
            "Derivatives daily complete: %d strategy directives, "
            "%d approved, %d roll orders, NAV=$%.0f",
            len(all_directives), len(approved), len(roll_orders),
            signals["nav"],
        )

    except Exception as exc:
        if orders_submitted:
            # Orders already went to IBKR on this attempt — a daemon
            # retry would re-evaluate and re-submit, so this must NOT
            # surface as a fatal error.
            _record_warning(summary, "post_submission", exc)
        else:
            logger.error("Derivatives daily failed: %s", exc, exc_info=True)
            summary["errors"].append(str(exc))
    finally:
        try:
            ib.disconnect()
            logger.info("Disconnected from IBKR")
        except Exception:
            pass

    return summary


# ── CLI ───────────────────────────────────────────────────────────────

def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run the daily derivatives pipeline (options, futures, FOP).",
    )
    parser.add_argument(
        "--host", type=str, default="127.0.0.1",
        help="IBKR host (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port", type=int, default=None,
        help="IBKR port (default: 4002 for paper, 4001 for live)",
    )
    parser.add_argument(
        "--client-id", type=int, default=10,
        help="IBKR client ID (default: 10)",
    )
    parser.add_argument(
        "--account", type=str, default="",
        help="IBKR account ID (optional)",
    )
    parser.add_argument(
        "--paper", action="store_true",
        help="Use paper trading port (4002)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log directives without submitting orders",
    )
    parser.add_argument(
        "--max-margin-util", type=float, default=0.60,
        help="Maximum margin utilisation threshold (default: 0.60)",
    )

    args = parser.parse_args(argv)

    # Resolve port
    if args.port is not None:
        port = args.port
    elif args.paper:
        port = 4002
    else:
        port = 4001

    dry_run = args.dry_run

    summary = run_derivatives_daily(
        host=args.host,
        port=port,
        client_id=args.client_id,
        account=args.account,
        dry_run=dry_run,
        max_margin_util=args.max_margin_util,
    )

    # Print summary
    print(f"\n{'='*60}")
    print(f"Derivatives Daily Summary — {summary['date']}")
    print(f"{'='*60}")
    print(f"  Mode:                {summary.get('trading_mode', '?')}"
          f"{' (DRY RUN)' if summary['dry_run'] else ''}")
    print(f"  Steps completed:     {', '.join(summary['steps_completed'])}")
    print(f"  Positions synced:    {summary.get('position_count', 'N/A')}")
    print(f"  NAV:                 ${summary.get('nav', 0):,.0f}")
    print(f"  VIX:                 {summary.get('vix', 'N/A')}")
    print(f"  ES Price:            {summary.get('es_price', 'N/A')}")
    print(f"  Market situation:    {summary.get('market_situation', 'N/A')}")
    print(f"  Strategies enabled:  {summary.get('strategies_enabled', 'N/A')}")
    print(f"  Roll directives:     {summary.get('roll_directives', 0)}")
    print(f"  Strategy directives: {summary.get('strategy_directives', 0)}")
    print(f"  Lifecycle directives:{summary.get('lifecycle_directives', 0)}")
    print(f"  Approved:            {summary.get('approved_directives', 0)}")
    print(f"  Blocked by risk:     {summary.get('blocked_directives', 0)}")
    print(f"  Greeks within budget:{summary.get('greeks_within_budget', 'N/A')}")
    print(f"  Futures positions:   {summary.get('futures_positions', 0)}")
    if summary.get("warnings"):
        print(f"  WARNINGS (no retry): {summary['warnings']}")
    if summary["errors"]:
        print(f"  ERRORS:              {summary['errors']}")
    print(f"{'='*60}")

    # Exit non-zero on FATAL errors only — warnings are post-submission
    # failures where a retry could double-submit orders.
    if summary["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
