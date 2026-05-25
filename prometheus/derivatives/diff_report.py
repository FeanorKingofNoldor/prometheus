"""Daily shadow-vs-legacy diff report.

After Phase 1d wired shadow mode and Phase 2 started building real
sleeves, every trading day produces:

* a set of rows in ``derivatives_shadow_decisions`` — what the new
  pipeline *would* have done, and
* a set of rows in ``engine_decisions`` (engine_name='OPTIONS') —
  what the legacy strategies actually submitted.

This module joins the two and emits a markdown report classifying
each pair as ``both`` / ``new_only`` / ``legacy_only``, with strike
divergence counts and IV-source distribution. Phase 2.8 reviews this
report to decide when each sleeve is ready to cut over.

Designed to be runnable both as a script (writes to ``/app/briefs``
on the prod box) and as a library (returns markdown for further
processing in the iris workflow).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger

from prometheus.derivatives.backtest import (
    DiffEntry,
    DiffSummary,
    LegacyOption,
    _default_template_to_strategy,
)

logger = get_logger(__name__)


# ── Row containers ───────────────────────────────────────────────────


@dataclass(frozen=True)
class ShadowDecisionRow:
    """One row loaded from ``derivatives_shadow_decisions``."""

    decision_id: int
    as_of_date: date
    sleeve: str
    template_name: str
    kind: str                       # DIRECTIVE | SKIP
    underlying: str | None
    right: str | None
    strike: float | None
    expiry: str | None
    quantity: int | None
    limit_price: float | None
    iv_used: float | None
    iv_source: str | None
    delta: float | None
    reason: str
    skip_reason: str | None


# ── Loaders ──────────────────────────────────────────────────────────


def load_shadow_decisions(
    db_manager: DatabaseManager, as_of_date: date,
) -> list[ShadowDecisionRow]:
    """Pull every shadow row for a date."""
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT decision_id, as_of_date, sleeve, template_name, kind,
                       underlying, right, strike, expiry, quantity,
                       limit_price, iv_used, iv_source, delta,
                       reason, skip_reason
                FROM derivatives_shadow_decisions
                WHERE as_of_date = %s
                ORDER BY sleeve, template_name, decision_id
                """,
                (as_of_date,),
            )
            return [
                ShadowDecisionRow(
                    decision_id=int(r[0]), as_of_date=r[1],
                    sleeve=str(r[2]), template_name=str(r[3]), kind=str(r[4]),
                    underlying=r[5], right=r[6],
                    strike=float(r[7]) if r[7] is not None else None,
                    expiry=r[8],
                    quantity=int(r[9]) if r[9] is not None else None,
                    limit_price=float(r[10]) if r[10] is not None else None,
                    iv_used=float(r[11]) if r[11] is not None else None,
                    iv_source=r[12],
                    delta=float(r[13]) if r[13] is not None else None,
                    reason=str(r[14] or ""), skip_reason=r[15],
                )
                for r in cur.fetchall()
            ]


def load_legacy_options_decisions(
    db_manager: DatabaseManager, as_of_date: date,
) -> list[LegacyOption]:
    """Extract legacy options orders from engine_decisions.

    Each ``engine_decisions`` row of ``engine_name='OPTIONS'`` carries
    a list of orders inside ``output_refs->orders``; we explode them
    into one ``LegacyOption`` per order.
    """
    out: list[LegacyOption] = []
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT output_refs
                FROM engine_decisions
                WHERE engine_name = 'OPTIONS' AND as_of_date = %s
                """,
                (as_of_date,),
            )
            for (output_refs,) in cur.fetchall():
                orders = (output_refs or {}).get("orders") or []
                for o in orders:
                    qty = int(o.get("quantity", 0) or 0)
                    if o.get("action", "").upper() == "SELL":
                        qty = -qty
                    out.append(LegacyOption(
                        as_of_date=as_of_date,
                        symbol=str(o.get("symbol", "")),
                        right=str(o.get("right", "")).upper(),
                        strike=float(o.get("strike", 0.0) or 0.0),
                        expiry=str(o.get("expiry", "")),
                        quantity=qty,
                        strategy=str(o.get("strategy", "")),
                    ))
    return out


# ── Diff (DB-source variant) ─────────────────────────────────────────


def diff_decisions(
    *,
    shadow: Iterable[ShadowDecisionRow],
    legacy: Iterable[LegacyOption],
    template_to_strategy: Mapping[str, str] | None = None,
) -> DiffSummary:
    """Pair shadow directives with legacy options orders by (date,
    sleeve mapping, symbol, right). Unmatched entries land in
    ``new_only`` or ``legacy_only`` buckets.
    """
    mapping = dict(template_to_strategy or _default_template_to_strategy())
    legacy_idx: dict[tuple[str, str, str], LegacyOption] = {}
    for lo in legacy:
        legacy_idx[(lo.strategy, lo.symbol.upper(), lo.right.upper())] = lo

    entries: list[DiffEntry] = []
    seen_legacy: set[tuple[str, str, str]] = set()

    for row in shadow:
        if row.kind != "DIRECTIVE":
            continue
        legacy_strategy = mapping.get(row.template_name, "")
        key = (
            legacy_strategy,
            (row.underlying or "").upper(),
            (row.right or "").upper(),
        )
        legacy_match = legacy_idx.get(key)
        if legacy_match is not None:
            seen_legacy.add(key)

        # Build a SleeveDirective-shaped object for DiffEntry; only the
        # fields downstream code reads matter.
        synthetic = _SyntheticShadowDirective(
            template_name=row.template_name,
            underlying=row.underlying or "",
            right=row.right or "",
            strike=row.strike or 0.0,
            expiry=row.expiry or "",
            quantity=row.quantity or 0,
        )
        entries.append(DiffEntry(
            as_of_date=row.as_of_date,
            template_or_strategy=row.template_name,
            new_side=synthetic,            # type: ignore[arg-type]
            legacy_side=legacy_match,
        ))

    for key, lo in legacy_idx.items():
        if key in seen_legacy:
            continue
        entries.append(DiffEntry(
            as_of_date=lo.as_of_date,
            template_or_strategy=lo.strategy,
            new_side=None,
            legacy_side=lo,
        ))

    return DiffSummary(entries=entries)


@dataclass(frozen=True)
class _SyntheticShadowDirective:
    """Minimal stand-in for ``SleeveDirective`` so DiffSummary can
    compute strike divergence without round-tripping through the
    full directive object."""
    template_name: str
    underlying: str
    right: str
    strike: float
    expiry: str
    quantity: int


# ── Markdown rendering ───────────────────────────────────────────────


def format_diff_report(
    *,
    as_of_date: date,
    shadow_rows: list[ShadowDecisionRow],
    legacy_decisions: list[LegacyOption],
    template_to_strategy: Mapping[str, str] | None = None,
) -> str:
    diff = diff_decisions(
        shadow=shadow_rows, legacy=legacy_decisions,
        template_to_strategy=template_to_strategy,
    )

    by_kind = diff.by_kind
    n_strike_div = diff.strike_divergence_count

    n_directives = sum(1 for r in shadow_rows if r.kind == "DIRECTIVE")
    n_skips = sum(1 for r in shadow_rows if r.kind == "SKIP")
    iv_sources: dict[str, int] = {}
    for r in shadow_rows:
        if r.kind == "DIRECTIVE" and r.iv_source:
            iv_sources[r.iv_source] = iv_sources.get(r.iv_source, 0) + 1

    lines: list[str] = []
    lines.append(f"# Derivatives shadow-vs-legacy diff — {as_of_date.isoformat()}")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- Shadow directives: **{n_directives}**")
    lines.append(f"- Shadow skips: **{n_skips}**")
    lines.append(f"- Legacy options orders: **{len(legacy_decisions)}**")
    lines.append("")
    lines.append("## Reconciliation")
    lines.append(f"- Both fired: **{by_kind.get('both', 0)}**")
    lines.append(f"- Strike divergence (both fired, different strikes): **{n_strike_div}**")
    lines.append(f"- New pipeline only: **{by_kind.get('new_only', 0)}**")
    lines.append(f"- Legacy only: **{by_kind.get('legacy_only', 0)}**")
    lines.append("")
    if iv_sources:
        lines.append("## IV source distribution (shadow directives)")
        for src, n in sorted(iv_sources.items(), key=lambda x: -x[1]):
            lines.append(f"- `{src}`: {n}")
        lines.append("")

    # Per-entry detail
    lines.append("## Per-entry detail")
    if not diff.entries:
        lines.append("_No entries today._")
    else:
        lines.append("")
        lines.append("| Status | Template/Strategy | Symbol | Right | Strike (new) | Strike (legacy) | Qty (new) | Qty (legacy) |")
        lines.append("|---|---|---|---|---|---|---|---|")
        for e in diff.entries:
            new_str = e.new_side.strike if e.new_side else "—"
            legacy_str = e.legacy_side.strike if e.legacy_side else "—"
            new_qty = e.new_side.quantity if e.new_side else "—"
            legacy_qty = e.legacy_side.quantity if e.legacy_side else "—"
            symbol = (
                e.new_side.underlying if e.new_side
                else (e.legacy_side.symbol if e.legacy_side else "?")
            )
            right = (
                e.new_side.right if e.new_side
                else (e.legacy_side.right if e.legacy_side else "?")
            )
            kind_marker = {
                "both": "BOTH",
                "new_only": "NEW ONLY",
                "legacy_only": "LEGACY ONLY",
            }.get(e.kind, e.kind)
            lines.append(
                f"| {kind_marker} | `{e.template_or_strategy}` | {symbol} | "
                f"{right} | {new_str} | {legacy_str} | {new_qty} | {legacy_qty} |"
            )

    lines.append("")
    return "\n".join(lines)


def run_daily_diff(
    db_manager: DatabaseManager,
    as_of_date: date,
    *,
    template_to_strategy: Mapping[str, str] | None = None,
) -> str:
    """Convenience: load both sides + format the report in one call."""
    shadow = load_shadow_decisions(db_manager, as_of_date)
    legacy = load_legacy_options_decisions(db_manager, as_of_date)
    return format_diff_report(
        as_of_date=as_of_date,
        shadow_rows=shadow,
        legacy_decisions=legacy,
        template_to_strategy=template_to_strategy,
    )


__all__ = [
    "ShadowDecisionRow",
    "load_shadow_decisions",
    "load_legacy_options_decisions",
    "diff_decisions",
    "format_diff_report",
    "run_daily_diff",
]
