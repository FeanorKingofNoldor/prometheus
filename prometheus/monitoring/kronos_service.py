"""Prometheus v2 – Kronos Service.

Assembles system context from the database and drives the LLM
conversation for the Kronos chat endpoint.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from apathis.llm.gateway import get_llm

logger = get_logger(__name__)

# ── System prompt ────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are **Kronos**, the meta-orchestrator of the Prometheus v2 trading system.

Your capabilities:
- Explain the system's current state (regime, portfolio, orders, risk).
- Propose backtests, configuration changes, or experiments.
- Analyse engine performance across regimes.
- Answer questions about Prometheus architecture and data flow.

You CANNOT directly execute changes — all actions require explicit user
approval via the Control API.

When you propose an action, format it clearly with a short description,
risk level (LOW / MODERATE / HIGH), and the parameters involved.

Always ground your answers in the **system context** provided below.
If you don't have enough data, say so honestly.
"""


# ── Context assembly ─────────────────────────────────────────────────


def _fetch_regime_context() -> str:
    """Latest regime state across regions."""
    db = get_db_manager()
    try:
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT region, regime_label, confidence, as_of_date
                FROM regimes
                ORDER BY as_of_date DESC, region
                LIMIT 10
                """
            )
            rows = cur.fetchall()
            cur.close()
        if not rows:
            return "Regime: no data available."
        lines = [f"- {r[0]}: {r[1]} (confidence={r[2]:.2f}, as_of={r[3]})" for r in rows]
        return "Current regimes:\n" + "\n".join(lines)
    except Exception as exc:
        logger.warning("[kronos] Failed to fetch regime context: %s", exc)
        return "Regime: unavailable."


def _fetch_portfolio_context(portfolio_id: str = "IBKR_PAPER") -> str:
    """Recent positions snapshot."""
    db = get_db_manager()
    try:
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT instrument_id, quantity, market_value, unrealized_pnl
                FROM positions_snapshots
                WHERE portfolio_id = %s
                ORDER BY as_of_date DESC, instrument_id
                LIMIT 20
                """,
                (portfolio_id,),
            )
            rows = cur.fetchall()
            cur.close()
        if not rows:
            return f"Portfolio ({portfolio_id}): no positions."
        total_mv = sum(r[2] or 0 for r in rows)
        total_pnl = sum(r[3] or 0 for r in rows)
        lines = [f"- {r[0]}: qty={r[1]}, mv=${r[2]:,.0f}, pnl=${r[3]:,.0f}" for r in rows]
        header = f"Portfolio {portfolio_id}: {len(rows)} positions, total MV=${total_mv:,.0f}, unrealised PnL=${total_pnl:,.0f}"
        return header + "\n" + "\n".join(lines)
    except Exception as exc:
        logger.warning("[kronos] Failed to fetch portfolio context: %s", exc)
        return f"Portfolio ({portfolio_id}): unavailable."


def _fetch_orders_context(portfolio_id: str = "IBKR_PAPER", limit: int = 10) -> str:
    """Recent orders."""
    db = get_db_manager()
    try:
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT instrument_id, side, quantity, status, timestamp
                FROM orders
                WHERE portfolio_id = %s
                ORDER BY timestamp DESC
                LIMIT %s
                """,
                (portfolio_id, limit),
            )
            rows = cur.fetchall()
            cur.close()
        if not rows:
            return "Recent orders: none."
        lines = [f"- {r[0]} {r[1]} {r[2]} status={r[3]} @ {r[4]}" for r in rows]
        return "Recent orders:\n" + "\n".join(lines)
    except Exception as exc:
        logger.warning("[kronos] Failed to fetch orders context: %s", exc)
        return "Recent orders: unavailable."


def _fetch_fragility_context() -> str:
    """Top fragility scores."""
    db = get_db_manager()
    try:
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT entity_id, fragility_score, as_of_date
                FROM fragility_measures
                ORDER BY as_of_date DESC, fragility_score DESC
                LIMIT 10
                """
            )
            rows = cur.fetchall()
            cur.close()
        if not rows:
            return "Fragility: no data."
        lines = [f"- {r[0]}: score={r[1]:.3f} (as_of={r[2]})" for r in rows]
        return "Top fragility:\n" + "\n".join(lines)
    except Exception as exc:
        logger.warning("[kronos] Failed to fetch fragility context: %s", exc)
        return "Fragility: unavailable."


def _fetch_intel_context() -> str:
    """Latest intel SITREP + flash alerts for Kronos context."""
    try:
        from apathis.intel.store import get_briefs

        # Latest daily SITREP
        sitreps = get_briefs(brief_type="daily_sitrep", limit=1)
        sitrep_text = ""
        if sitreps:
            s = sitreps[0]
            sitrep_text = f"Latest SITREP ({s.get('created_at', '?')}):\n{s.get('summary', 'N/A')}"

        # Active flash alerts
        alerts = get_briefs(brief_type="flash_alert", limit=5)
        alert_text = ""
        if alerts:
            lines = [f"- [{a.get('severity', '?').upper()}] {a.get('title', '?')}" for a in alerts]
            alert_text = "Active flash alerts:\n" + "\n".join(lines)

        parts = [p for p in [sitrep_text, alert_text] if p]
        return "\n\n".join(parts) if parts else "Intel: no briefs generated yet."
    except Exception as exc:
        logger.warning("[kronos] Failed to fetch intel context: %s", exc)
        return "Intel: unavailable."


def build_system_context() -> str:
    """Assemble a combined context string from all sources."""
    sections = [
        _fetch_regime_context(),
        _fetch_portfolio_context(),
        _fetch_orders_context(),
        _fetch_fragility_context(),
        _fetch_intel_context(),
    ]
    return "\n\n".join(sections)


# ── Chat orchestration ───────────────────────────────────────────────


# Tool-calling agent names for Kronos.
_KRONOS_TOOLS = [
    "get_current_date",
    "search_web",
    "query_fred_data",
    "get_nation_indicators",
    "search_wikipedia",
]


def kronos_chat(
    question: str,
    history: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """Run a Kronos chat turn.

    Args:
        question: The user's question.
        history: Previous messages ``[{"role": "user"|"assistant", "content": "..."}]``.

    Returns:
        A dict with keys ``answer``, ``proposals``, ``sources`` matching
        the ``KronosResponse`` schema.
    """
    context_text = build_system_context()

    # Build message list
    messages: List[Dict[str, str]] = [
        {"role": "system", "content": SYSTEM_PROMPT + "\n\n---\n\n" + context_text},
    ]

    # Append conversation history
    if history:
        for msg in history:
            role = msg.get("role", "user")
            if role in ("user", "assistant"):
                messages.append({"role": role, "content": msg.get("content", "")})

    messages.append({"role": "user", "content": question})

    logger.info("[kronos] Sending %d messages to LLM (with tools)", len(messages))

    # Try tool-agent first; fall back to plain LLM if it fails.
    try:
        from apathis.llm.agent import create_agent

        agent = create_agent(tool_names=_KRONOS_TOOLS, max_rounds=3)
        answer = agent.run(messages, temperature=0.4, max_tokens=2048)
    except Exception:
        logger.warning("[kronos] Tool-agent failed, falling back to plain LLM")
        llm = get_llm()
        answer = llm.complete(messages, temperature=0.4, max_tokens=2048)

    # Derive sources from which context sections had data
    sources: List[str] = []
    if "regimes" in context_text.lower() and "no data" not in context_text.lower().split("regime")[0]:
        sources.append("regimes")
    if "positions" in context_text.lower():
        sources.append("positions_snapshots")
    if "orders" in context_text.lower() and "none" not in context_text.lower().split("order")[-1][:20]:
        sources.append("orders")
    if "fragility" in context_text.lower() and "no data" not in context_text.lower().split("fragility")[0]:
        sources.append("fragility_measures")

    return {
        "answer": answer,
        "proposals": [],  # TODO: parse structured proposals from LLM output
        "sources": sources,
    }
