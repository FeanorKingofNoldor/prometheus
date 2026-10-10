from __future__ import annotations

import hashlib
import math
from dataclasses import asdict, dataclass
from datetime import datetime
from urllib.parse import urlparse

VERSION = "event-v1"
PORTFOLIO = "EVENT_PAPER"
STRATEGIES = ("CONTINUATION", "REVERSAL")
# Sign describes the affected entity's economic condition, not a return forecast.
EVENT_RULES = {
    "cut_production_of": ("object", -1),
    "increased_production_of": ("object", 1),
    "halted_flow_through": ("object", -1),
    "resumed_flow_through": ("object", 1),
    "designated_sanctioned": ("object", -1),
    "lifted_sanctions": ("object", 1),
    "suspended_operations_in": ("subject", -1),
    "exited_market_in": ("subject", -1),
    "imposed_embargo_on": ("object", -1),
    "export_control_on": ("object", -1),
}


@dataclass(frozen=True)
class Policy:
    version: str = VERSION
    book_usd: float = 50000
    max_gross: float = 25000
    max_position: float = 5000
    max_event: float = 10000
    risk_per_trade: float = 100
    max_positions: int = 8
    max_spread_bps: float = 40
    entry_age_hours: float = 72
    commission_per_share: float = 0.005
    min_commission: float = 1
    short_borrow_annual: float = 0.10
    drawdown_limit: float = 2500


@dataclass(frozen=True)
class Quote:
    bid: float
    ask: float
    observed_at: datetime
    market_data_type: int
    shortable: float = 0

    @property
    def mid(self):
        return (self.bid + self.ask) / 2

    def valid(self, now: datetime, max_spread_bps: float = 40):
        return (
            all(math.isfinite(v) and v > 0 for v in (self.bid, self.ask))
            and self.ask >= self.bid
            and self.market_data_type in (1, 3)
            and 0 <= (now - self.observed_at).total_seconds() <= 120
            and (self.ask - self.bid) / self.mid * 10000 <= max_spread_bps
        )

    def payload(self):
        return {
            **asdict(self),
            "observed_at": self.observed_at.isoformat(),
            "timing": "delayed_feed" if self.market_data_type == 3 else "realtime_feed",
        }


def stable_id(*parts):
    return hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()[:32]


def source_identity(claim):
    host = urlparse(claim.get("url") or "").hostname
    if not host or host.lower() in ("news.google.com", "t.me", "telegram.me", "news.yahoo.com"):
        return ""
    return (host or claim.get("source") or "").lower().removeprefix("www.")


def evidence_state(claims):
    # A copied quotation is one piece of evidence even on several websites.
    accepted = [
        c for c in claims if c["status"] != "rejected" and c["tense"] != "future" and c["extractor_confidence"] >= 0.85
    ]
    positive = [c for c in accepted if c["polarity"] == "asserted" and c["modality"] in ("asserted", "reported")]
    independent = set()
    quotes = set()
    for c in positive:
        quote = " ".join((c.get("quote") or c.get("title") or "").lower().split())
        identity = source_identity(c)
        if quote and quote not in quotes and identity:
            quotes.add(quote)
            independent.add(identity)
    confirmed = len(independent) >= 2 or any(
        c["source_tier"] == 1 and c["modality"] == "asserted" and source_identity(c) for c in positive
    )
    contradicted = any(c["polarity"] == "negated" or c["modality"] == "denied" for c in accepted)
    return {
        "confirmed": confirmed and not contradicted,
        "contradicted": contradicted,
        "sources": len(independent),
        "label": "contradicted" if contradicted else "corroborated" if confirmed else "unconfirmed",
    }


def exposures(graph, entity, condition_sign):
    """Walk explicit economic relationships at most two hops; retain the actual paths."""
    if entity[0] not in ("COMPANY", "RESOURCE", "CHOKEPOINT", "PIPELINE", "FACILITY", "PORT"):
        return {}
    found = {}
    frontier = [(entity, condition_sign, 1.0, [], {entity})]
    while frontier:
        node, sign, strength, path, visited = frontier.pop()
        if node[0] == "COMPANY":
            old = found.get(node[1])
            if old and old["sign"] != sign:
                found[node[1]] = {"sign": 0, "strength": 0, "path": []}
            elif old is None or (old["sign"] and strength > old["strength"]):
                found[node[1]] = {"sign": sign, "strength": strength, "path": path}
        if len(path) >= 2:
            continue
        links = []
        for e in graph.get_edges_to(node):
            if e.relation in ("depends_on", "imports_via", "routes_through", "bottlenecked_by", "requires"):
                links.append((e, e.source, sign))
            elif e.relation == "produces" and node[0] == "RESOURCE":
                links.append((e, e.source, -sign))
            elif e.relation == "competes_with" and node[0] == "COMPANY":
                links.append((e, e.source, -sign))
        for e in graph.get_edges_from(node):
            if e.relation in ("supplies", "consumed_by"):
                links.append((e, e.target, sign))
            elif e.relation == "competes_with" and node[0] == "COMPANY":
                links.append((e, e.target, -sign))
        for e, target, next_sign in links:
            weight = float(e.weight)
            if target in visited or not math.isfinite(weight) or weight <= 0:
                continue
            nxt = strength * min(weight, 1)
            if nxt < 0.15:
                continue
            step = {"from": list(e.source), "to": list(e.target), "relation": e.relation, "weight": weight}
            frontier.append((target, next_sign, nxt, path + [step], visited | {target}))
    return {k: v for k, v in found.items() if v["sign"]}


def entry_signal(strategy, sign, evidence, move, excess_move, peak, age_minutes):
    if age_minutes < 15:
        return None, "Observing initial market response"
    if strategy == "CONTINUATION":
        if not evidence["confirmed"]:
            return None, "Waiting for corroborated evidence"
        if 0.002 <= sign * move <= 0.02 and sign * excess_move >= 0.001:
            return sign, "Corroborated event; related asset moving with limited reaction"
        return None, "Continuation price conditions not met"
    if evidence["confirmed"]:
        return None, "Reversal requires weak or contradicted evidence"
    if peak >= 0.015 and peak - sign * move >= 0.003 and sign * move >= 0.005:
        return -sign, "Unconfirmed reaction has begun to reverse"
    return None, "Waiting for an overreaction followed by a pullback"


def trade_plan(strategy, side, quote, policy=Policy()):
    price = quote.ask if side > 0 else quote.bid
    stop_fraction, target_fraction, hours = (0.02, 0.04, 72) if strategy == "CONTINUATION" else (0.015, 0.03, 24)
    quantity = math.floor(min(policy.max_position / price, policy.risk_per_trade / (price * stop_fraction)))
    cost = max(policy.min_commission, quantity * policy.commission_per_share) * 2
    cost += quantity * (quote.ask - quote.bid)
    if side < 0:
        cost += quantity * price * policy.short_borrow_annual * hours / (365 * 24)
    return {
        "side": side,
        "quantity": quantity,
        "entry_limit": price,
        "stop": price * (1 - side * stop_fraction),
        "target": price * (1 + side * target_fraction),
        "stop_fraction": stop_fraction,
        "target_fraction": target_fraction,
        "hold_hours": hours,
        "estimated_round_trip_cost": cost,
        "target_payoff_after_estimated_cost": quantity * price * target_fraction - cost,
        "expected_return": None,
        "policy": asdict(policy),
    }
