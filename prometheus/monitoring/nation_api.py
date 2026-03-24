"""Prometheus v2 – Nation Profile API (read-only).

Exposes nation scores, macro indicators, and person profiles for the
C2 dashboard's Nation Intel page.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from apathis.core.database import get_db_manager
from apathis.nation.chokepoints import (
    get_chokepoint as _get_chokepoint,
)
from apathis.nation.chokepoints import (
    get_chokepoints as _get_chokepoints,
)
from apathis.nation.chokepoints import (
    update_status as _update_chokepoint_status,
)
from apathis.nation.conflicts import (
    get_all_conflicts as _get_all_conflicts,
)
from apathis.nation.conflicts import (
    get_conflict as _get_conflict,
)
from apathis.nation.conflicts import (
    get_nation_conflicts as _get_nation_conflicts,
)
from apathis.nation.contagion import get_dependencies_directed
from apathis.nation.flight_tracker import (
    get_flight_count as _get_flight_count,
)
from apathis.nation.flight_tracker import (
    get_flights as _get_flights,
)
from apathis.nation.industries import (
    get_all_industries as _get_all_industries,
)
from apathis.nation.industries import (
    get_industry_categories as _get_industry_categories,
)
from apathis.nation.industries import (
    get_industry_names as _get_industry_names,
)
from apathis.nation.industries import (
    get_industry_nations as _get_industry_nations,
)
from apathis.nation.industries import (
    get_nation_industries as _get_nation_industries,
)
from apathis.nation.nation_info import get_nation_info as _get_nation_info
from apathis.nation.naval_deployments import (
    get_deployment_count as _get_deployment_count,
)
from apathis.nation.naval_deployments import (
    get_deployments as _get_deployments,
)
from apathis.nation.naval_deployments import (
    get_deployments_for_conflict as _get_deployments_for_conflict,
)
from apathis.nation.ports import (
    get_port as _get_port,
)
from apathis.nation.ports import (
    get_ports as _get_ports,
)
from apathis.nation.resources import (
    get_all_resources as _get_all_resources,
)
from apathis.nation.resources import (
    get_nation_resources as _get_nation_resources,
)
from apathis.nation.resources import (
    get_resource_categories as _get_resource_categories,
)
from apathis.nation.resources import (
    get_resource_info as _get_resource_info,
)
from apathis.nation.resources import (
    get_resource_names as _get_resource_names,
)
from apathis.nation.resources import (
    get_resource_producers as _get_resource_producers,
)
from apathis.nation.trade_routes import get_trade_routes as _get_trade_routes
from apathis.nation.vessel_tracker import (
    get_vessel_count as _get_vessel_count,
)
from apathis.nation.vessel_tracker import (
    get_vessels as _get_vessels,
)
from fastapi import APIRouter, HTTPException, Path, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/nation", tags=["nation"])


# ── Response models ───────────────────────────────────────────────────


class NationScoresResponse(BaseModel):
    nation: str
    as_of_date: date
    economic_stability: float
    market_stability: float
    currency_risk: float
    political_stability: float
    contagion_risk: float
    policy_direction: Dict[str, float] = Field(default_factory=dict)
    leadership_risk: float
    leadership_composite: float
    opportunity_score: float
    composite_risk: float
    component_details: Dict[str, Any] = Field(default_factory=dict)


class NationScoreHistoryRow(BaseModel):
    as_of_date: date
    economic_stability: float
    market_stability: float
    currency_risk: float
    political_stability: float
    contagion_risk: float
    leadership_composite: float
    opportunity_score: float
    composite_risk: float


class MacroIndicatorResponse(BaseModel):
    series_id: str
    observation_date: date
    value: float
    direction: Optional[str] = None
    rate_of_change: Optional[float] = None
    source: str = "FRED"
    metadata: Dict[str, Any] = Field(default_factory=dict)


class PersonProfileResponse(BaseModel):
    profile_id: str
    person_name: str
    nation: str
    role: str
    role_tier: int
    in_role_since: date
    expected_term_end: Optional[date] = None
    policy_stance: Dict[str, float] = Field(default_factory=dict)
    scores: Dict[str, float] = Field(default_factory=dict)
    background: Dict[str, Any] = Field(default_factory=dict)
    behavioral: Dict[str, Any] = Field(default_factory=dict)
    confidence: float = 0.5
    last_updated: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ── Endpoints ─────────────────────────────────────────────────────────


# ── Map summary (bulk) ────────────────────────────────────────────────


class MapLeader(BaseModel):
    name: str
    role: str
    thumbnail_url: Optional[str] = None
    profile_id: Optional[str] = None


class MapNationSummary(BaseModel):
    nation: str
    as_of_date: Optional[date] = None
    composite_risk: float = 0.0
    economic_stability: float = 0.0
    market_stability: float = 0.0
    political_stability: float = 0.0
    contagion_risk: float = 0.0
    currency_risk: float = 0.0
    opportunity_score: float = 0.0
    leadership_risk: float = 0.0
    leader: Optional[MapLeader] = None
    dependencies: List[Dict[str, Any]] = Field(default_factory=list)


@router.get("/map-summary", response_model=List[MapNationSummary])
async def get_map_summary() -> List[MapNationSummary]:
    """Bulk endpoint for the GeoRisk map.

    Returns ALL scored nations with latest scores, top leader info,
    and bilateral dependency links.  Dynamically pulls from DB.
    """
    db = get_db_manager()

    # 1) Latest scores for every nation.
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT DISTINCT ON (nation)
                       nation, as_of_date,
                       composite_risk, economic_stability, market_stability,
                       political_stability, contagion_risk, currency_risk,
                       opportunity_score, leadership_risk
                FROM nation_scores
                ORDER BY nation, as_of_date DESC
                """
            )
            score_rows = cur.fetchall()
        finally:
            cur.close()

    # 2) Top leader per nation (Tier 1, first by role).
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT DISTINCT ON (nation)
                       nation, person_name, role, metadata, profile_id
                FROM person_profiles
                WHERE role_tier = 1
                ORDER BY nation, role ASC
                """
            )
            leader_rows = cur.fetchall()
        finally:
            cur.close()

    leaders_by_nation: Dict[str, MapLeader] = {}
    for r in leader_rows:
        thumb = None
        if r[3] and isinstance(r[3], dict):
            thumb = (r[3].get("wikipedia") or {}).get("thumbnail_url")
        leaders_by_nation[r[0]] = MapLeader(
            name=r[1],
            role=r[2],
            thumbnail_url=thumb,
            profile_id=r[4],
        )

    # 3) Build response.
    result: List[MapNationSummary] = []
    for r in score_rows:
        nation = r[0]
        dep_list = get_dependencies_directed(nation, min_weight=0.3, limit=6)

        result.append(
            MapNationSummary(
                nation=nation,
                as_of_date=r[1],
                composite_risk=float(r[2]),
                economic_stability=float(r[3]),
                market_stability=float(r[4]),
                political_stability=float(r[5]),
                contagion_risk=float(r[6]),
                currency_risk=float(r[7]),
                opportunity_score=float(r[8]),
                leadership_risk=float(r[9]),
                leader=leaders_by_nation.get(nation),
                dependencies=dep_list,
            )
        )

    return result


# ── Geo overlay endpoints ─────────────────────────────────────────────


@router.get("/chokepoints")
async def get_chokepoints_endpoint() -> List[Dict[str, Any]]:
    """Return all strategic chokepoints with current status."""
    return _get_chokepoints()


@router.get("/chokepoints/{chokepoint_id}")
async def get_chokepoint_endpoint(chokepoint_id: str) -> Dict[str, Any]:
    """Return a single chokepoint by ID."""
    cp = _get_chokepoint(chokepoint_id)
    if cp is None:
        raise HTTPException(404, f"Chokepoint '{chokepoint_id}' not found")
    return cp


@router.post("/chokepoints/{chokepoint_id}/status")
async def update_chokepoint_status(
    chokepoint_id: str,
    status: str = Query(..., description="OPEN | THREATENED | DISRUPTED | CLOSED"),
) -> Dict[str, str]:
    """Update the operational status of a chokepoint."""
    ok = _update_chokepoint_status(chokepoint_id, status)
    if not ok:
        raise HTTPException(400, "Invalid chokepoint ID or status")
    return {"status": "updated", "chokepoint_id": chokepoint_id, "new_status": status}


@router.get("/trade-routes")
async def get_trade_routes_endpoint(
    category: Optional[str] = Query(None, description="oil | gas | shipping | commodity"),
) -> List[Dict[str, Any]]:
    """Return major trade routes, optionally filtered by category."""
    return _get_trade_routes(category)


@router.get("/resources")
async def get_resources_endpoint(
    nation: Optional[str] = Query(None, description="Filter by nation ISO3"),
    resource: Optional[str] = Query(None, description="Filter by resource name"),
) -> Dict[str, Any]:
    """Return resource data. Optionally filter by nation or resource."""
    if nation:
        return {"nation": nation.upper(), "resources": _get_nation_resources(nation)}
    if resource:
        return {"resource": resource, "producers": _get_resource_producers(resource)}
    return {
        "resources": _get_all_resources(),
        "categories": _get_resource_categories(),
        "resource_names": _get_resource_names(),
    }


# ── Conflict endpoints ─────────────────────────────────────────────────


@router.get("/conflicts")
async def get_conflicts_endpoint() -> List[Dict[str, Any]]:
    """Return all global conflicts and disputes."""
    return _get_all_conflicts()


@router.get("/conflicts/{conflict_id}")
async def get_conflict_endpoint(
    conflict_id: str = Path(..., description="Conflict ID"),
) -> Dict[str, Any]:
    """Return a single conflict by ID."""
    c = _get_conflict(conflict_id)
    if c is None:
        raise HTTPException(404, f"Conflict '{conflict_id}' not found")
    return c


# ── Port endpoints ─────────────────────────────────────────────────────


@router.get("/ports")
async def get_ports_endpoint(
    port_type: Optional[str] = Query(None, description="seaport | cargo_airport"),
) -> List[Dict[str, Any]]:
    """Return major seaports and cargo airports."""
    return _get_ports(port_type)


@router.get("/ports/{port_id}")
async def get_port_endpoint(port_id: str) -> Dict[str, Any]:
    """Return a single port by ID."""
    p = _get_port(port_id)
    if p is None:
        raise HTTPException(404, f"Port '{port_id}' not found")
    return p


# ── Industry endpoints ─────────────────────────────────────────────────


@router.get("/industries")
async def get_industries_endpoint(
    nation: Optional[str] = Query(None, description="Filter by nation ISO3"),
    industry: Optional[str] = Query(None, description="Filter by industry name"),
) -> Dict[str, Any]:
    """Return industry data. Optionally filter by nation or industry."""
    if nation:
        return {"nation": nation.upper(), "industries": _get_nation_industries(nation)}
    if industry:
        return {"industry": industry, "nations": _get_industry_nations(industry)}
    return {
        "industries": _get_all_industries(),
        "categories": _get_industry_categories(),
        "industry_names": _get_industry_names(),
    }


@router.get("/{nation}/conflicts")
async def get_nation_conflicts_endpoint(
    nation: str = Path(..., description="Nation ISO3 code"),
) -> List[Dict[str, Any]]:
    """Return conflicts involving a specific nation."""
    return _get_nation_conflicts(nation)


@router.get("/{nation}/industries")
async def get_nation_industries_endpoint(
    nation: str = Path(..., description="Nation ISO3 code"),
) -> List[Dict[str, Any]]:
    """Return core industries for a specific nation."""
    return _get_nation_industries(nation)


@router.get("/{nation}/industry-health")
async def get_nation_industry_health(
    nation: str = Path(..., description="Nation ISO3 code"),
) -> List[Dict[str, Any]]:
    """Return latest industry health scores for a nation."""
    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT DISTINCT ON (industry)
                       industry, as_of_date, health_score,
                       pmi_component, output_trend,
                       regulatory_pressure, sentiment,
                       growth_yoy_pct, metadata
                FROM nation_industry_health
                WHERE nation = %s
                ORDER BY industry, as_of_date DESC
                """,
                (nation.upper(),),
            )
            rows = cur.fetchall()
        except Exception:
            # Table may not exist yet
            return []
        finally:
            cur.close()

    def _reg_label(v: float | None) -> str:
        if v is None:
            return "unknown"
        if v < 0.35:
            return "low"
        if v < 0.65:
            return "moderate"
        return "high"

    def _sent_label(v: float | None) -> str:
        if v is None:
            return "neutral"
        if v > 0.2:
            return "positive"
        if v < -0.2:
            return "negative"
        return "neutral"

    return [
        {
            "industry": r[0],
            "as_of_date": str(r[1]),
            "health_score": float(r[2]),
            "pmi_component": float(r[3]) if r[3] is not None else None,
            "output_trend": (r[4] or "stable").lower(),
            "regulatory_pressure": _reg_label(float(r[5]) if r[5] is not None else None),
            "sentiment": _sent_label(float(r[6]) if r[6] is not None else None),
            "growth_yoy_pct": float(r[7]) if r[7] is not None else None,
            "metadata": r[8] or {},
        }
        for r in rows
    ]


@router.get("/resource-info")
async def get_resource_info_endpoint(
    resource: Optional[str] = Query(None, description="Resource key (e.g. crude_oil). Omit to list all."),
) -> Dict[str, Any] | list:
    """Return descriptive metadata about a resource type."""
    return _get_resource_info(resource)


@router.get("/info")
async def get_nation_info_endpoint(
    nation: Optional[str] = Query(None, description="Nation ISO3 code. Omit to list all."),
) -> Dict[str, Any] | list:
    """Return descriptive metadata about a nation."""
    return _get_nation_info(nation)


# ── Live tracking endpoints ────────────────────────────────────────────


@router.get("/vessels")
async def get_vessels_endpoint(
    category: Optional[str] = Query(None, description="military | commercial | law_enforcement | all"),
) -> List[Dict[str, Any]]:
    """Return currently tracked vessels from AIS stream."""
    return _get_vessels(category)


@router.get("/vessels/count")
async def get_vessel_count_endpoint() -> Dict[str, int]:
    """Return vessel counts by category."""
    return _get_vessel_count()


@router.get("/flights")
async def get_flights_endpoint(
    category: Optional[str] = Query(None, description="military | cargo | passenger | all"),
) -> List[Dict[str, Any]]:
    """Return currently tracked aircraft from OpenSky."""
    return _get_flights(category)


@router.get("/flights/count")
async def get_flight_count_endpoint() -> Dict[str, int]:
    """Return aircraft counts by category."""
    return _get_flight_count()


@router.get("/naval-deployments")
async def get_naval_deployments_endpoint(
    category: Optional[str] = Query(None, description="carrier | destroyer | amphibious | lcs"),
    nation: Optional[str] = Query(None, description="Nation ISO3 code"),
) -> List[Dict[str, Any]]:
    """Return current naval deployments (OSINT)."""
    return _get_deployments(category, nation)


@router.get("/naval-deployments/count")
async def get_naval_deployment_count_endpoint() -> Dict[str, int]:
    """Return naval deployment counts by ship type."""
    return _get_deployment_count()


@router.get("/conflicts/{conflict_id}/assets")
async def get_conflict_assets(
    conflict_id: str = Path(..., description="Conflict ID"),
) -> Dict[str, Any]:
    """Return military assets linked to a specific conflict."""
    deployments = _get_deployments_for_conflict(conflict_id)
    # Also get flights linked to this conflict's nations
    conflict = _get_conflict(conflict_id)
    linked_flights: List[Dict[str, Any]] = []
    if conflict:
        party_nations = set()
        for p in conflict.get("parties", []):
            party_nations.update(p.get("nations", []))
        all_flights = _get_flights("military")
        linked_flights = [f for f in all_flights if f.get("flag_iso3") in party_nations]
    return {
        "conflict_id": conflict_id,
        "naval_deployments": deployments,
        "military_flights": linked_flights[:200],  # cap to avoid huge payload
        "counts": {
            "deployments": len(deployments),
            "flights": len(linked_flights),
        },
    }


@router.get("/list", response_model=List[Dict[str, Any]])
async def list_nations() -> List[Dict[str, Any]]:
    """List all nations with scored profiles."""
    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT nation, MAX(as_of_date) AS latest,
                       MAX(composite_risk) AS composite
                FROM nation_scores
                GROUP BY nation
                ORDER BY nation
                """
            )
            rows = cur.fetchall()
        finally:
            cur.close()

    return [
        {
            "nation": r[0],
            "latest_date": str(r[1]),
            "composite_risk": float(r[2]),
        }
        for r in rows
    ]


@router.get("/{nation}/scores", response_model=Optional[NationScoresResponse])
async def get_nation_scores(
    nation: str = Path(..., description="Nation ISO3 code (e.g. USA)"),
) -> Optional[NationScoresResponse]:
    """Get the latest nation scores."""
    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT nation, as_of_date,
                       economic_stability, market_stability, currency_risk,
                       political_stability, contagion_risk, policy_direction,
                       leadership_risk, leadership_composite,
                       opportunity_score, composite_risk,
                       component_details
                FROM nation_scores
                WHERE nation = %s
                ORDER BY as_of_date DESC
                LIMIT 1
                """,
                (nation.upper(),),
            )
            row = cur.fetchone()
        finally:
            cur.close()

    if row is None:
        return None

    return NationScoresResponse(
        nation=row[0],
        as_of_date=row[1],
        economic_stability=float(row[2]),
        market_stability=float(row[3]),
        currency_risk=float(row[4]),
        political_stability=float(row[5]),
        contagion_risk=float(row[6]),
        policy_direction=row[7] or {},
        leadership_risk=float(row[8]),
        leadership_composite=float(row[9]),
        opportunity_score=float(row[10]),
        composite_risk=float(row[11]),
        component_details=row[12] or {},
    )


@router.get("/{nation}/scores/history", response_model=List[NationScoreHistoryRow])
async def get_nation_score_history(
    nation: str = Path(..., description="Nation ISO3 code"),
    days: int = Query(90, ge=1, le=1826),
) -> List[NationScoreHistoryRow]:
    """Get nation score history for the last N days."""
    db = get_db_manager()
    start = date.today() - timedelta(days=days)

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT as_of_date,
                       economic_stability, market_stability, currency_risk,
                       political_stability, contagion_risk,
                       leadership_composite, opportunity_score, composite_risk
                FROM nation_scores
                WHERE nation = %s AND as_of_date >= %s
                ORDER BY as_of_date ASC
                """,
                (nation.upper(), start),
            )
            rows = cur.fetchall()
        finally:
            cur.close()

    return [
        NationScoreHistoryRow(
            as_of_date=r[0],
            economic_stability=float(r[1]),
            market_stability=float(r[2]),
            currency_risk=float(r[3]),
            political_stability=float(r[4]),
            contagion_risk=float(r[5]),
            leadership_composite=float(r[6]),
            opportunity_score=float(r[7]),
            composite_risk=float(r[8]),
        )
        for r in rows
    ]


@router.get("/{nation}/indicators", response_model=List[MacroIndicatorResponse])
async def get_nation_indicators(
    nation: str = Path(..., description="Nation ISO3 code"),
) -> List[MacroIndicatorResponse]:
    """Get the latest value for each macro indicator series."""
    db = get_db_manager()

    with db.get_historical_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT DISTINCT ON (series_id)
                       series_id, observation_date, value,
                       direction, rate_of_change, source, metadata
                FROM nation_macro_indicators
                WHERE nation = %s
                ORDER BY series_id, observation_date DESC
                """,
                (nation.upper(),),
            )
            rows = cur.fetchall()
        finally:
            cur.close()

    return [
        MacroIndicatorResponse(
            series_id=r[0],
            observation_date=r[1],
            value=float(r[2]),
            direction=r[3],
            rate_of_change=float(r[4]) if r[4] is not None else None,
            source=r[5],
            metadata=r[6] or {},
        )
        for r in rows
    ]


@router.get("/{nation}/persons", response_model=List[PersonProfileResponse])
async def get_nation_persons(
    nation: str = Path(..., description="Nation ISO3 code"),
    tier: Optional[int] = Query(None, ge=1, le=3),
) -> List[PersonProfileResponse]:
    """Get person profiles for a nation."""
    db = get_db_manager()

    sql = """
        SELECT profile_id, person_name, nation, role, role_tier,
               in_role_since, expected_term_end,
               policy_stance, scores, background, behavioral,
               confidence, last_updated, metadata
        FROM person_profiles
        WHERE nation = %s
    """
    params: list = [nation.upper()]

    if tier is not None:
        sql += " AND role_tier <= %s"
        params.append(tier)

    sql += " ORDER BY role_tier ASC, role ASC"

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            rows = cur.fetchall()
        finally:
            cur.close()

    return [
        PersonProfileResponse(
            profile_id=r[0],
            person_name=r[1],
            nation=r[2],
            role=r[3],
            role_tier=int(r[4]),
            in_role_since=r[5],
            expected_term_end=r[6],
            policy_stance=r[7] or {},
            scores=r[8] or {},
            background=r[9] or {},
            behavioral=r[10] or {},
            confidence=float(r[11]),
            last_updated=str(r[12]) if r[12] else None,
            metadata=r[13] or {},
        )
        for r in rows
    ]
