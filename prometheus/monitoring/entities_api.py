"""Prometheus v2 – Entities API (read-only).

This module exposes "entity" data for the C2 UI.

Current scope:
- Issuers (companies/sovereigns/etc.) from `runtime_db.issuers`.
- Profile snapshots from `runtime_db.profiles` (if the table exists).

The API is intentionally read-only: it does not build or mutate profiles.
"""

from __future__ import annotations

import math
from datetime import date
from typing import Any, Dict, List, Mapping, Optional, Tuple

from apathis.core.database import get_db_manager
from fastapi import APIRouter, HTTPException, Path, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/entities", tags=["entities"])


def _table_exists(*, table_name: str) -> bool:
    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = %s
                )
                """,
                (table_name,),
            )
            (exists,) = cur.fetchone()
        finally:
            cur.close()
    return bool(exists)


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


class EntitySummary(BaseModel):
    issuer_id: str
    issuer_type: str
    name: str

    country: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None

    has_profile: bool = False
    latest_profile_as_of_date: Optional[date] = None

    fragility_score: Optional[float] = None
    soft_target_class: Optional[str] = None
    in_portfolio: bool = False


class EntityListResponse(BaseModel):
    total: int
    entities: List[EntitySummary]


class InstrumentSummary(BaseModel):
    instrument_id: str
    issuer_id: str
    market_id: str
    asset_class: str
    symbol: str
    currency: str

    exchange: Optional[str] = None
    status: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ProfileSnapshotResponse(BaseModel):
    issuer_id: str
    as_of_date: date
    structured: Dict[str, Any] = Field(default_factory=dict)
    risk_flags: Dict[str, float] = Field(default_factory=dict)


class FragilityHistoryRow(BaseModel):
    as_of_date: date
    fragility_score: float


class EntityDetail(EntitySummary):
    issuer_metadata: Dict[str, Any] = Field(default_factory=dict)
    instruments: List[InstrumentSummary] = Field(default_factory=list)

    profile: Optional[ProfileSnapshotResponse] = None
    fragility_history: List[FragilityHistoryRow] = Field(default_factory=list)
    position_value: Optional[float] = None


class ProfileIndexRow(BaseModel):
    as_of_date: date
    risk_flags: Dict[str, float] = Field(default_factory=dict)


@router.get("/types", response_model=List[str])
async def list_entity_types() -> List[str]:
    """List distinct issuer types available in the runtime DB."""
    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT DISTINCT issuer_type FROM issuers WHERE issuer_type IS NOT NULL ORDER BY issuer_type"
            )
            rows = cur.fetchall()
        finally:
            cur.close()
    return [str(r[0]) for r in rows if r and r[0] is not None]


@router.get("/sectors", response_model=List[str])
async def list_sectors(
    issuer_type: Optional[str] = Query(None, description="Filter sectors to a specific issuer_type"),
) -> List[str]:
    """List distinct sectors, optionally filtered by issuer_type."""
    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            if issuer_type and str(issuer_type).strip():
                cur.execute(
                    "SELECT DISTINCT sector FROM issuers WHERE sector IS NOT NULL AND sector != '' AND issuer_type = %s ORDER BY sector",
                    (str(issuer_type).strip().upper(),),
                )
            else:
                cur.execute(
                    "SELECT DISTINCT sector FROM issuers WHERE sector IS NOT NULL AND sector != '' ORDER BY sector"
                )
            rows = cur.fetchall()
        finally:
            cur.close()
    return [str(r[0]) for r in rows if r and r[0] is not None]


@router.get("/countries", response_model=List[str])
async def list_countries() -> List[str]:
    """List distinct countries."""
    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT DISTINCT country FROM issuers WHERE country IS NOT NULL AND country != '' ORDER BY country"
            )
            rows = cur.fetchall()
        finally:
            cur.close()
    return [str(r[0]) for r in rows if r and r[0] is not None]


@router.get("", response_model=EntityListResponse)
async def list_entities(
    q: Optional[str] = Query(None, description="Search substring for issuer_id or name"),
    issuer_type: Optional[str] = Query(None, description="Filter by issuer_type (e.g. COMPANY, SOVEREIGN)"),
    sector: Optional[str] = Query(None, description="Filter by sector"),
    country: Optional[str] = Query(None, description="Filter by country"),
    in_portfolio: Optional[str] = Query(None, description="Filter to entities in a portfolio (portfolio_id)"),
    only_with_profiles: bool = Query(False, description="If true, only return issuers with at least one profile snapshot"),
    limit: int = Query(50, ge=1, le=2000),
    offset: int = Query(0, ge=0),
) -> EntityListResponse:
    db = get_db_manager()

    profiles_exist = _table_exists(table_name="profiles")

    where: List[str] = []
    params: List[Any] = []

    if issuer_type is not None and str(issuer_type).strip() != "":
        where.append("i.issuer_type = %s")
        params.append(str(issuer_type).strip().upper())

    if q is not None and str(q).strip() != "":
        q_like = f"%{str(q).strip()}%"
        where.append("(i.issuer_id ILIKE %s OR i.name ILIKE %s)")
        params.extend([q_like, q_like])

    if sector is not None and str(sector).strip() != "":
        where.append("i.sector = %s")
        params.append(str(sector).strip())

    if country is not None and str(country).strip() != "":
        where.append("i.country = %s")
        params.append(str(country).strip())

    # ── Profile join ──
    if profiles_exist:
        profile_join = (
            "LEFT JOIN (SELECT issuer_id, MAX(as_of_date) AS latest_as_of_date "
            "FROM profiles GROUP BY issuer_id) p ON p.issuer_id = i.issuer_id"
        )
        if only_with_profiles:
            where.append("p.latest_as_of_date IS NOT NULL")
        select_profile_col = "p.latest_as_of_date"
    else:
        profile_join = ""
        select_profile_col = "NULL::date AS latest_as_of_date"
        if only_with_profiles:
            return EntityListResponse(total=0, entities=[])

    # ── Fragility join (latest per issuer via best instrument) ──
    fragility_join = """
        LEFT JOIN LATERAL (
            SELECT fm.fragility_score
            FROM instruments inst
            JOIN fragility_measures fm
              ON fm.entity_type = 'INSTRUMENT'
             AND fm.entity_id = inst.instrument_id
            WHERE inst.issuer_id = i.issuer_id
            ORDER BY fm.as_of_date DESC
            LIMIT 1
        ) frag ON true
    """

    # ── Soft target class join (latest per issuer) ──
    stc_join = """
        LEFT JOIN LATERAL (
            SELECT stc.soft_target_class
            FROM instruments inst
            JOIN soft_target_classes stc
              ON stc.entity_type = 'INSTRUMENT'
             AND stc.entity_id = inst.instrument_id
            WHERE inst.issuer_id = i.issuer_id
            ORDER BY stc.as_of_date DESC
            LIMIT 1
        ) stc ON true
    """

    # ── In-portfolio join ──
    portfolio_filter_id = None
    if in_portfolio is not None and str(in_portfolio).strip() != "":
        portfolio_filter_id = str(in_portfolio).strip()

    portfolio_join = """
        LEFT JOIN LATERAL (
            SELECT 1 AS flag
            FROM positions_snapshots ps
            JOIN instruments inst ON inst.instrument_id = ps.instrument_id
            WHERE inst.issuer_id = i.issuer_id
              AND ps.portfolio_id = %s
              AND ps.as_of_date = (
                  SELECT MAX(as_of_date) FROM positions_snapshots WHERE portfolio_id = %s
              )
            LIMIT 1
        ) port ON true
    """
    portfolio_params = [portfolio_filter_id or "IBKR_PAPER", portfolio_filter_id or "IBKR_PAPER"]

    if portfolio_filter_id:
        where.append("port.flag IS NOT NULL")

    where_sql = ""
    if where:
        where_sql = " WHERE " + " AND ".join(where)

    # ── Count query (without LIMIT/OFFSET) ──
    count_sql = (
        "SELECT COUNT(*) FROM issuers i "
        + profile_join + " "
        + fragility_join + " "
        + stc_join + " "
        + portfolio_join + " "
        + where_sql
    )
    count_params = list(portfolio_params) + list(params)

    # ── Main query ──
    main_sql = (
        "SELECT i.issuer_id, i.issuer_type, i.name, i.country, i.sector, i.industry, "
        + select_profile_col
        + ", frag.fragility_score, stc.soft_target_class, port.flag"
        + " FROM issuers i "
        + profile_join + " "
        + fragility_join + " "
        + stc_join + " "
        + portfolio_join + " "
        + where_sql
        + " ORDER BY i.name ASC, i.issuer_id ASC LIMIT %s OFFSET %s"
    )
    main_params = list(portfolio_params) + list(params) + [int(limit), int(offset)]

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(count_sql, tuple(count_params))
            (total,) = cur.fetchone()

            cur.execute(main_sql, tuple(main_params))
            rows = cur.fetchall()
        finally:
            cur.close()

    out: List[EntitySummary] = []
    for row in rows:
        (
            issuer_id,
            issuer_type_db,
            name,
            country_db,
            sector_db,
            industry,
            latest_profile_as_of_date,
            fragility_score,
            soft_target_class,
            in_portfolio_flag,
        ) = row
        out.append(
            EntitySummary(
                issuer_id=str(issuer_id),
                issuer_type=str(issuer_type_db),
                name=str(name),
                country=str(country_db) if country_db is not None else None,
                sector=str(sector_db) if sector_db is not None else None,
                industry=str(industry) if industry is not None else None,
                has_profile=latest_profile_as_of_date is not None,
                latest_profile_as_of_date=latest_profile_as_of_date,
                fragility_score=float(fragility_score) if fragility_score is not None else None,
                soft_target_class=str(soft_target_class) if soft_target_class is not None else None,
                in_portfolio=in_portfolio_flag is not None,
            )
        )

    return EntityListResponse(total=int(total), entities=out)


# ============================================================================
# Compare Metrics — multi-instrument history for comparison charts
# ============================================================================


class ComparePricesResponse(BaseModel):
    """Aligned price/metric histories for up to 10 instruments."""
    instruments: List[str] = Field(default_factory=list)
    series: Dict[str, List[Dict[str, Any]]] = Field(default_factory=dict)


def _fetch_close_prices(
    db: Any, instrument_ids: List[str], days: int,
) -> Dict[str, List[Tuple[date, float]]]:
    """Fetch close prices grouped by instrument_id."""
    raw: Dict[str, List[Tuple[date, float]]] = {}
    with db.get_historical_connection() as conn:
        cur = conn.cursor()
        try:
            ph = ",".join(["%s"] * len(instrument_ids))
            cur.execute(
                f"""
                SELECT instrument_id, trade_date, close
                FROM prices_daily
                WHERE instrument_id IN ({ph})
                  AND trade_date >= CURRENT_DATE - INTERVAL '{int(days)} days'
                ORDER BY instrument_id, trade_date ASC
                """,
                instrument_ids,
            )
            for iid, td, val in cur.fetchall():
                k = str(iid)
                if k not in raw:
                    raw[k] = []
                if val is not None:
                    raw[k].append((td, float(val)))
        finally:
            cur.close()
    return raw


def _compute_derived(
    raw: Dict[str, List[Tuple[date, float]]], metric: str,
) -> Dict[str, List[Dict[str, Any]]]:
    """Compute normalized, cumulative_return, or rolling_vol from close prices."""
    out: Dict[str, List[Dict[str, Any]]] = {}
    for iid, pts in raw.items():
        if not pts:
            continue
        if metric == "normalized":
            base = pts[0][1]
            if base == 0:
                continue
            out[iid] = [
                {"date": d.isoformat(), "value": round(v / base * 100, 2)}
                for d, v in pts
            ]
        elif metric == "cumulative_return":
            base = pts[0][1]
            if base == 0:
                continue
            out[iid] = [
                {"date": d.isoformat(), "value": round((v / base - 1) * 100, 2)}
                for d, v in pts
            ]
        elif metric == "rolling_vol":
            window = 20
            if len(pts) < window + 1:
                continue
            log_ret: List[Tuple[date, float]] = []
            for i in range(1, len(pts)):
                prev_v, cur_v = pts[i - 1][1], pts[i][1]
                if prev_v > 0 and cur_v > 0:
                    log_ret.append((pts[i][0], math.log(cur_v / prev_v)))
                else:
                    log_ret.append((pts[i][0], 0.0))
            result: List[Dict[str, Any]] = []
            for i in range(window - 1, len(log_ret)):
                wr = [r for _, r in log_ret[i - window + 1 : i + 1]]
                mean = sum(wr) / window
                var = sum((r - mean) ** 2 for r in wr) / max(window - 1, 1)
                vol = math.sqrt(var) * math.sqrt(252) * 100  # annualised %
                result.append({"date": log_ret[i][0].isoformat(), "value": round(vol, 2)})
            out[iid] = result
    return out


DERIVED_METRICS = {"normalized", "cumulative_return", "rolling_vol"}
PRICE_METRICS = {"close", "volume", "adjusted_close", "open", "high", "low"}


@router.get("/compare_prices", response_model=ComparePricesResponse)
async def compare_entity_prices(
    ids: str = Query(..., description="Comma-separated issuer_ids (max 10)"),
    days: int = Query(365, ge=7, le=1826, description="Number of trading days of history"),
    metric: str = Query("close", description="Metric to compare"),
) -> ComparePricesResponse:
    """Return price / risk / derived metric histories for multiple issuers."""
    db = get_db_manager()

    issuer_ids = [s.strip() for s in str(ids).split(",") if s.strip()][:10]
    if not issuer_ids:
        return ComparePricesResponse()

    # Map issuer_id → primary instrument_id
    inst_map: Dict[str, str] = {}
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            placeholders = ",".join(["%s"] * len(issuer_ids))
            cur.execute(
                f"""
                SELECT DISTINCT ON (issuer_id) issuer_id, instrument_id
                FROM instruments
                WHERE issuer_id IN ({placeholders})
                  AND asset_class = 'EQUITY'
                  AND status = 'ACTIVE'
                ORDER BY issuer_id, instrument_id
                """,
                issuer_ids,
            )
            for iid, inst_id in cur.fetchall():
                inst_map[str(iid)] = str(inst_id)
        finally:
            cur.close()

    if not inst_map:
        return ComparePricesResponse(instruments=issuer_ids)

    instrument_ids = list(inst_map.values())
    series: Dict[str, List[Dict[str, Any]]] = {}

    if metric == "fragility":
        # Anchor to the latest fragility date — NOT CURRENT_DATE
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                placeholders = ",".join(["%s"] * len(instrument_ids))
                cur.execute(
                    f"""
                    SELECT entity_id, as_of_date, fragility_score
                    FROM fragility_measures
                    WHERE entity_type = 'INSTRUMENT'
                      AND entity_id IN ({placeholders})
                      AND as_of_date >= (
                          SELECT MAX(as_of_date) FROM fragility_measures
                          WHERE entity_type = 'INSTRUMENT'
                      ) - INTERVAL '{int(days)} days'
                    ORDER BY entity_id, as_of_date ASC
                    """,
                    instrument_ids,
                )
                for eid, as_of, fscore in cur.fetchall():
                    eid_str = str(eid)
                    if eid_str not in series:
                        series[eid_str] = []
                    series[eid_str].append({
                        "date": as_of.isoformat(),
                        "value": round(float(fscore), 4) if fscore is not None else None,
                    })
            finally:
                cur.close()

    elif metric in DERIVED_METRICS:
        raw = _fetch_close_prices(db, instrument_ids, days)
        series = _compute_derived(raw, metric)

    else:
        metric_col = metric if metric in PRICE_METRICS else "close"
        with db.get_historical_connection() as conn:
            cur = conn.cursor()
            try:
                placeholders = ",".join(["%s"] * len(instrument_ids))
                cur.execute(
                    f"""
                    SELECT instrument_id, trade_date, {metric_col}
                    FROM prices_daily
                    WHERE instrument_id IN ({placeholders})
                      AND trade_date >= CURRENT_DATE - INTERVAL '{int(days)} days'
                    ORDER BY instrument_id, trade_date ASC
                    """,
                    instrument_ids,
                )
                for inst_id, trade_date, value in cur.fetchall():
                    iid_str = str(inst_id)
                    if iid_str not in series:
                        series[iid_str] = []
                    series[iid_str].append({
                        "date": trade_date.isoformat(),
                        "value": round(float(value), 4) if value is not None else None,
                    })
            finally:
                cur.close()

    # Re-key by issuer_id instead of instrument_id for the frontend
    result_series: Dict[str, List[Dict[str, Any]]] = {}
    for issuer_id, inst_id in inst_map.items():
        if inst_id in series:
            result_series[issuer_id] = series[inst_id]

    return ComparePricesResponse(
        instruments=list(inst_map.keys()),
        series=result_series,
    )


@router.get("/{issuer_id}", response_model=EntityDetail)
async def get_entity_detail(
    issuer_id: str = Path(..., description="Issuer ID (entity id)", min_length=1),
    as_of_date: Optional[date] = Query(None, description="Optional profile as_of_date (uses latest <= date)"),
) -> EntityDetail:
    db = get_db_manager()

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT issuer_type, name, country, sector, industry, metadata
                FROM issuers
                WHERE issuer_id = %s
                """,
                (issuer_id,),
            )
            row = cur.fetchone()
        finally:
            cur.close()

    if row is None:
        raise HTTPException(status_code=404, detail=f"Unknown issuer_id={issuer_id!r}")

    issuer_type_db, name, country, sector, industry, metadata = row

    # Instruments for issuer.
    instruments: List[InstrumentSummary] = []
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT instrument_id, issuer_id, market_id, asset_class, symbol, exchange, currency, status, metadata
                FROM instruments
                WHERE issuer_id = %s
                ORDER BY instrument_id ASC
                """,
                (issuer_id,),
            )
            inst_rows = cur.fetchall()
        finally:
            cur.close()

    for (
        instrument_id,
        issuer_id_db,
        market_id,
        asset_class,
        symbol,
        exchange,
        currency,
        status,
        inst_meta,
    ) in inst_rows:
        instruments.append(
            InstrumentSummary(
                instrument_id=str(instrument_id),
                issuer_id=str(issuer_id_db),
                market_id=str(market_id),
                asset_class=str(asset_class),
                symbol=str(symbol),
                exchange=str(exchange) if exchange is not None else None,
                currency=str(currency),
                status=str(status) if status is not None else None,
                metadata=dict(_as_mapping(inst_meta)),
            )
        )

    # Latest (or as_of) profile snapshot if profiles table exists.
    profile: Optional[ProfileSnapshotResponse] = None
    if _table_exists(table_name="profiles"):
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                if as_of_date is None:
                    cur.execute(
                        """
                        SELECT as_of_date, structured, risk_flags
                        FROM profiles
                        WHERE issuer_id = %s
                        ORDER BY as_of_date DESC
                        LIMIT 1
                        """,
                        (issuer_id,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT as_of_date, structured, risk_flags
                        FROM profiles
                        WHERE issuer_id = %s AND as_of_date <= %s
                        ORDER BY as_of_date DESC
                        LIMIT 1
                        """,
                        (issuer_id, as_of_date),
                    )
                prow = cur.fetchone()
            finally:
                cur.close()

        if prow is not None:
            prof_date, structured, risk_flags = prow
            profile = ProfileSnapshotResponse(
                issuer_id=str(issuer_id),
                as_of_date=prof_date,
                structured=dict(_as_mapping(structured)),
                risk_flags={str(k): float(v) for k, v in dict(_as_mapping(risk_flags)).items()},
            )

    latest_profile_date = profile.as_of_date if profile is not None else None

    # Fragility history (last 90 points via any instrument).
    fragility_history: List[FragilityHistoryRow] = []
    instrument_ids = [inst.instrument_id for inst in instruments]
    if instrument_ids:
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT DISTINCT ON (fm.as_of_date) fm.as_of_date, fm.fragility_score
                    FROM fragility_measures fm
                    WHERE fm.entity_type = 'INSTRUMENT'
                      AND fm.entity_id = ANY(%s)
                    ORDER BY fm.as_of_date DESC
                    LIMIT 90
                    """,
                    (instrument_ids,),
                )
                frag_rows = cur.fetchall()
            finally:
                cur.close()
        fragility_history = [
            FragilityHistoryRow(as_of_date=r[0], fragility_score=float(r[1]))
            for r in reversed(frag_rows)
        ]

    # Latest fragility score for summary fields.
    frag_score = fragility_history[-1].fragility_score if fragility_history else None

    # Soft target class.
    stc_val: Optional[str] = None
    if instrument_ids:
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT stc.soft_target_class
                    FROM soft_target_classes stc
                    WHERE stc.entity_type = 'INSTRUMENT'
                      AND stc.entity_id = ANY(%s)
                    ORDER BY stc.as_of_date DESC
                    LIMIT 1
                    """,
                    (instrument_ids,),
                )
                stc_row = cur.fetchone()
            finally:
                cur.close()
        if stc_row:
            stc_val = str(stc_row[0])

    # Position value.
    position_value: Optional[float] = None
    if instrument_ids:
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT SUM(ps.market_value)
                    FROM positions_snapshots ps
                    WHERE ps.instrument_id = ANY(%s)
                      AND ps.portfolio_id = 'IBKR_PAPER'
                      AND ps.as_of_date = (
                          SELECT MAX(as_of_date) FROM positions_snapshots WHERE portfolio_id = 'IBKR_PAPER'
                      )
                    """,
                    (instrument_ids,),
                )
                pv_row = cur.fetchone()
            finally:
                cur.close()
        if pv_row and pv_row[0] is not None:
            position_value = float(pv_row[0])

    return EntityDetail(
        issuer_id=str(issuer_id),
        issuer_type=str(issuer_type_db),
        name=str(name),
        country=str(country) if country is not None else None,
        sector=str(sector) if sector is not None else None,
        industry=str(industry) if industry is not None else None,
        issuer_metadata=dict(_as_mapping(metadata)),
        instruments=instruments,
        has_profile=latest_profile_date is not None,
        latest_profile_as_of_date=latest_profile_date,
        profile=profile,
        fragility_score=frag_score,
        soft_target_class=stc_val,
        in_portfolio=position_value is not None,
        fragility_history=fragility_history,
        position_value=position_value,
    )


@router.get("/{issuer_id}/profiles", response_model=List[ProfileIndexRow])
async def list_entity_profiles(
    issuer_id: str = Path(..., description="Issuer ID (entity id)", min_length=1),
    limit: int = Query(200, ge=1, le=2000),
) -> List[ProfileIndexRow]:
    """List available profile snapshots for an issuer (most recent first)."""

    if not _table_exists(table_name="profiles"):
        return []

    db = get_db_manager()

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT as_of_date, risk_flags
                FROM profiles
                WHERE issuer_id = %s
                ORDER BY as_of_date DESC
                LIMIT %s
                """,
                (issuer_id, int(limit)),
            )
            rows = cur.fetchall()
        finally:
            cur.close()

    out: List[ProfileIndexRow] = []
    for as_of_db, risk_flags in rows:
        out.append(
            ProfileIndexRow(
                as_of_date=as_of_db,
                risk_flags={str(k): float(v) for k, v in dict(_as_mapping(risk_flags)).items()},
            )
        )
    return out


