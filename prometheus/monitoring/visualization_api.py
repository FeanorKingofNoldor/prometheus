"""Prometheus v2 – Visualization API.

This module provides endpoints for ANT_HILL 3D visualization: scenes,
execution traces, DB schema views, and embedding space data.

Initially returns static/template data for UI development.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from apathis.core.database import get_db_manager
from fastapi import APIRouter, HTTPException, Path, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api", tags=["visualization"])


# ============================================================================
# Response Models
# ============================================================================


class SceneMetadata(BaseModel):
    """Metadata for available scenes."""

    view_id: str
    display_name: str
    layout_type: str = "standard"
    description: str = ""


class SceneData(BaseModel):
    """Complete scene graph for 3D rendering."""

    view_id: str
    layout_type: str = "standard"
    nodes: Dict[str, Any] = Field(default_factory=dict)
    connections: List[Dict[str, Any]] = Field(default_factory=list)


class TraceMetadata(BaseModel):
    """Metadata for execution traces."""

    trace_id: str
    market_id: str
    mode: str  # LIVE, PAPER, BACKTEST
    start_time: str
    end_time: Optional[str] = None


class TraceData(BaseModel):
    """Execution trace events."""

    trace_id: str
    events: List[Dict[str, Any]] = Field(default_factory=list)


class DBTableData(BaseModel):
    """Database table snapshot."""

    table_name: str
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    total_count: int = 0


class EmbeddingSpaceData(BaseModel):
    """Embedding space visualization data."""

    space_id: str
    vectors: List[Dict[str, Any]] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ============================================================================
# Endpoints
# ============================================================================


@router.get("/scenes", response_model=List[SceneMetadata])
async def get_scenes() -> List[SceneMetadata]:
    """Return list of available ANT_HILL scenes."""
    return [
        SceneMetadata(
            view_id="root",
            display_name="Prometheus v2 System Architecture",
            layout_type="standard",
            description="Top-level system view showing all engines and data flows",
        ),
        SceneMetadata(
            view_id="regime",
            display_name="Regime Engine Internals",
            layout_type="standard",
            description="Regime detection pipeline and state machine",
        ),
        SceneMetadata(
            view_id="stability",
            display_name="Stability Engine Internals",
            layout_type="timeline",
            description="Stability computation and soft target identification",
        ),
        SceneMetadata(
            view_id="portfolio",
            display_name="Portfolio Engine Internals",
            layout_type="network",
            description="Portfolio optimization and risk management",
        ),
        SceneMetadata(
            view_id="orchestration",
            display_name="DAG Orchestrator",
            layout_type="dag",
            description="Job scheduling and pipeline orchestration",
        ),
        SceneMetadata(
            view_id="runtime_db",
            display_name="Runtime Database Schema",
            layout_type="schema",
            description="Real-time operational tables",
        ),
        SceneMetadata(
            view_id="historical_db",
            display_name="Historical Database Schema",
            layout_type="schema",
            description="Market data and derived features",
        ),
        SceneMetadata(
            view_id="encoders",
            display_name="Encoder Embedding Spaces",
            layout_type="embedding_space",
            description="Joint embedding visualization",
        ),
    ]


@router.get("/scene/{view_id}", response_model=SceneData)
async def get_scene(
    view_id: str = Path(..., description="Scene view identifier")
) -> SceneData:
    """Return complete scene graph for a specific view.

    This is the primary data source for ANT_HILL 3D rendering.
    """
    # Mock root scene
    if view_id == "root":
        return SceneData(
            view_id="root",
            layout_type="standard",
            nodes={
                "regime": {
                    "type": "subsystem",
                    "label": "Regime Engine",
                    "pos": [0, 30, 0],
                    "color": "#22c55e",
                },
                "stability": {
                    "type": "subsystem",
                    "label": "Stability Engine",
                    "pos": [15, 30, 0],
                    "color": "#3b82f6",
                },
                "fragility": {
                    "type": "subsystem",
                    "label": "Fragility Engine",
                    "pos": [30, 30, 0],
                    "color": "#f59e0b",
                },
                "assessment": {
                    "type": "subsystem",
                    "label": "Assessment Engine",
                    "pos": [45, 30, 0],
                    "color": "#8b5cf6",
                },
                "portfolio": {
                    "type": "subsystem",
                    "label": "Portfolio Engine",
                    "pos": [60, 30, 0],
                    "color": "#ec4899",
                },
                "orchestrator": {
                    "type": "subsystem",
                    "label": "DAG Orchestrator",
                    "pos": [30, 50, 0],
                    "color": "#06b6d4",
                },
                "runtime_db": {
                    "type": "database",
                    "label": "Runtime DB",
                    "pos": [15, 10, 0],
                    "color": "#14b8a6",
                },
                "historical_db": {
                    "type": "database",
                    "label": "Historical DB",
                    "pos": [45, 10, 0],
                    "color": "#10b981",
                },
            },
            connections=[
                {
                    "from": "regime",
                    "to": "runtime_db",
                    "data_type": "RegimeState",
                    "bidirectional": False,
                },
                {
                    "from": "stability",
                    "to": "runtime_db",
                    "data_type": "StabilityMetrics",
                    "bidirectional": False,
                },
                {
                    "from": "fragility",
                    "to": "runtime_db",
                    "data_type": "FragilityAlpha",
                    "bidirectional": False,
                },
                {
                    "from": "assessment",
                    "to": "runtime_db",
                    "data_type": "UniverseScores",
                    "bidirectional": False,
                },
                {
                    "from": "portfolio",
                    "to": "runtime_db",
                    "data_type": "TargetWeights",
                    "bidirectional": False,
                },
                {
                    "from": "orchestrator",
                    "to": "regime",
                    "data_type": "JobTrigger",
                    "bidirectional": False,
                },
                {
                    "from": "orchestrator",
                    "to": "stability",
                    "data_type": "JobTrigger",
                    "bidirectional": False,
                },
                {
                    "from": "historical_db",
                    "to": "regime",
                    "data_type": "MarketData",
                    "bidirectional": False,
                },
                {
                    "from": "historical_db",
                    "to": "stability",
                    "data_type": "MarketData",
                    "bidirectional": False,
                },
            ],
        )

    # Default empty scene for other views
    return SceneData(
        view_id=view_id,
        layout_type="standard",
        nodes={},
        connections=[],
    )


@router.get("/traces", response_model=List[TraceMetadata])
async def get_traces(
    market_id: Optional[str] = Query(None, description="Filter by market"),
    mode: Optional[str] = Query(None, description="Filter by mode"),
) -> List[TraceMetadata]:
    """Return list of available execution traces."""
    return [
        TraceMetadata(
            trace_id="trace_001",
            market_id="US_EQ",
            mode="BACKTEST",
            start_time="2024-11-28T09:00:00Z",
            end_time="2024-11-28T16:00:00Z",
        ),
        TraceMetadata(
            trace_id="trace_002",
            market_id="EU_EQ",
            mode="PAPER",
            start_time="2024-11-28T08:00:00Z",
            end_time=None,  # Still running
        ),
    ]


@router.get("/traces/{trace_id}", response_model=TraceData)
async def get_trace(
    trace_id: str = Path(..., description="Trace identifier")
) -> TraceData:
    """Return execution trace events for playback."""
    return TraceData(
        trace_id=trace_id,
        events=[
            {
                "timestamp": "2024-11-28T09:00:00Z",
                "event_type": "function_call",
                "node": "regime.get_regime",
                "data": {"region": "US", "date": "2024-11-28"},
            },
            {
                "timestamp": "2024-11-28T09:00:01.250Z",
                "event_type": "function_return",
                "node": "regime.get_regime",
                "data": {"regime": "STABLE_EXPANSION", "confidence": 0.82},
            },
            {
                "timestamp": "2024-11-28T09:00:02Z",
                "event_type": "data_flow",
                "from_node": "regime",
                "to_node": "runtime_db",
                "data_type": "RegimeState",
            },
        ],
    )


RUNTIME_TABLE_WHITELIST = {
    # Decision logging & execution
    "engine_decisions",
    "decision_outcomes",
    "executed_actions",
    "orders",
    "fills",
    "positions_snapshots",
    # Engine outputs
    "regimes",
    "stability_vectors",
    "fragility_measures",
    "instrument_scores",
    "universes",
    "universe_members",
    "target_portfolios",
    "portfolio_risk_reports",
    # Backtesting
    "backtest_runs",
    "backtest_trades",
    "backtest_daily_equity",
}

HISTORICAL_TABLE_WHITELIST = {
    "prices_daily",
    "returns_daily",
    "volatility_daily",
    "instrument_factors_daily",
    "factors_daily",
    "news_articles",
    "news_links",
    "text_embeddings",
    "numeric_window_embeddings",
    "joint_embeddings",
}


@router.get("/db/runtime/{table}", response_model=DBTableData)
async def get_runtime_table(
    table: str = Path(..., description="Table name"),
    limit: int = Query(100, description="Row limit"),
    as_of_date: Optional[date] = Query(None, description="As-of date filter"),
) -> DBTableData:
    """Return runtime DB table snapshot.

    This implementation queries the runtime Postgres DB via
    :func:`get_db_manager`. Only tables in ``RUNTIME_TABLE_WHITELIST`` are
    exposed to avoid SQL injection and accidental access to internal
    tables.
    """

    if table not in RUNTIME_TABLE_WHITELIST:
        raise HTTPException(status_code=404, detail=f"Table '{table}' is not exposed via this API")

    db_manager = get_db_manager()
    rows: List[Dict[str, Any]] = []

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            # Basic snapshot; for now we do not apply as_of_date filtering
            # generically as not all tables have an as_of_date column.
            sql = f"SELECT * FROM {table} LIMIT %s"
            cursor.execute(sql, (limit,))
            fetched = cursor.fetchall()
            if not fetched:
                return DBTableData(table_name=table, rows=[], total_count=0)

            colnames = [desc[0] for desc in cursor.description]
            for row in fetched:
                rows.append({col: value for col, value in zip(colnames, row)})
        finally:
            cursor.close()

    return DBTableData(table_name=table, rows=rows, total_count=len(rows))


@router.get("/db/historical/{table}", response_model=DBTableData)
async def get_historical_table(
    table: str = Path(..., description="Table name"),
    limit: int = Query(100, description="Row limit"),
    date_lte: Optional[date] = Query(None, description="Date <= filter"),
) -> DBTableData:
    """Return historical DB table snapshot.

    Only tables in ``HISTORICAL_TABLE_WHITELIST`` are exposed. The
    ``date_lte`` parameter is currently advisory and may be ignored for
    tables without a date column.
    """

    if table not in HISTORICAL_TABLE_WHITELIST:
        raise HTTPException(status_code=404, detail=f"Table '{table}' is not exposed via this API")

    db_manager = get_db_manager()
    rows: List[Dict[str, Any]] = []

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            # Naive implementation: simple LIMIT query. Later we can add
            # table-specific WHERE clauses based on schema knowledge.
            sql = f"SELECT * FROM {table} LIMIT %s"
            cursor.execute(sql, (limit,))
            fetched = cursor.fetchall()
            if not fetched:
                return DBTableData(table_name=table, rows=[], total_count=0)

            colnames = [desc[0] for desc in cursor.description]
            for row in fetched:
                rows.append({col: value for col, value in zip(colnames, row)})
        finally:
            cursor.close()

    return DBTableData(table_name=table, rows=rows, total_count=len(rows))


@router.get("/embedding_space/{space_id}", response_model=EmbeddingSpaceData)
async def get_embedding_space(
    space_id: str = Path(..., description="Embedding space identifier"),
    limit: int = Query(1000, description="Vector limit"),
) -> EmbeddingSpaceData:
    """Return embedding space vectors for 3D scatter visualization."""
    return EmbeddingSpaceData(
        space_id=space_id,
        vectors=[
            {
                "id": "AAPL",
                "vector": [0.25, 0.82, -0.15],
                "cluster": "TECH",
                "color": "#22c55e",
            },
            {
                "id": "MSFT",
                "vector": [0.28, 0.85, -0.12],
                "cluster": "TECH",
                "color": "#22c55e",
            },
            {
                "id": "JPM",
                "vector": [-0.45, 0.22, 0.65],
                "cluster": "FINANCE",
                "color": "#3b82f6",
            },
        ],
        metadata={
            "dimensions": 128,
            "projected_dimensions": 3,
            "projection_method": "UMAP",
            "num_vectors": 3,
        },
    )
