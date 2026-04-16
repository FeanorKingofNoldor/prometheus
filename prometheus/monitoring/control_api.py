"""Prometheus v2 – Control API.

This module provides write-side endpoints for the C2 UI to launch
backtests, create synthetic datasets, schedule DAGs, and apply config
changes.

All control operations are logged and tracked via an in-process job
runner.
"""

from __future__ import annotations

import json
import os
import socket
from datetime import date, datetime  # noqa: F401  (datetime kept for type hints)

from prometheus.orchestration.clock import now_utc
from typing import Any, Dict, List, Optional

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from fastapi import APIRouter, Body, HTTPException, Path
from pydantic import BaseModel, Field

from prometheus.books.registry import AllocatorSleeveSpec, BookKind, load_book_registry
from prometheus.monitoring.job_runner import JobRecord, create_job, get_job, submit_job

logger = get_logger(__name__)


router = APIRouter(prefix="/api/control", tags=["control"])


# ============================================================================
# Request/Response Models
# ============================================================================


class BacktestRequest(BaseModel):
    """Request to run a backtest."""

    strategy_id: str
    start_date: str
    end_date: str
    market_ids: list[str] = Field(default_factory=list)
    config_overrides: Dict[str, Any] = Field(default_factory=dict)


class AllocatorBacktestRequest(BaseModel):
    """Request to run fast allocator backtests (prom2_cpp) via the backend.

    This endpoint is intended for knob-tuning: callers can supply per-run
    overrides without editing the YAML registry.
    """

    book_id: str = Field(default="US_EQ_ALLOCATOR")
    start_date: date
    end_date: date

    run_name: Optional[str] = Field(
        default=None,
        description="Optional human-readable label for these run_id(s)",
        max_length=128,
    )

    sleeve_id: Optional[str] = Field(default=None, description="Optional sleeve_id filter")

    # C++ controls.
    cpp_threads: int = 0
    cpp_verbose: bool = False

    instrument_limit: int = 0

    persist_to_db: bool = True
    persist_meta_to_db: bool = False

    disable_risk: bool = False

    # Overrides.
    config_overrides: Dict[str, Any] = Field(default_factory=dict)
    global_sleeve_overrides: Dict[str, Any] = Field(default_factory=dict)
    sleeve_overrides: Dict[str, Dict[str, Any]] = Field(default_factory=dict)


class SyntheticDatasetRequest(BaseModel):
    """Request to create synthetic scenario dataset."""

    dataset_name: str
    scenario_type: str
    num_samples: int = 1000
    parameters: Dict[str, Any] = Field(default_factory=dict)


class DAGScheduleRequest(BaseModel):
    """Request to schedule DAG execution."""

    market_id: str
    dag_name: str
    force: bool = False
    parameters: Dict[str, Any] = Field(default_factory=dict)


class ConfigChangeRequest(BaseModel):
    """Request to apply config change."""

    engine_name: str
    config_key: str
    config_value: Any
    reason: str
    requires_approval: bool = True


class JobResponse(BaseModel):
    """Response for job submission."""

    job_id: str
    status: str = "PENDING"
    message: str = ""


class JobStatus(BaseModel):
    """Job status and progress."""

    job_id: str
    type: str
    status: str
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress_pct: float = 0.0
    message: str = ""
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class AllocatorSleeveSummary(BaseModel):
    sleeve_id: str

    portfolio_max_names: Optional[int] = None
    portfolio_hysteresis_buffer: Optional[int] = None
    portfolio_per_instrument_max_weight: Optional[float] = None

    hedge_instrument_ids: list[str] = Field(default_factory=list)
    hedge_sizing_mode: str = "fragility_linear"

    max_hedge_allocation: float = 0.5
    fragility_threshold: float = 0.30
    profitability_weight: Optional[float] = None


class AllocatorBookSummary(BaseModel):
    book_id: str
    region: str
    market_id: str
    default_sleeve_id: Optional[str] = None

    sleeves: list[AllocatorSleeveSummary] = Field(default_factory=list)


class AllocatorRegistryResponse(BaseModel):
    books: list[AllocatorBookSummary] = Field(default_factory=list)
    allowed_config_override_keys: list[str] = Field(default_factory=list)
    allowed_sleeve_override_keys: list[str] = Field(default_factory=list)


_ALLOWED_ALLOCATOR_CFG_OVERRIDE_KEYS = {
    # Backtest.
    "initial_cash",
    # Risk.
    "apply_risk",
    "per_name_risk_cap",
    # MarketSituation knobs.
    "recovery_fragility_threshold",
    "crisis_fragility_override_threshold",
    "recovery_requires_stress_transition",
    # Long-universe knobs.
    "stab_window_days",
    "min_avg_volume",
    "max_soft_target_score",
    "exclude_breakers",
    "assessment_horizon_days",
    "assessment_score_weight",
    "stability_risk_alpha",
    "stability_risk_horizon_steps",
    # Instrument selection.
    "instrument_ids",
    "history_lookback_calendar_days",
    "hard_exclusion_list",
}

_ALLOWED_ALLOCATOR_SLEEVE_OVERRIDE_KEYS = {
    "universe_max_size",
    "portfolio_max_names",
    "portfolio_hysteresis_buffer",
    "portfolio_per_instrument_max_weight",
    "hedge_instrument_ids",
    "hedge_sizing_mode",
    "fragility_threshold",
    "max_hedge_allocation",
    "hedge_allocation_overrides",
    "hedge_allocation_floors",
    "hedge_allocation_caps",
    "profitability_weight",
}


def _job_status_from_record(rec: JobRecord) -> JobStatus:
    return JobStatus(
        job_id=rec.job_id,
        type=rec.type,
        status=rec.status,
        created_at=rec.created_at,
        started_at=rec.started_at,
        completed_at=rec.completed_at,
        progress_pct=float(rec.progress_pct or 0.0),
        message=rec.message,
        result=rec.result,
        error=rec.error,
    )


def _validate_override_keys(overrides: Dict[str, Any], *, allowed: set[str], label: str) -> None:
    bad = sorted(k for k in overrides.keys() if k not in allowed)
    if bad:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported {label} override keys: {bad}",
        )


# ============================================================================
# Endpoints
# ============================================================================


@router.post("/run_backtest", response_model=JobResponse)
async def run_backtest(request: BacktestRequest = Body(...)) -> JobResponse:
    """Submit a sleeve backtest for async execution.

    Runs the full STAB → Assessment → Universe → Portfolio pipeline over
    the requested date range using the pilot sleeve defaults. Results are
    persisted to ``backtest_runs`` / ``backtest_daily_equity``.

    ``strategy_id`` is used as both the sleeve_id and strategy identifier.
    Pass optional ``config_overrides`` keys ``market_id``, ``initial_cash``,
    and ``disable_risk`` to customise the run.
    """

    try:
        start_date = date.fromisoformat(request.start_date)
        end_date = date.fromisoformat(request.end_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {exc}") from exc

    if end_date < start_date:
        raise HTTPException(status_code=400, detail="end_date must be >= start_date")

    ovr = request.config_overrides or {}
    market_id = str(ovr.get("market_id", "US_EQ"))
    initial_cash = float(ovr.get("initial_cash", 1_000_000.0))
    apply_risk = not bool(ovr.get("disable_risk", False))

    def _run() -> Dict[str, Any]:
        from apathis.core.config import get_config
        from apathis.core.database import DatabaseManager
        from apathis.core.time import TradingCalendar, TradingCalendarConfig

        from prometheus.backtest.campaign import _run_backtest_for_sleeve
        from prometheus.backtest.config import SleeveConfig

        sleeve_id = str(request.strategy_id)
        base = sleeve_id
        cfg = SleeveConfig(
            sleeve_id=base,
            strategy_id=str(request.strategy_id),
            market_id=market_id,
            universe_id=f"{base}_UNIVERSE",
            portfolio_id=f"{base}_PORTFOLIO",
            assessment_strategy_id=f"{base}_ASSESS",
            assessment_horizon_days=21,
            assessment_backend="basic",
            assessment_use_joint_context=False,
            stability_risk_alpha=0.5,
            stability_risk_horizon_steps=1,
            regime_risk_alpha=0.0,
            lambda_score_weight=0.0,
        )

        config = get_config()
        db_manager = DatabaseManager(config)
        calendar = TradingCalendar(TradingCalendarConfig(market=market_id))

        summary = _run_backtest_for_sleeve(
            db_manager=db_manager,
            calendar=calendar,
            market_id=market_id,
            start_date=start_date,
            end_date=end_date,
            cfg=cfg,
            initial_cash=initial_cash,
            apply_risk=apply_risk,
            lambda_provider=None,
        )

        m = summary.metrics or {}
        return {
            "run_id": summary.run_id,
            "sleeve_id": summary.sleeve_id,
            "strategy_id": summary.strategy_id,
            "cumulative_return": float(m.get("cumulative_return", 0.0)),
            "annualised_sharpe": float(m.get("annualised_sharpe", 0.0)),
            "max_drawdown": float(m.get("max_drawdown", 0.0)),
        }

    rec = submit_job(
        prefix="backtest",
        job_type="BACKTEST",
        message=f"Backtest {request.strategy_id} {request.start_date}→{request.end_date} mkt={market_id}",
        fn=_run,
    )

    return JobResponse(job_id=rec.job_id, status=rec.status, message=f"Backtest job submitted: {rec.job_id}")


@router.post("/run_allocator_backtest", response_model=JobResponse)
async def run_allocator_backtest(request: AllocatorBacktestRequest = Body(...)) -> JobResponse:
    """Submit allocator backtest job (prom2_cpp allocator runner).

    Persists results to runtime DB by default (backtest_runs +
    backtest_daily_equity), and returns the produced run_id(s) in the job
    result.
    """

    if request.end_date < request.start_date:
        raise HTTPException(status_code=400, detail="end_date must be >= start_date")

    _validate_override_keys(
        request.config_overrides,
        allowed=_ALLOWED_ALLOCATOR_CFG_OVERRIDE_KEYS,
        label="config_overrides",
    )
    _validate_override_keys(
        request.global_sleeve_overrides,
        allowed=_ALLOWED_ALLOCATOR_SLEEVE_OVERRIDE_KEYS,
        label="global_sleeve_overrides",
    )

    for sid, ovr in request.sleeve_overrides.items():
        if not isinstance(ovr, dict):
            raise HTTPException(status_code=400, detail=f"sleeve_overrides[{sid!r}] must be an object")
        _validate_override_keys(
            ovr,
            allowed=_ALLOWED_ALLOCATOR_SLEEVE_OVERRIDE_KEYS,
            label=f"sleeve_overrides[{sid}]",
        )

    registry = load_book_registry()
    book = registry.get(str(request.book_id))
    if book is None:
        raise HTTPException(status_code=400, detail=f"Unknown book_id={request.book_id!r}")
    if book.kind != BookKind.ALLOCATOR:
        raise HTTPException(status_code=400, detail=f"book_id={request.book_id!r} is kind={book.kind}, expected ALLOCATOR")

    # Select allocator sleeves.
    sleeves: list[tuple[str, AllocatorSleeveSpec]] = []
    for sid, spec in book.sleeves.items():
        if not isinstance(spec, AllocatorSleeveSpec):
            continue
        if request.sleeve_id is not None and sid != request.sleeve_id:
            continue
        sleeves.append((sid, spec))

    if request.sleeve_id is not None and not sleeves:
        raise HTTPException(status_code=400, detail=f"Unknown sleeve_id={request.sleeve_id!r} for book_id={request.book_id!r}")

    if not sleeves:
        raise HTTPException(status_code=400, detail="No allocator sleeves selected")

    # Validate sleeve_overrides keys refer to selected sleeves.
    selected_ids = {sid for sid, _ in sleeves}
    unknown_override_ids = sorted(k for k in request.sleeve_overrides.keys() if k not in selected_ids)
    if unknown_override_ids:
        raise HTTPException(status_code=400, detail=f"sleeve_overrides contains unknown sleeve_ids: {unknown_override_ids}")

    sleeves_cfg: list[Dict[str, Any]] = []
    for sid, spec in sleeves:
        merged: Dict[str, Any] = {}
        merged.update(request.global_sleeve_overrides)
        merged.update(request.sleeve_overrides.get(sid, {}))

        # Start from registry values.
        max_names = int(merged.get("portfolio_max_names", spec.portfolio_max_names or 0) or 0)
        hysteresis = int(merged.get("portfolio_hysteresis_buffer", spec.portfolio_hysteresis_buffer or 0) or 0)
        per_inst_max = float(merged.get("portfolio_per_instrument_max_weight", spec.portfolio_per_instrument_max_weight or 0.05) or 0.05)

        universe_max_explicit = "universe_max_size" in merged
        if universe_max_explicit:
            universe_max = int(merged.get("universe_max_size") or 0)
            if universe_max <= 0:
                raise HTTPException(status_code=400, detail=f"universe_max_size must be > 0 (sleeve_id={sid})")
        else:
            universe_max = max(200, 10 * max_names) if max_names > 0 else 200

        hedge_ids_val = merged.get("hedge_instrument_ids", list(spec.hedge_instrument_ids))
        if hedge_ids_val is None:
            hedge_ids: list[str] = []
        elif isinstance(hedge_ids_val, (list, tuple)):
            hedge_ids = [str(x) for x in hedge_ids_val]
        else:
            raise HTTPException(status_code=400, detail=f"hedge_instrument_ids must be a list (sleeve_id={sid})")

        def _mapf(v: Any) -> Dict[str, float]:
            if v is None:
                return {}
            if not isinstance(v, dict):
                raise HTTPException(status_code=400, detail=f"hedge_* maps must be objects (sleeve_id={sid})")
            out: Dict[str, float] = {}
            for k, x in v.items():
                out[str(k)] = float(x)
            return out

        sleeves_cfg.append(
            {
                "sleeve_id": str(spec.sleeve_id),
                "universe_max_size": int(universe_max),
                "portfolio_max_names": int(max_names),
                "portfolio_hysteresis_buffer": int(hysteresis),
                "portfolio_per_instrument_max_weight": float(per_inst_max),
                "hedge_instrument_ids": hedge_ids,
                "hedge_sizing_mode": str(merged.get("hedge_sizing_mode", spec.hedge_sizing_mode)),
                "fragility_threshold": float(merged.get("fragility_threshold", spec.fragility_threshold)),
                "max_hedge_allocation": float(merged.get("max_hedge_allocation", spec.max_hedge_allocation)),
                "hedge_allocation_overrides": _mapf(merged.get("hedge_allocation_overrides", spec.hedge_allocation_overrides or {})),
                "hedge_allocation_floors": _mapf(merged.get("hedge_allocation_floors", spec.hedge_allocation_floors or {})),
                "hedge_allocation_caps": _mapf(merged.get("hedge_allocation_caps", spec.hedge_allocation_caps or {})),
                "profitability_weight": float(merged.get("profitability_weight", spec.profitability_weight or 0.0) or 0.0),
            }
        )

    run_name = (str(request.run_name).strip() if request.run_name is not None else "")
    if not run_name:
        run_name = ""

    cfg: Dict[str, Any] = {
        "market_id": str(book.market_id),
        "regime_region": str(book.region),
        "base_prefix": str(request.book_id),
        "start": request.start_date.isoformat(),
        "end": request.end_date.isoformat(),
        "sleeves": sleeves_cfg,
        "instrument_limit": int(request.instrument_limit),
        "num_threads": int(request.cpp_threads),
        "verbose": bool(request.cpp_verbose),
        "persist_to_db": bool(request.persist_to_db),
        "persist_meta_to_db": bool(request.persist_meta_to_db),
        "disable_risk": bool(request.disable_risk),
    }

    if run_name:
        cfg["run_name"] = run_name

    def _coerce_bool(key: str, v: Any) -> bool:
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            s = v.strip().lower()
            if s in {"true", "1", "yes", "y"}:
                return True
            if s in {"false", "0", "no", "n"}:
                return False
        raise HTTPException(status_code=400, detail=f"Invalid boolean for {key}: {v!r}")

    def _coerce_str_list(key: str, v: Any) -> list[str]:
        if v is None:
            return []
        if isinstance(v, str):
            parts = [p.strip() for p in v.split(",")]
            return [p for p in parts if p]
        if isinstance(v, (list, tuple)):
            return [str(x) for x in v]
        raise HTTPException(status_code=400, detail=f"Invalid list for {key}: expected array or csv string")

    float_keys = {
        "initial_cash",
        "per_name_risk_cap",
        "recovery_fragility_threshold",
        "crisis_fragility_override_threshold",
        "min_avg_volume",
        "max_soft_target_score",
        "assessment_score_weight",
        "stability_risk_alpha",
    }
    int_keys = {
        "stab_window_days",
        "assessment_horizon_days",
        "stability_risk_horizon_steps",
        "history_lookback_calendar_days",
    }
    bool_keys = {
        "apply_risk",
        "recovery_requires_stress_transition",
        "exclude_breakers",
    }
    list_keys = {"instrument_ids", "hard_exclusion_list"}

    # Apply config overrides last (so they can intentionally override top-level fields).
    for k, v in request.config_overrides.items():
        if k in float_keys:
            cfg[k] = float(v)
        elif k in int_keys:
            cfg[k] = int(v)
        elif k in bool_keys:
            cfg[k] = _coerce_bool(k, v)
        elif k in list_keys:
            cfg[k] = _coerce_str_list(k, v)
        else:
            # Should be unreachable due to allowlist validation.
            cfg[k] = v

    def _job() -> Dict[str, Any]:
        try:
            import prom2_cpp  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "prom2_cpp not available. Build it (./cpp/scripts/build.sh) and run backend with PYTHONPATH=cpp/build"
            ) from exc

        results = prom2_cpp.run_allocator_backtests(cfg)

        # Attach run_name to results for UI display.
        if run_name:
            try:
                for rr in results:
                    if isinstance(rr, dict):
                        rr.setdefault("run_name", run_name)
            except Exception:
                pass

            # Best-effort persistence: store run_name alongside other run metadata.
            if cfg.get("persist_to_db") or cfg.get("persist_meta_to_db"):
                try:
                    run_ids: list[str] = []
                    for rr in results:
                        if isinstance(rr, dict):
                            rid = rr.get("run_id")
                            if isinstance(rid, str) and rid:
                                run_ids.append(rid)

                    if run_ids:
                        patch_json = json.dumps({"run_name": run_name})
                        db = get_db_manager()
                        with db.get_runtime_connection() as conn:
                            cur = conn.cursor()
                            try:
                                for rid in run_ids:
                                    cur.execute(
                                        "UPDATE backtest_runs SET config_json = config_json || %s::jsonb WHERE run_id = %s",
                                        (patch_json, rid),
                                    )
                                conn.commit()
                            finally:
                                cur.close()
                except Exception:
                    # Do not fail the run if we cannot update metadata.
                    pass

        return {"results": results, "cfg": cfg}

    msg = f"Allocator backtest {request.book_id}"
    if run_name:
        msg += f" name={run_name}"
    msg += f" from {request.start_date.isoformat()} to {request.end_date.isoformat()} sleeves={len(sleeves_cfg)}"

    rec = submit_job(
        prefix="allocator",
        job_type="ALLOCATOR_BACKTEST",
        message=msg,
        fn=_job,
    )

    return JobResponse(job_id=rec.job_id, status=rec.status, message=f"Allocator backtest job submitted: {rec.job_id}")


@router.get("/allocator_registry", response_model=AllocatorRegistryResponse)
async def allocator_registry() -> AllocatorRegistryResponse:
    """Expose allocator books/sleeves for UI discoverability."""

    registry = load_book_registry()

    books: list[AllocatorBookSummary] = []
    for _, book in sorted(registry.items()):
        if book.kind != BookKind.ALLOCATOR:
            continue

        sleeves: list[AllocatorSleeveSummary] = []
        for sid, spec in sorted(book.sleeves.items()):
            if not isinstance(spec, AllocatorSleeveSpec):
                continue

            sleeves.append(
                AllocatorSleeveSummary(
                    sleeve_id=str(spec.sleeve_id),
                    portfolio_max_names=spec.portfolio_max_names,
                    portfolio_hysteresis_buffer=spec.portfolio_hysteresis_buffer,
                    portfolio_per_instrument_max_weight=spec.portfolio_per_instrument_max_weight,
                    hedge_instrument_ids=[str(x) for x in spec.hedge_instrument_ids],
                    hedge_sizing_mode=str(spec.hedge_sizing_mode),
                    max_hedge_allocation=float(spec.max_hedge_allocation),
                    fragility_threshold=float(spec.fragility_threshold),
                    profitability_weight=spec.profitability_weight,
                )
            )

        books.append(
            AllocatorBookSummary(
                book_id=str(book.book_id),
                region=str(book.region),
                market_id=str(book.market_id),
                default_sleeve_id=str(book.default_sleeve_id) if book.default_sleeve_id else None,
                sleeves=sleeves,
            )
        )

    return AllocatorRegistryResponse(
        books=books,
        allowed_config_override_keys=sorted(_ALLOWED_ALLOCATOR_CFG_OVERRIDE_KEYS),
        allowed_sleeve_override_keys=sorted(_ALLOWED_ALLOCATOR_SLEEVE_OVERRIDE_KEYS),
    )


@router.post("/create_synthetic_dataset", response_model=JobResponse)
async def create_synthetic_dataset(request: SyntheticDatasetRequest = Body(...)) -> JobResponse:
    """Submit synthetic dataset creation job.

    This endpoint is currently a stub. It creates a job entry but does not
    execute.
    """

    rec = create_job(
        prefix="synthetic",
        job_type="SYNTHETIC_DATASET",
        status="PENDING",
        message=f"Creating {request.dataset_name} with {request.num_samples} samples",
    )

    return JobResponse(
        job_id=rec.job_id,
        status=rec.status,
        message=f"Synthetic dataset job submitted: {rec.job_id}",
    )


@router.post("/schedule_dag", response_model=JobResponse)
async def schedule_dag(request: DAGScheduleRequest = Body(...)) -> JobResponse:
    """Schedule DAG execution for a market.

    Supports:
    - ``dag_name="intel"`` — builds and registers an intel DAG
    - ``dag_name="market"`` — builds a market pipeline DAG
    """
    from datetime import date as date_cls

    if request.dag_name == "intel":
        from prometheus.orchestration.dag import build_intel_dag

        as_of = date_cls.today()
        is_sunday = as_of.weekday() == 6

        def _run_intel_dag() -> Dict[str, Any]:
            from apathis.intel.pipeline import run_daily_sitrep, run_flash_check, run_weekly_assessment

            from prometheus.monitoring.report_service import generate_log_report

            dag = build_intel_dag(as_of, is_sunday=is_sunday)
            results: Dict[str, Any] = {"dag_id": dag.dag_id, "jobs_run": []}

            # Execute jobs in dependency order
            run_flash_check()
            results["jobs_run"].append("intel_flash_check")

            run_daily_sitrep()
            results["jobs_run"].append("intel_daily_sitrep")

            if is_sunday:
                run_weekly_assessment()
                results["jobs_run"].append("intel_weekly_assessment")

            generate_log_report("log_daily")
            results["jobs_run"].append("intel_log_health")

            return results

        rec = submit_job(
            prefix="dag",
            job_type="DAG_EXECUTION",
            message=f"Intel DAG for {as_of.isoformat()}",
            fn=_run_intel_dag,
        )

        return JobResponse(
            job_id=rec.job_id,
            status=rec.status,
            message=f"Intel DAG submitted: {rec.job_id}",
        )

    # Default: create a pending job for other DAG types
    rec = create_job(
        prefix="dag",
        job_type="DAG_EXECUTION",
        status="PENDING",
        message=f"Scheduling {request.dag_name} for {request.market_id}",
    )

    return JobResponse(
        job_id=rec.job_id,
        status=rec.status,
        message=f"DAG execution job submitted: {rec.job_id}",
    )


@router.post("/apply_config_change", response_model=JobResponse)
async def apply_config_change(request: ConfigChangeRequest = Body(...)) -> JobResponse:
    """Apply configuration change.

    If requires_approval is True, stages the change for review.
    Otherwise applies immediately (not wired up yet).
    """

    if request.requires_approval:
        rec = create_job(
            prefix="config",
            job_type="CONFIG_CHANGE",
            status="STAGED",
            message=f"Config change: {request.engine_name}.{request.config_key}",
        )
        return JobResponse(
            job_id=rec.job_id,
            status=rec.status,
            message=f"Config change staged for approval: {rec.job_id}",
        )

    def _job() -> Dict[str, Any]:
        raise NotImplementedError("Config change application is not implemented in control_api yet")

    rec = submit_job(
        prefix="config",
        job_type="CONFIG_CHANGE",
        message=f"Config change: {request.engine_name}.{request.config_key}",
        fn=_job,
    )

    return JobResponse(
        job_id=rec.job_id,
        status=rec.status,
        message=f"Config change job submitted: {rec.job_id}",
    )


# ============================================================================
# IBKR Connection Status
# ============================================================================


class IbkrEndpointStatus(BaseModel):
    """Status of a single IBKR endpoint."""

    label: str
    host: str
    port: int
    reachable: bool
    latency_ms: Optional[float] = None
    error: Optional[str] = None


class IbkrStatusResponse(BaseModel):
    """Overall IBKR connection status."""

    status: str  # "connected" | "degraded" | "disconnected"
    mode: str  # "PAPER" | "LIVE"
    account: str
    endpoints: List[IbkrEndpointStatus] = Field(default_factory=list)


def _tcp_probe(host: str, port: int, timeout: float = 2.0) -> tuple[bool, float, str]:
    """TCP connect probe. Returns (reachable, latency_ms, error)."""
    import time

    t0 = time.monotonic()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            latency = (time.monotonic() - t0) * 1000.0
            return True, latency, ""
    except OSError as e:
        latency = (time.monotonic() - t0) * 1000.0
        return False, latency, str(e)


@router.get("/ibkr_status", response_model=IbkrStatusResponse)
async def ibkr_status() -> IbkrStatusResponse:
    """Lightweight IBKR connection health check via TCP probes.

    Checks if IB Gateway and TWS ports are reachable without importing
    ib_insync or creating a full broker connection.
    """
    host = "127.0.0.1"
    # Paper trading ports
    probes = [
        ("Gateway (Paper)", host, 4002),
        ("TWS (Paper)", host, 7497),
    ]

    endpoints: List[IbkrEndpointStatus] = []

    for label, h, p in probes:
        reachable, latency, error = _tcp_probe(h, p)
        if reachable:
            pass
        endpoints.append(
            IbkrEndpointStatus(
                label=label,
                host=h,
                port=p,
                reachable=reachable,
                latency_ms=round(latency, 1),
                error=error or None,
            )
        )

    # Determine overall status.
    # Gateway and TWS are mutually exclusive (can't log in to both),
    # so "connected" means at least one is reachable — not all.
    gateway_up = any(ep.reachable for ep in endpoints if "Gateway" in ep.label)
    tws_up = any(ep.reachable for ep in endpoints if "TWS" in ep.label)
    if gateway_up or tws_up:
        status = "connected"
    else:
        status = "disconnected"

    reachable_count = sum(1 for ep in endpoints if ep.reachable)
    logger.debug("[ibkr_status] %s — %d/%d endpoints reachable", status, reachable_count, len(endpoints))
    configured_account = os.getenv("IBKR_PAPER_ACCOUNT")

    return IbkrStatusResponse(
        status=status,
        mode="PAPER",
        account=configured_account if configured_account else "AUTO",
        endpoints=endpoints,
    )


class SyncDataRequest(BaseModel):
    """Request to sync data from external sources."""

    sources: list[str] = Field(
        default_factory=lambda: ["ibkr", "engines", "nations"],
        description="Data sources to sync: ibkr, engines, nations, all",
    )
    portfolio_id: str = "IBKR_PAPER"


class SyncDataResponse(BaseModel):
    """Response from data sync."""

    job_id: str
    status: str
    sources_requested: list[str]
    message: str


@router.post("/sync_data", response_model=SyncDataResponse)
async def sync_data(request: SyncDataRequest = Body(...)) -> SyncDataResponse:
    """Trigger data sync from external sources (IBKR, engines, etc.).

    This endpoint kicks off a background sync job that:
    - Connects to IBKR and pulls latest positions, orders, account state
    - Persists positions to positions_snapshots and account data to portfolio_risk_reports
    - Refreshes engine outputs (regime, stability, fragility, assessment)
    - Updates portfolio and execution data in the runtime DB
    """
    sources = request.sources
    if "all" in sources:
        sources = ["ibkr", "engines", "nations"]
    portfolio_id = str(request.portfolio_id or "").strip() or "IBKR_PAPER"

    if "ibkr" in sources and not portfolio_id.upper().startswith("IBKR_"):
        raise HTTPException(
            status_code=400,
            detail=(
                "IBKR sync requires an IBKR_* portfolio_id. "
                f"Received portfolio_id={portfolio_id!r}."
            ),
        )

    logger.info("[sync] sync_data endpoint called — sources=%s, portfolio_id=%s", sources, portfolio_id)

    def _sync_job() -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        logger.info("[sync] Starting sync job — sources=%s, portfolio_id=%s", sources, portfolio_id)

        if "ibkr" in sources:
            try:
                logger.info("[sync/ibkr] Creating paper broker...")
                from prometheus.execution.broker_factory import create_paper_broker

                broker = create_paper_broker(auto_connect=True)
                logger.info("[sync/ibkr] Broker connected, running sync...")

                broker.sync()
                positions = broker.get_positions()
                account = broker.get_account_state()
                logger.info(
                    "[sync/ibkr] Sync complete — %d positions, %d account keys",
                    len(positions), len(account),
                )

                # Log each position for visibility
                for inst_id, pos in positions.items():
                    logger.info(
                        "[sync/ibkr]   position: %s qty=%.2f avg_cost=%.4f mkt_val=%.2f pnl=%.2f",
                        inst_id, pos.quantity, pos.avg_cost, pos.market_value, pos.unrealized_pnl,
                    )

                # Log key account values
                for key in ("NetLiquidation", "TotalCashValue", "GrossPositionValue",
                            "UnrealizedPnL", "RealizedPnL", "BuyingPower"):
                    if key in account:
                        logger.info("[sync/ibkr]   account: %s = %s", key, account[key])

                # ── Persist positions to DB ──────────────────────────
                if positions:
                    logger.info("[sync/ibkr] Persisting %d positions to positions_snapshots...", len(positions))
                    try:
                        from datetime import date as _date
                        from datetime import datetime as _dt
                        from datetime import timezone as _tz

                        from prometheus.execution.storage import record_positions_snapshot

                        record_positions_snapshot(
                            get_db_manager(),
                            portfolio_id=portfolio_id,
                            positions=positions,
                            as_of_date=_date.today(),
                            mode="PAPER",
                            timestamp=_dt.now(_tz.utc),
                        )
                        logger.info("[sync/ibkr] Positions persisted successfully.")
                    except Exception as e:
                        logger.error("[sync/ibkr] Failed to persist positions: %s", e, exc_info=True)
                        results["ibkr_persist_error"] = str(e)
                else:
                    logger.warning("[sync/ibkr] No positions returned from IBKR — nothing to persist.")

                # ── Persist account summary to DB ────────────────────
                if account:
                    logger.info("[sync/ibkr] Persisting account summary to portfolio_risk_reports...")
                    try:
                        from datetime import date as _date

                        from apathis.core.ids import generate_uuid
                        from psycopg2.extras import Json as _Json

                        net_liq = float(account.get("NetLiquidation", 0))
                        total_cash = float(account.get("TotalCashValue", 0))
                        gross_pos = float(account.get("GrossPositionValue", 0))
                        # Compute exposures relative to net liquidation
                        net_exposure = gross_pos / net_liq if net_liq else 0.0
                        gross_exposure = gross_pos / net_liq if net_liq else 0.0
                        leverage = gross_pos / net_liq if net_liq else 0.0

                        db = get_db_manager()
                        with db.get_runtime_connection() as conn:
                            cur = conn.cursor()
                            try:
                                cur.execute(
                                    """
                                    INSERT INTO portfolio_risk_reports (
                                        report_id, portfolio_id, as_of_date,
                                        portfolio_value, cash, net_exposure,
                                        gross_exposure, leverage, risk_metrics,
                                        exposures_by_sector, exposures_by_factor,
                                        scenario_pnl, metadata, created_at
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                                    """,
                                    (
                                        generate_uuid(),
                                        portfolio_id,
                                        _date.today(),
                                        net_liq,
                                        total_cash,
                                        net_exposure,
                                        gross_exposure,
                                        leverage,
                                        _Json({
                                            "net_liquidation": net_liq,
                                            "total_cash": total_cash,
                                            "gross_position_value": gross_pos,
                                            "unrealized_pnl": float(account.get("UnrealizedPnL", 0)),
                                            "realized_pnl": float(account.get("RealizedPnL", 0)),
                                            "buying_power": float(account.get("BuyingPower", 0)),
                                        }),
                                        _Json({}),
                                        _Json({}),
                                        _Json({}),
                                        _Json({"source": "ibkr_sync", "account_raw_keys": len(account)}),
                                    ),
                                )
                                conn.commit()
                                logger.info("[sync/ibkr] Account summary persisted (NLV=%.2f).", net_liq)
                            finally:
                                cur.close()
                    except Exception as e:
                        logger.error("[sync/ibkr] Failed to persist account summary: %s", e, exc_info=True)

                # Disconnect broker cleanly
                try:
                    if hasattr(broker, 'client') and broker.client is not None:
                        broker.client.disconnect()
                    elif hasattr(broker, 'inner') and hasattr(broker.inner, 'client'):
                        broker.inner.client.disconnect()
                    logger.info("[sync/ibkr] Broker disconnected cleanly.")
                except Exception:
                    pass

                results["ibkr"] = {
                    "status": "ok",
                    "positions": len(positions),
                    "account_keys": len(account),
                    "positions_persisted": len(positions),
                }
            except Exception as e:
                logger.error("[sync/ibkr] IBKR sync failed: %s", e, exc_info=True)
                results["ibkr"] = {"status": "error", "error": str(e)}

        if "engines" in sources:
            try:
                logger.info("[sync/engines] Checking engine/DB connectivity...")
                db = get_db_manager()
                with db.get_runtime_connection() as conn:
                    cur = conn.cursor()
                    # Count some key tables so we can report data state
                    counts: Dict[str, int] = {}
                    for table in ("positions_snapshots", "orders", "fills",
                                  "target_portfolios", "portfolio_risk_reports"):
                        try:
                            cur.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
                            row = cur.fetchone()
                            counts[table] = row[0] if row else 0
                        except Exception:
                            counts[table] = -1
                    cur.close()
                logger.info("[sync/engines] DB reachable — table row counts: %s", counts)
                results["engines"] = {"status": "ok", "message": "DB reachable", "row_counts": counts}
            except Exception as e:
                logger.error("[sync/engines] Engine/DB check failed: %s", e, exc_info=True)
                results["engines"] = {"status": "error", "error": str(e)}

        # ── Benchmark price sync (SPY, etc.) ─────────────────
        try:
            logger.info("[sync/benchmark] Syncing benchmark prices (SPY.US)...")
            from datetime import date as _date
            from datetime import timedelta as _td

            from apathis.data.writer import DataWriter as _DataWriter
            from apathis.data_ingestion.eodhd_client import EodhdClient as _EodhdClient
            from apathis.data_ingestion.eodhd_prices import ingest_eodhd_prices_for_instrument

            _end = _date.today()
            _start = _end - _td(days=60)
            _client = _EodhdClient()
            _writer = _DataWriter(db_manager=get_db_manager())
            _res = ingest_eodhd_prices_for_instrument(
                instrument_id="SPY.US",
                eodhd_symbol="SPY.US",
                start_date=_start,
                end_date=_end,
                currency="USD",
                client=_client,
                writer=_writer,
            )
            logger.info("[sync/benchmark] SPY.US: %d bars written", _res.bars_written)
            results["benchmark"] = {"status": "ok", "bars_written": _res.bars_written}
        except Exception as e:
            logger.error("[sync/benchmark] Benchmark sync failed: %s", e, exc_info=True)
            results["benchmark"] = {"status": "error", "error": str(e)}

        if "nations" in sources:
            try:
                logger.info("[sync/nations] Re-scoring all nations...")
                from datetime import date as _date

                from apathis.nation.engine import NationScoringEngine
                from apathis.nation.model_basic import BasicNationScoringModel
                from apathis.nation.storage import (
                    NationMacroStorage,
                    NationScoreStorage,
                    PersonProfileStorage,
                )

                db = get_db_manager()
                macro_st = NationMacroStorage(db)
                prof_st = PersonProfileStorage(db)
                score_st = NationScoreStorage(db)
                model = BasicNationScoringModel(macro_storage=macro_st, profile_storage=prof_st)
                engine = NationScoringEngine(model=model, storage=score_st)

                # Discover all nations with macro data.
                with db.get_historical_connection() as conn:
                    cur = conn.cursor()
                    try:
                        cur.execute("SELECT DISTINCT nation FROM nation_macro_indicators")
                        nation_rows = cur.fetchall()
                    finally:
                        cur.close()

                scored = []
                for (nation_code,) in nation_rows:
                    try:
                        engine.score_and_save(nation_code, _date.today())
                        scored.append(nation_code)
                    except Exception:
                        logger.exception("[sync/nations] Failed to score %s", nation_code)

                logger.info("[sync/nations] Scored %d nations: %s", len(scored), scored)
                results["nations"] = {"status": "ok", "scored": scored}
            except Exception as e:
                logger.error("[sync/nations] Nation scoring failed: %s", e, exc_info=True)
                results["nations"] = {"status": "error", "error": str(e)}

        logger.info("[sync] Sync job complete — results=%s", results)
        return results

    rec = submit_job(
        prefix="sync",
        job_type="DATA_SYNC",
        message=f"Syncing data from: {', '.join(sources)}",
        fn=_sync_job,
    )

    return SyncDataResponse(
        job_id=rec.job_id,
        status=rec.status,
        sources_requested=sources,
        message=f"Sync job submitted: {rec.job_id}",
    )


@router.get("/jobs/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str = Path(..., description="Job identifier")) -> JobStatus:
    """Query job status and progress.

    Used by UI to poll for job completion and display progress.
    """

    rec = get_job(job_id)
    if rec is not None:
        return _job_status_from_record(rec)

    return JobStatus(
        job_id=job_id,
        type="UNKNOWN",
        status="NOT_FOUND",
        created_at=now_utc(),
        message=f"Job {job_id} not found in registry",
    )
