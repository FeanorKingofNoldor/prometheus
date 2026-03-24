"""Prometheus v2 – Synthetic Scenario Engine.

This module implements the Synthetic Scenario Engine, supporting both
scenario-path generation (spec 170) and full synthetic market-reality
generation for out-of-sample backtester validation.

Scenario families (original):
  - Type A – historical windows
  - BOOTSTRAP – day-level bootstrap
  - STRESSED – worst-day stress

Market-reality generation (new):
  - BLOCK_BOOTSTRAP – block-bootstrapped full OHLCV price panels with
    sector ETFs, derived fragility, and lambda score handling.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from apathis.core.database import DatabaseManager
from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger
from apathis.data.reader import DataReader
from apathis.sector.health import SECTOR_ETF_MAP

from .storage import ScenarioPathRow, ScenarioStorage
from .types import RealityConfig, ScenarioRequest, ScenarioSetRef, SyntheticReality

logger = get_logger(__name__)


@dataclass
class SyntheticScenarioEngine:
    """Generate and manage synthetic scenario sets and market realities.

    Scenario families (original):

    - ``HISTORICAL``: contiguous historical windows (Type A).
    - ``BOOTSTRAP``: day-level bootstrap of returns.
    - ``STRESSED``: stress scenarios from worst historical days.

    Market-reality generation (new):

    - :meth:`generate_reality` / :meth:`generate_realities` produce
      complete synthetic market histories (OHLCV prices, sector ETF
      prices, fragility scores, lambda tables) suitable for direct
      consumption by the C++ backtester.
    """

    db_manager: DatabaseManager
    data_reader: DataReader

    def __post_init__(self) -> None:
        self._storage = ScenarioStorage(db_manager=self.db_manager)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_scenario_set(self, request: ScenarioRequest) -> ScenarioSetRef:
        """Generate and persist a scenario set described by ``request``.

        Supported categories (case-insensitive):

        - ``HISTORICAL``: contiguous historical windows.
        - ``BOOTSTRAP``: day-level bootstrap of return rows.
        - ``STRESSED``: stress scenarios built from worst historical days,
          optionally scaled via ``request.generator_spec``.
        """

        if request.horizon_days <= 0:
            msg = "horizon_days must be positive"
            raise ValueError(msg)
        if request.num_paths <= 0:
            msg = "num_paths must be positive"
            raise ValueError(msg)
        if not request.markets:
            msg = "markets must not be empty"
            raise ValueError(msg)

        category = request.category.upper()
        if category not in {"HISTORICAL", "BOOTSTRAP", "STRESSED"}:
            msg = f"Unsupported scenario category: {request.category!r}"
            raise NotImplementedError(msg)

        # Create scenario_set row first so we have an identifier for
        # subsequent path rows.
        set_ref = self._storage.create_scenario_set(request=request, created_by="system")

        instrument_ids = self._load_instruments_for_markets(request.markets)
        if not instrument_ids:
            logger.warning(
                "SyntheticScenarioEngine.generate_scenario_set: no instruments for markets %s",
                request.markets,
            )
            return set_ref

        rows: List[ScenarioPathRow] = []
        H = request.horizon_days
        num_paths = request.num_paths

        rng = np.random.default_rng()

        if category == "HISTORICAL":
            windows, instrument_ids = self._build_historical_windows(
                instrument_ids=instrument_ids,
                horizon_days=request.horizon_days,
                base_start=request.base_date_start,
                base_end=request.base_date_end,
            )

            if not windows:
                logger.warning(
                    "SyntheticScenarioEngine.generate_scenario_set: no viable windows for request %s",
                    request.name,
                )
                return set_ref

            # Sample with replacement over contiguous windows.
            window_indices = rng.integers(low=0, high=len(windows), size=num_paths)

            for scenario_id, window_idx in enumerate(window_indices):
                window_returns = windows[window_idx]
                # Ensure window is (H, N) in time-major order.
                if window_returns.shape[0] < H:
                    continue

                for h in range(H):
                    row_returns = window_returns[h]
                    for inst_idx, inst_id in enumerate(instrument_ids):
                        r = float(row_returns[inst_idx])
                        rows.append(
                            ScenarioPathRow(
                                scenario_id=scenario_id,
                                horizon_index=h + 1,  # use 1..H; 0 reserved for baseline
                                instrument_id=inst_id,
                                factor_id="__INSTRUMENT__",
                                macro_id="__NONE__",
                                return_value=r,
                            )
                        )

                # Insert baseline horizon_index=0 rows with zero return.
                for inst_id in instrument_ids:
                    rows.append(
                        ScenarioPathRow(
                            scenario_id=scenario_id,
                            horizon_index=0,
                            instrument_id=inst_id,
                            factor_id="__INSTRUMENT__",
                            macro_id="__NONE__",
                            return_value=0.0,
                        )
                    )

        else:
            # For BOOTSTRAP/STRESSED, work directly with the full returns
            # panel and construct paths by sampling rows.
            returns, instrument_ids = self._build_returns_panel(
                instrument_ids=instrument_ids,
                base_start=request.base_date_start,
                base_end=request.base_date_end,
            )
            if returns.size == 0:
                logger.warning(
                    "SyntheticScenarioEngine.generate_scenario_set: empty returns panel for request %s",
                    request.name,
                )
                return set_ref

            num_days, _ = returns.shape

            # Optional stress configuration for STRESSED category.
            stress_q = 0.1
            stress_scale = 1.5
            if request.generator_spec is not None:
                stress_q = float(request.generator_spec.get("stress_quantile", stress_q))
                stress_scale = float(request.generator_spec.get("stress_scale", stress_scale))

            if category == "STRESSED":
                # Score days by cross-sectional mean return and focus on the
                # lowest quantile (worst days).
                day_scores = returns.mean(axis=1)
                q = max(min(stress_q, 0.5), 0.0)
                threshold = np.quantile(day_scores, q) if 0.0 < q < 1.0 else np.min(day_scores)
                candidate_indices = np.where(day_scores <= threshold)[0]
                if candidate_indices.size == 0:
                    candidate_indices = np.arange(num_days)
            else:  # BOOTSTRAP
                candidate_indices = np.arange(num_days)

            for scenario_id in range(num_paths):
                # Sample H days with replacement from candidate_indices.
                day_indices = rng.integers(low=0, high=candidate_indices.size, size=H)
                for h, idx in enumerate(day_indices, start=1):
                    row_returns = returns[candidate_indices[idx], :]
                    if category == "STRESSED":
                        row_returns = row_returns * stress_scale
                    for inst_idx, inst_id in enumerate(instrument_ids):
                        r = float(row_returns[inst_idx])
                        rows.append(
                            ScenarioPathRow(
                                scenario_id=scenario_id,
                                horizon_index=h,
                                instrument_id=inst_id,
                                factor_id="__INSTRUMENT__",
                                macro_id="__NONE__",
                                return_value=r,
                            )
                        )

                for inst_id in instrument_ids:
                    rows.append(
                        ScenarioPathRow(
                            scenario_id=scenario_id,
                            horizon_index=0,
                            instrument_id=inst_id,
                            factor_id="__INSTRUMENT__",
                            macro_id="__NONE__",
                            return_value=0.0,
                        )
                    )

        self._storage.save_scenario_paths(set_ref.scenario_set_id, rows)

        logger.info(
            "SyntheticScenarioEngine.generate_scenario_set: id=%s category=%s H=%d paths=%d instruments=%d",
            set_ref.scenario_set_id,
            request.category,
            request.horizon_days,
            request.num_paths,
            len(instrument_ids),
        )

        return set_ref

    def list_scenario_sets(self, category: str | None = None) -> List[ScenarioSetRef]:
        """Return scenario sets, optionally filtered by category."""

        return self._storage.list_scenario_sets(category=category)

    def get_scenario_set_metadata(self, scenario_set_id: str) -> Dict[str, object]:
        """Return raw metadata for a scenario set."""

        return self._storage.get_scenario_set_metadata(scenario_set_id)

    # ------------------------------------------------------------------
    # Market-reality generation
    # ------------------------------------------------------------------

    def generate_realities(
        self,
        config: RealityConfig,
        max_workers: int = 0,
    ) -> List[SyntheticReality]:
        """Generate multiple synthetic market realities.

        Each reality is an independent full market history produced by
        the block bootstrap with a different random seed derived from
        ``config.seed``.

        When the C++ engine is available, realities are generated in
        parallel using ``max_workers`` threads (default: all cores).
        The GIL is released during C++ computation so true parallelism
        is achieved.
        """

        import os
        from concurrent.futures import ThreadPoolExecutor, as_completed

        if max_workers <= 0:
            max_workers = os.cpu_count() or 1

        # --- Load source data ONCE (shared across all realities) ------
        category = config.category.upper()
        if category != "BLOCK_BOOTSTRAP":
            msg = f"Unsupported reality category: {config.category!r}"
            raise NotImplementedError(msg)

        if config.base_date_end is None:
            raise ValueError("base_date_end is required")

        instrument_ids = self._load_instruments_for_markets(config.markets)
        etf_map = SECTOR_ETF_MAP
        etf_ids = sorted(etf_map.keys())
        all_ids = instrument_ids + [e for e in etf_ids if e not in set(instrument_ids)]

        base_start = config.base_date_start or (config.base_date_end - timedelta(days=365 * 10))
        df = self.data_reader.read_prices(
            instrument_ids=all_ids,
            start_date=base_start,
            end_date=config.base_date_end,
        )
        if df.empty:
            raise ValueError(f"No price data for reality generation ({config.name})")

        close_panel = (
            df.pivot_table(index="trade_date", columns="instrument_id", values="close")
            .sort_index()
        )
        min_obs = max(config.block_length * 2, 20)
        close_panel = close_panel.dropna(axis=1, thresh=min_obs)

        if close_panel.shape[1] == 0 or close_panel.shape[0] < config.block_length + 1:
            raise ValueError(f"Insufficient data for block bootstrap ({close_panel.shape})")

        panel_ids = [str(c) for c in close_panel.columns]

        # Pre-compute shared numpy source arrays.
        source_close = close_panel.to_numpy(dtype=np.float64, na_value=0.0)
        np.nan_to_num(source_close, copy=False, nan=0.0)

        source_high = source_low = source_volume = None
        for fld in ["high", "low", "volume"]:
            piv = df.pivot_table(index="trade_date", columns="instrument_id", values=fld)
            piv = piv.sort_index().reindex(columns=close_panel.columns)
            arr = piv.to_numpy(dtype=np.float64, na_value=0.0)
            np.nan_to_num(arr, copy=False, nan=0.0)
            if fld == "high":
                source_high = arr
            elif fld == "low":
                source_low = arr
            else:
                source_volume = arr

        # Pre-compute shared metadata: sectors, ETF mapping, trade dates.
        H = config.horizon_days
        synth_trade_dates = pd.bdate_range(
            start="2000-01-03", periods=H, freq="B"
        ).date.tolist()
        trade_dates_int = [d.year * 10000 + d.month * 100 + d.day for d in synth_trade_dates]

        real_inst_to_sector: Dict[str, str] = {}
        real_ids_list = list(panel_ids)
        if real_ids_list:
            sql_sec = """
                SELECT i.instrument_id, COALESCE(ic.sector, 'Unknown')
                FROM instruments i
                LEFT JOIN issuer_classifications ic ON i.issuer_id = ic.issuer_id
                WHERE i.instrument_id = ANY(%s)
            """
            with self.db_manager.get_runtime_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql_sec, (real_ids_list,))
                    for rid, sec in cursor.fetchall():
                        real_inst_to_sector[rid] = sec
                finally:
                    cursor.close()
        for etf_id, sec_name in etf_map.items():
            real_inst_to_sector[etf_id] = sec_name
        panel_sectors = [real_inst_to_sector.get(rid, "Unknown") for rid in panel_ids]

        # --- Try C++ engine -------------------------------------------
        try:
            import prom2_cpp as prom2
            use_cpp = True
        except ImportError:
            prom2 = None
            use_cpp = False
            logger.warning("prom2_cpp not available, falling back to serial Python engine")

        base_seed = config.seed

        # --- Build one reality from pre-loaded data -------------------
        def _build_one(i: int) -> SyntheticReality:
            child_seed = (base_seed + i) if base_seed is not None else None
            child_name = f"{config.name}_R{i:03d}"

            reality_id = generate_uuid()
            prefix = f"SYNTH_{reality_id[:8]}"

            if use_cpp:
                cpp_cfg = {
                    "horizon_days": H,
                    "block_length": config.block_length,
                    "seed": child_seed if child_seed is not None else 42,
                    "base_price": config.base_price,
                }
                cpp_result = prom2.generate_reality(
                    source_close, source_high, source_low, source_volume, cpp_cfg,
                )
            else:
                cpp_result = None

            real_to_synth: Dict[str, str] = {}
            for rid in panel_ids:
                real_to_synth[rid] = f"{prefix}_{rid}"
            synth_ids = [real_to_synth[r] for r in panel_ids]

            synth_etf_ids: Dict[str, str] = {}
            for etf_id_k, sec_name_v in etf_map.items():
                if etf_id_k in real_to_synth:
                    synth_etf_ids[real_to_synth[etf_id_k]] = sec_name_v

            fragility_df: Optional[pd.DataFrame] = None
            if config.include_fragility and cpp_result is not None:
                fragility_df = pd.DataFrame({
                    "as_of_date": synth_trade_dates,
                    "fragility_score": np.round(cpp_result["fragility"], 6),
                })

            rng = np.random.default_rng(child_seed)
            lambda_df: Optional[pd.DataFrame] = None
            if config.lambda_mode != "none" and config.lambda_csv_path is not None:
                child_cfg = RealityConfig(
                    name=child_name,
                    category=config.category,
                    horizon_days=H,
                    num_realities=1,
                    block_length=config.block_length,
                    markets=config.markets,
                    base_date_start=config.base_date_start,
                    base_date_end=config.base_date_end,
                    seed=child_seed,
                    include_fragility=config.include_fragility,
                    lambda_mode=config.lambda_mode,
                    lambda_noise_std=config.lambda_noise_std,
                    lambda_csv_path=config.lambda_csv_path,
                    base_price=config.base_price,
                )
                lambda_df = self._prepare_lambda_scores(
                    config=child_cfg,
                    synth_trade_dates=synth_trade_dates,
                    rng=rng,
                )

            n_assets = len(panel_ids)
            meta: Dict[str, Any] = {
                "category": config.category,
                "block_length": config.block_length,
                "horizon_days": H,
                "n_instruments": n_assets,
                "n_sector_etfs": len(synth_etf_ids),
                "source_days": source_close.shape[0],
                "seed": child_seed,
                "engine": "cpp" if use_cpp else "python",
            }

            # For Python fallback, build prices_df.
            prices_df: Optional[pd.DataFrame] = None
            if not use_cpp:
                rng2 = np.random.default_rng(child_seed)
                close_arr = close_panel.to_numpy(dtype=float)
                returns = np.diff(close_arr, axis=0) / np.where(
                    close_arr[:-1] != 0, close_arr[:-1], 1.0
                )
                np.nan_to_num(returns, copy=False, nan=0.0, posinf=0.0, neginf=0.0)
                synth_returns = self._block_bootstrap(
                    returns=returns, horizon=H, block_length=config.block_length, rng=rng2,
                )
                synth_close = np.empty((H, n_assets), dtype=float)
                synth_close[0] = config.base_price * (1.0 + synth_returns[0])
                for t in range(1, H):
                    synth_close[t] = synth_close[t - 1] * (1.0 + synth_returns[t])
                synth_close = np.maximum(synth_close, 0.01)

                # Minimal prices_df for Python-path compatibility.
                rows_list: List[Dict[str, Any]] = []
                for t_idx in range(H):
                    for a_idx, sid in enumerate(synth_ids):
                        rows_list.append({
                            "instrument_id": sid,
                            "trade_date": synth_trade_dates[t_idx],
                            "open": round(float(synth_close[t_idx, a_idx]), 4),
                            "high": round(float(synth_close[t_idx, a_idx]), 4),
                            "low": round(float(synth_close[t_idx, a_idx]), 4),
                            "close": round(float(synth_close[t_idx, a_idx]), 4),
                            "adjusted_close": round(float(synth_close[t_idx, a_idx]), 4),
                            "volume": 1000,
                            "currency": "USD",
                            "metadata": {},
                        })
                prices_df = pd.DataFrame(rows_list)

            logger.info(
                "SyntheticScenarioEngine._build_one: id=%s instruments=%d days=%d [%s]",
                reality_id, n_assets, H, "C++" if use_cpp else "Python",
            )

            return SyntheticReality(
                reality_id=reality_id,
                config=RealityConfig(
                    name=child_name,
                    category=config.category,
                    horizon_days=H,
                    num_realities=1,
                    block_length=config.block_length,
                    markets=config.markets,
                    base_date_start=config.base_date_start,
                    base_date_end=config.base_date_end,
                    seed=child_seed,
                    include_fragility=config.include_fragility,
                    lambda_mode=config.lambda_mode,
                    lambda_noise_std=config.lambda_noise_std,
                    lambda_csv_path=config.lambda_csv_path,
                    base_price=config.base_price,
                ),
                prices_df=prices_df,
                instrument_ids=synth_ids,
                sector_etf_ids=synth_etf_ids,
                real_to_synth=real_to_synth,
                fragility_df=fragility_df,
                lambda_df=lambda_df,
                metadata=meta,
                cpp_arrays=cpp_result,
                trade_dates=synth_trade_dates,
                trade_dates_int=trade_dates_int,
                panel_ids=panel_ids,
                panel_sectors=panel_sectors,
            )

        # --- Execute in parallel (GIL released in C++) ----------------
        n = config.num_realities
        if use_cpp and n > 1:
            workers = min(max_workers, n)
            realities: List[Optional[SyntheticReality]] = [None] * n
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(_build_one, i): i for i in range(n)}
                for fut in as_completed(futures):
                    idx = futures[fut]
                    realities[idx] = fut.result()
            result_list = [r for r in realities if r is not None]
        else:
            result_list = [_build_one(i) for i in range(n)]

        logger.info(
            "SyntheticScenarioEngine.generate_realities: generated %d realities for %r (workers=%d)",
            len(result_list),
            config.name,
            min(max_workers, n) if use_cpp and n > 1 else 1,
        )
        return result_list

    def generate_reality(self, config: RealityConfig) -> SyntheticReality:
        """Generate a single synthetic market reality.

        Uses the C++ engine for compute-heavy work (block bootstrap,
        OHLCV synthesis, fragility derivation) and keeps lambda score
        handling in Python.
        """

        category = config.category.upper()
        if category != "BLOCK_BOOTSTRAP":
            msg = f"Unsupported reality category: {config.category!r}"
            raise NotImplementedError(msg)

        if config.base_date_end is None:
            msg = "base_date_end is required for reality generation"
            raise ValueError(msg)
        if config.horizon_days <= 0:
            msg = "horizon_days must be positive"
            raise ValueError(msg)

        reality_id = generate_uuid()
        prefix = f"SYNTH_{reality_id[:8]}"

        # 1) Load instruments and sector ETF ids.
        instrument_ids = self._load_instruments_for_markets(config.markets)
        etf_map = SECTOR_ETF_MAP  # real ETF id -> sector name
        etf_ids = sorted(etf_map.keys())

        # Combined universe for joint bootstrap.
        all_ids = instrument_ids + [e for e in etf_ids if e not in set(instrument_ids)]

        # 2) Load full OHLCV price history.
        base_start = config.base_date_start or (config.base_date_end - timedelta(days=365 * 10))
        df = self.data_reader.read_prices(
            instrument_ids=all_ids,
            start_date=base_start,
            end_date=config.base_date_end,
        )
        if df.empty:
            msg = f"No price data found for reality generation ({config.name})"
            raise ValueError(msg)

        # 3) Build aligned panels (pivot by trade_date).
        close_panel = (
            df.pivot_table(index="trade_date", columns="instrument_id", values="close")
            .sort_index()
        )
        # Drop columns with insufficient data.
        min_obs = max(config.block_length * 2, 20)
        close_panel = close_panel.dropna(axis=1, thresh=min_obs)

        if close_panel.shape[1] == 0 or close_panel.shape[0] < config.block_length + 1:
            msg = f"Insufficient data for block bootstrap ({close_panel.shape})"
            raise ValueError(msg)

        # Aligned instrument list.
        panel_ids = [str(c) for c in close_panel.columns]
        n_assets = len(panel_ids)

        # Build source numpy arrays [T, N] for C++.
        source_close = close_panel.to_numpy(dtype=np.float64, na_value=0.0)
        np.nan_to_num(source_close, copy=False, nan=0.0)

        for fld in ["high", "low", "volume"]:
            piv = df.pivot_table(index="trade_date", columns="instrument_id", values=fld)
            piv = piv.sort_index().reindex(columns=close_panel.columns)
            arr = piv.to_numpy(dtype=np.float64, na_value=0.0)
            np.nan_to_num(arr, copy=False, nan=0.0)
            if fld == "high":
                source_high = arr
            elif fld == "low":
                source_low = arr
            else:
                source_volume = arr

        H = config.horizon_days

        # 4) Call C++ engine for the heavy computation.
        try:
            import prom2_cpp as prom2

            cpp_cfg = {
                "horizon_days": H,
                "block_length": config.block_length,
                "seed": config.seed if config.seed is not None else 42,
                "base_price": config.base_price,
            }

            cpp_result = prom2.generate_reality(
                source_close, source_high, source_low, source_volume, cpp_cfg,
            )
            use_cpp = True
        except ImportError:
            logger.warning("prom2_cpp not available, falling back to Python engine")
            use_cpp = False
            cpp_result = None

        if not use_cpp:
            return self._generate_reality_python(
                config=config,
                reality_id=reality_id,
                prefix=prefix,
                panel_ids=panel_ids,
                close_panel=close_panel,
                df=df,
                etf_map=etf_map,
            )

        # 5) Build synthetic instrument ID mapping.
        real_to_synth: Dict[str, str] = {}
        for rid in panel_ids:
            real_to_synth[rid] = f"{prefix}_{rid}"

        synth_ids = [real_to_synth[r] for r in panel_ids]

        # Sector ETF mapping.
        synth_etf_ids: Dict[str, str] = {}
        for etf_id, sector_name in etf_map.items():
            if etf_id in real_to_synth:
                synth_etf_ids[real_to_synth[etf_id]] = sector_name

        # 6) Synthetic trade dates.
        synth_trade_dates = pd.bdate_range(
            start="2000-01-03", periods=H, freq="B"
        ).date.tolist()
        # YYYYMMDD ints for the C++ DB writer.
        trade_dates_int = [d.year * 10000 + d.month * 100 + d.day for d in synth_trade_dates]

        # 7) Look up sectors for each real instrument.
        real_inst_to_sector: Dict[str, str] = {}
        real_ids = list(real_to_synth.keys())
        if real_ids:
            sql_sec = """
                SELECT i.instrument_id, COALESCE(ic.sector, 'Unknown')
                FROM instruments i
                LEFT JOIN issuer_classifications ic ON i.issuer_id = ic.issuer_id
                WHERE i.instrument_id = ANY(%s)
            """
            with self.db_manager.get_runtime_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql_sec, (real_ids,))
                    for rid, sec in cursor.fetchall():
                        real_inst_to_sector[rid] = sec
                finally:
                    cursor.close()
        for etf_id, sec_name in etf_map.items():
            real_inst_to_sector[etf_id] = sec_name
        panel_sectors = [real_inst_to_sector.get(rid, "Unknown") for rid in panel_ids]

        # 8) Build fragility DataFrame from C++ array.
        fragility_df: Optional[pd.DataFrame] = None
        if config.include_fragility and cpp_result is not None:
            frag_arr = cpp_result["fragility"]
            fragility_df = pd.DataFrame({
                "as_of_date": synth_trade_dates,
                "fragility_score": np.round(frag_arr, 6),
            })

        # 9) Lambda scores (kept in Python — small data).
        rng = np.random.default_rng(config.seed)
        lambda_df: Optional[pd.DataFrame] = None
        if config.lambda_mode != "none" and config.lambda_csv_path is not None:
            lambda_df = self._prepare_lambda_scores(
                config=config,
                synth_trade_dates=synth_trade_dates,
                rng=rng,
            )

        metadata: Dict[str, Any] = {
            "category": config.category,
            "block_length": config.block_length,
            "horizon_days": H,
            "n_instruments": n_assets,
            "n_sector_etfs": len(synth_etf_ids),
            "source_days": source_close.shape[0],
            "seed": config.seed,
            "engine": "cpp",
        }

        logger.info(
            "SyntheticScenarioEngine.generate_reality: id=%s instruments=%d etfs=%d days=%d [C++]",
            reality_id,
            n_assets,
            len(synth_etf_ids),
            H,
        )

        return SyntheticReality(
            reality_id=reality_id,
            config=config,
            prices_df=None,
            instrument_ids=synth_ids,
            sector_etf_ids=synth_etf_ids,
            real_to_synth=real_to_synth,
            fragility_df=fragility_df,
            lambda_df=lambda_df,
            metadata=metadata,
            cpp_arrays=cpp_result,
            trade_dates=synth_trade_dates,
            trade_dates_int=trade_dates_int,
            panel_ids=panel_ids,
            panel_sectors=panel_sectors,
        )

    # ------------------------------------------------------------------
    # Python fallback (used when C++ engine is unavailable)
    # ------------------------------------------------------------------

    def _generate_reality_python(
        self,
        *,
        config: RealityConfig,
        reality_id: str,
        prefix: str,
        panel_ids: List[str],
        close_panel: pd.DataFrame,
        df: pd.DataFrame,
        etf_map: Dict[str, str],
    ) -> SyntheticReality:
        """Pure-Python reality generation (slow fallback)."""

        rng = np.random.default_rng(config.seed)
        H = config.horizon_days

        ohlcv_fields = ["open", "high", "low", "close", "adjusted_close", "volume"]
        source_panels: Dict[str, pd.DataFrame] = {}
        for fld in ohlcv_fields:
            piv = df.pivot_table(index="trade_date", columns="instrument_id", values=fld)
            piv = piv.sort_index().reindex(columns=close_panel.columns)
            source_panels[fld] = piv

        close_arr = close_panel.to_numpy(dtype=float)
        returns = np.diff(close_arr, axis=0) / np.where(
            close_arr[:-1] != 0, close_arr[:-1], 1.0
        )
        np.nan_to_num(returns, copy=False, nan=0.0, posinf=0.0, neginf=0.0)
        n_days, n_assets = returns.shape

        hist_high = source_panels["high"].to_numpy(dtype=float)
        hist_low = source_panels["low"].to_numpy(dtype=float)
        hist_close_vals = source_panels["close"].to_numpy(dtype=float)
        range_ratio = (hist_high - hist_low) / np.where(
            hist_close_vals > 0, hist_close_vals, 1.0
        )
        np.nan_to_num(range_ratio, copy=False, nan=0.02, posinf=0.05, neginf=0.0)
        median_range = np.nanmedian(range_ratio, axis=0)
        median_range = np.clip(median_range, 0.005, 0.10)

        hist_vol = source_panels["volume"].to_numpy(dtype=float)
        hist_vol = np.where(hist_vol > 0, hist_vol, 1.0)
        log_vol_mean = np.nanmean(np.log(hist_vol), axis=0)
        log_vol_std = np.nanstd(np.log(hist_vol), axis=0)
        log_vol_std = np.clip(log_vol_std, 0.1, 2.0)

        synth_returns = self._block_bootstrap(
            returns=returns, horizon=H, block_length=config.block_length, rng=rng,
        )

        base_px = config.base_price
        synth_close = np.empty((H, n_assets), dtype=float)
        synth_close[0] = base_px * (1.0 + synth_returns[0])
        for t in range(1, H):
            synth_close[t] = synth_close[t - 1] * (1.0 + synth_returns[t])
        synth_close = np.maximum(synth_close, 0.01)

        half_range = median_range[np.newaxis, :] / 2.0
        noise = rng.standard_normal((H, n_assets)) * 0.3
        synth_high = synth_close * (1.0 + half_range * (1.0 + np.abs(noise)))
        synth_low = synth_close * (1.0 - half_range * (1.0 + np.abs(noise)))
        synth_low = np.maximum(synth_low, 0.01)

        open_noise = rng.standard_normal((H, n_assets)) * 0.002
        synth_open = synth_close * (1.0 + open_noise)
        synth_high = np.maximum(synth_high, np.maximum(synth_open, synth_close))
        synth_low = np.minimum(synth_low, np.minimum(synth_open, synth_close))
        synth_low = np.maximum(synth_low, 0.01)

        synth_adj_close = synth_close.copy()

        synth_volume = np.exp(
            rng.normal(loc=log_vol_mean[np.newaxis, :], scale=log_vol_std[np.newaxis, :], size=(H, n_assets))
        ).astype(int)
        synth_volume = np.maximum(synth_volume, 1)

        real_to_synth: Dict[str, str] = {}
        for rid in panel_ids:
            real_to_synth[rid] = f"{prefix}_{rid}"
        synth_ids = [real_to_synth[r] for r in panel_ids]

        synth_etf_ids: Dict[str, str] = {}
        for etf_id, sector_name in etf_map.items():
            if etf_id in real_to_synth:
                synth_etf_ids[real_to_synth[etf_id]] = sector_name

        synth_trade_dates = pd.bdate_range(
            start="2000-01-03", periods=H, freq="B"
        ).date.tolist()

        rows_list: List[Dict[str, Any]] = []
        for t_idx in range(H):
            td = synth_trade_dates[t_idx]
            for a_idx, synth_id in enumerate(synth_ids):
                rows_list.append({
                    "instrument_id": synth_id,
                    "trade_date": td,
                    "open": round(float(synth_open[t_idx, a_idx]), 4),
                    "high": round(float(synth_high[t_idx, a_idx]), 4),
                    "low": round(float(synth_low[t_idx, a_idx]), 4),
                    "close": round(float(synth_close[t_idx, a_idx]), 4),
                    "adjusted_close": round(float(synth_adj_close[t_idx, a_idx]), 4),
                    "volume": int(synth_volume[t_idx, a_idx]),
                    "currency": "USD",
                    "metadata": {},
                })

        prices_df = pd.DataFrame(rows_list)

        fragility_df: Optional[pd.DataFrame] = None
        if config.include_fragility:
            fragility_df = self._derive_fragility_scores(
                synth_close=synth_close,
                synth_returns=synth_returns,
                trade_dates=synth_trade_dates,
            )

        lambda_df: Optional[pd.DataFrame] = None
        if config.lambda_mode != "none" and config.lambda_csv_path is not None:
            lambda_df = self._prepare_lambda_scores(
                config=config,
                synth_trade_dates=synth_trade_dates,
                rng=rng,
            )

        logger.info(
            "SyntheticScenarioEngine.generate_reality: id=%s instruments=%d etfs=%d days=%d [Python]",
            reality_id, len(synth_ids), len(synth_etf_ids), H,
        )

        return SyntheticReality(
            reality_id=reality_id,
            config=config,
            prices_df=prices_df,
            instrument_ids=synth_ids,
            sector_etf_ids=synth_etf_ids,
            real_to_synth=real_to_synth,
            fragility_df=fragility_df,
            lambda_df=lambda_df,
            metadata={
                "category": config.category,
                "block_length": config.block_length,
                "horizon_days": H,
                "n_instruments": len(synth_ids),
                "n_sector_etfs": len(synth_etf_ids),
                "source_days": n_days,
                "seed": config.seed,
                "engine": "python",
            },
            trade_dates=synth_trade_dates,
        )

    # ------------------------------------------------------------------
    # Internal helpers — scenario paths
    # ------------------------------------------------------------------

    def _load_instruments_for_markets(self, markets: List[str]) -> List[str]:
        """Return instrument_ids for the given markets from runtime DB.

        Excludes synthetic instruments (prefixed with ``SYNTH_``) so that
        leftover data from prior campaign runs does not contaminate the
        source universe.
        """

        sql = """
            SELECT instrument_id
            FROM instruments
            WHERE market_id = ANY(%s)
              AND status = 'ACTIVE'
              AND instrument_id NOT LIKE 'SYNTH_%%'
            ORDER BY instrument_id
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (list(markets),))
                rows = cursor.fetchall()
            finally:
                cursor.close()

        return [inst_id for (inst_id,) in rows]

    def _build_historical_windows(
        self,
        instrument_ids: List[str],
        horizon_days: int,
        base_start: date | None,
        base_end: date | None,
    ) -> tuple[List[np.ndarray], List[str]]:
        """Construct contiguous windows of simple returns.

        Returns
        -------
        windows:
            List of numpy arrays with shape (H, N) where H is the horizon
            length and N is the number of instruments.
        instrument_ids:
            The ordered list of instrument_ids corresponding to the columns
            in each window. This may be a strict subset of the input
            ``instrument_ids`` when some requested instruments have no
            price history in the requested window.
        """

        returns, aligned_instrument_ids = self._build_returns_panel(
            instrument_ids=instrument_ids,
            base_start=base_start,
            base_end=base_end,
        )
        if returns.size == 0:
            return [], []

        num_days, num_instruments = returns.shape
        if num_instruments == 0 or num_days < horizon_days:
            return [], []

        windows: List[np.ndarray] = []
        # Build all possible contiguous windows of length horizon_days.
        for start_idx in range(0, num_days - horizon_days + 1):
            window = returns[start_idx : start_idx + horizon_days, :]
            # Shape (H, N)
            windows.append(window)

        return windows, aligned_instrument_ids

    def _build_returns_panel(
        self,
        instrument_ids: List[str],
        base_start: date | None,
        base_end: date | None,
    ) -> tuple[np.ndarray, List[str]]:
        """Return a panel of simple daily returns for instruments.

        Returns
        -------
        returns:
            2D numpy array with shape (T, N) where T is the number of
            trading days with non-null returns and N is the number of
            instruments with price history.
        instrument_ids:
            The ordered list of instrument_ids corresponding to the columns
            in ``returns``.
        """

        if base_end is None:
            msg = "base_date_end must be provided for scenario generation"
            raise ValueError(msg)
        if base_start is None:
            base_start = base_end

        df = self.data_reader.read_prices(
            instrument_ids=instrument_ids,
            start_date=base_start,
            end_date=base_end,
        )
        if df.empty:
            return np.zeros((0, 0), dtype=float), []

        prices = (
            df[["instrument_id", "trade_date", "close"]]
            .pivot(index="trade_date", columns="instrument_id", values="close")
            .sort_index()
        )
        if prices.empty:
            return np.zeros((0, 0), dtype=float), []

        aligned_instrument_ids = [str(col) for col in prices.columns]

        returns = prices.pct_change().dropna(how="all")
        if returns.empty:
            return np.zeros((0, 0), dtype=float), []

        values = returns.to_numpy(dtype=float)
        return values, aligned_instrument_ids

    # ------------------------------------------------------------------
    # Internal helpers — market-reality generation
    # ------------------------------------------------------------------

    @staticmethod
    def _block_bootstrap(
        returns: np.ndarray,
        horizon: int,
        block_length: int,
        rng: np.random.Generator,
    ) -> np.ndarray:
        """Block-bootstrap daily returns preserving cross-section.

        Parameters
        ----------
        returns : (T, N) array of daily returns.
        horizon : number of synthetic days to produce.
        block_length : length of each contiguous block.
        rng : numpy Generator for reproducibility.

        Returns
        -------
        synth : (horizon, N) array of bootstrapped returns.
        """

        T, N = returns.shape
        B = min(block_length, T)  # clamp if history shorter than block
        if B < 1:
            B = 1

        # Maximum valid starting index for a block.
        max_start = T - B
        if max_start < 0:
            max_start = 0

        synth = np.empty((horizon, N), dtype=float)
        pos = 0

        while pos < horizon:
            start = int(rng.integers(0, max_start + 1))
            end = min(start + B, T)
            chunk_len = end - start
            take = min(chunk_len, horizon - pos)
            synth[pos : pos + take, :] = returns[start : start + take, :]
            pos += take

        return synth

    @staticmethod
    def _derive_fragility_scores(
        synth_close: np.ndarray,
        synth_returns: np.ndarray,
        trade_dates: List[date],
    ) -> pd.DataFrame:
        """Derive a simplified market fragility score from synthetic prices.

        Components (blended with weights 0.4 / 0.3 / 0.3):
          1. Vol component: rolling 21d realised vol percentile within 252d.
          2. Drawdown component: drawdown from rolling 252d high.
          3. Breadth component: fraction of instruments with negative 21d
             return.

        Returns a DataFrame with columns ``[as_of_date, fragility_score]``.
        """

        H, N = synth_close.shape

        # Equal-weight index.
        index_close = synth_close.mean(axis=1)  # (H,)

        # Daily log returns of index.
        idx_lr = np.zeros(H)
        for t in range(1, H):
            if index_close[t] > 0.0 and index_close[t - 1] > 0.0:
                idx_lr[t] = np.log(index_close[t] / index_close[t - 1])

        # Rolling 21d vol.
        vol_21 = np.full(H, np.nan)
        for t in range(21, H):
            vol_21[t] = np.std(idx_lr[t - 20 : t + 1], ddof=1) * np.sqrt(252)

        # Percentile rank of vol within 252d.
        vol_pctile = np.full(H, 0.5)
        for t in range(252, H):
            window = vol_21[t - 251 : t + 1]
            valid = window[~np.isnan(window)]
            if len(valid) >= 2:
                vol_pctile[t] = float(np.sum(valid <= vol_21[t])) / len(valid)

        # Drawdown from 252d rolling high.
        running_max = np.full(H, np.nan)
        drawdown = np.zeros(H)
        for t in range(H):
            lookback = max(0, t - 251)
            running_max[t] = np.max(index_close[lookback : t + 1])
            if running_max[t] > 0:
                drawdown[t] = (running_max[t] - index_close[t]) / running_max[t]
        # Normalise drawdown to [0, 1] (already is, but clip).
        drawdown = np.clip(drawdown, 0.0, 1.0)

        # Breadth: fraction of instruments with negative 21d return.
        breadth_neg = np.full(H, 0.5)
        for t in range(21, H):
            rets_21 = synth_close[t] / np.where(synth_close[t - 21] > 0, synth_close[t - 21], 1.0) - 1.0
            breadth_neg[t] = float(np.sum(rets_21 < 0)) / max(N, 1)

        # Blend.
        fragility = 0.4 * vol_pctile + 0.3 * drawdown + 0.3 * breadth_neg
        fragility = np.clip(fragility, 0.0, 1.0)

        rows = []
        for t in range(H):
            rows.append({
                "as_of_date": trade_dates[t],
                "fragility_score": round(float(fragility[t]), 6),
            })

        return pd.DataFrame(rows)

    @staticmethod
    def _prepare_lambda_scores(
        config: RealityConfig,
        synth_trade_dates: List[date],
        rng: np.random.Generator,
    ) -> pd.DataFrame:
        """Prepare lambda scores for a synthetic reality.

        Modes:
          - ``passthrough``: use real CSV as-is, re-dated to match
            synthetic trade dates.
          - ``noisy``: add Gaussian noise to score columns.
          - ``shuffle``: permute dates within each cluster.
        """

        csv_path = config.lambda_csv_path
        if csv_path is None:
            return pd.DataFrame()

        src = pd.read_csv(csv_path)
        if src.empty:
            return pd.DataFrame()

        # Identify score columns.
        score_cols = [c for c in src.columns if c.startswith("lambda_score_h")]
        if not score_cols:
            logger.warning("_prepare_lambda_scores: no lambda_score_h* columns in %s", csv_path)
            return pd.DataFrame()

        src["as_of_date"] = pd.to_datetime(src["as_of_date"]).dt.date

        # Get unique source dates in order.
        src_dates = sorted(src["as_of_date"].unique())
        n_src = len(src_dates)
        n_synth = len(synth_trade_dates)

        if n_src == 0:
            return pd.DataFrame()

        # Build date mapping: synthetic dates -> source dates (cyclically).
        date_map = {synth_trade_dates[i]: src_dates[i % n_src] for i in range(n_synth)}

        mode = config.lambda_mode.lower()

        if mode == "shuffle":
            # Permute the date mapping randomly.
            shuffled_src = list(src_dates)
            rng.shuffle(shuffled_src)
            date_map = {synth_trade_dates[i]: shuffled_src[i % n_src] for i in range(n_synth)}

        # Build output: for each synth date, look up the mapped source date's rows.
        out_rows: List[Dict[str, Any]] = []
        src_grouped = {d: group for d, group in src.groupby("as_of_date")}

        for synth_d in synth_trade_dates:
            mapped_d = date_map[synth_d]
            if mapped_d not in src_grouped:
                continue
            for _, row in src_grouped[mapped_d].iterrows():
                out_row = row.to_dict()
                out_row["as_of_date"] = synth_d
                if mode == "noisy":
                    for sc in score_cols:
                        out_row[sc] = float(out_row[sc]) + rng.normal(0, config.lambda_noise_std)
                out_rows.append(out_row)

        return pd.DataFrame(out_rows)
