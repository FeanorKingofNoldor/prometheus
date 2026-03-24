"""Prometheus v2 – Synthetic scenario storage helpers.

This module provides a thin storage layer around the scenario-related
runtime database tables used by the Synthetic Scenario Engine:

- scenario_sets / scenario_paths (scenario shock storage)
- prices_daily / fragility_measures / instruments (reality storage)

The goal is to keep database access logic out of the core engine
implementation while remaining explicit and easy to inspect.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
from apathis.core.database import DatabaseManager
from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger
from psycopg2.extras import Json, execute_values

from .types import ScenarioRequest, ScenarioSetRef, SyntheticReality

logger = get_logger(__name__)


@dataclass(frozen=True)
class ScenarioPathRow:
    """In-memory representation of a scenario_paths row.

    Attributes:
        scenario_id: Index of the path within the set (0..num_paths-1).
        horizon_index: Step index within the path (0..H).
        instrument_id: Optional instrument identifier.
        factor_id: Optional factor identifier.
        macro_id: Optional macro identifier.
        return_value: Shock as a return relative to baseline.
        price: Optional price level associated with the step.
        shock_metadata: Optional free-form metadata for diagnostics.
    """

    scenario_id: int
    horizon_index: int
    instrument_id: Optional[str]
    factor_id: Optional[str]
    macro_id: Optional[str]
    return_value: float
    price: Optional[float] = None
    shock_metadata: Optional[Dict[str, object]] = None


@dataclass
class ScenarioStorage:
    """Persistence helper for synthetic scenario sets and paths."""

    db_manager: DatabaseManager

    def create_scenario_set(
        self,
        request: ScenarioRequest,
        created_by: Optional[str] = None,
    ) -> ScenarioSetRef:
        """Insert a new scenario set definition and return its reference."""

        scenario_set_id = generate_uuid()

        sql = """
            INSERT INTO scenario_sets (
                scenario_set_id,
                name,
                description,
                category,
                horizon_days,
                num_paths,
                base_universe_filter,
                base_date_start,
                base_date_end,
                regime_filter,
                generator_spec,
                created_at,
                created_by,
                tags,
                metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s)
        """

        base_universe_filter = Json(request.universe_filter or {})
        regime_filter = request.regime_filter
        generator_spec = Json(request.generator_spec or {})

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (
                        scenario_set_id,
                        request.name,
                        request.description,
                        request.category,
                        request.horizon_days,
                        request.num_paths,
                        base_universe_filter,
                        request.base_date_start,
                        request.base_date_end,
                        regime_filter,
                        generator_spec,
                        created_by,
                        None,
                        Json({}),
                    ),
                )
                conn.commit()
            finally:
                cursor.close()

        return ScenarioSetRef(
            scenario_set_id=scenario_set_id,
            name=request.name,
            category=request.category,
            horizon_days=request.horizon_days,
            num_paths=request.num_paths,
        )

    def save_scenario_paths(
        self,
        scenario_set_id: str,
        rows: Iterable[ScenarioPathRow],
    ) -> None:
        """Persist a batch of scenario paths for a given set.

        Existing rows for the set are not deleted; callers should ensure
        they either insert a complete set in one go or clear previous
        paths first if necessary.
        """

        sql = """
            INSERT INTO scenario_paths (
                scenario_set_id,
                scenario_id,
                horizon_index,
                instrument_id,
                factor_id,
                macro_id,
                return_value,
                price,
                shock_metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        rows = list(rows)
        if not rows:
            return

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                for r in rows:
                    cursor.execute(
                        sql,
                        (
                            scenario_set_id,
                            r.scenario_id,
                            r.horizon_index,
                            r.instrument_id,
                            r.factor_id,
                            r.macro_id,
                            float(r.return_value),
                            r.price,
                            Json(r.shock_metadata or {}),
                        ),
                    )
                conn.commit()
            finally:
                cursor.close()

    def list_scenario_sets(self, category: Optional[str] = None) -> List[ScenarioSetRef]:
        """Return a list of scenario sets, optionally filtered by category."""

        if category is None:
            sql = """
                SELECT scenario_set_id, name, category, horizon_days, num_paths
                FROM scenario_sets
                ORDER BY created_at DESC
            """
            params: tuple[object, ...] = ()
        else:
            sql = """
                SELECT scenario_set_id, name, category, horizon_days, num_paths
                FROM scenario_sets
                WHERE category = %s
                ORDER BY created_at DESC
            """
            params = (category,)

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params or None)
                rows = cursor.fetchall()
            finally:
                cursor.close()

        return [
            ScenarioSetRef(
                scenario_set_id=set_id,
                name=name,
                category=category_db,
                horizon_days=int(horizon_days),
                num_paths=int(num_paths),
            )
            for set_id, name, category_db, horizon_days, num_paths in rows
        ]

    def get_scenario_set_metadata(self, scenario_set_id: str) -> Dict[str, object]:
        """Return raw metadata for a scenario set.

        The result includes configuration fields that may be useful to
        callers wishing to reconstruct or analyse the set.
        """

        sql = """
            SELECT
                name,
                description,
                category,
                horizon_days,
                num_paths,
                base_universe_filter,
                base_date_start,
                base_date_end,
                regime_filter,
                generator_spec,
                tags,
                metadata
            FROM scenario_sets
            WHERE scenario_set_id = %s
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (scenario_set_id,))
                row = cursor.fetchone()
            finally:
                cursor.close()

        if row is None:
            msg = f"scenario_set not found: {scenario_set_id}"
            raise ValueError(msg)

        (
            name,
            description,
            category,
            horizon_days,
            num_paths,
            base_universe_filter,
            base_date_start,
            base_date_end,
            regime_filter,
            generator_spec,
            tags,
            metadata,
        ) = row

        return {
            "scenario_set_id": scenario_set_id,
            "name": name,
            "description": description,
            "category": category,
            "horizon_days": int(horizon_days),
            "num_paths": int(num_paths),
            "base_universe_filter": base_universe_filter or {},
            "base_date_start": base_date_start,
            "base_date_end": base_date_end,
            "regime_filter": regime_filter or [],
            "generator_spec": generator_spec or {},
            "tags": tags or [],
            "metadata": metadata or {},
        }

    # ------------------------------------------------------------------
    # Market-reality persistence
    # ------------------------------------------------------------------

    def write_reality(self, reality: SyntheticReality) -> None:
        """Persist a full synthetic reality to the database.

        Writes:
        - Synthetic instrument rows into ``instruments``.
        - Price rows into ``prices_daily`` (historical DB).
        - Fragility rows into ``fragility_measures`` (runtime DB).
        - Metadata row into ``scenario_sets`` with category ``REALITY``.
        """

        self._write_reality_instruments(reality)
        self._write_reality_prices(reality)
        if reality.fragility_df is not None and not reality.fragility_df.empty:
            self._write_reality_fragility(reality)
        self._write_reality_metadata(reality)

        logger.info(
            "ScenarioStorage.write_reality: id=%s instruments=%d prices=%d",
            reality.reality_id,
            len(reality.instrument_ids),
            len(reality.prices_df),
        )

    def write_reality_lambda_csv(
        self,
        reality: SyntheticReality,
        output_dir: str | Path,
    ) -> Path:
        """Write lambda score CSV for a reality.

        Returns the path to the written CSV.
        """

        out = Path(output_dir) / reality.reality_id / "lambda_scores.csv"
        out.parent.mkdir(parents=True, exist_ok=True)

        if reality.lambda_df is not None and not reality.lambda_df.empty:
            reality.lambda_df.to_csv(out, index=False)
        else:
            # Write an empty CSV with the expected header.
            pd.DataFrame(
                columns=[
                    "as_of_date", "market_id", "sector", "soft_target_class",
                    "lambda_value", "lambda_score_h5", "lambda_score_h21", "lambda_score_h63",
                ]
            ).to_csv(out, index=False)

        logger.info("ScenarioStorage.write_reality_lambda_csv: %s", out)
        return out

    def cleanup_reality(self, reality_id: str) -> int:
        """Delete all synthetic artefacts for a reality.

        Returns total number of rows deleted.
        """

        prefix = f"SYNTH_{reality_id[:8]}%"
        total = 0

        # prices_daily (historical DB).
        with self.db_manager.get_historical_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "DELETE FROM prices_daily WHERE instrument_id LIKE %s",
                    (prefix,),
                )
                total += cursor.rowcount
                conn.commit()
            finally:
                cursor.close()

        # instruments + issuer_classifications + fragility_measures (runtime DB).
        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                iss_pattern = f"SYNTH_{reality_id[:8]}_ISS_%"
                cursor.execute(
                    "DELETE FROM issuer_classifications WHERE issuer_id LIKE %s",
                    (iss_pattern,),
                )
                total += cursor.rowcount

                cursor.execute(
                    "DELETE FROM instruments WHERE instrument_id LIKE %s",
                    (prefix,),
                )
                total += cursor.rowcount

                cursor.execute(
                    "DELETE FROM issuers WHERE issuer_id LIKE %s",
                    (iss_pattern,),
                )
                total += cursor.rowcount

                synth_entity = f"SYNTH_{reality_id[:8]}%"
                cursor.execute(
                    "DELETE FROM fragility_measures WHERE entity_id LIKE %s",
                    (synth_entity,),
                )
                total += cursor.rowcount

                cursor.execute(
                    "DELETE FROM scenario_sets WHERE scenario_set_id = %s",
                    (reality_id,),
                )
                total += cursor.rowcount

                conn.commit()
            finally:
                cursor.close()

        logger.info("ScenarioStorage.cleanup_reality: id=%s deleted=%d rows", reality_id, total)
        return total

    # ------------------------------------------------------------------
    # Private reality helpers
    # ------------------------------------------------------------------

    def _write_reality_instruments(self, reality: SyntheticReality) -> None:
        """Insert synthetic issuers, instruments, and issuer_classifications."""

        from apathis.sector.health import SECTOR_ETF_MAP

        # Look up sector for each real instrument.
        real_inst_to_sector: Dict[str, str] = {}
        real_ids = list(reality.real_to_synth.keys())
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

        # Sector ETFs get their sector name directly.
        for etf_id, sec_name in SECTOR_ETF_MAP.items():
            real_inst_to_sector[etf_id] = sec_name

        market_id = reality.config.markets[0] if reality.config.markets else "US_EQ"
        prefix = f"SYNTH_{reality.reality_id[:8]}"

        if not reality.real_to_synth:
            return

        # Build per-instrument rows: issuer, instrument, classification.
        issuer_rows = []
        inst_rows = []
        cls_rows = []

        for real_id, synth_id in reality.real_to_synth.items():
            sector = real_inst_to_sector.get(real_id, "Unknown")
            # One synthetic issuer per instrument (preserves sector granularity).
            synth_issuer_id = f"{prefix}_ISS_{real_id}"

            issuer_rows.append((
                synth_issuer_id,
                "CORPORATE",   # issuer_type
                synth_id,      # name
                sector,        # sector (needed by C++ backtester load_instrument_meta)
            ))

            inst_rows.append((
                synth_id,
                synth_issuer_id,
                market_id,
                "EQUITY",
                synth_id,  # symbol
                "USD",
                "ACTIVE",
            ))

            cls_rows.append((
                synth_issuer_id,
                "SYNTH",       # taxonomy
                "2000-01-01",  # effective_start
                sector,
                "synthetic_engine",  # source
            ))

        sql_iss = """
            INSERT INTO issuers (issuer_id, issuer_type, name, sector)
            VALUES %s
            ON CONFLICT (issuer_id) DO NOTHING
        """
        sql_inst = """
            INSERT INTO instruments (
                instrument_id, issuer_id, market_id, asset_class,
                symbol, currency, status
            ) VALUES %s
            ON CONFLICT (instrument_id) DO NOTHING
        """
        sql_cls = """
            INSERT INTO issuer_classifications (
                issuer_id, taxonomy, effective_start,
                sector, source
            ) VALUES %s
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                execute_values(cursor, sql_iss, issuer_rows, page_size=500)
                execute_values(cursor, sql_inst, inst_rows, page_size=500)
                execute_values(cursor, sql_cls, cls_rows, page_size=500)
                conn.commit()
            finally:
                cursor.close()

    def _write_reality_prices(self, reality: SyntheticReality) -> None:
        """Insert synthetic price rows into the historical DB."""

        df = reality.prices_df
        if df.empty:
            return

        sql = """
            INSERT INTO prices_daily (
                instrument_id, trade_date, open, high, low, close,
                adjusted_close, volume, currency, metadata
            ) VALUES %s
            ON CONFLICT (instrument_id, trade_date) DO NOTHING
        """

        rows = [
            (
                r["instrument_id"],
                r["trade_date"],
                float(r["open"]),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
                float(r["adjusted_close"]),
                int(r["volume"]),
                str(r.get("currency", "USD")),
                Json(r.get("metadata") or {}),
            )
            for _, r in df.iterrows()
        ]

        with self.db_manager.get_historical_connection() as conn:
            cursor = conn.cursor()
            try:
                execute_values(cursor, sql, rows, page_size=1000)
                conn.commit()
            finally:
                cursor.close()

    def _write_reality_fragility(self, reality: SyntheticReality) -> None:
        """Insert synthetic fragility rows into the runtime DB."""

        fdf = reality.fragility_df
        if fdf is None or fdf.empty:
            return

        entity_id = f"SYNTH_{reality.reality_id[:8]}_US_EQ"

        sql = """
            INSERT INTO fragility_measures (
                fragility_id, entity_type, entity_id, as_of_date,
                fragility_score, metadata
            ) VALUES %s
            ON CONFLICT (fragility_id) DO NOTHING
        """

        rows = [
            (
                f"SYNTH_{reality.reality_id[:8]}_{r['as_of_date']}",
                "MARKET",
                entity_id,
                r["as_of_date"],
                float(r["fragility_score"]),
                Json({"reality_id": reality.reality_id}),
            )
            for _, r in fdf.iterrows()
        ]

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                execute_values(cursor, sql, rows, page_size=500)
                conn.commit()
            finally:
                cursor.close()

    def _write_reality_metadata(self, reality: SyntheticReality) -> None:
        """Record reality metadata in scenario_sets."""

        sql = """
            INSERT INTO scenario_sets (
                scenario_set_id, name, description, category,
                horizon_days, num_paths,
                base_universe_filter, base_date_start, base_date_end,
                regime_filter, generator_spec,
                created_at, created_by, tags, metadata
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                NOW(), %s, %s, %s
            )
            ON CONFLICT (scenario_set_id) DO NOTHING
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (
                        reality.reality_id,
                        reality.config.name,
                        f"Synthetic reality {reality.reality_id[:8]}",
                        "BOOTSTRAP",
                        reality.config.horizon_days,
                        1,
                        Json({}),
                        reality.config.base_date_start,
                        reality.config.base_date_end,
                        None,
                        Json(reality.metadata),
                        "system",
                        None,
                        Json({}),
                    ),
                )
                conn.commit()
            finally:
                cursor.close()
