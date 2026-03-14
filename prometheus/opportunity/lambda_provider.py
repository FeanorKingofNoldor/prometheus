"""Lambda (opportunity-density) score providers.

This module defines thin adapters that expose lambda / lambda_hat scores
via the ``get_cluster_score(...)`` protocol expected by
:class:`prometheus.universe.engine.BasicUniverseModel`.

The initial implementation focuses on bridging experiment outputs from
``run_opportunity_density_experiment.py`` (predictions CSV) into a
cluster-level lookup keyed by
``(as_of_date, market_id, sector, soft_target_class)``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd


ClusterKey = Tuple[date, str, str, str]


@dataclass
class CsvLambdaClusterScoreProvider:
    """Lookup lambda-based scores from a predictions CSV.

    The CSV is expected to be produced by
    :mod:`prometheus.scripts.run_opportunity_density_experiment` using
    the ``--predictions-output`` option. At minimum it must contain the
    following columns:

    - ``as_of_date`` (date)
    - ``market_id``
    - ``sector``
    - ``soft_target_class``
    - ``lambda_hat`` (or another numeric column specified via
      ``score_column``)

    Optionally, it may contain an ``experiment_id`` column. When
    ``experiment_id`` is provided here, the provider will restrict the
    lookup table to matching rows; otherwise, all rows in the file are
    used.

    The provider is **read-only** and intended for research/offline use
    (e.g. wiring lambda_hat into BasicUniverseModel in research runs or
    backtests). It does not perform any extrapolation: if a cluster is
    missing for a given date, ``get_cluster_score`` returns ``None``.
    """

    csv_path: Path
    experiment_id: str | None = None
    score_column: str = "lambda_hat"

    def __post_init__(self) -> None:
        if not self.csv_path.exists():  # pragma: no cover - defensive
            msg = f"Lambda predictions CSV not found: {self.csv_path}"
            raise FileNotFoundError(msg)

        df = pd.read_csv(self.csv_path)
        if "as_of_date" not in df.columns:
            raise ValueError("Lambda predictions CSV must contain an 'as_of_date' column")
        required = {"market_id", "sector", "soft_target_class"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(
                f"Lambda predictions CSV missing required columns: {sorted(missing)}",
            )

        if self.experiment_id is not None:
            if "experiment_id" not in df.columns:
                raise ValueError(
                    "Lambda predictions CSV must contain an 'experiment_id' column "
                    "when experiment_id is specified in CsvLambdaClusterScoreProvider.",
                )
            df = df[df["experiment_id"] == self.experiment_id].copy()
            if df.empty:
                raise ValueError(
                    f"No rows found for experiment_id={self.experiment_id!r} "
                    f"in {self.csv_path}",
                )

        if self.score_column not in df.columns:
            raise ValueError(
                f"score_column {self.score_column!r} not found in predictions CSV columns: "
                f"{sorted(df.columns)}",
            )

        df["as_of_date"] = pd.to_datetime(df["as_of_date"]).dt.date

        # Build an in-memory lookup table. If multiple rows exist for the
        # same cluster/date (should not happen with well-formed inputs), we
        # keep the last one encountered.
        table: Dict[ClusterKey, float] = {}
        for _, row in df.iterrows():
            key: ClusterKey = (
                row["as_of_date"],
                str(row["market_id"]),
                str(row["sector"]),
                str(row["soft_target_class"]),
            )
            value = float(row[self.score_column])
            table[key] = value

        self._table = table

    def get_cluster_score(
        self,
        *,
        as_of_date: date,
        market_id: str,
        sector: str,
        soft_target_class: str,
    ) -> float | None:
        """Return the lambda-based score for a given cluster, if available.

        Parameters are expected to match the cluster keys produced by the
        universe engine (market_id, sector, soft_target_class). If no
        entry exists for the requested cluster/date, ``None`` is
        returned.
        """

        key: ClusterKey = (as_of_date, market_id, sector, soft_target_class)
        return self._table.get(key)
