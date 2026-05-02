"""Analyse alignment between lambda forecasts and universe membership.

This offline script joins cluster-level lambda forecasts (lambda_hat)
from ``run_opportunity_density_experiment.py`` with instrument-level
universe membership decisions from ``universe_members``.

For a given experiment_id, universe_id, and date range it reports how
lambda_hat is distributed across included vs excluded instruments and
CORE/SATELLITE/EXCLUDED tiers.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger

from prometheus.universe.engine import UniverseMember, UniverseStorage

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


@dataclass(frozen=True)
class JoinedUniverseRow:
    """Single instrument-level row joined with lambda forecast.

    Attributes:
        as_of_date: Date of the universe and lambda forecast.
        entity_id: Instrument identifier.
        included: Whether the instrument is included in the universe.
        tier: Universe tier (e.g. CORE/SATELLITE/EXCLUDED).
        market_id: Market identifier.
        sector: Sector name.
        soft_target_class: STAB soft target class label.
        lambda_value: Realised lambda_t(x) used as feature.
        lambda_next: Realised lambda_{t+1}(x) for the cluster.
        lambda_hat: Forecast lambda_{t+1}(x) from the experiment.
    """

    as_of_date: date
    entity_id: str
    included: bool
    tier: str
    market_id: str
    sector: str
    soft_target_class: str
    lambda_value: float
    lambda_next: float
    lambda_hat: float


def _load_lambda_predictions(path: Path, experiment_id: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "experiment_id" not in df.columns:
        raise ValueError("Predictions CSV must contain an 'experiment_id' column")

    df = df[df["experiment_id"] == experiment_id].copy()
    if df.empty:
        raise ValueError(f"No rows found for experiment_id={experiment_id!r} in predictions CSV")

    if "as_of_date" not in df.columns:
        raise ValueError("Predictions CSV must contain an 'as_of_date' column")

    df["as_of_date"] = pd.to_datetime(df["as_of_date"]).dt.date

    required = {
        "market_id",
        "sector",
        "soft_target_class",
        "lambda_value",
        "lambda_next",
        "lambda_hat",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Predictions CSV missing required columns: {sorted(missing)}")

    return df


def _extract_cluster_keys(member: UniverseMember) -> Optional[Tuple[str, str, str]]:
    """Return (market_id, sector, soft_target_class) for a universe member.

    For backfilled universes it is common for ``soft_target_class`` to be
    absent from ``reasons`` if STAB was not run for that date. In this
    case we default to ``"UNKNOWN"`` so that such instruments can still be
    joined to lambda clusters that use the ``UNKNOWN`` bucket.

    Returns None only if ``market_id`` or ``sector`` is missing.
    """

    reasons = member.reasons or {}
    market_id = reasons.get("market_id")
    sector = reasons.get("sector")
    soft_class = reasons.get("soft_target_class", "UNKNOWN")

    if market_id is None or sector is None:
        return None

    return str(market_id), str(sector), str(soft_class)


def _join_for_date(
    storage: UniverseStorage,
    df_day: pd.DataFrame,
    as_of_date: date,
    universe_id: str,
) -> List[JoinedUniverseRow]:
    """Join universe members with lambda predictions for a single date."""

    members = storage.get_universe(
        as_of_date=as_of_date,
        universe_id=universe_id,
        entity_type="INSTRUMENT",
        included_only=False,
    )
    if not members:
        logger.info(
            "No universe_members found for as_of_date=%s universe_id=%s; skipping",
            as_of_date,
            universe_id,
        )
        return []

    # Index lambda predictions by (market_id, sector, soft_target_class).
    idx = df_day.set_index(["market_id", "sector", "soft_target_class"])

    rows: List[JoinedUniverseRow] = []
    skipped_missing_cluster = 0
    skipped_missing_lambda = 0

    for m in members:
        keys = _extract_cluster_keys(m)
        if keys is None:
            skipped_missing_cluster += 1
            continue

        market_id_key, sector_key, soft_class_key = keys

        try:
            rec = idx.loc[keys]
        except KeyError:
            skipped_missing_lambda += 1
            continue

        # If multiple rows somehow exist for the same cluster/day, use the
        # first; this should not happen for well-formed lambda CSVs.
        if isinstance(rec, pd.DataFrame):
            rec = rec.iloc[0]

        rows.append(
            JoinedUniverseRow(
                as_of_date=as_of_date,
                entity_id=str(m.entity_id),
                included=bool(m.included),
                tier=str(m.tier),
                market_id=str(market_id_key),
                sector=str(sector_key),
                soft_target_class=str(soft_class_key),
                lambda_value=float(rec["lambda_value"]),
                lambda_next=float(rec["lambda_next"]),
                lambda_hat=float(rec["lambda_hat"]),
            ),
        )

    if skipped_missing_cluster or skipped_missing_lambda:
        logger.info(
            "Date %s universe=%s: joined %d rows, skipped %d (no cluster info) and %d (no lambda)",
            as_of_date,
            universe_id,
            len(rows),
            skipped_missing_cluster,
            skipped_missing_lambda,
        )

    return rows


def _compute_alignment_metrics(df: pd.DataFrame) -> None:
    """Print simple statistics on lambda_hat vs universe inclusion/tier."""

    if df.empty:
        print("No joined rows to analyse.")
        return

    df = df.copy()
    df["included_int"] = df["included"].astype(int)

    overall_mean_incl = float(df.loc[df["included"], "lambda_hat"].mean())
    overall_mean_excl = float(df.loc[~df["included"], "lambda_hat"].mean())
    overall_diff = overall_mean_incl - overall_mean_excl

    corr_included = float(df["included_int"].corr(df["lambda_hat"]))

    # Per-date uplift: mean(lambda_hat | included) - mean(lambda_hat | excluded)
    per_date_uplifts: List[float] = []
    for as_of_date, g in df.groupby("as_of_date"):
        g_incl = g[g["included"]]
        g_excl = g[~g["included"]]
        if g_incl.empty or g_excl.empty:
            continue
        uplift = float(g_incl["lambda_hat"].mean() - g_excl["lambda_hat"].mean())
        per_date_uplifts.append(uplift)

    if per_date_uplifts:
        uplifts_arr = np.asarray(per_date_uplifts, dtype=float)
        avg_uplift = float(uplifts_arr.mean())
        uplift_std_err = float(uplifts_arr.std(ddof=1) / np.sqrt(len(uplifts_arr)))
        num_days = len(uplifts_arr)
    else:
        avg_uplift = float("nan")
        uplift_std_err = float("nan")
        num_days = 0

    print("Universe / lambda_hat alignment summary")
    print("--------------------------------------")
    print(f"Total instrument-rows joined : {df.shape[0]}")
    print(f"Num distinct dates           : {df['as_of_date'].nunique()}")
    print()
    print("Mean lambda_hat by inclusion status:")
    print(f"  included   : {overall_mean_incl:.6f}")
    print(f"  excluded   : {overall_mean_excl:.6f}")
    print(f"  difference : {overall_diff:.6f}")
    print()
    print(f"Corr(included, lambda_hat)   : {corr_included:.4f}")
    print()
    print(
        "Per-date uplift in lambda_hat (mean(included) - mean(excluded)): "
        f"{avg_uplift:.6f} (std err {uplift_std_err:.6f}, days={num_days})",
    )
    print()

    # Tier-level averages for a quick sanity check.
    tier_stats = (
        df.groupby("tier")["lambda_hat"]
        .agg(["count", "mean", "std"])
        .reset_index()
        .sort_values("mean", ascending=False)
    )
    print("Mean lambda_hat by universe tier:")
    print(tier_stats.to_string(index=False))


# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Analyse how lambda forecasts (lambda_hat) for a given experiment "
            "line up with universe membership decisions over a date range."
        ),
    )

    parser.add_argument(
        "--lambda-predictions",
        type=str,
        required=True,
        help=(
            "Path to lambda predictions CSV produced by "
            "run_opportunity_density_experiment.py with --predictions-output."
        ),
    )
    parser.add_argument(
        "--experiment-id",
        type=str,
        required=True,
        help="Experiment ID to filter predictions by",
    )
    parser.add_argument(
        "--universe-id",
        type=str,
        required=True,
        help="Universe ID to load from universe_members (e.g. CORE_EQ_US)",
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        required=True,
        help="Start date (YYYY-MM-DD) for as_of_date range",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        required=True,
        help="End date (YYYY-MM-DD) for as_of_date range",
    )

    args = parser.parse_args(argv)

    start_date: date = args.start
    end_date: date = args.end
    if end_date < start_date:
        parser.error("--end must be >= --start")

    preds_path = Path(args.lambda_predictions)
    if not preds_path.exists():
        raise SystemExit(f"Predictions CSV not found: {preds_path}")

    logger.info("Loading lambda predictions from %s for experiment_id=%s", preds_path, args.experiment_id)
    df_preds = _load_lambda_predictions(preds_path, args.experiment_id)

    # Restrict to requested date window
    mask = (df_preds["as_of_date"] >= start_date) & (df_preds["as_of_date"] <= end_date)
    df_preds = df_preds.loc[mask].copy()
    if df_preds.empty:
        raise SystemExit("No lambda predictions in the requested date range")

    db_manager = get_db_manager()
    storage = UniverseStorage(db_manager=db_manager)

    joined_rows: List[JoinedUniverseRow] = []
    for as_of in sorted(df_preds["as_of_date"].unique()):
        df_day = df_preds[df_preds["as_of_date"] == as_of]
        rows = _join_for_date(
            storage=storage,
            df_day=df_day,
            as_of_date=as_of,
            universe_id=args.universe_id,
        )
        if rows:
            joined_rows.extend(rows)

    if not joined_rows:
        print("No joined universe/lambda rows were produced; check universe_id and date range.")
        return

    df_joined = pd.DataFrame(
        [
            {
                "as_of_date": r.as_of_date,
                "entity_id": r.entity_id,
                "included": r.included,
                "tier": r.tier,
                "market_id": r.market_id,
                "sector": r.sector,
                "soft_target_class": r.soft_target_class,
                "lambda_value": r.lambda_value,
                "lambda_next": r.lambda_next,
                "lambda_hat": r.lambda_hat,
            }
            for r in joined_rows
        ]
    )

    _compute_alignment_metrics(df_joined)


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
