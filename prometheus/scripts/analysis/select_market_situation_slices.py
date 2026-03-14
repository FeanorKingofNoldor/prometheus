"""Select regime/fragility-targeted backtest slices from computed MarketSituation labels.

This analysis CLI produces a set of contiguous windows (“slices”) for each
MarketSituation label, using the same inputs as the daily pipeline:

- Regime hazard overlay (down_risk / up_risk) from the market-proxy overlay cache
- Rule-based regime label thresholds (MarketProxyRegimeModelConfig)
- Market fragility scores from fragility_measures (MARKET, entity_id=<market_id>)
- MarketSituation via classify_market_situation

It is intended to support the sleeve testing plan:
- one full-cycle run (e.g. 2015 → latest available)
- several targeted runs on CRISIS / RISK_OFF / RECOVERY / RISK_ON slices

Outputs:
- JSON to stdout (and optionally to --output)
- A concise human-readable summary to stderr via logger.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.core.time import TradingCalendar, TradingCalendarConfig
from apathis.fragility.storage import FragilityStorage
from prometheus.meta.market_situation import MarketSituation, MarketSituationConfig, classify_market_situation
from apathis.regime.model_proxy import MarketProxyRegimeModelConfig
from apathis.regime.overlay_cache import ensure_overlay_csv
from apathis.regime.types import RegimeLabel


logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _latest_trade_date_for_suffix(db_manager: DatabaseManager, *, instrument_suffix: str) -> date:
    """Return the max trade_date in historical prices_daily for instrument_id like %.SUFFIX."""

    suffix = str(instrument_suffix).strip()
    if not suffix:
        raise ValueError("instrument_suffix must be non-empty")

    like = f"%.{suffix}"

    sql = """
        SELECT MAX(trade_date)
        FROM prices_daily
        WHERE instrument_id LIKE %s
    """

    with db_manager.get_historical_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (like,))
            (max_date,) = cur.fetchone()
        finally:
            cur.close()

    if not isinstance(max_date, date):
        raise RuntimeError(
            f"No prices_daily rows found for instrument_suffix={instrument_suffix!r}; cannot infer end date"
        )

    return max_date


def _read_hazard_csv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    if df.empty:
        return pd.DataFrame(columns=["as_of_date", "down_risk", "up_risk"])

    if "as_of_date" not in df.columns:
        raise ValueError(f"Overlay CSV missing as_of_date column: {csv_path}")

    for col in ("down_risk", "up_risk"):
        if col not in df.columns:
            raise ValueError(f"Overlay CSV missing {col} column: {csv_path}")

    df = df[["as_of_date", "down_risk", "up_risk"]].copy()
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce").dt.date
    df["down_risk"] = pd.to_numeric(df["down_risk"], errors="coerce")
    df["up_risk"] = pd.to_numeric(df["up_risk"], errors="coerce")
    df = df.dropna(subset=["as_of_date"]).drop_duplicates(subset=["as_of_date"], keep="last")
    return df


def _classify_regime(
    *,
    down_risk: float | None,
    up_risk: float | None,
    cfg: MarketProxyRegimeModelConfig,
) -> RegimeLabel | None:
    if down_risk is None or up_risk is None:
        return None

    down = float(down_risk)
    up = float(up_risk)

    if down >= float(cfg.crisis_down_risk):
        return RegimeLabel.CRISIS
    if down >= float(cfg.risk_off_down_risk):
        return RegimeLabel.RISK_OFF
    if up >= float(cfg.carry_up_risk) and down <= float(cfg.carry_max_down_risk):
        return RegimeLabel.CARRY
    return RegimeLabel.NEUTRAL


def _to_float_or_none(value: object) -> float | None:
    try:
        if value is None:
            return None
        x = float(value)  # type: ignore[arg-type]
        if np.isnan(x):
            return None
        return x
    except Exception:
        return None


def _build_fragility_series(
    *,
    db_manager: DatabaseManager,
    entity_id: str,
    start_date: date,
    end_date: date,
    trading_days: List[date],
) -> Tuple[Dict[date, float], Dict[date, str]]:
    """Return (fragility_score_by_date, fragility_class_by_date) with forward fill.

    Uses the latest stored fragility measure with as_of_date <= trading day
    to avoid lookahead.
    """

    storage = FragilityStorage(db_manager=db_manager)

    measures = []
    try:
        prev = storage.get_latest_measure(
            "MARKET",
            entity_id,
            as_of_date=start_date,
            inclusive=False,
        )
        if prev is not None:
            measures.append(prev)
    except Exception:
        # Best-effort; proceed with in-window history only.
        measures = []

    measures.extend(storage.get_history("MARKET", entity_id, start_date, end_date))
    measures = sorted(measures, key=lambda m: m.as_of_date)

    score_by_date: Dict[date, float] = {}
    class_by_date: Dict[date, str] = {}

    idx = 0
    last_score: float | None = None
    last_class: str | None = None

    for d in trading_days:
        while idx < len(measures) and measures[idx].as_of_date <= d:
            last_score = float(measures[idx].fragility_score)
            last_class = str(measures[idx].class_label.value)
            idx += 1

        if last_score is not None:
            score_by_date[d] = float(last_score)
        if last_class is not None:
            class_by_date[d] = str(last_class)

    return score_by_date, class_by_date


def _segments_for_labels(
    *,
    trading_days: List[date],
    situation_by_date: Dict[date, MarketSituation],
    allowed: set[MarketSituation],
) -> List[Tuple[MarketSituation, int, int]]:
    """Return segments as (label, start_idx, end_idx) inclusive indices into trading_days."""

    segs: List[Tuple[MarketSituation, int, int]] = []

    cur_label: MarketSituation | None = None
    cur_start: int | None = None

    for i, d in enumerate(trading_days):
        lab = situation_by_date.get(d)
        if lab not in allowed:
            lab = None

        if lab is None:
            if cur_label is not None and cur_start is not None:
                segs.append((cur_label, cur_start, i - 1))
            cur_label = None
            cur_start = None
            continue

        if cur_label is None:
            cur_label = lab
            cur_start = i
            continue

        if lab == cur_label:
            continue

        # label changed
        if cur_start is not None:
            segs.append((cur_label, cur_start, i - 1))
        cur_label = lab
        cur_start = i

    if cur_label is not None and cur_start is not None:
        segs.append((cur_label, cur_start, len(trading_days) - 1))

    return segs


def _mean(xs: List[float]) -> float | None:
    if not xs:
        return None
    return float(sum(xs) / len(xs))


def _coverage(num_present: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return float(num_present) / float(total)


def _segment_stats(
    *,
    label: MarketSituation,
    start_idx: int,
    end_idx: int,
    trading_days: List[date],
    down_by_date: Dict[date, float],
    up_by_date: Dict[date, float],
    frag_by_date: Dict[date, float],
) -> Dict[str, object]:
    days = trading_days[start_idx : end_idx + 1]
    n = len(days)

    down_vals: List[float] = []
    up_vals: List[float] = []
    frag_vals: List[float] = []

    hazard_present = 0
    frag_present = 0

    for d in days:
        down = down_by_date.get(d)
        up = up_by_date.get(d)
        if down is not None and up is not None:
            hazard_present += 1
            down_vals.append(float(down))
            up_vals.append(float(up))

        fr = frag_by_date.get(d)
        if fr is not None:
            frag_present += 1
            frag_vals.append(float(fr))

    return {
        "label": label.value,
        "segment_start": days[0].isoformat() if days else None,
        "segment_end": days[-1].isoformat() if days else None,
        "segment_len_trading_days": int(n),
        "start_idx": int(start_idx),
        "end_idx": int(end_idx),
        "hazard_coverage": float(_coverage(hazard_present, n)),
        "fragility_coverage": float(_coverage(frag_present, n)),
        "mean_down_risk": _mean(down_vals),
        "mean_up_risk": _mean(up_vals),
        "mean_fragility_score": _mean(frag_vals),
    }


def _rank_key(label: MarketSituation, stats: Dict[str, object], *, regime_cfg: MarketProxyRegimeModelConfig) -> tuple:
    """Lower is better (for sort)."""

    md = stats.get("mean_down_risk")
    mu = stats.get("mean_up_risk")
    mf = stats.get("mean_fragility_score")

    mean_down = float(md) if isinstance(md, (int, float)) else -1.0
    mean_up = float(mu) if isinstance(mu, (int, float)) else -1.0
    mean_frag = float(mf) if isinstance(mf, (int, float)) else -1.0

    length = int(stats.get("segment_len_trading_days") or 0)

    # Negative because we sort ascending.
    if label == MarketSituation.CRISIS:
        return (-mean_down, -mean_frag, -length)
    if label == MarketSituation.RISK_OFF:
        return (-mean_down, -length)
    if label == MarketSituation.RECOVERY:
        return (-mean_frag, -length)
    if label == MarketSituation.RISK_ON:
        # Sanity: do not pick a “risk-on” segment with elevated down_risk.
        if mean_down >= float(regime_cfg.risk_off_down_risk):
            return (float("inf"),)
        return (-mean_up, -length)

    return (float("inf"),)


def _select_segments(
    *,
    candidates_by_label: Dict[MarketSituation, List[Dict[str, object]]],
    trading_days: List[date],
    min_gap_trading_days: int,
    warmup_trading_days: int,
    regime_cfg: MarketProxyRegimeModelConfig,
    max_per_label: int,
) -> Dict[str, List[Dict[str, object]]]:
    idx_to_date = {i: d for i, d in enumerate(trading_days)}

    selected: Dict[str, List[Dict[str, object]]] = {}

    for label, candidates in candidates_by_label.items():
        if not candidates:
            selected[label.value] = []
            continue

        ranked = sorted(candidates, key=lambda s: _rank_key(label, s, regime_cfg=regime_cfg))
        # Drop any with inf key (e.g., risk-on sanity filter)
        ranked = [s for s in ranked if not (isinstance(_rank_key(label, s, regime_cfg=regime_cfg), tuple) and _rank_key(label, s, regime_cfg=regime_cfg)[0] == float("inf"))]

        if not ranked:
            selected[label.value] = []
            continue

        out: List[Dict[str, object]] = []

        def _add(seg: Dict[str, object]) -> None:
            start_idx = int(seg.get("start_idx") or 0)
            end_idx = int(seg.get("end_idx") or 0)

            warm_idx = max(0, start_idx - int(warmup_trading_days))
            run_start = idx_to_date.get(warm_idx)
            run_end = idx_to_date.get(end_idx)

            seg = dict(seg)
            seg["run_start"] = run_start.isoformat() if isinstance(run_start, date) else None
            seg["run_end"] = run_end.isoformat() if isinstance(run_end, date) else None
            seg["warmup_trading_days"] = int(warmup_trading_days)
            out.append(seg)

        _add(ranked[0])

        if max_per_label > 1:
            primary_start_idx = int(ranked[0].get("start_idx") or 0)
            for seg in ranked[1:]:
                if len(out) >= max_per_label:
                    break
                start_idx = int(seg.get("start_idx") or 0)
                if abs(start_idx - primary_start_idx) >= int(min_gap_trading_days):
                    _add(seg)
                    break

        selected[label.value] = out

    return selected


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Select MarketSituation slices (CRISIS/RISK_OFF/RECOVERY/RISK_ON) from regime hazard + fragility data."
        )
    )

    parser.add_argument("--market-id", type=str, default="US_EQ", help="Market id (default: US_EQ)")
    parser.add_argument("--start", type=_parse_date, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        help=(
            "End date (YYYY-MM-DD). If omitted, inferred from historical prices_daily max(trade_date) for --instrument-suffix."
        ),
    )
    parser.add_argument(
        "--instrument-suffix",
        type=str,
        default="US",
        help="Instrument_id suffix used to infer latest trade_date when --end is omitted (default: US)",
    )
    parser.add_argument(
        "--hazard-profile",
        type=str,
        default=None,
        help=(
            "Optional hazard overlay profile name used by the market-proxy regime detector. "
            "If omitted, uses the default profile."
        ),
    )

    # MarketSituation thresholds (override defaults from prometheus/meta/market_situation.py)
    parser.add_argument(
        "--recovery-fragility-threshold",
        type=float,
        default=None,
        help=(
            "Override MarketSituationConfig.recovery_fragility_threshold (default: 0.30). "
            "Higher values reduce RECOVERY labels and increase RISK_ON/NEUTRAL."
        ),
    )
    parser.add_argument(
        "--crisis-fragility-override-threshold",
        type=float,
        default=None,
        help=(
            "Override MarketSituationConfig.crisis_fragility_override_threshold (default: 0.75)."
        ),
    )
    parser.add_argument(
        "--recovery-requires-stress-transition",
        action="store_true",
        help=(
            "If set, enable MarketSituationConfig.recovery_requires_stress_transition. "
            "NOTE: this tends to label only the transition day(s) as RECOVERY."
        ),
    )

    parser.add_argument(
        "--min-segment-days",
        type=int,
        default=60,
        help="Minimum contiguous segment length in trading days (default: 60)",
    )
    parser.add_argument(
        "--fallback-min-segment-days",
        type=int,
        default=30,
        help="Fallback minimum length if no segments exist for a label (default: 30)",
    )
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=0.80,
        help="Minimum required coverage fraction for hazard/fragility inputs (default: 0.80)",
    )
    parser.add_argument(
        "--warmup-trading-days",
        type=int,
        default=126,
        help="Warmup trading days to prepend before each segment (default: 126)",
    )
    parser.add_argument(
        "--min-gap-trading-days",
        type=int,
        default=252,
        help="Minimum trading-day gap between primary and secondary segment for same label (default: 252)",
    )
    parser.add_argument(
        "--max-per-label",
        type=int,
        default=1,
        help="Max segments to select per label (default: 1)",
    )

    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional JSON output path (also always prints JSON to stdout)",
    )
    parser.add_argument(
        "--print-end-date",
        action="store_true",
        help="Print the resolved end date and exit (useful for scripts)",
    )

    args = parser.parse_args(argv)

    cfg = get_config()
    db_manager = DatabaseManager(cfg)

    start_date = args.start
    end_date = args.end
    if end_date is None:
        end_date = _latest_trade_date_for_suffix(db_manager, instrument_suffix=str(args.instrument_suffix))

    if args.print_end_date:
        print(end_date.isoformat())
        return

    if end_date < start_date:
        raise SystemExit("end must be >= start")

    market_id = str(args.market_id)

    cal = TradingCalendar(TradingCalendarConfig(market=market_id), db_manager=db_manager)
    trading_days = cal.trading_days_between(start_date, end_date)
    if not trading_days:
        raise SystemExit("No trading days in requested window")

    # 1) Hazard overlay (down_risk/up_risk)
    overlay_path = ensure_overlay_csv(
        db_manager=db_manager,
        start_date=start_date,
        end_date=end_date,
        profile_name=str(args.hazard_profile) if args.hazard_profile is not None else None,
    )
    hazard_df = _read_hazard_csv(str(overlay_path))

    down_by_date: Dict[date, float] = {}
    up_by_date: Dict[date, float] = {}

    for _, row in hazard_df.iterrows():
        d = row.get("as_of_date")
        if not isinstance(d, date):
            continue
        down = _to_float_or_none(row.get("down_risk"))
        up = _to_float_or_none(row.get("up_risk"))
        if down is not None:
            down_by_date[d] = float(down)
        if up is not None:
            up_by_date[d] = float(up)

    # 2) Fragility series (forward-filled)
    frag_by_date, frag_class_by_date = _build_fragility_series(
        db_manager=db_manager,
        entity_id=market_id,
        start_date=start_date,
        end_date=end_date,
        trading_days=trading_days,
    )

    # 3) Regime labels from hazard thresholds
    regime_cfg = MarketProxyRegimeModelConfig()
    regime_by_date: Dict[date, RegimeLabel | None] = {}
    prev_regime_by_date: Dict[date, RegimeLabel | None] = {}

    prev: RegimeLabel | None = None
    for d in trading_days:
        down = down_by_date.get(d)
        up = up_by_date.get(d)
        label = _classify_regime(
            down_risk=down,
            up_risk=up,
            cfg=regime_cfg,
        )
        regime_by_date[d] = label
        prev_regime_by_date[d] = prev
        prev = label

    # 4) MarketSituation labels per day
    situation_cfg = MarketSituationConfig(
        recovery_fragility_threshold=(
            float(args.recovery_fragility_threshold)
            if args.recovery_fragility_threshold is not None
            else MarketSituationConfig().recovery_fragility_threshold
        ),
        crisis_fragility_override_threshold=(
            float(args.crisis_fragility_override_threshold)
            if args.crisis_fragility_override_threshold is not None
            else MarketSituationConfig().crisis_fragility_override_threshold
        ),
        recovery_requires_stress_transition=bool(args.recovery_requires_stress_transition),
    )

    situation_by_date: Dict[date, MarketSituation] = {}
    for d in trading_days:
        frag = frag_by_date.get(d)
        situation_by_date[d] = classify_market_situation(
            regime_label=regime_by_date.get(d),
            prev_regime_label=prev_regime_by_date.get(d),
            fragility_score=frag,
            config=situation_cfg,
        )

    allowed = {MarketSituation.CRISIS, MarketSituation.RISK_OFF, MarketSituation.RECOVERY, MarketSituation.RISK_ON}
    segs = _segments_for_labels(trading_days=trading_days, situation_by_date=situation_by_date, allowed=allowed)

    # Compute stats and filter.
    min_len = int(args.min_segment_days)
    if min_len <= 0:
        min_len = 1
    fallback_len = int(args.fallback_min_segment_days)
    if fallback_len <= 0:
        fallback_len = 1
    min_cov = float(args.min_coverage)
    if min_cov < 0.0:
        min_cov = 0.0
    if min_cov > 1.0:
        min_cov = 1.0

    candidates_by_label: Dict[MarketSituation, List[Dict[str, object]]] = {lab: [] for lab in allowed}

    # First pass: collect all segments.
    for lab, s0, s1 in segs:
        stats = _segment_stats(
            label=lab,
            start_idx=s0,
            end_idx=s1,
            trading_days=trading_days,
            down_by_date=down_by_date,
            up_by_date=up_by_date,
            frag_by_date=frag_by_date,
        )
        candidates_by_label[lab].append(stats)

    # Second pass: apply length+coverage filters per label with fallback.
    filtered_by_label: Dict[MarketSituation, List[Dict[str, object]]] = {}
    for lab in allowed:
        cands = candidates_by_label.get(lab, [])
        if not cands:
            filtered_by_label[lab] = []
            continue

        def _passes(x: Dict[str, object], *, min_days: int) -> bool:
            n = int(x.get("segment_len_trading_days") or 0)
            if n < min_days:
                return False
            haz_cov = float(x.get("hazard_coverage") or 0.0)
            if haz_cov < min_cov:
                return False
            if lab in {MarketSituation.CRISIS, MarketSituation.RECOVERY}:
                frag_cov = float(x.get("fragility_coverage") or 0.0)
                if frag_cov < min_cov:
                    return False
            return True

        kept = [x for x in cands if _passes(x, min_days=min_len)]
        if not kept:
            kept = [x for x in cands if _passes(x, min_days=fallback_len)]
        filtered_by_label[lab] = kept

    selected = _select_segments(
        candidates_by_label=filtered_by_label,
        trading_days=trading_days,
        min_gap_trading_days=int(args.min_gap_trading_days),
        warmup_trading_days=int(args.warmup_trading_days),
        regime_cfg=regime_cfg,
        max_per_label=int(args.max_per_label),
    )

    output: Dict[str, Any] = {
        "market_id": market_id,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "hazard_overlay_csv": str(overlay_path),
        "fragility_entity_id": market_id,
        "regime_thresholds": asdict(regime_cfg),
        "situation_thresholds": asdict(situation_cfg),
        "selection_params": {
            "min_segment_days": int(min_len),
            "fallback_min_segment_days": int(fallback_len),
            "min_coverage": float(min_cov),
            "warmup_trading_days": int(args.warmup_trading_days),
            "min_gap_trading_days": int(args.min_gap_trading_days),
            "max_per_label": int(args.max_per_label),
        },
        "selected_slices": selected,
    }

    # Lightweight log summary.
    for lab in [MarketSituation.CRISIS, MarketSituation.RISK_OFF, MarketSituation.RECOVERY, MarketSituation.RISK_ON]:
        picks = selected.get(lab.value) or []
        if not picks:
            logger.warning("No slice selected for %s", lab.value)
            continue
        for i, s in enumerate(picks, start=1):
            logger.info(
                "Selected %s[%d]: segment=%s..%s run=%s..%s len=%s down=%.3f up=%.3f frag=%.3f",
                lab.value,
                i,
                s.get("segment_start"),
                s.get("segment_end"),
                s.get("run_start"),
                s.get("run_end"),
                s.get("segment_len_trading_days"),
                float(s.get("mean_down_risk") or 0.0),
                float(s.get("mean_up_risk") or 0.0),
                float(s.get("mean_fragility_score") or 0.0),
            )

    if args.output:
        try:
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2, sort_keys=True)
        except Exception:  # pragma: no cover
            logger.exception("Failed writing output JSON to %s", args.output)

    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
