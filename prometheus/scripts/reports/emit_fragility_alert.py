"""Emit latest market fragility alert for TUI/monitoring.

Writes a compact JSON payload to results/fragility_alerts/alert.json
that the TUI can poll/render. Also prints a human-readable summary.

Usage:
    python -m prometheus.scripts.reports.emit_fragility_alert --market-id US_EQ
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Sequence

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.fragility.storage import FragilityStorage


def _severity(score: float) -> str:
    if score >= 0.7:
        return "critical"
    if score >= 0.5:
        return "high"
    if score >= 0.3:
        return "moderate"
    return "info"


def _top_components(components: dict[str, float], n: int = 3) -> list[tuple[str, float]]:
    if not components:
        return []
    # Drop raw fields to keep payload small
    filtered = {k: v for k, v in components.items() if not k.endswith("_raw")}
    return sorted(filtered.items(), key=lambda kv: kv[1], reverse=True)[:n]


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Emit latest market fragility alert")
    parser.add_argument("--market-id", type=str, default="US_EQ")
    parser.add_argument("--output-dir", type=str, default="results/fragility_alerts")
    args = parser.parse_args(argv)

    config = get_config()
    db_manager = DatabaseManager(config)
    storage = FragilityStorage(db_manager=db_manager)

    measure = storage.get_latest_measure("MARKET", args.market_id)
    if measure is None:
        print(f"No fragility data for market {args.market_id}")
        return

    sev = _severity(measure.fragility_score)
    top_components = _top_components(measure.components, n=3)

    payload = {
        "market_id": args.market_id,
        "as_of": str(measure.as_of_date),
        "fragility_score": measure.fragility_score,
        "fragility_class": measure.class_label.value,
        "severity": sev,
        "top_components": top_components,
    }

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "alert.json"
    out_path.write_text(json.dumps(payload, indent=2))

    print(f"[{sev.upper()}] {args.market_id} fragility={measure.fragility_score:.3f} class={measure.class_label.value} as_of={measure.as_of_date}")
    if top_components:
        comps = ", ".join(f"{k}:{v:.2f}" for k, v in top_components)
        print(f"Drivers: {comps}")
    print(f"Saved {out_path}")


if __name__ == "__main__":
    main()
