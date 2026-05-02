"""Run an interactive, gated lambda factorial backtest campaign.

This CLI orchestrates a full lambda-factorial research campaign end-to-end:

1) (Optional) STAB backfill into runtime DB (soft_target_classes).
2) Multi-horizon opportunity-density (lambda) backfill to a raw CSV.
3) Transform raw lambda into a *score table* CSV consumable by:
   - C++ LambdaScoreTable (column order contract), and
   - Python CsvLambdaClusterScoreProvider (named columns).
4) Python-backend smoke factorial backtest with coverage gates.
5) C++ full-range factorial backtest.

The runner is intentionally interactive:
- It asks for confirmation before each step.
- It runs explicit data quality gates after each step.
- It writes a campaign directory containing logs, artifacts, and a manifest.

Typical usage
-------------

  ./scripts/campaigns/run_lambda_factorial_campaign.sh \
    --market-id US_EQ \
    --start 1997-01-02 --end 2024-12-31 \
    --horizons 5 21 63 \
    --lambda-weight 10.0 \
    --cpp-threads 32 --cpp-verbose --cpp-persist

Notes
-----
- The output root defaults to logs/campaigns/lambda_factorial (gitignored).
- Use --yes to run non-interactively (CI / automation).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
import yaml
from apatheon.core.config import get_config
from apatheon.core.database import DatabaseManager

# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _utc_now() -> datetime:
    return datetime.utcnow()


def _fmt_ts(ts: datetime) -> str:
    return ts.strftime("%Y%m%d_%H%M%S")


def _confirm(prompt: str, *, default_no: bool = True) -> bool:
    """Return True if user confirms.

    If default_no is True, empty input counts as "no".
    """

    suffix = "[y/N]" if default_no else "[Y/n]"
    raw = input(f"{prompt} {suffix} ").strip().lower()
    if raw == "":
        return not default_no
    return raw in {"y", "yes"}


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _tail_text(path: Path, *, max_lines: int = 40) -> str:
    try:
        lines = path.read_text(errors="replace").splitlines()
    except FileNotFoundError:
        return ""
    if len(lines) <= max_lines:
        return "\n".join(lines)
    return "\n".join(lines[-max_lines:])


def _run_subprocess(
    *,
    cmd: List[str],
    log_path: Path,
    cwd: Optional[Path] = None,
    env_overrides: Optional[Dict[str, str]] = None,
) -> int:
    """Run a subprocess, redirecting stdout/stderr to a log file."""

    _ensure_dir(log_path.parent)

    env = os.environ.copy()
    if env_overrides:
        env.update({k: str(v) for k, v in env_overrides.items()})

    with log_path.open("wb") as f:
        proc = subprocess.Popen(
            cmd,
            stdout=f,
            stderr=subprocess.STDOUT,
            env=env,
            cwd=str(cwd) if cwd is not None else None,
        )
        return int(proc.wait())


def _git_head() -> str | None:
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL)
        return out.decode("utf-8", errors="replace").strip()
    except Exception:
        return None


def _stable_json(obj: Any) -> str:
    """Stable JSON encoding for cache keys."""

    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def _short_hash(text: str, *, n: int = 12) -> str:
    h = hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()
    return h[: int(n)]


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)


def _append_daily_log(*, repo_root: Path, lines: List[str]) -> None:
    """Append a small block to logs/daily/YYYY-MM-DD.md."""

    day = date.today().isoformat()
    path = repo_root / "logs" / "daily" / f"{day}.md"
    _ensure_dir(path.parent)

    block = "\n".join(lines).rstrip() + "\n"
    with path.open("a", encoding="utf-8") as f:
        f.write(block)


# ---------------------------------------------------------------------------
# Campaign model
# ---------------------------------------------------------------------------


@dataclass
class StepResult:
    name: str
    started_at_utc: str
    finished_at_utc: str
    command: List[str]
    log_path: str
    exit_code: int
    gate_passed: bool
    gate_summary: str


@dataclass
class CampaignPaths:
    repo_root: Path
    campaign_dir: Path
    manifest_path: Path
    logs_dir: Path
    artifacts_dir: Path
    results_dir: Path


def _default_campaign_id(*, market_id: str, start: date, end: date) -> str:
    ts = _fmt_ts(_utc_now())
    head = _git_head() or "nogit"
    head_short = head[:10]
    return f"{ts}_{market_id}_{start.isoformat()}_{end.isoformat()}_{head_short}"


def _write_manifest(path: Path, payload: Dict[str, Any]) -> None:
    _ensure_dir(path.parent)
    text = yaml.safe_dump(payload, sort_keys=False)
    path.write_text(text, encoding="utf-8")


def _load_manifest(path: Path) -> Dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid manifest YAML at {path}")
    return data


# ---------------------------------------------------------------------------
# Gates
# ---------------------------------------------------------------------------


def _gate_lambda_raw_csv(
    *,
    path: Path,
    max_unknown_frac: float,
) -> tuple[bool, str]:
    if not path.exists():
        return False, f"missing output CSV: {path}"

    # Prefer instrument-weighted UNKNOWN fraction when the raw file includes num_instruments.
    # Row-based UNKNOWN fraction can look inflated because each date contributes at most one
    # UNKNOWN cluster row, regardless of how small that cluster is.
    header = pd.read_csv(path, nrows=0)
    cols = set(header.columns)

    usecols = ["soft_target_class"]
    has_weights = "num_instruments" in cols
    if has_weights:
        usecols.append("num_instruments")

    df = pd.read_csv(path, usecols=usecols)
    if df.empty:
        return False, "lambda raw CSV is empty"

    soft = df["soft_target_class"].astype(str)
    unknown = soft == "UNKNOWN"

    unknown_frac_row = float(unknown.mean())

    unknown_frac = unknown_frac_row
    if has_weights:
        w = pd.to_numeric(df["num_instruments"], errors="coerce").fillna(0.0)
        total = float(w.sum())
        if total > 0:
            unknown_frac = float(w[unknown].sum() / total)

    ok = unknown_frac <= float(max_unknown_frac)

    if has_weights:
        return (
            ok,
            f"UNKNOWN_frac(row)={unknown_frac_row:.4%}, "
            f"UNKNOWN_frac(weighted)={unknown_frac:.4%} (threshold={max_unknown_frac:.2%})",
        )

    return ok, f"UNKNOWN_frac={unknown_frac_row:.4%} (threshold={max_unknown_frac:.2%})"


def _gate_lambda_scores_csv(
    *,
    path: Path,
    horizons: Sequence[int],
    max_unknown_frac: float,
    min_nonnull_frac: float,
) -> tuple[bool, str]:
    if not path.exists():
        return False, f"missing lambda score table: {path}"

    required = [
        "as_of_date",
        "market_id",
        "sector",
        "soft_target_class",
        "lambda_value",
    ] + [f"lambda_score_h{int(h)}" for h in horizons]

    df = pd.read_csv(path, nrows=10)
    missing = [c for c in required if c not in df.columns]
    if missing:
        return False, f"lambda score table missing columns: {missing}"

    # Coverage check on full file (only required cols).
    usecols = ["soft_target_class"] + [f"lambda_score_h{int(h)}" for h in horizons]
    df2 = pd.read_csv(path, usecols=usecols)
    if df2.empty:
        return False, "lambda score table is empty"

    unknown_frac = float((df2["soft_target_class"].astype(str) == "UNKNOWN").mean())
    nonnull_fracs = {c: float(df2[c].notna().mean()) for c in usecols if c != "soft_target_class"}

    ok = True
    reasons: List[str] = []

    if unknown_frac > float(max_unknown_frac):
        ok = False
        reasons.append(f"UNKNOWN_frac={unknown_frac:.4%} > {max_unknown_frac:.2%}")

    for col, frac in nonnull_fracs.items():
        if frac < float(min_nonnull_frac):
            ok = False
            reasons.append(f"{col}_nonnull={frac:.4%} < {min_nonnull_frac:.2%}")

    if ok:
        summary = (
            f"UNKNOWN_frac={unknown_frac:.4%}; "
            + ", ".join(f"{k}={v:.4%}" for k, v in nonnull_fracs.items())
        )
        return True, summary

    return False, "; ".join(reasons)


def _gate_python_smoke_lambda_coverage(
    *,
    db_manager: DatabaseManager,
    run_ids: Sequence[str],
    min_coverage: float,
) -> tuple[bool, str]:
    if not run_ids:
        return False, "no run_ids found in smoke output"

    sql = "SELECT run_id, metrics_json FROM backtest_runs WHERE run_id = ANY(%s)"
    rows: List[tuple[str, Any]]
    with db_manager.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (list(run_ids),))
            rows = cur.fetchall()
        finally:
            cur.close()

    if not rows:
        return False, "no backtest_runs rows found for smoke run_ids"

    covs: List[float] = []
    missing = 0

    for _rid, metrics in rows:
        metrics_obj: Any = metrics
        if isinstance(metrics_obj, str):
            try:
                metrics_obj = json.loads(metrics_obj)
            except Exception:
                metrics_obj = None

        if not isinstance(metrics_obj, dict):
            missing += 1
            continue

        cov = metrics_obj.get("lambda_score_coverage_over_run")
        if isinstance(cov, (int, float)):
            covs.append(float(cov))
        else:
            missing += 1

    if not covs:
        return False, f"no lambda_score_coverage_over_run found (missing={missing}/{len(rows)})"

    min_cov = min(covs)
    ok = min_cov >= float(min_coverage)
    return ok, f"min_lambda_score_coverage_over_run={min_cov:.4f} (threshold={min_coverage:.2f})"


def _parse_run_ids_from_factorial_log(log_path: Path) -> List[str]:
    """Extract run_id values from run_lambda_factorial_backtests (python backend) output."""

    if not log_path.exists():
        return []

    run_ids: List[str] = []
    lines = log_path.read_text(errors="replace").splitlines()

    header_seen = False
    for line in lines:
        if line.strip().startswith("run_id,sleeve_id"):
            header_seen = True
            continue
        if not header_seen:
            continue

        # Expected CSV row: run_id,sleeve_id,horizon,mode,...
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 4:
            continue
        rid = parts[0]
        # Very light UUID shape check.
        if len(rid) == 36 and rid.count("-") == 4:
            run_ids.append(rid)

    return run_ids


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Interactive lambda factorial campaign runner")

    parser.add_argument("--market-id", type=str, required=True)
    parser.add_argument("--start", type=_parse_date, required=True)
    parser.add_argument("--end", type=_parse_date, required=True)

    parser.add_argument("--horizons", type=int, nargs="+", default=[5, 21, 63])
    parser.add_argument(
        "--max-abs-daily-return",
        type=float,
        default=5.0,
        help=(
            "Skip instruments whose price series contains an extreme single-day return with abs(ret) > "
            "threshold when computing raw lambda. Use 0 to disable. Default: 5.0 (500 percent)."
        ),
    )
    parser.add_argument("--lambda-weight", type=float, default=10.0)
    parser.add_argument("--universe-max-size", type=int, default=200)
    parser.add_argument(
        "--universe-sector-max-names",
        type=int,
        default=0,
        help="Optional per-sector name cap during universe selection (default: 0 = disabled)",
    )
    parser.add_argument(
        "--portfolio-max-names",
        type=int,
        default=0,
        help="Optional top-K cap at the portfolio stage (default: 0 = disabled)",
    )
    parser.add_argument(
        "--portfolio-hysteresis-buffer",
        type=int,
        default=0,
        help=(
            "Optional rank buffer for top-K portfolios to reduce churn. "
            "If set to B>0 and --portfolio-max-names=K, held names are kept until rank > K+B. "
            "Default: 0 = disabled."
        ),
    )
    parser.add_argument(
        "--portfolio-per-instrument-max-weight",
        type=float,
        default=0.0,
        help="Optional per-name cap inside the portfolio model (default: 0 = sleeve default)",
    )

    parser.add_argument("--smoke-start", type=_parse_date, default=None)
    parser.add_argument("--smoke-end", type=_parse_date, default=None)

    parser.add_argument("--cpp-threads", type=int, default=0)
    parser.add_argument("--cpp-verbose", action="store_true")
    parser.add_argument("--cpp-persist", action="store_true")
    parser.add_argument("--cpp-persist-execution", action="store_true")
    parser.add_argument("--cpp-persist-meta", action="store_true")
    parser.add_argument(
        "--cpp-python",
        type=str,
        default="python3",
        help="Python executable used for the C++ backtest step (default: python3)",
    )
    parser.add_argument("--cpp-pythonpath", type=str, default="cpp/build")

    parser.add_argument("--campaign-id", type=str, default=None)
    parser.add_argument("--out-root", type=str, default="logs/campaigns/lambda_factorial")

    # Optional persistent caches for expensive artifacts (lambda raw / scores).
    parser.add_argument(
        "--cache",
        action="store_true",
        help="Enable persistent caches under --cache-root for lambda raw/scores artifacts",
    )
    parser.add_argument(
        "--cache-root",
        type=str,
        default="data/cache",
        help="Cache root directory (relative to repo root). Default: data/cache",
    )

    parser.add_argument("--yes", action="store_true", help="Run non-interactively (auto-confirm all steps)")

    # Gates / thresholds
    parser.add_argument("--max-unknown-frac", type=float, default=0.01)
    parser.add_argument("--min-score-nonnull-frac", type=float, default=0.99)
    parser.add_argument("--min-smoke-lambda-coverage", type=float, default=0.80)

    # Step toggles
    parser.add_argument("--skip-stab", action="store_true", help="Skip STAB backfill step")
    parser.add_argument("--skip-smoke", action="store_true", help="Skip Python smoke run")
    parser.add_argument("--skip-cpp", action="store_true", help="Skip C++ full run")

    args = parser.parse_args(argv)

    if args.end < args.start:
        raise SystemExit("--end must be >= --start")

    horizons = sorted(set(int(h) for h in args.horizons))
    if any(h <= 0 for h in horizons):
        raise SystemExit("--horizons must be positive")

    repo_root = Path(__file__).resolve().parents[3]

    campaign_id = args.campaign_id or _default_campaign_id(
        market_id=str(args.market_id),
        start=args.start,
        end=args.end,
    )

    out_root = repo_root / str(args.out_root)
    campaign_dir = out_root / campaign_id

    paths = CampaignPaths(
        repo_root=repo_root,
        campaign_dir=campaign_dir,
        manifest_path=campaign_dir / "manifest.yaml",
        logs_dir=campaign_dir / "logs",
        artifacts_dir=campaign_dir / "artifacts",
        results_dir=campaign_dir / "results",
    )

    _ensure_dir(paths.logs_dir)
    _ensure_dir(paths.artifacts_dir)
    _ensure_dir(paths.results_dir)

    # Derive default smoke window if not provided.
    smoke_end = args.smoke_end or args.end
    smoke_start = args.smoke_start
    if smoke_start is None:
        smoke_start = max(args.start, smoke_end - timedelta(days=365))

    # Manifest initial payload.
    manifest: Dict[str, Any] = {
        "campaign_id": campaign_id,
        "created_at_utc": _utc_now().isoformat() + "Z",
        "git_head": _git_head(),
        "config": {
            "market_id": str(args.market_id),
            "start": args.start.isoformat(),
            "end": args.end.isoformat(),
            "horizons": horizons,
            "max_abs_daily_return": float(args.max_abs_daily_return),
            "lambda_weight": float(args.lambda_weight),
            "universe_max_size": int(args.universe_max_size),
            "universe_sector_max_names": int(args.universe_sector_max_names),
            "portfolio_max_names": int(args.portfolio_max_names),
            "portfolio_hysteresis_buffer": int(args.portfolio_hysteresis_buffer),
            "portfolio_per_instrument_max_weight": float(args.portfolio_per_instrument_max_weight),
            "smoke_start": smoke_start.isoformat(),
            "smoke_end": smoke_end.isoformat(),
            "cache": {
                "enabled": bool(args.cache),
                "root": str(args.cache_root),
            },
            "cpp": {
                "threads": int(args.cpp_threads),
                "verbose": bool(args.cpp_verbose),
                "persist": bool(args.cpp_persist),
                "persist_execution": bool(args.cpp_persist_execution),
                "persist_meta": bool(args.cpp_persist_meta),
                "python": str(args.cpp_python),
                "pythonpath": str(args.cpp_pythonpath),
            },
        },
        "paths": {
            "campaign_dir": str(paths.campaign_dir),
            "logs_dir": str(paths.logs_dir),
            "artifacts_dir": str(paths.artifacts_dir),
            "results_dir": str(paths.results_dir),
        },
        "steps": {},
    }

    _write_manifest(paths.manifest_path, manifest)

    _append_daily_log(
        repo_root=repo_root,
        lines=[
            f"## lambda_factorial campaign: {campaign_id}",
            f"- created_at_utc: {manifest['created_at_utc']}",
            f"- market_id: {args.market_id}",
            f"- range: {args.start} → {args.end}",
            f"- campaign_dir: {paths.campaign_dir}",
            "",
        ],
    )

    config = get_config()
    db_manager = DatabaseManager(config)

    def _record_step(result: StepResult) -> None:
        manifest = _load_manifest(paths.manifest_path)
        steps = manifest.setdefault("steps", {})
        steps[result.name] = {
            "started_at_utc": result.started_at_utc,
            "finished_at_utc": result.finished_at_utc,
            "command": result.command,
            "log_path": result.log_path,
            "exit_code": int(result.exit_code),
            "gate_passed": bool(result.gate_passed),
            "gate_summary": str(result.gate_summary),
        }
        _write_manifest(paths.manifest_path, manifest)

        _append_daily_log(
            repo_root=repo_root,
            lines=[
                f"### step: {result.name}",
                f"- started_at_utc: {result.started_at_utc}",
                f"- finished_at_utc: {result.finished_at_utc}",
                f"- exit_code: {result.exit_code}",
                f"- gate_passed: {result.gate_passed}",
                f"- gate: {result.gate_summary}",
                f"- log: {result.log_path}",
                "",
            ],
        )

    def _maybe_confirm(msg: str) -> bool:
        if args.yes:
            return True
        return _confirm(msg, default_no=True)

    # ------------------------------------------------------------------
    # Step 1: STAB backfill
    # ------------------------------------------------------------------
    if not args.skip_stab:
        step_name = "stab_backfill"
        log_path = paths.logs_dir / "stab_backfill.log"
        report_path = paths.artifacts_dir / "stab" / "stab_backfill_report.json"

        cmd = [
            "python",
            "-m",
            "prometheus.scripts.backfill.backfill_soft_target_classes",
            "--market-id",
            str(args.market_id),
            "--start",
            args.start.isoformat(),
            "--end",
            args.end.isoformat(),
            "--window-days",
            "63",
            "--on-conflict",
            "replace",
            "--report-out",
            str(report_path),
        ]

        print(f"\nSTEP: {step_name}")
        print(f"  command: {' '.join(cmd)}")
        print(f"  log    : {log_path}")
        print(f"  report : {report_path}")

        if not _maybe_confirm("Run STAB backfill?"):
            print("Skipping STAB backfill by user choice.")
        else:
            t0 = _utc_now()
            code = _run_subprocess(cmd=cmd, log_path=log_path, cwd=repo_root)
            t1 = _utc_now()

            gate_ok = False
            gate_summary = ""
            if code == 0 and report_path.exists():
                try:
                    rep = json.loads(report_path.read_text(encoding="utf-8"))
                    cov = rep.get("coverage", {}) if isinstance(rep, dict) else {}
                    post = cov.get("post_warmup_frac_written")
                    if isinstance(post, (int, float)):
                        gate_ok = float(post) >= 0.95
                        gate_summary = f"post_warmup_frac_written={float(post):.4%} (>=95%)"
                    else:
                        gate_ok = True
                        gate_summary = "report present (no coverage field)"
                except Exception as exc:  # pragma: no cover
                    gate_ok = False
                    gate_summary = f"failed to parse STAB report: {exc}"
            else:
                gate_ok = False
                gate_summary = f"exit_code={code}; report_exists={report_path.exists()}"

            _record_step(
                StepResult(
                    name=step_name,
                    started_at_utc=t0.isoformat() + "Z",
                    finished_at_utc=t1.isoformat() + "Z",
                    command=cmd,
                    log_path=str(log_path),
                    exit_code=code,
                    gate_passed=gate_ok,
                    gate_summary=gate_summary,
                )
            )

            print("\n--- log tail ---")
            print(_tail_text(log_path))

            if not gate_ok:
                raise SystemExit(f"Gate failed for {step_name}: {gate_summary}")

    # ------------------------------------------------------------------
    # Step 2: Lambda raw backfill (multi-horizon)
    # ------------------------------------------------------------------
    step_name = "lambda_backfill_multihorizon"
    log_path = paths.logs_dir / "lambda_backfill_multihorizon.log"
    raw_csv = paths.artifacts_dir / "lambda_raw" / "lambda_multihorizon_raw.csv"
    raw_report_path = paths.artifacts_dir / "lambda_raw" / "lambda_multihorizon_raw_report.json"

    cache_enabled = bool(args.cache)
    cache_root = (repo_root / str(args.cache_root)).resolve() if cache_enabled else None

    cache_raw_csv: Path | None = None
    cache_key_raw: str | None = None
    if cache_enabled:
        payload = {
            "kind": "lambda_raw_multihorizon",
            "version": "v3_bulk_resumable_adjclose",
            "market_id": str(args.market_id),
            "start": args.start.isoformat(),
            "end": args.end.isoformat(),
            "horizons": [int(h) for h in horizons],
            "min_cluster_size": 5,
            "max_abs_daily_return": float(args.max_abs_daily_return),
        }
        cache_key_raw = _short_hash(_stable_json(payload))
        h_str = "-".join(str(int(h)) for h in horizons)
        fname = (
            f"lambda_multihorizon_raw_{args.market_id}_{args.start.isoformat()}_{args.end.isoformat()}"
            f"_h{h_str}_mc5_{cache_key_raw}.csv"
        )
        cache_raw_csv = cache_root / "lambda_raw" / fname

    # Decide where the backfill should write.
    compute_out = cache_raw_csv if cache_raw_csv is not None else raw_csv

    cmd = [
        "python",
        "-m",
        "prometheus.scripts.backfill.backfill_opportunity_density_multihorizon",
        "--market",
        str(args.market_id),
        "--start",
        args.start.isoformat(),
        "--end",
        args.end.isoformat(),
        "--horizons",
    ] + [str(int(h)) for h in horizons] + [
        "--min-cluster-size",
        "5",
        "--max-abs-daily-return",
        str(float(args.max_abs_daily_return)),
        "--resume",
        "--report-out",
        str(raw_report_path),
        "--output",
        str(compute_out),
    ]

    print(f"\nSTEP: {step_name}")
    print(f"  command: {' '.join(cmd)}")
    print(f"  log    : {log_path}")
    print(f"  output : {raw_csv}")
    if cache_raw_csv is not None:
        print(f"  cache  : {cache_raw_csv}")

    if not _maybe_confirm("Run lambda multihorizon backfill?"):
        raise SystemExit("Aborted by user.")

    # Cache hit path: reuse if present and passes gate.
    if cache_raw_csv is not None and cache_raw_csv.exists():
        ok, summary = _gate_lambda_raw_csv(path=cache_raw_csv, max_unknown_frac=float(args.max_unknown_frac))
        if ok:
            _ensure_dir(log_path.parent)
            log_path.write_text(
                f"CACHE HIT: {cache_raw_csv}\n{summary}\n",
                encoding="utf-8",
            )
            _copy_file(cache_raw_csv, raw_csv)

            now = _utc_now()
            _record_step(
                StepResult(
                    name=step_name,
                    started_at_utc=now.isoformat() + "Z",
                    finished_at_utc=now.isoformat() + "Z",
                    command=["CACHE_HIT", str(cache_raw_csv)],
                    log_path=str(log_path),
                    exit_code=0,
                    gate_passed=True,
                    gate_summary=f"cache_hit; {summary}",
                )
            )

            print("\n--- log tail ---")
            print(_tail_text(log_path))
        else:
            # Cache exists but does not pass gate; recompute into cache.
            t0 = _utc_now()
            code = _run_subprocess(cmd=cmd, log_path=log_path, cwd=repo_root)
            t1 = _utc_now()

            gate_ok, gate_summary = _gate_lambda_raw_csv(
                path=compute_out,
                max_unknown_frac=float(args.max_unknown_frac),
            )

            if code == 0 and gate_ok and cache_raw_csv is not None and compute_out == cache_raw_csv:
                _copy_file(cache_raw_csv, raw_csv)

            _record_step(
                StepResult(
                    name=step_name,
                    started_at_utc=t0.isoformat() + "Z",
                    finished_at_utc=t1.isoformat() + "Z",
                    command=cmd,
                    log_path=str(log_path),
                    exit_code=code,
                    gate_passed=(code == 0 and gate_ok),
                    gate_summary=gate_summary,
                )
            )

            print("\n--- log tail ---")
            print(_tail_text(log_path))

            if code != 0:
                raise SystemExit(f"Step failed: {step_name} (exit_code={code})")
            if not gate_ok:
                raise SystemExit(f"Gate failed for {step_name}: {gate_summary}")
    else:
        # No cache: compute.
        t0 = _utc_now()
        code = _run_subprocess(cmd=cmd, log_path=log_path, cwd=repo_root)
        t1 = _utc_now()

        gate_ok, gate_summary = _gate_lambda_raw_csv(
            path=compute_out,
            max_unknown_frac=float(args.max_unknown_frac),
        )

        if code == 0 and gate_ok and cache_raw_csv is not None and compute_out == cache_raw_csv:
            _copy_file(cache_raw_csv, raw_csv)
        elif code == 0 and gate_ok and compute_out == raw_csv:
            # No cache: already wrote into artifacts.
            pass
        elif code == 0 and gate_ok and compute_out != raw_csv:
            # Defensive: if compute_out differs, ensure artifacts are populated.
            _copy_file(Path(str(compute_out)), raw_csv)

        _record_step(
            StepResult(
                name=step_name,
                started_at_utc=t0.isoformat() + "Z",
                finished_at_utc=t1.isoformat() + "Z",
                command=cmd,
                log_path=str(log_path),
                exit_code=code,
                gate_passed=(code == 0 and gate_ok),
                gate_summary=gate_summary,
            )
        )

        print("\n--- log tail ---")
        print(_tail_text(log_path))

        if code != 0:
            raise SystemExit(f"Step failed: {step_name} (exit_code={code})")
        if not gate_ok:
            raise SystemExit(f"Gate failed for {step_name}: {gate_summary}")

    # ------------------------------------------------------------------
    # Step 3: Lambda score table transform
    # ------------------------------------------------------------------
    step_name = "lambda_score_table"
    log_path = paths.logs_dir / "lambda_score_table.log"
    scores_csv = paths.artifacts_dir / "lambda_scores" / "lambda_cluster_scores_smoothed.csv"
    report_path = paths.artifacts_dir / "lambda_scores" / "lambda_score_table_report.json"

    cache_scores_csv: Path | None = None
    if cache_enabled and cache_root is not None and cache_key_raw is not None:
        payload = {
            "kind": "lambda_score_table",
            "version": "v1",
            "raw_key": str(cache_key_raw),
            "horizons": [int(h) for h in horizons],
            "transform": "log1p",
            "winsor_pct": [1, 99],
            "normalize": "per-date-robust-z",
            "smoothing": "ewma",
            "ewma_span": 20,
        }
        key = _short_hash(_stable_json(payload))
        h_str = "-".join(str(int(h)) for h in horizons)
        fname = f"lambda_cluster_scores_smoothed_{args.market_id}_{args.start.isoformat()}_{args.end.isoformat()}_h{h_str}_{key}.csv"
        cache_scores_csv = cache_root / "lambda_scores" / fname

    compute_scores_out = cache_scores_csv if cache_scores_csv is not None else scores_csv

    cmd = [
        "python",
        "-m",
        "prometheus.scripts.analysis.make_lambda_score_table",
        "--input",
        str(raw_csv),
        "--output",
        str(compute_scores_out),
        "--horizons",
    ] + [str(int(h)) for h in horizons] + [
        "--transform",
        "log1p",
        "--winsor-pct",
        "1",
        "99",
        "--normalize",
        "per-date-robust-z",
        "--smoothing",
        "ewma",
        "--ewma-span",
        "20",
        "--report-out",
        str(report_path),
        "--strict",
        "--max-unknown-frac",
        str(float(args.max_unknown_frac)),
        "--min-score-nonnull-frac",
        str(float(args.min_score_nonnull_frac)),
    ]

    print(f"\nSTEP: {step_name}")
    print(f"  command: {' '.join(cmd)}")
    print(f"  log    : {log_path}")
    print(f"  output : {scores_csv}")
    if cache_scores_csv is not None:
        print(f"  cache  : {cache_scores_csv}")
    print(f"  report : {report_path}")

    if not _maybe_confirm("Build lambda score table?"):
        raise SystemExit("Aborted by user.")

    if cache_scores_csv is not None and cache_scores_csv.exists():
        ok, summary = _gate_lambda_scores_csv(
            path=cache_scores_csv,
            horizons=horizons,
            max_unknown_frac=float(args.max_unknown_frac),
            min_nonnull_frac=float(args.min_score_nonnull_frac),
        )
        if ok:
            _ensure_dir(log_path.parent)
            log_path.write_text(
                f"CACHE HIT: {cache_scores_csv}\n{summary}\n",
                encoding="utf-8",
            )
            _copy_file(cache_scores_csv, scores_csv)

            now = _utc_now()
            _record_step(
                StepResult(
                    name=step_name,
                    started_at_utc=now.isoformat() + "Z",
                    finished_at_utc=now.isoformat() + "Z",
                    command=["CACHE_HIT", str(cache_scores_csv)],
                    log_path=str(log_path),
                    exit_code=0,
                    gate_passed=True,
                    gate_summary=f"cache_hit; {summary}",
                )
            )

            print("\n--- log tail ---")
            print(_tail_text(log_path))
        else:
            t0 = _utc_now()
            code = _run_subprocess(cmd=cmd, log_path=log_path, cwd=repo_root)
            t1 = _utc_now()

            gate_ok, gate_summary = _gate_lambda_scores_csv(
                path=compute_scores_out,
                horizons=horizons,
                max_unknown_frac=float(args.max_unknown_frac),
                min_nonnull_frac=float(args.min_score_nonnull_frac),
            )

            if code == 0 and gate_ok and cache_scores_csv is not None and compute_scores_out == cache_scores_csv:
                _copy_file(cache_scores_csv, scores_csv)

            _record_step(
                StepResult(
                    name=step_name,
                    started_at_utc=t0.isoformat() + "Z",
                    finished_at_utc=t1.isoformat() + "Z",
                    command=cmd,
                    log_path=str(log_path),
                    exit_code=code,
                    gate_passed=(code == 0 and gate_ok),
                    gate_summary=gate_summary,
                )
            )

            print("\n--- log tail ---")
            print(_tail_text(log_path))

            if code != 0:
                raise SystemExit(f"Step failed: {step_name} (exit_code={code})")
            if not gate_ok:
                raise SystemExit(f"Gate failed for {step_name}: {gate_summary}")
    else:
        t0 = _utc_now()
        code = _run_subprocess(cmd=cmd, log_path=log_path, cwd=repo_root)
        t1 = _utc_now()

        gate_ok, gate_summary = _gate_lambda_scores_csv(
            path=compute_scores_out,
            horizons=horizons,
            max_unknown_frac=float(args.max_unknown_frac),
            min_nonnull_frac=float(args.min_score_nonnull_frac),
        )

        if code == 0 and gate_ok and cache_scores_csv is not None and compute_scores_out == cache_scores_csv:
            _copy_file(cache_scores_csv, scores_csv)

        _record_step(
            StepResult(
                name=step_name,
                started_at_utc=t0.isoformat() + "Z",
                finished_at_utc=t1.isoformat() + "Z",
                command=cmd,
                log_path=str(log_path),
                exit_code=code,
                gate_passed=(code == 0 and gate_ok),
                gate_summary=gate_summary,
            )
        )

        print("\n--- log tail ---")
        print(_tail_text(log_path))

        if code != 0:
            raise SystemExit(f"Step failed: {step_name} (exit_code={code})")
        if not gate_ok:
            raise SystemExit(f"Gate failed for {step_name}: {gate_summary}")


    # ------------------------------------------------------------------
    # Step 4: Python smoke factorial run
    # ------------------------------------------------------------------
    if not args.skip_smoke:
        step_name = "factorial_python_smoke"
        log_path = paths.logs_dir / "factorial_python_smoke.log"

        cmd = [
            "python",
            "-m",
            "prometheus.scripts.run.run_lambda_factorial_backtests",
            "--backend",
            "python",
            "--market-id",
            str(args.market_id),
            "--start",
            smoke_start.isoformat(),
            "--end",
            smoke_end.isoformat(),
            "--lambda-scores-csv",
            str(scores_csv),
            "--horizons",
        ] + [str(int(h)) for h in horizons] + [
            "--universe-max-size",
            str(int(args.universe_max_size)),
            "--universe-sector-max-names",
            str(int(args.universe_sector_max_names)),
            "--portfolio-max-names",
            str(int(args.portfolio_max_names)),
            "--portfolio-hysteresis-buffer",
            str(int(args.portfolio_hysteresis_buffer)),
            "--portfolio-per-instrument-max-weight",
            str(float(args.portfolio_per_instrument_max_weight)),
            "--lambda-weight",
            str(float(args.lambda_weight)),
            "--max-workers",
            "1",
        ]

        print(f"\nSTEP: {step_name}")
        print(f"  command: {' '.join(cmd)}")
        print(f"  log    : {log_path}")

        if not _maybe_confirm("Run Python smoke factorial backtest?"):
            raise SystemExit("Aborted by user.")

        t0 = _utc_now()
        code = _run_subprocess(cmd=cmd, log_path=log_path, cwd=repo_root)
        t1 = _utc_now()

        run_ids = _parse_run_ids_from_factorial_log(log_path)
        gate_ok, gate_summary = _gate_python_smoke_lambda_coverage(
            db_manager=db_manager,
            run_ids=run_ids,
            min_coverage=float(args.min_smoke_lambda_coverage),
        )

        _record_step(
            StepResult(
                name=step_name,
                started_at_utc=t0.isoformat() + "Z",
                finished_at_utc=t1.isoformat() + "Z",
                command=cmd,
                log_path=str(log_path),
                exit_code=code,
                gate_passed=(code == 0 and gate_ok),
                gate_summary=gate_summary,
            )
        )

        print("\n--- log tail ---")
        print(_tail_text(log_path))

        if code != 0:
            raise SystemExit(f"Step failed: {step_name} (exit_code={code})")
        if not gate_ok:
            raise SystemExit(f"Gate failed for {step_name}: {gate_summary}")

    # ------------------------------------------------------------------
    # Step 5: C++ full factorial run
    # ------------------------------------------------------------------
    if not args.skip_cpp:
        step_name = "factorial_cpp_full"
        log_path = paths.logs_dir / "factorial_cpp_full.log"

        cmd = [
            str(args.cpp_python),
            "-m",
            "prometheus.scripts.run.run_lambda_factorial_backtests",
            "--backend",
            "cpp",
            "--market-id",
            str(args.market_id),
            "--start",
            args.start.isoformat(),
            "--end",
            args.end.isoformat(),
            "--lambda-scores-csv",
            str(scores_csv),
            "--horizons",
        ] + [str(int(h)) for h in horizons] + [
            "--universe-max-size",
            str(int(args.universe_max_size)),
            "--universe-sector-max-names",
            str(int(args.universe_sector_max_names)),
            "--portfolio-max-names",
            str(int(args.portfolio_max_names)),
            "--portfolio-hysteresis-buffer",
            str(int(args.portfolio_hysteresis_buffer)),
            "--portfolio-per-instrument-max-weight",
            str(float(args.portfolio_per_instrument_max_weight)),
            "--lambda-weight",
            str(float(args.lambda_weight)),
            "--cpp-threads",
            str(int(args.cpp_threads)),
        ]

        if args.cpp_verbose:
            cmd.append("--cpp-verbose")
        if args.cpp_persist:
            cmd.append("--cpp-persist")
        if args.cpp_persist_execution:
            cmd.append("--cpp-persist-execution")
        if args.cpp_persist_meta:
            cmd.append("--cpp-persist-meta")

        # Use campaign_id as base_prefix to avoid collisions when persisting.
        cmd += ["--base-prefix", str(campaign_id)]

        env_overrides = {
            "PYTHONPATH": str((repo_root / str(args.cpp_pythonpath)).resolve()),
        }

        print(f"\nSTEP: {step_name}")
        print(f"  command: {' '.join(cmd)}")
        print(f"  log    : {log_path}")
        print(f"  env    : PYTHONPATH={env_overrides['PYTHONPATH']}")

        if not _maybe_confirm("Run C++ full factorial backtest?"):
            raise SystemExit("Aborted by user.")

        t0 = _utc_now()
        code = _run_subprocess(
            cmd=cmd,
            log_path=log_path,
            cwd=repo_root,
            env_overrides=env_overrides,
        )
        t1 = _utc_now()

        gate_ok = code == 0
        gate_summary = "exit_code=0" if gate_ok else f"exit_code={code}"

        _record_step(
            StepResult(
                name=step_name,
                started_at_utc=t0.isoformat() + "Z",
                finished_at_utc=t1.isoformat() + "Z",
                command=cmd,
                log_path=str(log_path),
                exit_code=code,
                gate_passed=gate_ok,
                gate_summary=gate_summary,
            )
        )

        print("\n--- log tail ---")
        print(_tail_text(log_path))

        if code != 0:
            raise SystemExit(f"Step failed: {step_name} (exit_code={code})")

    print("\nCampaign complete.")
    print(f"Campaign dir: {paths.campaign_dir}")
    print(f"Manifest    : {paths.manifest_path}")


if __name__ == "__main__":  # pragma: no cover
    main()
