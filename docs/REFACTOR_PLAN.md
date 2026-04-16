# Refactor Plan: Splitting Monolithic Files

This document captures the recommended split for three files that have
grown past the point where they're tractable in code review or
navigation. The split is **deferred** — it requires careful import-graph
audits and broad test coverage. Tracked here so the work can be picked
up safely in a dedicated PR.

## Why this is deferred

Each file is imported by 10+ consumers. Moving symbols breaks every
import and every test that patches them. Doing it surgically requires:

1. A frozen test snapshot to assert behaviour parity
2. Per-symbol move + import-shim cycle (keep the old name as a
   re-export for one release, then delete)
3. A grep audit for string-based references (decorators, dynamic
   imports, JSON config)

## File 1: `prometheus/pipeline/tasks.py` (4,125 lines)

### Current natural cleavage points

| Lines | Function | Proposed module |
|------:|----------|-----------------|
| 105–326 | DailyUniverseLambdaConfig + load helpers | `pipeline/_configs.py` |
| 328–886 | `_get_region_instruments`, `run_signals_for_run` | `pipeline/signals.py` |
| 887–1230 | sector health + `run_universes_for_run` | `pipeline/universes.py` |
| 1231–2441 | `run_books_for_run` | `pipeline/books.py` |
| 2442–2972 | ExecutionConfig + `run_execution_for_run` | `pipeline/execution.py` |
| 2973–3675 | OptionsExecutionConfig + `run_options_for_run` + helpers | `pipeline/options.py` |
| 3676–3796 | price/weight loaders | `pipeline/_loaders.py` |
| 3797–4086 | meta runners | `pipeline/meta.py` |
| 4087–end  | `advance_run` orchestrator | stays in `tasks.py` |

### Migration plan

```python
# tasks.py becomes a thin facade for backward compatibility:
from prometheus.pipeline.signals import run_signals_for_run  # noqa: F401
from prometheus.pipeline.universes import run_universes_for_run  # noqa: F401
from prometheus.pipeline.books import run_books_for_run  # noqa: F401
from prometheus.pipeline.execution import run_execution_for_run, ExecutionConfig  # noqa: F401
from prometheus.pipeline.options import run_options_for_run, OptionsExecutionConfig  # noqa: F401
from prometheus.pipeline.meta import run_meta_for_strategy, run_backtest_campaign_and_meta_for_strategy  # noqa: F401
```

Existing imports keep working; new code imports from the new modules.
After two release cycles, delete the re-exports and update remaining
callers.

## File 2: `prometheus/monitoring/api.py` (4,144 lines)

The monitoring API has already been partially split into sub-routers
(`nation_api`, `intel_api`, `entities_api`, `control_api`,
`meta_api`, etc.). The remaining 4k lines in `api.py` should be split
along the same router-per-domain seam:

| Section | Proposed router |
|---------|-----------------|
| Status / health endpoints | `monitoring/status_api.py` |
| Backtest endpoints | `monitoring/backtest_api.py` |
| Signals / engine endpoints | `monitoring/engines_api.py` |
| Portfolio / risk endpoints | `monitoring/portfolio_api.py` |
| Trading / execution endpoints | `monitoring/trading_api.py` |
| Misc utilities | stay in `api.py` |

Each new file follows the existing pattern (declare an `APIRouter`,
register in `app.py`).

## File 3: `prometheus/execution/options_strategy.py` (3,600 lines)

This is the easiest split: each strategy class is independent. Group by
trade structure:

| Strategy classes | Proposed module |
|------------------|-----------------|
| `VixTailHedgeStrategy`, `VixTailHedgeConfig` | `execution/strategies/vix_tail.py` |
| `IronCondorStrategy`, `IronButterflyStrategy` + configs | `execution/strategies/iron_*.py` |
| `ShortPutStrategy`, `WheelStrategy` | `execution/strategies/short_put.py` |
| `BullCallSpreadStrategy`, `MomentumCallStrategy`, `LEAPSStrategy` | `execution/strategies/calls.py` |
| `FuturesOverlayStrategy`, `FuturesOptionStrategy` | `execution/strategies/futures.py` |
| Shared base classes (`OptionStrategy`, `TradeAction`, `TradeDirective`) | `execution/strategies/base.py` |

`execution/options_strategy.py` becomes a re-export module:

```python
from prometheus.execution.strategies.base import OptionStrategy, TradeAction  # noqa
from prometheus.execution.strategies.vix_tail import VixTailHedgeStrategy, VixTailHedgeConfig  # noqa
# ... etc
```

## Estimated effort

- File 1 (`tasks.py`): 1–2 days of focused work, plus full test run.
  Risk: medium (heavy DB-touching code, lots of internal cross-refs).
- File 2 (`api.py`): 1 day. Risk: low (already follows router pattern).
- File 3 (`options_strategy.py`): 1 day. Risk: low (strategies are
  largely independent).

## Ordering recommendation

1. Start with `options_strategy.py` (lowest risk, builds confidence)
2. Then `monitoring/api.py` (low risk, follows existing pattern)
3. Finally `pipeline/tasks.py` (high risk, do last when familiar with
   the migration pattern)

## Acceptance criteria

- `pytest -q` passes before and after each split with identical results
- `python -m compileall -q prometheus` is silent
- `ruff check prometheus` reports the same warnings (or fewer)
- `grep -r "from prometheus.pipeline.tasks import" prometheus/` returns
  the same call sites with the same imported names
