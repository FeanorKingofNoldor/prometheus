"""folio — btop-style live IBKR portfolio monitor (Textual TUI).

Reads an immutable `Snapshot` from `PortfolioModel` on a ~0.7s poll timer and
repaints a btop-analog grid: an account header (Day P&L hero), a plotext NAV
braille graph, block-meter allocation + risk panels, and a sortable/filterable
positions DataTable with per-row sparklines and option greeks.

The model owns ib_insync in its own thread; this app never touches that loop —
it only reads `get_snapshot()`. Read-only throughout.
"""

from __future__ import annotations

import sys

from .model import AccountSnap, PortfolioModel, Snapshot

try:
    import plotext
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.containers import Horizontal, Vertical
    from textual.reactive import reactive
    from textual.widgets import DataTable, Footer, Static
except ImportError as exc:  # pragma: no cover
    sys.stderr.write(
        "folio needs textual + plotext.\n"
        f"Underlying error: {exc}\n"
    )
    raise

# --- btop-ish neon palette --------------------------------------------------
NEON_GREEN = "#39FF14"
NEON_RED = "#FF1F4B"
NEON_CYAN = "#22d3ee"
NEON_MAGENTA = "#e879f9"
NEON_YELLOW = "#FFE600"
NEON_BLUE = "#60a5fa"
DIM = "#8a8f98"
PANEL_BG = "#16171c"
DARK_BG = "#0b0c0e"
TEXT_FG = "#d6dae0"

_BLOCKS = " ▁▂▃▄▅▆▇█"
_BAR_FULL = "▓"
_BAR_EMPTY = "░"


def _money(v: float, width: int = 0) -> str:
    s = f"${v:,.0f}"
    return f"{s:>{width}}" if width else s


def _pnl_color(v: float) -> str:
    if v > 0:
        return NEON_GREEN
    if v < 0:
        return NEON_RED
    return DIM


def _pnl(v: float, fmt: str) -> str:
    return f"[{_pnl_color(v)}]{format(v, fmt)}[/]"


def _sparkline(vals: list[float], width: int = 12) -> str:
    """Hand-rolled 8-level unicode-block sparkline of the last `width` values."""
    pts = [v for v in vals if v == v][-width:]
    if len(pts) < 2:
        return "[dim]·[/]"
    lo, hi = min(pts), max(pts)
    span = hi - lo
    out = []
    for v in pts:
        idx = 0 if span == 0 else int((v - lo) / span * (len(_BLOCKS) - 1))
        out.append(_BLOCKS[idx])
    color = NEON_GREEN if pts[-1] >= pts[0] else NEON_RED
    return f"[{color}]{''.join(out)}[/]"


def _meter(pct: float, width: int, color: str) -> str:
    """A btop-style ▓░ block meter for a 0..100 percentage."""
    pct = max(0.0, min(100.0, pct))
    filled = int(round(pct / 100.0 * width))
    return f"[{color}]{_BAR_FULL * filled}[/][dim]{_BAR_EMPTY * (width - filled)}[/]"


def _age(ts: float) -> str:
    import time
    if not ts:
        return "—"
    s = int(time.time() - ts)
    return f"{s}s" if s < 60 else f"{s // 60}m{s % 60}s"


# ---------------------------------------------------------------------------
# Panels
# ---------------------------------------------------------------------------


class AccountHeader(Static):
    """Top bar: identity + connection/data badges + Day P&L hero + risk fields."""

    DEFAULT_CSS = """
    AccountHeader {
        height: 5;
        border: round #22d3ee;
        background: #16171c;
        color: #d6dae0;
        padding: 0 1;
    }
    """

    snap: reactive[Snapshot | None] = reactive(None)

    def render(self) -> str:
        s = self.snap
        if s is None:
            return "[dim]starting…[/]"
        mode = (f"[black on {NEON_RED}] LIVE [/]" if s.mode == "live"
                else f"[black on {NEON_CYAN}] PAPER [/]")
        if s.connected:
            conn = f"[{NEON_GREEN}]● connected[/]"
        elif s.error:
            conn = f"[{NEON_RED}]● {s.error}[/]"
        else:
            conn = f"[{NEON_YELLOW}]● connecting…[/]"
        data_col = {"live": NEON_GREEN, "delayed": NEON_YELLOW}.get(s.market_data, DIM)
        badge = f"[{data_col}]{s.market_data} data[/]"
        a = s.acct
        hero = (f"[b {_pnl_color(a.day_pnl)}]Day P&L {_money(a.day_pnl)} "
                f"({a.day_pct:+.2f}%)[/]")
        line1 = (f"{mode}  [b {NEON_CYAN}]{s.account or '—'}[/]  {conn}  {badge}"
                 f"   [dim]pos[/] {s.streamed}/{s.total}")
        line2 = (f"[dim]NetLiq[/] [b]{_money(a.net_liq)}[/]   {hero}   "
                 f"[dim]Unreal[/] {_pnl(a.unrealized_pnl, ',.0f')}   "
                 f"[dim]Lev[/] {a.leverage:.2f}x")
        line3 = (f"[dim]BuyPwr[/] {_money(a.buying_power)}   "
                 f"[dim]ExcessLiq[/] {_money(a.excess_liquidity)}   "
                 f"[dim]MaintMrg[/] {_money(a.maint_margin)}   "
                 f"[dim]Cash[/] {_money(a.cash)}")
        return "\n".join([line1, line2, line3])


class PnLPanel(Static):
    """Per-position P&L as horizontal bars (winners green, losers red), sorted by
    impact — far more useful than a flat intraday NAV line (we only have NAV
    since launch, so a line is flat). A slim NAV trend rides on top once the
    session accrues a little history."""

    DEFAULT_CSS = """
    PnLPanel {
        height: 1fr;
        border: round #e879f9;
        background: #16171c;
        color: #d6dae0;
        padding: 0 1;
    }
    """

    snap: reactive[Snapshot | None] = reactive(None)

    def render(self):
        from rich.text import Text
        s = self.snap
        head = f"[b {NEON_MAGENTA}]P&L by position[/]  [dim](unrealized)[/]"
        if s is None or not s.positions:
            return Text.from_markup(head + "\n\n[dim]no positions[/]")
        rows = [r for r in s.positions if r.market_value]
        if not rows:
            return Text.from_markup(head + "\n\n[dim]waiting for prices…[/]")
        size = self.content_size
        width = max(48, size.width or 80)
        height = max(8, size.height or 16)
        lines = [head]
        # slim NAV trend if we've accrued session history
        if s.nav_hist and len(s.nav_hist) >= 2:
            base, cur = s.nav_hist[0], s.nav_hist[-1]
            pct = ((cur - base) / base * 100.0) if base else 0.0
            spark = _sparkline(s.nav_hist, min(40, width - 26))
            lines.append(f"[dim]NAV[/] {spark} [{_pnl_color(cur - base)}]{pct:+.2f}%[/]")
        ranked = sorted(rows, key=lambda r: abs(r.unrealized_pnl), reverse=True)
        rowcap = max(4, height - len(lines) - 2)
        top = ranked[:rowcap]
        maxabs = max((abs(r.unrealized_pnl) for r in top), default=1.0) or 1.0
        barw = max(10, width - 24)
        for r in top:
            pnl = r.unrealized_pnl
            n = int(round(abs(pnl) / maxabs * barw))
            color = _pnl_color(pnl)
            bar = f"[{color}]{'█' * n}[/][dim]{'·' * (barw - n)}[/]"
            lines.append(f"[{NEON_CYAN}]{r.symbol[:6]:<6}[/] {bar} [{color}]{pnl:>+11,.0f}[/]")
        tot = sum(r.unrealized_pnl for r in rows)
        lines.append(f"[dim]total unrealized[/] [{_pnl_color(tot)}]{tot:>+,.0f}[/]")
        return Text.from_markup("\n".join(lines))


class AllocationPanel(Static):
    """Asset-class + sector block meters, long/short split, top-5 concentration."""

    DEFAULT_CSS = """
    AllocationPanel {
        width: 1fr;
        height: 1fr;
        border: round #39FF14;
        background: #16171c;
        color: #d6dae0;
        padding: 0 1;
        overflow-y: auto;
    }
    """

    snap: reactive[Snapshot | None] = reactive(None)

    _COLORS = [NEON_GREEN, NEON_CYAN, NEON_MAGENTA, NEON_YELLOW, NEON_BLUE,
               "#fb923c", "#a3e635", "#f472b6"]

    def render(self) -> str:
        s = self.snap
        lines = [f"[b {NEON_GREEN}]Allocation[/]"]
        if s is None or not s.positions:
            lines.append("[dim]no positions[/]")
            return "\n".join(lines)
        rows = s.positions
        gross = sum(abs(r.market_value) for r in rows) or 1.0

        def _bars(groups: dict[str, float], title: str) -> None:
            lines.append(f"[b {DIM}]{title}[/]")
            ranked = sorted(groups.items(), key=lambda kv: kv[1], reverse=True)
            for i, (name, val) in enumerate(ranked[:6]):
                pct = val / gross * 100.0
                color = self._COLORS[i % len(self._COLORS)]
                lines.append(f"{name[:14]:<14}{_meter(pct, 14, color)} {pct:5.1f}%")

        by_class: dict[str, float] = {}
        for r in rows:
            by_class[r.asset_class] = by_class.get(r.asset_class, 0.0) + abs(r.market_value)
        _bars(by_class, "Asset class")

        by_sector: dict[str, float] = {}
        for r in rows:
            if r.asset_class == "Equity":
                by_sector[r.sector] = by_sector.get(r.sector, 0.0) + abs(r.market_value)
        if by_sector:
            lines.append("")
            _bars(by_sector, "Equity sectors")

        longs = sum(r.market_value for r in rows if r.market_value > 0)
        shorts = sum(-r.market_value for r in rows if r.market_value < 0)
        ls = longs + shorts or 1.0
        lines.append("")
        lines.append(f"[b {DIM}]Long / Short[/]")
        lines.append(f"{'Long':<14}{_meter(longs / ls * 100, 14, NEON_GREEN)} "
                     f"{longs / ls * 100:5.1f}%")
        lines.append(f"{'Short':<14}{_meter(shorts / ls * 100, 14, NEON_RED)} "
                     f"{shorts / ls * 100:5.1f}%")

        top5 = sum(abs(r.market_value) for r in rows[:5]) / gross * 100.0
        lines.append("")
        lines.append(f"[dim]Top-5 concentration[/] [b]{top5:.1f}%[/]")
        return "\n".join(lines)


class RiskPanel(Static):
    """Margin-used meter, leverage gauge, largest-position weight, benchmarks."""

    DEFAULT_CSS = """
    RiskPanel {
        width: 1fr;
        height: 1fr;
        border: round #FFE600;
        background: #16171c;
        color: #d6dae0;
        padding: 0 1;
    }
    """

    snap: reactive[Snapshot | None] = reactive(None)

    def render(self) -> str:
        s = self.snap
        lines = [f"[b {NEON_YELLOW}]Risk[/]"]
        if s is None:
            lines.append("[dim]—[/]")
            return "\n".join(lines)
        a: AccountSnap = s.acct
        margin_pct = (a.maint_margin / a.net_liq * 100.0) if a.net_liq else 0.0
        mcolor = NEON_GREEN if margin_pct < 30 else NEON_YELLOW if margin_pct < 60 else NEON_RED
        lines.append(f"[b {DIM}]Margin used[/]  (maint/NLV)")
        lines.append(f"{_meter(margin_pct, 18, mcolor)} {margin_pct:5.1f}%")

        lev = a.leverage
        lpct = min(100.0, lev / 3.0 * 100.0)  # gauge maxes at 3x
        lcolor = NEON_GREEN if lev < 1.2 else NEON_YELLOW if lev < 2.0 else NEON_RED
        lines.append("")
        lines.append(f"[b {DIM}]Leverage[/]  (gross/NLV, gauge→3x)")
        lines.append(f"{_meter(lpct, 18, lcolor)} [b]{lev:.2f}x[/]")

        lines.append("")
        if s.positions:
            top = max(s.positions, key=lambda r: abs(r.weight))
            wcolor = NEON_GREEN if top.weight < 15 else NEON_YELLOW if top.weight < 30 else NEON_RED
            lines.append(f"[b {DIM}]Largest position[/]")
            lines.append(f"[{NEON_CYAN}]{top.symbol}[/]  "
                         f"{_meter(top.weight, 14, wcolor)} {top.weight:.1f}%")
        else:
            lines.append("[dim]Largest position —[/]")

        lines.append("")
        lines.append(f"[dim]Excess liq[/] {_money(a.excess_liquidity)}")
        lines.append("[dim]benchmarks: —[/]")
        return "\n".join(lines)


class PositionsTable(DataTable):
    """Sortable / filterable positions grid with sparklines + greeks."""

    DEFAULT_CSS = """
    PositionsTable {
        height: 1fr;
        border: round #60a5fa;
        background: #16171c;
        color: #d6dae0;
    }
    PositionsTable > .datatable--header {
        background: #16171c;
        color: #60a5fa;
        text-style: bold;
    }
    PositionsTable > .datatable--cursor {
        background: #243044;
    }
    """

    _COLUMNS = [
        ("SYM", 8), ("TYPE", 5), ("QTY", 9), ("AVG", 9), ("LAST", 9),
        ("MKT VAL", 13), ("UPL $", 11), ("UPL %", 8), ("DAY %", 8),
        ("WT%", 6), ("CHART", 14), ("Δ/Θ/V", 16),
    ]

    def on_mount(self) -> None:
        self.cursor_type = "row"
        self.zebra_stripes = True
        for name, width in self._COLUMNS:
            self.add_column(name, width=width, key=name)

    def update_rows(self, snap: Snapshot, sort_key: str, filt: str) -> None:
        rows = snap.positions
        if filt:
            f = filt.lower()
            rows = [r for r in rows if f in r.symbol.lower()]
        rows = _sort_rows(rows, sort_key)
        self.clear()
        from rich.text import Text
        for r in rows:
            upl_pct = (r.unrealized_pnl / abs(r.market_value) * 100.0) if r.market_value else 0.0
            if r.delta is not None:
                greeks = (f"[{NEON_CYAN}]{r.delta:+.2f}/"
                          f"{(r.theta or 0):+.2f}/{(r.vega or 0):.2f}[/]")
            else:
                greeks = "[dim]—[/]"
            self.add_row(
                Text.from_markup(f"[b {NEON_CYAN}]{r.symbol[:8]}[/]"),
                r.sec_type,
                Text.from_markup(f"{r.qty:,.0f}", justify="right"),
                Text.from_markup(f"{r.avg_cost:,.2f}", justify="right"),
                Text.from_markup(f"{r.last:,.2f}", justify="right"),
                Text.from_markup(f"{r.market_value:,.0f}", justify="right"),
                Text.from_markup(_pnl(r.unrealized_pnl, ',.0f'), justify="right"),
                Text.from_markup(_pnl(upl_pct, '+.2f'), justify="right"),
                Text.from_markup(_pnl(r.day_pct, '+.2f'), justify="right"),
                Text.from_markup(f"{r.weight:.1f}", justify="right"),
                Text.from_markup(_sparkline(r.price_hist)),
                Text.from_markup(greeks),
            )


_SORTS = ["MKT VAL", "UPL $", "DAY %", "WT%", "SYM"]


def _sort_rows(rows, key):
    if key == "SYM":
        return sorted(rows, key=lambda r: r.symbol)
    field = {
        "MKT VAL": lambda r: abs(r.market_value),
        "UPL $": lambda r: r.unrealized_pnl,
        "DAY %": lambda r: r.day_pct,
        "WT%": lambda r: r.weight,
    }[key]
    return sorted(rows, key=field, reverse=True)


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------


class FolioApp(App):
    """btop-style live IBKR portfolio monitor."""

    TITLE = "folio"
    CSS = """
    Screen {
        background: #0b0c0e;
        color: #d6dae0;
    }
    #top_row {
        height: 1fr;
    }
    #meters_row {
        height: 1fr;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "quit"),
        Binding("s", "cycle_sort", "sort"),
        Binding("slash", "filter", "filter"),
        Binding("a", "switch_account", "switch acct"),
        Binding("p", "toggle_pause", "pause"),
        Binding("escape", "clear_filter", "clear", show=False),
    ]

    def __init__(self, mode: str = "paper") -> None:
        super().__init__()
        self._mode = mode
        self.model = PortfolioModel(mode=mode)
        self._sort_idx = 0
        self._filter = ""
        self._paused = False
        self._timer = None

    def compose(self) -> ComposeResult:
        yield AccountHeader(id="header")
        with Horizontal(id="top_row"):
            yield PnLPanel(id="nav")
        with Horizontal(id="meters_row"):
            yield AllocationPanel(id="alloc")
            yield RiskPanel(id="risk")
        yield PositionsTable(id="positions")
        yield Footer()

    def on_mount(self) -> None:
        self.model.start()
        self._timer = self.set_interval(0.7, self._refresh)
        self._refresh()

    def on_unmount(self) -> None:
        try:
            self.model.stop()
        except Exception:
            pass

    # ----- refresh ------------------------------------------------------

    def _refresh(self) -> None:
        if self._paused:
            return
        snap = self.model.get_snapshot()
        self.query_one("#header", AccountHeader).snap = snap
        self.query_one("#nav", PnLPanel).snap = snap
        self.query_one("#alloc", AllocationPanel).snap = snap
        self.query_one("#risk", RiskPanel).snap = snap
        self.query_one("#positions", PositionsTable).update_rows(
            snap, _SORTS[self._sort_idx], self._filter)
        self._update_subtitle(snap)

    def _update_subtitle(self, snap: Snapshot) -> None:
        conn = "connected" if snap.connected else ("error" if snap.error else "connecting")
        pause = " · [PAUSED]" if self._paused else ""
        flt = f" · /{self._filter}" if self._filter else ""
        self.sub_title = (f"{snap.mode} · {conn} · sort:{_SORTS[self._sort_idx]} · "
                          f"upd {_age(snap.last_update)} ago{flt}{pause}")

    # ----- actions ------------------------------------------------------

    def action_cycle_sort(self) -> None:
        self._sort_idx = (self._sort_idx + 1) % len(_SORTS)
        self._refresh()

    def action_toggle_pause(self) -> None:
        self._paused = not self._paused
        self._update_subtitle(self.model.get_snapshot())

    def action_switch_account(self) -> None:
        new_mode = "live" if self.model.mode == "paper" else "paper"
        self.model.switch(new_mode)
        self._mode = new_mode
        self._refresh()

    def action_filter(self) -> None:
        self._prompt_filter()

    def action_clear_filter(self) -> None:
        if self._filter:
            self._filter = ""
            self._refresh()

    def _prompt_filter(self) -> None:
        from textual.widgets import Input

        if self.query("#filter_input"):
            return

        inp = Input(placeholder="filter symbol… (Enter apply · Esc cancel)",
                    id="filter_input", value=self._filter)
        inp.styles.dock = "bottom"
        inp.styles.border = ("round", NEON_BLUE)
        self.mount(inp)
        inp.focus()

    def on_input_submitted(self, event) -> None:
        if event.input.id == "filter_input":
            self._filter = event.value.strip()
            event.input.remove()
            self._refresh()

    def on_input_changed(self, event) -> None:
        if event.input.id == "filter_input":
            self._filter = event.value.strip()
            self._refresh()


def run(mode: str) -> int:
    FolioApp(mode=mode).run()
    return 0
