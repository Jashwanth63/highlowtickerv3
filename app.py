#!/usr/bin/env python3
"""
HighLow TUI: terminal UI for session highs/lows with multi-broker support.
Run from highlow-tui directory: python app.py
"""
import asyncio
from collections import deque
import json
import math
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import os, certifi
os.environ.setdefault("SSL_CERT_FILE", certifi.where())
os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())

# Ensure project root and core are on path
_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / 'core'))

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.widgets import DataTable, Static, Header, Footer, Button
from textual.screen import Screen
from rich.text import Text
from rich.style import Style

from brokers.base import BrokerType
from brokers.manager import BrokerManager, PersistedTickerData
from providers._subscription import wall_clock_counts

MAX_TABLE_ROWS = 50
RATE_BAR_WIDTH = 18
RATE_TIMEFRAMES = ["20m", "5m", "1m", "30s"]
HISTORY_POINTS = 48
SIGNAL_LOG_LIMIT = 24
EASTERN_TZ = ZoneInfo("America/New_York")

BROKER_DISPLAY = {
    BrokerType.YAHOO: "Yahoo",
    BrokerType.IBKR: "IBKR",
    BrokerType.TOS: "TOS",
}
BROKER_CYCLE = [BrokerType.IBKR, BrokerType.TOS, BrokerType.YAHOO]


def make_bar(value: float, max_val: float, width: int = RATE_BAR_WIDTH, reverse: bool = False) -> str:
    filled = min(int(value / max_val * width), width) if max_val > 0 else 0
    bar = "█" * filled + "░" * (width - filled)
    return bar[::-1] if reverse else bar


HIGHLIGHT_STYLES = {
    "flash_high": Style(),
    "flash_low": Style(),
    "week52_high": Style(color="white", bgcolor="rgb(20,83,45)"),
    "week52_low": Style(color="white", bgcolor="rgb(127,29,29)"),
    "yellow": Style(color="black", bgcolor="yellow"),
    "orange": Style(color="black", bgcolor="orange1"),
    "purple":       Style(color="white", bgcolor="purple"),
    "volume_spike": Style(color="black", bgcolor="rgb(244,114,182)"),
    "default":      Style(),
}


def load_highlight_config():
    path = _ROOT / "config" / "highlight.json"
    default = {
        "thresholds": {
            "consecutiveCount": 1,
            "significantPercentChange": 0.5,
            "volumeSpikeRatio": 2.0,
            "volumeSpikeWindow": 60,
        },
        "colors": {},
    }
    if not path.exists():
        return default
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return default


def save_highlight_config(config):
    path = _ROOT / "config" / "highlight.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(config, f, indent=2)


def safe_pct(value, default: float = 0.0) -> float:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return default
    return value if math.isfinite(value) else default


def safe_ratio(numerator: float, denominator: float, default: float = 0.0) -> float:
    denominator = safe_pct(denominator)
    if denominator == 0.0:
        return default
    return safe_pct(numerator) / denominator


def compute_signal_state(high_counts: dict, low_counts: dict, tracked_count: int = 0, volume_spike_count: int = 0) -> dict:
    def imbalance(tf: str) -> float:
        highs = int(high_counts.get(tf, 0) or 0)
        lows = int(low_counts.get(tf, 0) or 0)
        total = highs + lows
        return (highs - lows) / total if total else 0.0

    imbalance_20m = imbalance("20m")
    imbalance_5m = imbalance("5m")
    imbalance_1m = imbalance("1m")
    imbalance_30s = imbalance("30s")
    impulse = imbalance_1m - imbalance_20m
    expansion_1m = int(high_counts.get("1m", 0) or 0) + int(low_counts.get("1m", 0) or 0)
    universe = max(int(tracked_count or 0), 1)
    expansion_rate = expansion_1m / universe
    volume_breadth = min(safe_ratio(volume_spike_count, universe), 1.0)

    score = (
        0.40 * imbalance_1m +
        0.30 * imbalance_5m +
        0.20 * impulse +
        0.10 * (volume_breadth if imbalance_1m >= 0 else -volume_breadth)
    )

    if expansion_1m < 3:
        regime = "Idle"
    elif score >= 0.35 and imbalance_20m >= 0:
        regime = "Bull Trend"
    elif score <= -0.35 and imbalance_20m <= 0:
        regime = "Bear Trend"
    elif impulse >= 0.30 and imbalance_30s > 0:
        regime = "Bull Reversal"
    elif impulse <= -0.30 and imbalance_30s < 0:
        regime = "Bear Reversal"
    elif abs(score) >= 0.20:
        regime = "Directional"
    else:
        regime = "Chop"

    return {
        "score": score,
        "regime": regime,
        "imbalance_20m": imbalance_20m,
        "imbalance_5m": imbalance_5m,
        "imbalance_1m": imbalance_1m,
        "imbalance_30s": imbalance_30s,
        "impulse": impulse,
        "expansion_1m": expansion_1m,
        "expansion_rate": expansion_rate,
        "volume_breadth": volume_breadth,
        "ratio_1m": safe_ratio(high_counts.get("1m", 0) or 0, max(int(low_counts.get("1m", 0) or 0), 1), default=0.0),
    }


def render_signal_summary(signal: dict) -> str:
    score = signal["score"]
    regime = signal["regime"]
    regime_style = "green" if score > 0.1 else "red" if score < -0.1 else "yellow"
    score_style = "green" if score > 0.1 else "red" if score < -0.1 else "white"
    return (
        f"[bold]Signal[/] "
        f"[{regime_style}]{regime}[/{regime_style}]  "
        f"[dim]Score[/] [{score_style}]{score:+.2f}[/{score_style}]  "
        f"[dim]1m I[/] {signal['imbalance_1m']:+.2f}  "
        f"[dim]20m I[/] {signal['imbalance_20m']:+.2f}  "
        f"[dim]Impulse[/] {signal['impulse']:+.2f}  "
        f"[dim]1m Ratio[/] {signal['ratio_1m']:.2f}  "
        f"[dim]Exp[/] {signal['expansion_1m']}  "
        f"[dim]Vol[/] {signal['volume_breadth']:.2f}"
    )


def render_sparkline(values) -> str:
    ticks = " .:-=+*#%@"
    vals = [max(0.0, safe_pct(v)) for v in values]
    if not vals:
        return ""
    max_val = max(vals)
    if max_val <= 0:
        return ticks[0] * len(vals)
    last_idx = len(ticks) - 1
    return "".join(ticks[min(last_idx, int(round((value / max_val) * last_idx)))] for value in vals)


def render_net_sparkline(values) -> str:
    ticks = "▁▂▃▄▅▆▇█"
    vals = [int(safe_pct(v)) for v in values]
    if not vals:
        return ""
    max_abs = max((abs(v) for v in vals), default=0)
    if max_abs == 0:
        return "·" * len(vals)

    chars = []
    last_idx = len(ticks) - 1
    for value in vals:
        if value == 0:
            chars.append("·")
            continue
        idx = min(last_idx, int(round((abs(value) / max_abs) * last_idx)))
        chars.append(ticks[idx])
    return "".join(chars)


def orient_net_sparkline(sparkline: str, net_last: int) -> str:
    return sparkline[::-1] if net_last < 0 else sparkline


def render_history_chart(history: dict) -> str:
    lines = ["[bold]Net Breadth History[/] [dim](highs-lows)[/]"]
    for tf in RATE_TIMEFRAMES:
        series = history.get(tf, {})
        highs = list(series.get("highs", ()))
        lows = list(series.get("lows", ()))
        h_last = int(highs[-1]) if highs else 0
        l_last = int(lows[-1]) if lows else 0
        net_series = [int(h) - int(l) for h, l in zip(highs, lows)]
        net_last = h_last - l_last
        net_style = "green" if net_last > 0 else "red" if net_last < 0 else "white"
        sparkline = orient_net_sparkline(render_net_sparkline(net_series), net_last)
        lines.append(
            f"[dim]{tf:>3}[/dim] "
            f"[{net_style}]{sparkline}[/{net_style}] "
            f"[dim]Net[/dim] [{net_style}]{net_last:+d}[/{net_style}] "
            f"[dim]({h_last}/{l_last})[/dim]"
        )
    return "\n".join(lines)


def format_signal_log_entry(ts: float, signal: dict) -> str:
    stamp = datetime.fromtimestamp(ts, EASTERN_TZ).strftime("%H:%M:%S")
    return f"{stamp}  {signal['regime']}  {signal['score']:+.2f}"


def render_signal_history(entries) -> str:
    lines = ["[bold]Signal History[/]"]
    if not entries:
        lines.append("[dim]Waiting for signal changes...[/dim]")
        return "\n".join(lines)
    lines.extend(entries)
    return "\n".join(lines)


def compute_highlights(data, is_highs, week52_set, thresholds, suppress_yellow=False, volume_spikes=None):
    """Return highlight type for every entry in O(n) — no per-row scanning."""
    if not data:
        return []
    volume_spikes = volume_spikes or set()
    n = len(data)
    consec_threshold = thresholds.get("consecutiveCount", 1) + 1
    sig_pct = thresholds.get("significantPercentChange", 0.5)
    flash_type = "flash_high" if is_highs else "flash_low"
    week52_type = "week52_high" if is_highs else "week52_low"

    run_end = [1] * n
    for i in range(1, n):
        if data[i]["symbol"] == data[i - 1]["symbol"]:
            run_end[i] = run_end[i - 1] + 1
    run_size = list(run_end)
    for i in range(n - 2, -1, -1):
        if data[i]["symbol"] == data[i + 1]["symbol"]:
            run_size[i] = run_size[i + 1]

    highlights = []
    last_pct: dict = {}
    for i, e in enumerate(data):
        sym = e["symbol"]
        pct = safe_pct(e.get("percentChange"))
        if i == 0:
            h = flash_type
        elif sym in week52_set:
            h = week52_type
        elif e.get("count") == 1 and not suppress_yellow:
            h = "yellow"
        elif run_size[i] >= consec_threshold:
            h = "orange"
        elif sym in last_pct and abs(pct - last_pct[sym]) > sig_pct:
            h = "purple"
        elif sym in volume_spikes:
            h = "volume_spike"
        else:
            h = "default"
        highlights.append(h)
        last_pct[sym] = pct
    return highlights


class HighLowTUI(App):
    CSS = """
    Screen {
        layout: vertical;
    }
    #header-row {
        height: 1;
        padding: 0 1;
        layout: horizontal;
    }
    #app-title {
        width: 1fr;
    }
    #rate-bars {
        height: auto;
        padding: 0 0;
        margin: 0 0;
    }
    #signal-bar {
        height: 1;
        padding: 0 0;
        margin: 0 0 1 0;
    }
    #history-chart {
        height: auto;
        padding: 0 0;
        margin: 0 0 1 0;
    }
    #main-content {
        height: 1fr;
        layout: horizontal;
    }
    #left-panels {
        width: 1fr;
        height: 1fr;
        layout: vertical;
    }
    #tables-container {
        height: 1fr;
        layout: horizontal;
    }
    .table-box {
        width: 1fr;
        height: 1fr;
        border: solid cyan;
        margin: 0 0;
    }
    #signal-history-box {
        width: 28;
        height: 1fr;
        border: solid cyan;
        margin: 0 0 0 1;
    }
    #watchlist-box {
        width: 40;
        height: 1fr;
        border: solid magenta;
        margin: 0 0 0 1;
    }
    #watchlist-header {
        height: 1;
        padding: 0 1;
    }
    #watchlist-scroll {
        height: 1fr;
    }
    #watchlist-table {
        height: auto;
    }
    DataTable {
        height: 1fr;
    }
    #connection-status {
        width: auto;
        text-align: right;
    }
    #broker-toggle {
        width: auto;
        padding: 0 2;
    }
    """

    BINDINGS = [
        ("s", "settings", "Settings"),
        ("q", "quit", "Quit"),
        ("b", "cycle_broker", "Broker"),
        ("d", "disconnect_broker", "Disconnect"),
    ]

    def __init__(
        self,
        broker_manager: BrokerManager,
        initial_broker: BrokerType = BrokerType.YAHOO,
        signal_emitter=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._broker_manager = broker_manager
        self._initial_broker = initial_broker
        self._signal_emitter = signal_emitter
        self._tracked_symbols = list(broker_manager.symbols)
        self.last_state = {}
        self.current_prices = {}
        self.count_history = {
            tf: {
                "highs": deque(maxlen=HISTORY_POINTS),
                "lows": deque(maxlen=HISTORY_POINTS),
            }
            for tf in RATE_TIMEFRAMES
        }
        self.signal_history = deque(maxlen=SIGNAL_LOG_LIMIT)
        self._last_logged_regime = None
        self.week52_highs: set = set()
        self.week52_lows: set = set()
        self.volume_spikes: set = set()
        self.connection_status = "connecting"
        self.last_update_time = None
        self.highlight_config = load_highlight_config()
        self._w_status = None
        self._w_rate_bars = None
        self._w_signal_bar = None
        self._w_history_chart = None
        self._w_highs = None
        self._w_lows = None
        self._w_signal_history = None
        self._w_broker_toggle = None
        self._w_lows_label = None
        self._w_highs_label = None
        self._w_watchlist = None
        self._w_watchlist_header = None
        self._stream_task = None
        self._start_time = time.time()
        self._last_refresh_time = 0.0  # throttle UI refreshes
        self._MIN_REFRESH_INTERVAL = 0.25  # seconds — cap at 4 fps

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="header-row"):
            yield Static("[bold]HighLow TUI[/]  [dim]s: Settings  b: Broker  d: Disconnect  q: Quit[/]", id="app-title")
            yield Static(self._broker_toggle_label(), id="broker-toggle")
            yield Static("● connecting", id="connection-status")
        yield Static(id="rate-bars")
        yield Static("", id="signal-bar")
        yield Static("", id="history-chart")
        with Horizontal(id="main-content"):
            with Vertical(id="left-panels"):
                with Horizontal(id="tables-container"):
                    with Vertical(classes="table-box"):
                        yield Static("Session new lows", id="lows-label")
                        yield DataTable(id="lows-table", cursor_type="row", zebra_stripes=True)
                    with Vertical(classes="table-box"):
                        yield Static("Session new highs", id="highs-label")
                        yield DataTable(id="highs-table", cursor_type="row", zebra_stripes=True)
                    with Vertical(id="signal-history-box"):
                        yield Static("", id="signal-history")
            with Vertical(id="watchlist-box"):
                yield Static("[bold magenta]Watchlist[/]", id="watchlist-header")
                with VerticalScroll(id="watchlist-scroll"):
                    yield DataTable(id="watchlist-table", cursor_type="row", zebra_stripes=True)
        yield Footer()

    def _broker_toggle_label(self) -> str:
        active = self._broker_manager.active_broker
        parts = []
        for bt in BROKER_CYCLE:
            name = BROKER_DISPLAY[bt]
            if bt == active:
                parts.append(f"[bold cyan][{name}][/]")
            else:
                parts.append(f"[dim]{name}[/]")
        return "  ".join(parts)

    def on_mount(self) -> None:
        self._w_status = self.query_one("#connection-status", Static)
        self._w_rate_bars = self.query_one("#rate-bars", Static)
        self._w_signal_bar = self.query_one("#signal-bar", Static)
        self._w_history_chart = self.query_one("#history-chart", Static)
        self._w_highs = self.query_one("#highs-table", DataTable)
        self._w_lows = self.query_one("#lows-table", DataTable)
        self._w_signal_history = self.query_one("#signal-history", Static)
        self._w_lows_label = self.query_one("#lows-label", Static)
        self._w_highs_label = self.query_one("#highs-label", Static)
        self._w_broker_toggle = self.query_one("#broker-toggle", Static)
        self._w_watchlist = self.query_one("#watchlist-table", DataTable)
        self._w_watchlist_header = self.query_one("#watchlist-header", Static)

        for table in (self._w_highs, self._w_lows):
            table.add_column("Symbol", width=6)
            table.add_column("Count", width=5)
            table.add_column("Price", width=9)
            table.add_column("% Chg", width=8)

        # Watchlist columns: Symbol | Price | SessHi | SessLo | % Chg
        self._w_watchlist.add_column("Sym", width=6)
        self._w_watchlist.add_column("Price", width=9)
        self._w_watchlist.add_column("Hi%", width=7)
        self._w_watchlist.add_column("Lo%", width=7)
        self._w_watchlist.add_column("% Chg", width=7)

        if self._signal_emitter:
            asyncio.create_task(self._signal_emitter.start())
        self._stream_task = asyncio.create_task(self._connect_and_stream())

    async def _connect_and_stream(self) -> None:
        """Connect to the initial broker and start streaming."""
        try:
            await self._broker_manager.connect(self._initial_broker)
            self.connection_status = "connected"
            self._refresh_status()
            await self._data_loop()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.connection_status = f"error: {e}"
            self._refresh_status()

    async def _data_loop(self) -> None:
        try:
            async for update in self._broker_manager.stream():
                self._apply_market_update(update)
                self.last_update_time = time.time()
                # Throttle UI refreshes to avoid freezing the event loop
                now = time.time()
                if now - self._last_refresh_time >= self._MIN_REFRESH_INTERVAL:
                    self._refresh_ui()
                    self._last_refresh_time = now
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.connection_status = f"error: {e}"
            self._refresh_status()

    def _apply_market_update(self, update):
        """Process a MarketUpdate — snapshots are already merged into
        BrokerManager.state by the time we get here. We just need to
        update the lightweight UI-side dicts used by rate bars / signals.
        """
        if not update or not update.snapshots:
            return

        state = self._broker_manager.state
        current_prices = {}
        percent_change = {}

        for snap in update.snapshots:
            if not snap.valid:
                continue
            current_prices[snap.symbol] = snap.price
            percent_change[snap.symbol] = snap.percent_change
            # 52-week tracking
            if snap.week52_high is not None and snap.session_high >= snap.week52_high:
                self.week52_highs.add(snap.symbol)
            if snap.week52_low is not None and snap.session_low <= snap.week52_low:
                self.week52_lows.add(snap.symbol)

        self.current_prices.update(current_prices)
        if current_prices:
            self._tracked_symbols = list(self.current_prices.keys())

        # Build last_state dict consumed by rate bars / signal computation
        self.last_state = {
            "highCounts": wall_clock_counts(state.high_timestamps),
            "lowCounts": wall_clock_counts(state.low_timestamps),
            "percentChange": percent_change,
            "currentPrices": current_prices,
            "indexPrices": {
                "SPY": self.current_prices.get("SPY", 0.0),
                "DIA": self.current_prices.get("DIA", 0.0),
                "QQQ": self.current_prices.get("QQQ", 0.0),
            },
        }

    def _build_session_entries(self, side: str) -> list:
        """Build sorted session-high or session-low entries directly from
        PersistedState. No delta tracking needed — the source of truth is
        BrokerManager.state.
        """
        entries = []
        for td in self._broker_manager.state.all_tickers():
            count = td.high_count if side == "high" else td.low_count
            if count <= 0 or not td.valid:
                continue
            price = td.session_high if side == "high" else td.session_low
            entries.append({
                "symbol": td.symbol,
                "count": count,
                "price": price,
                "percentChange": td.percent_change,
            })
        # Sort by count descending, then symbol ascending
        entries.sort(key=lambda e: (-e["count"], e["symbol"]))
        return entries[:MAX_TABLE_ROWS]

    def _refresh_status(self):
        dot = "[green]●[/green]" if self.connection_status == "connected" else "[red]●[/red]"
        meta = self._broker_manager.get_metadata()
        name = meta["name"]
        self._w_status.update(f"{dot} [dim]{name}[/dim]  {self.connection_status}")
        if self._w_broker_toggle:
            self._w_broker_toggle.update(self._broker_toggle_label())

    def _record_count_history(self, high_counts: dict, low_counts: dict) -> None:
        for tf in RATE_TIMEFRAMES:
            self.count_history[tf]["highs"].append(int(high_counts.get(tf, 0) or 0))
            self.count_history[tf]["lows"].append(int(low_counts.get(tf, 0) or 0))

    def _record_signal_change(self, signal: dict, ts: float | None = None) -> None:
        regime = signal.get("regime")
        if not regime or regime == self._last_logged_regime:
            return
        ts = ts or time.time()
        self.signal_history.appendleft(format_signal_log_entry(ts, signal))
        self._last_logged_regime = regime

    @staticmethod
    def _render_rate_bars(high_counts: dict, low_counts: dict, width: int) -> str:
        max_val = max(
            max((high_counts.get(t, 0) for t in RATE_TIMEFRAMES), default=1),
            max((low_counts.get(t, 0) for t in RATE_TIMEFRAMES), default=1),
            1,
        )
        bar_w = max(4, (width - 13) // 2)
        lines = [f"[dim]{'Lows':>{bar_w + 4}}  Highs[/dim]"]
        for tf in RATE_TIMEFRAMES:
            lc = low_counts.get(tf, 0)
            hc = high_counts.get(tf, 0)
            l_bar = make_bar(lc, max_val, bar_w, reverse=True)
            h_bar = make_bar(hc, max_val, bar_w)
            lines.append(
                f"[dim]{lc:>3d}[/dim] [red]{l_bar}[/red] [dim]{tf:>3s}[/dim] [green]{h_bar}[/green] [dim]{hc:<3d}[/dim]"
            )
        return "\n".join(lines)

    @staticmethod
    def _build_session_rows(entries, is_highs, week52_set, thresholds, prefix, suppress_yellow=False, volume_spikes=None):
        """Build list of (key, cells) for session-high or session-low entries."""
        highlights = compute_highlights(entries, is_highs, week52_set, thresholds, suppress_yellow=suppress_yellow, volume_spikes=volume_spikes)
        rows = []
        for i, (e, h) in enumerate(zip(entries, highlights)):
            style = HIGHLIGHT_STYLES.get(h, HIGHLIGHT_STYLES["default"])
            pct = safe_pct(e.get("percentChange"))
            sign = "+" if pct >= 0 else ""
            pct_style = style + Style(color="green" if pct >= 0 else "red")
            key = f"{prefix}_{e['symbol']}"
            cells = (
                Text(f"{e['symbol']:<6}", style=style),
                Text(f"{e['count']:>5}", style=style),
                Text(f"{e.get('price', 0):>9.2f}", style=style),
                Text(f"{sign}{pct:.2f}%".rjust(8), style=pct_style),
            )
            rows.append((key, cells))
        return rows

    @staticmethod
    def _sync_table(table: DataTable, rows):
        """Rebuild a DataTable while preserving the user's scroll position.

        Saves scroll_y, clears, re-adds rows in the correct sorted order,
        then restores scroll_y. This is the only reliable way to keep the
        visual order consistent with the data order.
        """
        # Save scroll position
        saved_y = table.scroll_y

        table.clear()
        for key, cells in rows:
            table.add_row(*cells, key=key)

        # Restore scroll position (clamped by Textual automatically)
        if saved_y > 0:
            table.scroll_y = saved_y

    def _build_snapshot_entries(self):
        percent_change = self.last_state.get("percentChange") or {}
        entries = []
        for sym in self._tracked_symbols:
            price = self.current_prices.get(sym)
            if price is None:
                continue
            pct = safe_pct(percent_change.get(sym, 0.0))
            entries.append({
                "symbol": sym,
                "count": 0,
                "price": price,
                "percentChange": pct,
            })
        losers = sorted(entries, key=lambda e: (e["percentChange"], e["symbol"]))[:MAX_TABLE_ROWS]
        gainers = sorted(entries, key=lambda e: (-e["percentChange"], e["symbol"]))[:MAX_TABLE_ROWS]
        return losers, gainers

    @staticmethod
    def _build_snapshot_table(table: DataTable, entries, prefix: str):
        rows = []
        for e in entries:
            pct = safe_pct(e.get("percentChange"))
            price_style = Style()
            pct_style = Style(color="green" if pct >= 0 else "red")
            key = f"{prefix}_{e['symbol']}"
            cells = (
                Text(f"{e['symbol']:<6}"),
                Text("    -"),
                Text(f"{e.get('price', 0):>9.2f}", style=price_style),
                Text(f"{pct:+.2f}%".rjust(8), style=pct_style),
            )
            rows.append((key, cells))
        HighLowTUI._sync_table(table, rows)

    @staticmethod
    def _is_equity_market_session(now: datetime | None = None) -> bool:
        now = now or datetime.now(EASTERN_TZ)
        if now.weekday() >= 5:
            return False
        minutes = now.hour * 60 + now.minute
        return 4 * 60 <= minutes < 20 * 60

    def _has_fresh_quotes(self) -> bool:
        if not self.current_prices or self.last_update_time is None:
            return False
        meta = self._broker_manager.get_metadata()
        refresh_rate = float(meta.get("refresh_rate", 0.0) or 0.0)
        freshness_window = max(30.0, refresh_rate * 2.5) if refresh_rate > 0 else 30.0
        return (time.time() - self.last_update_time) <= freshness_window

    def _has_session_activity(self) -> bool:
        """True if any ticker has recorded a new high or low this session."""
        for td in self._broker_manager.state.all_tickers():
            if td.high_count > 0 or td.low_count > 0:
                return True
        return False

    def _should_show_snapshot(self) -> bool:
        if self._has_session_activity() or not self.current_prices:
            return False
        meta = self._broker_manager.get_metadata()
        if meta.get("is_realtime"):
            return not self._has_fresh_quotes()
        return not (self._has_fresh_quotes() and self._is_equity_market_session())

    def _refresh_watchlist(self) -> None:
        """Update the watchlist panel in-place to preserve scroll position."""
        watchlist_data = self._broker_manager.get_watchlist_data()

        rows = []
        for td in watchlist_data:
            key = f"wl_{td.symbol}"
            if not td.valid or td.price <= 0:
                cells = (
                    Text(f"{td.symbol:<6}", style=Style(dim=True)),
                    Text("     NA", style=Style(dim=True)),
                    Text("    NA", style=Style(dim=True)),
                    Text("    NA", style=Style(dim=True)),
                    Text("    NA", style=Style(dim=True)),
                )
            else:
                open_price = td.open_price if td.open_price > 0 else td.price
                hi_pct = round((td.session_high - open_price) / open_price * 100, 2) if open_price > 0 else 0.0
                lo_pct = round((td.session_low - open_price) / open_price * 100, 2) if open_price > 0 else 0.0
                chg_pct = td.percent_change

                price_style = Style(color="green" if chg_pct >= 0 else "red")
                hi_style = Style(color="green")
                lo_style = Style(color="red")
                chg_style = Style(color="green" if chg_pct >= 0 else "red")

                cells = (
                    Text(f"{td.symbol:<6}"),
                    Text(f"{td.price:>8.2f}", style=price_style),
                    Text(f"{hi_pct:+.2f}%", style=hi_style),
                    Text(f"{lo_pct:+.2f}%", style=lo_style),
                    Text(f"{chg_pct:+.1f}%", style=chg_style),
                )
            rows.append((key, cells))

        self._sync_table(self._w_watchlist, rows)

        count = len(watchlist_data)
        broker_name = self._broker_manager.get_metadata()["name"]
        self._w_watchlist_header.update(
            f"[bold magenta]Watchlist[/] [dim]({count} tickers · {broker_name})[/]"
        )

    def _refresh_ui(self):
        self._refresh_status()
        state = self._broker_manager.state
        high_counts = wall_clock_counts(state.high_timestamps)
        low_counts = wall_clock_counts(state.low_timestamps)
        self._record_count_history(high_counts, low_counts)
        bar_width = self._w_rate_bars.size.width or 80
        self._w_rate_bars.update(self._render_rate_bars(high_counts, low_counts, bar_width))
        signal = compute_signal_state(
            high_counts,
            low_counts,
            tracked_count=len(self._tracked_symbols),
            volume_spike_count=len(self.volume_spikes),
        )
        self._record_signal_change(signal, self.last_update_time or time.time())
        self._w_signal_bar.update(render_signal_summary(signal))
        self._w_history_chart.update(render_history_chart(self.count_history))
        self._w_signal_history.update(render_signal_history(self.signal_history))

        # Route signal to external consumers (NinjaTrader, etc.)
        if self._signal_emitter:
            self._signal_emitter.emit(signal)

        thresholds = self.highlight_config.get("thresholds", {})

        # Update watchlist
        self._refresh_watchlist()

        suppress = time.time() - self._start_time < 300
        show_snapshot = self._should_show_snapshot()
        if show_snapshot:
            losers, gainers = self._build_snapshot_entries()
            self._w_lows_label.update("Watchlist snapshot: biggest losers")
            self._w_highs_label.update("Watchlist snapshot: biggest gainers")
            self._build_snapshot_table(self._w_lows, losers, "snapshot_l")
            self._build_snapshot_table(self._w_highs, gainers, "snapshot_h")
            return

        self._w_lows_label.update("Session new lows")
        self._w_highs_label.update("Session new highs")

        # Build session tables directly from PersistedState — always sorted
        high_entries = self._build_session_entries("high")
        high_rows = self._build_session_rows(high_entries, True, self.week52_highs, thresholds, "h", suppress_yellow=suppress, volume_spikes=self.volume_spikes)
        self._sync_table(self._w_highs, high_rows)

        low_entries = self._build_session_entries("low")
        low_rows = self._build_session_rows(low_entries, False, self.week52_lows, thresholds, "l", suppress_yellow=suppress, volume_spikes=self.volume_spikes)
        self._sync_table(self._w_lows, low_rows)

    def action_settings(self) -> None:
        self.push_screen(SettingsScreen(self.highlight_config, self._on_settings_save))

    def _on_settings_save(self, config):
        self.highlight_config = config
        save_highlight_config(config)
        self._refresh_ui()

    async def action_cycle_broker(self) -> None:
        """Cycle to the next broker in the list."""
        current = self._broker_manager.active_broker
        if current is None:
            next_broker = self._initial_broker
        else:
            try:
                idx = BROKER_CYCLE.index(current)
                next_broker = BROKER_CYCLE[(idx + 1) % len(BROKER_CYCLE)]
            except ValueError:
                next_broker = BROKER_CYCLE[0]

        await self._switch_to_broker(next_broker)

    async def action_disconnect_broker(self) -> None:
        """Disconnect from the current broker (data persisted)."""
        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass
            self._stream_task = None

        await self._broker_manager.disconnect()
        self.connection_status = "disconnected"
        self._refresh_status()

    async def _switch_to_broker(self, broker_type: BrokerType) -> None:
        """Switch broker: cancel stream → disconnect → reconnect → restart."""
        # Cancel active stream
        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass
            self._stream_task = None

        self.connection_status = "switching..."
        self._refresh_status()

        # Switch broker (data persists in BrokerManager.state)
        try:
            await self._broker_manager.switch_broker(broker_type)
            self.connection_status = "connected"
            self._refresh_status()
            self._stream_task = asyncio.create_task(self._data_loop())
        except Exception as e:
            self.connection_status = f"error: {e}"
            self._refresh_status()


class SettingsScreen(Screen):
    def __init__(self, initial_config, on_save, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.initial_config = initial_config
        self.on_save_cb = on_save

    def compose(self) -> ComposeResult:
        t = self.initial_config.get("thresholds", {})
        yield Static("[bold]Highlight settings[/] (edit config/highlight.json for colors)")
        yield Static(f"Consecutive count (orange): {t.get('consecutiveCount', 1)}")
        yield Static(f"Significant % change (purple): {t.get('significantPercentChange', 0.5)}")
        yield Static("\n[dim]Close with Escape. Edit config/highlight.json and press s again to reload.[/]")
        yield Button("Close", variant="primary", id="close-btn")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "close-btn":
            try:
                cfg = load_highlight_config()
                if self.on_save_cb:
                    self.on_save_cb(cfg)
            except Exception:
                pass
            self.dismiss()


def main():
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=_ROOT / ".env")

    from core.app_config import load_config, get_default_broker, get_broker_config, get_signal_routing_config, ConfigError

    # --activate <key>  — bind key to this machine and exit
    if "--activate" in sys.argv:
        from core.license import get_license_key, activate, save_license_key
        idx = sys.argv.index("--activate")
        key = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else get_license_key()
        if not key:
            print("Usage: python app.py --activate <key>", file=sys.stderr)
            sys.exit(1)
        try:
            bound_key = activate(key)
            save_license_key(bound_key)
            print("Key activated and saved to ~/.highlowticker/config.toml")
        except RuntimeError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)
        sys.exit(0)

    # Validate key if present — warn only, never block
    from core.license import get_license_key, validate
    result = validate(get_license_key())
    if result.message:
        print(f"[license] {result.message}", file=sys.stderr)
    if result.valid and not result.machine_bound:
        print("[license] Key not yet bound to this machine. Run: python app.py --activate", file=sys.stderr)

    equity_symbols = _load_symbols()

    try:
        cfg = load_config()
        default_broker = get_default_broker(cfg)
        broker_config = get_broker_config(cfg)
    except ConfigError as e:
        print(f"[HighlowTicker] Config error: {e}", file=sys.stderr)
        sys.exit(1)

    # Create broker manager
    broker_manager = BrokerManager(symbols=equity_symbols, config=broker_config)

    # Signal routing (TCP server for NinjaTrader / external consumers)
    signal_emitter = None
    signal_cfg = get_signal_routing_config(cfg)
    if signal_cfg["enabled"]:
        from core.signal_emitter import SignalEmitter
        signal_emitter = SignalEmitter(
            host=signal_cfg["host"],
            port=signal_cfg["port"],
        )
        print(f"[SignalRouting] enabled on {signal_cfg['host']}:{signal_cfg['port']}", file=sys.stderr)

    app = HighLowTUI(
        broker_manager=broker_manager,
        initial_broker=default_broker,
        signal_emitter=signal_emitter,
    )
    app.run()


def _load_symbols() -> list[str]:
    """Load equity symbol list from tickers.json with a safe fallback."""
    import json as _json
    tickers_path = _ROOT / "tickers" / "tickers.json"
    try:
        return _json.loads(tickers_path.read_text())["symbols"]
    except Exception:
        return ["SPY", "QQQ", "DIA", "AAPL", "MSFT", "NVDA", "TSLA", "AMZN"]


if __name__ == "__main__":
    main()
