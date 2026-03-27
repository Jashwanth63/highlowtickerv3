import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from typing import AsyncIterator
from datetime import datetime
from zoneinfo import ZoneInfo


class _MockProvider:
    async def connect(self) -> None: pass
    async def stream(self) -> AsyncIterator[dict]:
        return
        yield  # make it an async generator
    async def disconnect(self) -> None: pass
    def get_metadata(self) -> dict:
        return {"name": "mock", "refresh_rate": 1.0, "is_realtime": False}


def test_highlowtui_accepts_provider():
    """HighLowTUI must accept an equity provider without error."""
    from app import HighLowTUI
    provider = _MockProvider()
    app = HighLowTUI(equity_provider=provider)
    assert app._provider is provider
    assert app._equity_provider is provider
    assert app._crypto_provider is None


def test_highlowtui_defaults_to_equity_mode():
    from app import HighLowTUI
    provider = _MockProvider()
    app = HighLowTUI(equity_provider=provider)
    assert app._active_mode == "equity"


def test_ticker_falls_back_to_watchlist_prices_when_no_highs_or_lows():
    from app import HighLowTUI
    provider = _MockProvider()
    provider.symbols = ["SPY", "AAPL"]
    app = HighLowTUI(equity_provider=provider)
    app.last_state = {"percentChange": {"SPY": 0.25, "AAPL": -0.75}}
    app.current_prices = {"SPY": 501.23, "AAPL": 180.45}
    app._tracked_symbols = ["SPY", "AAPL"]

    app._build_ticker_text()

    plain = app._ticker_text.plain
    assert "SPY 501.23 (+0.25%)" in plain
    assert "AAPL 180.45 (-0.75%)" in plain


def test_snapshot_entries_split_losers_and_gainers():
    from app import HighLowTUI
    provider = _MockProvider()
    provider.symbols = ["SPY", "AAPL", "MSFT"]
    app = HighLowTUI(equity_provider=provider)
    app.last_state = {"percentChange": {"SPY": 0.25, "AAPL": -1.25, "MSFT": 2.0}}
    app.current_prices = {"SPY": 501.23, "AAPL": 180.45, "MSFT": 420.0}
    app._tracked_symbols = ["SPY", "AAPL", "MSFT"]

    losers, gainers = app._build_snapshot_entries()

    assert losers[0]["symbol"] == "AAPL"
    assert gainers[0]["symbol"] == "MSFT"


def test_scanner_entries_are_deduped_and_zero_pct_filtered():
    from app import HighLowTUI

    entries = [
        {"symbol": "MDB", "count": 12, "price": 242.85, "percentChange": -5.71},
        {"symbol": "MDB", "count": 11, "price": 243.10, "percentChange": -5.50},
        {"symbol": "PLTR", "count": 5, "price": 155.69, "percentChange": 0.0},
        {"symbol": "", "count": 1, "price": 0.0, "percentChange": -1.0},
        {"symbol": "GM", "count": 9, "price": 76.79, "percentChange": -1.41},
    ]

    prepared = HighLowTUI._prepare_scanner_entries(entries)

    assert [entry["symbol"] for entry in prepared] == ["MDB", "GM"]


def test_signal_state_detects_bear_trend():
    from app import compute_signal_state

    signal = compute_signal_state(
        {"20m": 3, "5m": 1, "1m": 1, "30s": 0},
        {"20m": 18, "5m": 9, "1m": 7, "30s": 5},
        tracked_count=100,
        volume_spike_count=6,
    )

    assert signal["regime"] == "Bear Trend"
    assert signal["score"] < 0


def test_signal_state_detects_bull_reversal():
    from app import compute_signal_state

    signal = compute_signal_state(
        {"20m": 3, "5m": 4, "1m": 9, "30s": 5},
        {"20m": 7, "5m": 2, "1m": 1, "30s": 0},
        tracked_count=100,
        volume_spike_count=2,
    )

    assert signal["regime"] == "Bull Reversal"
    assert signal["impulse"] > 0


def test_render_history_chart_includes_all_timeframes():
    from collections import deque
    from app import RATE_TIMEFRAMES, render_history_chart

    history = {
        tf: {
            "highs": deque([1, 2, 3], maxlen=10),
            "lows": deque([3, 2, 1], maxlen=10),
        }
        for tf in RATE_TIMEFRAMES
    }

    chart = render_history_chart(history)

    for tf in RATE_TIMEFRAMES:
        assert tf in chart
    assert "Net Breadth History" in chart
    assert "Net" in chart


def test_record_count_history_appends_latest_counts():
    from app import HighLowTUI
    provider = _MockProvider()
    app = HighLowTUI(equity_provider=provider)

    app._record_count_history({"1m": 4}, {"1m": 7})

    assert app.count_history["1m"]["highs"][-1] == 4
    assert app.count_history["1m"]["lows"][-1] == 7


def test_signal_history_logs_only_on_regime_change():
    from app import HighLowTUI
    provider = _MockProvider()
    app = HighLowTUI(equity_provider=provider)

    app._record_signal_change({"regime": "Bull Trend", "score": 0.45}, ts=1_000.0)
    app._record_signal_change({"regime": "Bull Trend", "score": 0.55}, ts=1_001.0)
    app._record_signal_change({"regime": "Chop", "score": 0.05}, ts=1_002.0)

    assert len(app.signal_history) == 2
    assert "Chop" in app.signal_history[0]
    assert "Bull Trend" in app.signal_history[1]


def test_render_signal_history_has_placeholder():
    from app import render_signal_history

    rendered = render_signal_history([])

    assert "Signal History" in rendered
    assert "Waiting for signal changes" in rendered


def test_market_session_helper_covers_extended_hours():
    from app import HighLowTUI

    assert HighLowTUI._is_equity_market_session(datetime(2026, 3, 23, 4, 0, tzinfo=ZoneInfo("America/New_York")))
    assert HighLowTUI._is_equity_market_session(datetime(2026, 3, 23, 19, 59, tzinfo=ZoneInfo("America/New_York")))
    assert not HighLowTUI._is_equity_market_session(datetime(2026, 3, 23, 3, 59, tzinfo=ZoneInfo("America/New_York")))
    assert not HighLowTUI._is_equity_market_session(datetime(2026, 3, 23, 20, 0, tzinfo=ZoneInfo("America/New_York")))
    assert not HighLowTUI._is_equity_market_session(datetime(2026, 3, 22, 10, 0, tzinfo=ZoneInfo("America/New_York")))


def test_snapshot_hidden_when_fresh_quotes_arrive_during_extended_session(monkeypatch):
    from app import HighLowTUI
    provider = _MockProvider()
    provider.symbols = ["SPY"]
    app = HighLowTUI(equity_provider=provider)
    app.current_prices = {"SPY": 501.23}
    app.last_update_time = 1_000.0
    monkeypatch.setattr("app.time.time", lambda: 1_001.0)
    monkeypatch.setattr(app, "_is_equity_market_session", lambda now=None: True)

    assert app._should_show_snapshot() is False


def test_snapshot_shows_when_quotes_are_stale_outside_session(monkeypatch):
    from app import HighLowTUI
    provider = _MockProvider()
    provider.symbols = ["SPY"]
    app = HighLowTUI(equity_provider=provider)
    app.current_prices = {"SPY": 501.23}
    app.last_update_time = 1_000.0
    monkeypatch.setattr("app.time.time", lambda: 1_500.0)
    monkeypatch.setattr(app, "_is_equity_market_session", lambda now=None: False)

    assert app._should_show_snapshot() is True
