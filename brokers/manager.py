"""BrokerManager — orchestrates runtime broker switching with data persistence.

Responsibilities:
- Instantiate broker adapters from config
- Track session state (prices, highs, lows) across broker switches
- Persist accumulated data when disconnecting; propagate to new broker on reconnect
- Expose a single async stream that the TUI consumes
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import AsyncIterator, Callable, Dict, List, Optional, Set

from brokers.base import BrokerAdapter, BrokerType, MarketUpdate, TickerSnapshot


@dataclass
class PersistedTickerData:
    """Accumulated session data for a single ticker, survives broker switches."""
    symbol: str
    price: float = 0.0
    session_high: float = 0.0
    session_low: float = float("inf")
    open_price: float = 0.0
    percent_change: float = 0.0
    volume: float = 0.0
    high_count: int = 0
    low_count: int = 0
    week52_high: Optional[float] = None
    week52_low: Optional[float] = None
    valid: bool = True
    last_update: float = 0.0


@dataclass
class PersistedState:
    """Full session state that persists across broker switches."""
    tickers: Dict[str, PersistedTickerData] = field(default_factory=dict)
    high_timestamps: List[float] = field(default_factory=list)
    low_timestamps: List[float] = field(default_factory=list)

    def update_from_snapshot(self, snap: TickerSnapshot) -> None:
        """Merge a new snapshot into persisted state, tracking new highs/lows."""
        sym = snap.symbol
        ts = time.time()

        if sym not in self.tickers:
            self.tickers[sym] = PersistedTickerData(
                symbol=sym,
                price=snap.price,
                session_high=snap.session_high,
                session_low=snap.session_low,
                open_price=snap.open_price,
                percent_change=snap.percent_change,
                volume=snap.volume,
                week52_high=snap.week52_high,
                week52_low=snap.week52_low,
                valid=snap.valid,
                last_update=ts,
            )
            return

        td = self.tickers[sym]

        if not snap.valid:
            # Don't overwrite good data with invalid
            td.valid = False if td.price == 0 else td.valid
            return

        td.price = snap.price
        td.percent_change = snap.percent_change
        td.volume = snap.volume
        td.valid = snap.valid
        td.last_update = ts

        if snap.open_price > 0 and td.open_price == 0:
            td.open_price = snap.open_price

        # Recalculate percent_change from persisted open if needed
        if td.open_price > 0 and snap.price > 0:
            td.percent_change = round((snap.price - td.open_price) / td.open_price * 100, 2)

        # Track new session highs
        if snap.session_high > td.session_high:
            td.high_count += 1
            td.session_high = snap.session_high
            self.high_timestamps.append(ts)

        # Track new session lows
        if snap.session_low < td.session_low:
            td.low_count += 1
            td.session_low = snap.session_low
            self.low_timestamps.append(ts)

        # 52-week tracking
        if snap.week52_high is not None:
            td.week52_high = snap.week52_high
        if snap.week52_low is not None:
            td.week52_low = snap.week52_low

    def prune_timestamps(self, window: float = 1200.0) -> None:
        """Remove timestamps older than window seconds."""
        cutoff = time.time() - window
        self.high_timestamps = [t for t in self.high_timestamps if t > cutoff]
        self.low_timestamps = [t for t in self.low_timestamps if t > cutoff]

    def get_ticker(self, sym: str) -> Optional[PersistedTickerData]:
        return self.tickers.get(sym)

    def all_tickers(self) -> List[PersistedTickerData]:
        return list(self.tickers.values())

    def clear(self) -> None:
        self.tickers.clear()
        self.high_timestamps.clear()
        self.low_timestamps.clear()


def create_adapter(broker_type: BrokerType, symbols: List[str], config: dict) -> BrokerAdapter:
    """Factory: create the correct adapter from broker type + config."""
    if broker_type == BrokerType.YAHOO:
        from brokers.yahoo import YahooAdapter
        return YahooAdapter(symbols, poll_interval=config.get("poll_interval", 60.0))

    if broker_type == BrokerType.IBKR:
        from brokers.ibkr import IBKRAdapter
        ibkr_cfg = config.get("ibkr", {})
        return IBKRAdapter(
            symbols,
            host=ibkr_cfg.get("host", "127.0.0.1"),
            port=ibkr_cfg.get("port", 7497),
            client_id=ibkr_cfg.get("client_id", 1),
        )

    if broker_type == BrokerType.TOS:
        from brokers.tos import TOSAdapter
        tos_cfg = config.get("tos", {})
        return TOSAdapter(
            symbols,
            app_key=tos_cfg.get("app_key", ""),
            app_secret=tos_cfg.get("app_secret", ""),
            callback_url=tos_cfg.get("callback_url", "https://127.0.0.1"),
            token_path=tos_cfg.get("token_path", ""),
        )

    raise ValueError(f"Unknown broker type: {broker_type}")


class BrokerManager:
    """Manages the active broker adapter and persists data across switches."""

    def __init__(self, symbols: List[str], config: dict) -> None:
        self.symbols = symbols
        self.config = config
        self._adapter: Optional[BrokerAdapter] = None
        self._broker_type: Optional[BrokerType] = None
        self.state = PersistedState()
        self._stream_task: Optional[asyncio.Task] = None
        self._update_queue: asyncio.Queue[MarketUpdate] = asyncio.Queue(maxsize=500)
        self._stop = asyncio.Event()
        self._connected = False
        self._on_update: Optional[Callable[[MarketUpdate], None]] = None

    @property
    def active_broker(self) -> Optional[BrokerType]:
        return self._broker_type

    @property
    def adapter(self) -> Optional[BrokerAdapter]:
        return self._adapter

    @property
    def is_connected(self) -> bool:
        return self._connected

    def get_metadata(self) -> dict:
        if self._adapter:
            return self._adapter.get_metadata()
        return {"name": "Disconnected", "refresh_rate": 0, "is_realtime": False}

    async def connect(self, broker_type: BrokerType) -> None:
        """Connect to a specific broker. If already connected, disconnect first."""
        if self._connected:
            await self.disconnect()

        self._broker_type = broker_type
        self._adapter = create_adapter(broker_type, self.symbols, self.config)
        self._stop.clear()

        await self._adapter.connect()
        self._connected = True

    async def disconnect(self) -> None:
        """Disconnect current broker. Persisted state is preserved."""
        self._stop.set()

        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass
            self._stream_task = None

        if self._adapter:
            await self._adapter.disconnect()
            self._adapter = None
        self._connected = False
        self._broker_type = None

    async def switch_broker(self, new_broker: BrokerType) -> None:
        """Switch to a different broker, preserving accumulated session data.

        The persisted state (prices, highs, lows, counts) carries over.
        The new broker's data will merge into existing state seamlessly.
        """
        await self.disconnect()
        await self.connect(new_broker)

    async def stream(self) -> AsyncIterator[MarketUpdate]:
        """Yield MarketUpdates from the active broker, merging into persisted state."""
        if not self._adapter:
            return

        try:
            async for update in self._adapter.stream():
                if self._stop.is_set():
                    break
                # Merge each snapshot into persisted state
                for snap in update.snapshots:
                    self.state.update_from_snapshot(snap)
                self.state.prune_timestamps()
                yield update
        except asyncio.CancelledError:
            return

    def get_watchlist_data(self) -> List[PersistedTickerData]:
        """Return current watchlist data from persisted state, sorted by symbol."""
        return sorted(self.state.all_tickers(), key=lambda t: t.symbol)
