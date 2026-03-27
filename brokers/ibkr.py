"""Interactive Brokers adapter — real-time streaming via TWS/IB Gateway WebSocket API.

Connects to the IBKR Client Portal API or TWS API via ``ib_async`` (formerly ib_insync).
Requires TWS or IB Gateway running locally (default port 7497 for paper, 7496 for live).

Config keys (in config/brokers.toml under [ibkr]):
    host     = "127.0.0.1"
    port     = 7497
    client_id = 1
"""
from __future__ import annotations

import asyncio
import sys
import time
from typing import AsyncIterator, Dict, List, Optional

from brokers.base import BrokerAdapter, MarketUpdate, TickerSnapshot

# Reconnect parameters
RECONNECT_DELAY = 5.0
MAX_RECONNECT_ATTEMPTS = 10


class IBKRAdapter(BrokerAdapter):
    """Streams real-time market data from IBKR via ib_async (ib_insync)."""

    def __init__(
        self,
        symbols: List[str],
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 1,
    ) -> None:
        super().__init__(symbols)
        self._host = host
        self._port = port
        self._client_id = client_id
        self._ib = None  # ib_async.IB instance
        self._contracts: Dict[str, object] = {}
        self._tickers: Dict[str, object] = {}
        self._queue: asyncio.Queue[MarketUpdate] = asyncio.Queue()
        self._stop = asyncio.Event()
        self._session_highs: Dict[str, float] = {}
        self._session_lows: Dict[str, float] = {}
        self._open_prices: Dict[str, float] = {}
        self._connected = False

    async def connect(self) -> None:
        try:
            import ib_async
        except ImportError:
            raise RuntimeError(
                "ib_async is required for IBKR. Install with: pip install ib_async"
            )

        self._stop.clear()
        self._ib = ib_async.IB()
        await self._ib.connectAsync(
            host=self._host,
            port=self._port,
            clientId=self._client_id,
            readonly=True,
        )
        self._connected = True

        # Subscribe to market data for each symbol
        for sym in self.symbols:
            try:
                contract = ib_async.Stock(sym, "SMART", "USD")
                qualified = await self._ib.qualifyContractsAsync(contract)
                if qualified:
                    contract = qualified[0]
                    self._contracts[sym] = contract
                    ticker = self._ib.reqMktData(contract, "", False, False)
                    self._tickers[sym] = ticker
                else:
                    print(f"[IBKRAdapter] could not qualify contract for {sym}", file=sys.stderr)
            except Exception as exc:
                print(f"[IBKRAdapter] subscribe error for {sym}: {exc}", file=sys.stderr)

        # Register pending tickers callback
        self._ib.pendingTickersEvent += self._on_pending_tickers

    def _on_pending_tickers(self, tickers) -> None:
        """Callback from ib_async when ticker data updates arrive."""
        snapshots: List[TickerSnapshot] = []
        ts = time.time()

        for ticker in tickers:
            sym = ticker.contract.symbol if ticker.contract else None
            if not sym or sym not in self._contracts:
                continue

            price = ticker.marketPrice()
            if price is None or price != price:  # NaN check
                continue
            price = float(price)
            if price <= 0:
                continue

            high = float(ticker.high) if ticker.high and ticker.high == ticker.high else price
            low = float(ticker.low) if ticker.low and ticker.low == ticker.low else price
            open_price = float(ticker.open) if ticker.open and ticker.open == ticker.open else 0.0
            volume = float(ticker.volume) if ticker.volume and ticker.volume == ticker.volume else 0.0

            # Track session extremes
            if sym not in self._session_highs or high > self._session_highs[sym]:
                self._session_highs[sym] = high
            if sym not in self._session_lows or low < self._session_lows[sym]:
                self._session_lows[sym] = low
            if sym not in self._open_prices and open_price > 0:
                self._open_prices[sym] = open_price

            ref_open = self._open_prices.get(sym, open_price) or price
            pct = round((price - ref_open) / ref_open * 100, 2) if ref_open > 0 else 0.0

            snapshots.append(TickerSnapshot(
                symbol=sym,
                price=price,
                session_high=self._session_highs.get(sym, high),
                session_low=self._session_lows.get(sym, low),
                open_price=ref_open,
                volume=volume,
                percent_change=pct,
                valid=True,
            ))

        if snapshots:
            try:
                self._queue.put_nowait(MarketUpdate(snapshots=snapshots))
            except asyncio.QueueFull:
                pass  # drop oldest if queue is full

    async def stream(self) -> AsyncIterator[MarketUpdate]:
        try:
            while not self._stop.is_set():
                try:
                    update = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                    yield update
                except asyncio.TimeoutError:
                    # Yield heartbeat with current state so UI stays responsive
                    if self._tickers:
                        snapshots = self._build_current_snapshots()
                        if snapshots:
                            yield MarketUpdate(snapshots=snapshots)
                    continue
        except asyncio.CancelledError:
            return

    def _build_current_snapshots(self) -> List[TickerSnapshot]:
        """Build snapshots from current ticker state (for heartbeat updates)."""
        snapshots = []
        for sym, ticker in self._tickers.items():
            price = ticker.marketPrice()
            if price is None or price != price or float(price) <= 0:
                snapshots.append(TickerSnapshot(
                    symbol=sym, price=0, session_high=0,
                    session_low=0, open_price=0, valid=False,
                ))
                continue
            price = float(price)
            high = self._session_highs.get(sym, price)
            low = self._session_lows.get(sym, price)
            ref_open = self._open_prices.get(sym, price)
            pct = round((price - ref_open) / ref_open * 100, 2) if ref_open > 0 else 0.0
            snapshots.append(TickerSnapshot(
                symbol=sym, price=price, session_high=high,
                session_low=low, open_price=ref_open, volume=0,
                percent_change=pct, valid=True,
            ))
        return snapshots

    async def disconnect(self) -> None:
        self._stop.set()
        if self._ib and self._connected:
            for sym, ticker in self._tickers.items():
                try:
                    self._ib.cancelMktData(self._contracts[sym])
                except Exception:
                    pass
            self._ib.pendingTickersEvent -= self._on_pending_tickers
            self._ib.disconnect()
            self._connected = False
        self._tickers.clear()
        self._contracts.clear()

    def get_metadata(self) -> dict:
        return {"name": "IBKR", "refresh_rate": 0.0, "is_realtime": True}
