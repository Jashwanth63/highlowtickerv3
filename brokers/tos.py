"""ThinkOrSwim (Schwab) adapter — real-time streaming via Schwab's WebSocket API.

Uses the schwab-py SDK for authentication and streaming.
Requires a Schwab developer app with API access.

Config keys (in config/brokers.toml under [tos]):
    app_key      = "your-schwab-app-key"
    app_secret   = "your-schwab-app-secret"
    callback_url = "https://127.0.0.1"
    token_path   = "~/.highlowticker/schwab_token.json"
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import AsyncIterator, Dict, List, Optional

from brokers.base import BrokerAdapter, MarketUpdate, TickerSnapshot

LEVEL_ONE_FIELDS = [
    "symbol", "bidPrice", "askPrice", "lastPrice", "highPrice", "lowPrice",
    "openPrice", "totalVolume", "netPercentChange",
    "52WeekHigh", "52WeekLow",
]


class TOSAdapter(BrokerAdapter):
    """Streams real-time quotes from Schwab (ThinkOrSwim) via their streaming WebSocket."""

    def __init__(
        self,
        symbols: List[str],
        app_key: str = "",
        app_secret: str = "",
        callback_url: str = "https://127.0.0.1",
        token_path: str = "",
    ) -> None:
        super().__init__(symbols)
        self._app_key = app_key
        self._app_secret = app_secret
        self._callback_url = callback_url
        self._token_path = token_path or str(Path.home() / ".highlowticker" / "schwab_token.json")
        self._client = None
        self._stream_client = None
        self._queue: asyncio.Queue[MarketUpdate] = asyncio.Queue()
        self._stop = asyncio.Event()
        self._session_highs: Dict[str, float] = {}
        self._session_lows: Dict[str, float] = {}
        self._open_prices: Dict[str, float] = {}
        self._connected = False

    async def connect(self) -> None:
        try:
            import schwab
            from schwab.streaming import StreamClient
        except ImportError:
            raise RuntimeError(
                "schwab-py is required for ThinkOrSwim. Install with: pip install schwab-py"
            )

        # Create authenticated client
        try:
            self._client = schwab.auth.client_from_token_file(
                self._token_path,
                api_key=self._app_key,
                app_secret=self._app_secret,
            )
        except FileNotFoundError:
            # First-time auth: manual flow needed
            raise RuntimeError(
                f"Schwab token not found at {self._token_path}. "
                "Run initial OAuth flow first:\n"
                "  python -c \"import schwab; schwab.auth.client_from_manual_flow("
                f"'{self._app_key}', '{self._app_secret}', '{self._callback_url}', "
                f"'{self._token_path}')\""
            )

        self._stream_client = StreamClient(self._client)
        await self._stream_client.login()
        self._stop.clear()
        self._connected = True

        # Subscribe to Level 1 quotes
        await self._stream_client.level_one_equities_subs(
            symbols=self.symbols,
        )

        # Add handler for quote updates
        self._stream_client.add_level_one_equities_handler(self._on_level_one)

    def _on_level_one(self, msg: dict) -> None:
        """Handler called by schwab-py streaming client for Level 1 equity data."""
        snapshots: List[TickerSnapshot] = []

        content = msg.get("content", [])
        for item in content:
            sym = item.get("key", "")
            if not sym:
                continue

            price = item.get("LAST_PRICE") or item.get("3")
            if price is None:
                snapshots.append(TickerSnapshot(
                    symbol=sym, price=0, session_high=0,
                    session_low=0, open_price=0, valid=False,
                ))
                continue

            price = float(price)
            if price <= 0:
                snapshots.append(TickerSnapshot(
                    symbol=sym, price=0, session_high=0,
                    session_low=0, open_price=0, valid=False,
                ))
                continue

            high = float(item.get("HIGH_PRICE", item.get("12", 0)) or 0) or price
            low = float(item.get("LOW_PRICE", item.get("13", 0)) or 0) or price
            open_price = float(item.get("OPEN_PRICE", item.get("28", 0)) or 0) or 0
            volume = float(item.get("TOTAL_VOLUME", item.get("8", 0)) or 0)
            pct_change = float(item.get("NET_CHANGE_PERCENT", item.get("29", 0)) or 0)
            w52_high = item.get("52_WEEK_HIGH", item.get("30"))
            w52_low = item.get("52_WEEK_LOW", item.get("31"))

            # Track session extremes
            if sym not in self._session_highs or high > self._session_highs[sym]:
                self._session_highs[sym] = high
            if sym not in self._session_lows or low < self._session_lows[sym]:
                self._session_lows[sym] = low
            if sym not in self._open_prices and open_price > 0:
                self._open_prices[sym] = open_price

            ref_open = self._open_prices.get(sym, open_price)
            if pct_change == 0 and ref_open > 0:
                pct_change = round((price - ref_open) / ref_open * 100, 2)

            snapshots.append(TickerSnapshot(
                symbol=sym,
                price=price,
                session_high=self._session_highs.get(sym, high),
                session_low=self._session_lows.get(sym, low),
                open_price=ref_open,
                volume=volume,
                percent_change=pct_change,
                week52_high=float(w52_high) if w52_high else None,
                week52_low=float(w52_low) if w52_low else None,
                valid=True,
            ))

        if snapshots:
            try:
                self._queue.put_nowait(MarketUpdate(snapshots=snapshots))
            except asyncio.QueueFull:
                pass

    async def stream(self) -> AsyncIterator[MarketUpdate]:
        # Start the streaming message handler loop in background
        handler_task = asyncio.create_task(self._handle_messages())
        try:
            while not self._stop.is_set():
                try:
                    update = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                    yield update
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            pass
        finally:
            handler_task.cancel()
            try:
                await handler_task
            except asyncio.CancelledError:
                pass

    async def _handle_messages(self) -> None:
        """Background task that reads from the streaming WebSocket."""
        try:
            while not self._stop.is_set() and self._stream_client:
                await self._stream_client.handle_message()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            print(f"[TOSAdapter] stream handler error: {exc}", file=sys.stderr)

    async def disconnect(self) -> None:
        self._stop.set()
        if self._stream_client and self._connected:
            try:
                await self._stream_client.level_one_equities_unsubs(symbols=self.symbols)
            except Exception:
                pass
            try:
                await self._stream_client.logout()
            except Exception:
                pass
            self._connected = False
        self._stream_client = None
        self._client = None

    def get_metadata(self) -> dict:
        return {"name": "ThinkOrSwim", "refresh_rate": 0.0, "is_realtime": True}
