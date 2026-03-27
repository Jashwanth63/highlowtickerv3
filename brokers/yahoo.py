"""Yahoo Finance adapter — polling every 60 seconds (no real-time feed)."""
from __future__ import annotations

import asyncio
import math
import sys
from typing import AsyncIterator, Dict, List, Optional

import yfinance as yf

from brokers.base import BrokerAdapter, MarketUpdate, TickerSnapshot

POLL_INTERVAL = 60.0  # seconds


class YahooAdapter(BrokerAdapter):
    """Polls Yahoo Finance for 1-day/1-minute candles and emits normalised snapshots."""

    def __init__(self, symbols: List[str], poll_interval: float = POLL_INTERVAL) -> None:
        super().__init__(symbols)
        self.poll_interval = poll_interval

    # -- lifecycle -----------------------------------------------------------
    async def connect(self) -> None:
        pass  # no persistent connection

    async def stream(self) -> AsyncIterator[MarketUpdate]:
        while True:
            try:
                update = await asyncio.get_running_loop().run_in_executor(None, self._poll)
                if update is not None:
                    yield update
            except Exception as exc:
                print(f"[YahooAdapter] poll error: {exc}", file=sys.stderr)
            await asyncio.sleep(self.poll_interval)

    async def disconnect(self) -> None:
        pass

    def get_metadata(self) -> dict:
        return {"name": "Yahoo Finance", "refresh_rate": self.poll_interval, "is_realtime": False}

    # -- internal ------------------------------------------------------------
    @staticmethod
    def _safe(value: float, default: float = 0.0) -> float:
        try:
            value = float(value)
        except (TypeError, ValueError):
            return default
        return value if math.isfinite(value) else default

    def _poll(self) -> Optional[MarketUpdate]:
        data = yf.download(
            tickers=self.symbols,
            period="1d",
            interval="1m",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        if data is None or data.empty:
            return None

        snapshots: List[TickerSnapshot] = []
        for sym in self.symbols:
            try:
                sym_data = data[sym] if len(self.symbols) > 1 else data
                if sym_data.empty:
                    snapshots.append(TickerSnapshot(symbol=sym, price=0, session_high=0,
                                                    session_low=0, open_price=0, valid=False))
                    continue

                session_high = self._safe(sym_data["High"].max())
                session_low = self._safe(sym_data["Low"].min())
                current_price = self._safe(sym_data["Close"].iloc[-1])
                open_price = self._safe(sym_data["Open"].iloc[0])
                volume = self._safe(sym_data["Volume"].iloc[-1], default=0.0)

                has_valid_open = open_price != 0.0
                pct = round((current_price - open_price) / open_price * 100, 2) if has_valid_open and current_price else 0.0

                snapshots.append(TickerSnapshot(
                    symbol=sym,
                    price=current_price,
                    session_high=session_high,
                    session_low=session_low,
                    open_price=open_price,
                    volume=volume,
                    percent_change=pct,
                    valid=current_price > 0,
                ))
            except Exception as exc:
                print(f"[YahooAdapter] error processing {sym}: {exc}", file=sys.stderr)
                snapshots.append(TickerSnapshot(symbol=sym, price=0, session_high=0,
                                                session_low=0, open_price=0, valid=False))
        return MarketUpdate(snapshots=snapshots)
