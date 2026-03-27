"""Abstract base for all broker adapters.

Every broker adapter normalises its feed into a common ``MarketUpdate`` dict
so that the rest of the application is broker-agnostic.
"""
from __future__ import annotations

import abc
import enum
from dataclasses import dataclass, field
from typing import AsyncIterator, Dict, List, Optional


class BrokerType(str, enum.Enum):
    YAHOO = "yahoo"
    IBKR = "ibkr"
    TOS = "tos"


@dataclass
class TickerSnapshot:
    """Normalised per-ticker quote pushed by every adapter."""
    symbol: str
    price: float
    session_high: float
    session_low: float
    open_price: float
    volume: float = 0.0
    percent_change: float = 0.0
    week52_high: Optional[float] = None
    week52_low: Optional[float] = None
    valid: bool = True  # False → data unavailable, show as NA


@dataclass
class MarketUpdate:
    """A batch of normalised ticker snapshots emitted by an adapter."""
    snapshots: List[TickerSnapshot] = field(default_factory=list)


class BrokerAdapter(abc.ABC):
    """Interface that every broker must implement.

    Subclasses are free to use polling *or* persistent WebSocket connections
    internally — the caller only sees ``connect → stream → disconnect``.
    """

    def __init__(self, symbols: List[str]) -> None:
        self.symbols = list(symbols)

    # -- lifecycle -----------------------------------------------------------
    @abc.abstractmethod
    async def connect(self) -> None:
        """Open connection / start polling."""

    @abc.abstractmethod
    async def stream(self) -> AsyncIterator[MarketUpdate]:
        """Yield ``MarketUpdate`` objects indefinitely."""
        # Must be an async generator (yield); the ``...`` below keeps mypy happy.
        yield MarketUpdate()  # type: ignore[misc]  # pragma: no cover

    @abc.abstractmethod
    async def disconnect(self) -> None:
        """Clean up resources / close WebSocket."""

    # -- metadata ------------------------------------------------------------
    @abc.abstractmethod
    def get_metadata(self) -> dict:
        """Return ``{'name': str, 'refresh_rate': float, 'is_realtime': bool}``."""

    # -- helpers -------------------------------------------------------------
    @property
    def is_realtime(self) -> bool:
        return self.get_metadata().get("is_realtime", False)

    @property
    def name(self) -> str:
        return self.get_metadata().get("name", self.__class__.__name__)
