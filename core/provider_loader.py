"""Instantiate broker adapters from configuration.

This module bridges the config layer with the broker adapter layer.
"""
from typing import List

from brokers.base import BrokerAdapter, BrokerType
from brokers.manager import create_adapter


class ProviderLoadError(RuntimeError):
    pass


def load_broker(broker_type: BrokerType, symbols: List[str], config: dict) -> BrokerAdapter:
    """Create and return a BrokerAdapter for the given broker type."""
    try:
        return create_adapter(broker_type, symbols, config)
    except Exception as exc:
        raise ProviderLoadError(
            f"Failed to load broker '{broker_type.value}': {exc}"
        ) from exc
