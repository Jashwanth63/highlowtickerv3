"""Load and validate broker configuration.

Reads from ~/.highlowticker/config.toml or falls back to config/brokers.toml
in the project directory.
"""
from pathlib import Path
from typing import Optional

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]

from brokers.base import BrokerType

# Config search order: user home → project config
USER_CONFIG_PATH = Path.home() / ".highlowticker" / "config.toml"
PROJECT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "brokers.toml"

VALID_BROKERS = {b.value for b in BrokerType}


class ConfigError(ValueError):
    pass


def load_config() -> dict:
    """Return parsed config dict. Searches user home then project config."""
    for path in (USER_CONFIG_PATH, PROJECT_CONFIG_PATH):
        if path.exists():
            with open(path, "rb") as f:
                return tomllib.load(f)
    return {}


def get_default_broker(cfg: dict) -> BrokerType:
    """Return the default broker from config, defaulting to IBKR."""
    broker_str = cfg.get("general", {}).get("default_broker", "yahoo").lower()
    if broker_str not in VALID_BROKERS:
        raise ConfigError(
            f"Unknown broker '{broker_str}' in config. "
            f"Valid options: {', '.join(VALID_BROKERS)}"
        )
    return BrokerType(broker_str)


def get_broker_config(cfg: dict) -> dict:
    """Extract the full broker config dict for passing to BrokerManager."""
    return {
        "ibkr": cfg.get("ibkr", {}),
        "tos": cfg.get("tos", {}),
        "yahoo": cfg.get("yahoo", {}),
        "poll_interval": cfg.get("yahoo", {}).get("poll_interval", 60),
    }


def validate_broker_config(cfg: dict, broker_type: BrokerType) -> Optional[str]:
    """Return an error message if the broker config is incomplete, else None."""
    if broker_type == BrokerType.YAHOO:
        return None  # Yahoo needs no credentials

    if broker_type == BrokerType.IBKR:
        ibkr = cfg.get("ibkr", {})
        if not ibkr.get("host") or not ibkr.get("port"):
            return "IBKR config requires host and port. Check config/brokers.toml."
        return None

    if broker_type == BrokerType.TOS:
        tos = cfg.get("tos", {})
        if not tos.get("app_key") or not tos.get("app_secret"):
            return "TOS config requires app_key and app_secret. Check config/brokers.toml."
        return None

    return f"Unknown broker type: {broker_type}"


def get_signal_routing_config(cfg: dict) -> dict:
    """Extract signal routing config with defaults."""
    section = cfg.get("signal_routing", {})
    return {
        "enabled": bool(section.get("enabled", False)),
        "host": str(section.get("host", "127.0.0.1")),
        "port": int(section.get("port", 9137)),
    }
