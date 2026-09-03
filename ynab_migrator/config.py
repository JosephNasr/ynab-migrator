from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any, Dict


DEFAULT_CONFIG_FILE = "ynab-migrator.toml"
CONFIG_KEYS = {
    "source_token",
    "dest_token",
}
STRING_KEYS = {"source_token", "dest_token"}


def load_config(path: Path) -> Dict[str, Any]:
    """Load and validate a local ynab-migrator TOML configuration file."""
    with path.open("rb") as handle:
        payload = tomllib.load(handle)
    unknown = sorted(set(payload).difference(CONFIG_KEYS))
    if unknown:
        raise ValueError(f"unsupported config keys in {path}: {', '.join(unknown)}")
    missing = sorted(CONFIG_KEYS.difference(payload))
    if missing:
        raise ValueError(f"missing config keys in {path}: {', '.join(missing)}")
    for key in STRING_KEYS.intersection(payload):
        if not isinstance(payload[key], str):
            raise ValueError(f"config value {key!r} must be a string")
    return dict(payload)
