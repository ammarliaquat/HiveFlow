"""Canonical JSON conversion for control-plane evidence and telemetry."""

import json
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID


def to_json_safe(value: Any) -> Any:
    """Recursively convert supported scalar containers to JSON-native values."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return to_json_safe(value.value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): to_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json_safe(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized = [to_json_safe(item) for item in value]
        return sorted(normalized, key=lambda item: json.dumps(item, sort_keys=True))

    # NumPy scalar types expose item(), which returns the equivalent Python
    # scalar. Restrict this conversion so arbitrary domain objects cannot be
    # silently reduced to unstable audit representations.
    if value.__class__.__module__.split(".", 1)[0] == "numpy" and hasattr(value, "item"):
        return to_json_safe(value.item())

    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


def json_safe_dumps(value: Any, **kwargs: Any) -> str:
    """Serialize supported control-plane data after canonical normalization."""
    return json.dumps(to_json_safe(value), **kwargs)
