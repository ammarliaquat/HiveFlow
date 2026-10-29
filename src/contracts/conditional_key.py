"""Canonical values used by bounded-exception identity contracts.

The execution contract is deliberately database-neutral.  V1 only permits
signed 64-bit integer exception values; target/source adapters are responsible
for translating the typed payload to their native SQL representation.
"""

from __future__ import annotations

import hashlib
import json
from enum import Enum
from numbers import Integral
from typing import Any, Iterable, Mapping, Tuple

from src.exceptions import ExceptionCodecVersionError, ExceptionValueTypeError


class IdentityMode(str, Enum):
    """Identity semantics frozen for one table execution contract."""

    EXACT_KEY = "EXACT_KEY"
    BOUNDED_EXCEPTION_KEY = "BOUNDED_EXCEPTION_KEY"
    KEYLESS = "KEYLESS"


class Int64ExceptionCodec:
    """Encode and validate the V1 typed exception-key payload.

    The codec intentionally rejects coercive values (for example ``"1"`` or
    ``1.0``).  Coercion at a source/target boundary could make two policies
    with different key types appear identical.
    """

    CODEC_VERSION = "exception-key-v1"
    LOGICAL_TYPE = "INT64"
    MIN_VALUE = -(2**63)
    MAX_VALUE = 2**63 - 1

    @classmethod
    def normalize(cls, values: Iterable[Any]) -> Tuple[int, ...]:
        if values is None or isinstance(values, (str, bytes, bytearray)):
            raise ValueError("INT64 exception values must be an iterable of integers")
        try:
            raw_values = tuple(values)
        except TypeError as exc:
            raise ValueError("INT64 exception values must be an iterable of integers") from exc

        normalized = []
        for value in raw_values:
            if isinstance(value, bool) or not isinstance(value, Integral):
                raise ExceptionValueTypeError(
                    f"INT64 exception value must be an integer, got {type(value).__name__}"
                )
            integer = int(value)
            if integer < cls.MIN_VALUE or integer > cls.MAX_VALUE:
                raise ExceptionValueTypeError(f"INT64 exception value out of range: {integer}")
            normalized.append(integer)
        return tuple(sorted(set(normalized)))

    @classmethod
    def encode(cls, values: Iterable[Any]) -> dict:
        """Return the canonical JSON-compatible typed payload."""
        return {
            "codec_version": cls.CODEC_VERSION,
            "logical_type": cls.LOGICAL_TYPE,
            "values": list(cls.normalize(values)),
        }

    @classmethod
    def decode(cls, payload: Mapping[str, Any]) -> Tuple[int, ...]:
        """Validate and decode a canonical payload into sorted unique values."""
        if not isinstance(payload, Mapping):
            raise ValueError("INT64 exception payload must be an object")
        if payload.get("codec_version") != cls.CODEC_VERSION:
            raise ExceptionCodecVersionError(
                "Unsupported INT64 exception codec version: "
                f"{payload.get('codec_version')!r}"
            )
        if payload.get("logical_type") != cls.LOGICAL_TYPE:
            raise ValueError(
                "INT64 exception payload logical_type must be INT64, got "
                f"{payload.get('logical_type')!r}"
            )
        values = payload.get("values")
        if not isinstance(values, (list, tuple)):
            raise ValueError("INT64 exception payload values must be an array")
        return cls.normalize(values)

    @classmethod
    def fingerprint(cls, values_or_payload: Any) -> str:
        """Fingerprint the canonical typed payload deterministically."""
        payload = (
            cls.encode(values_or_payload)
            if not isinstance(values_or_payload, Mapping)
            else cls.encode(cls.decode(values_or_payload))
        )
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


def encode_int64_exception_values(values: Iterable[Any]) -> dict:
    """Functional alias for callers that do not need the codec class."""
    return Int64ExceptionCodec.encode(values)


def decode_int64_exception_values(payload: Mapping[str, Any]) -> Tuple[int, ...]:
    return Int64ExceptionCodec.decode(payload)


def fingerprint_int64_exception_values(values_or_payload: Any) -> str:
    return Int64ExceptionCodec.fingerprint(values_or_payload)


__all__ = [
    "IdentityMode",
    "Int64ExceptionCodec",
    "encode_int64_exception_values",
    "decode_int64_exception_values",
    "fingerprint_int64_exception_values",
]
