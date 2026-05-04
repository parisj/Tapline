"""JSON compatibility layer with optional orjson optimization.

orjson provides 3-10x faster JSON parsing; stdlib json is fallback.
"""

from __future__ import annotations

from typing import Any

try:
    import orjson

    def json_loads(data: bytes | str) -> dict[str, Any]:
        """Parse JSON from bytes or string."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        result: dict[str, Any] = orjson.loads(data)
        return result

    def json_dumps(obj: dict[str, Any]) -> bytes:
        """Serialize dict to compact JSON bytes."""
        result: bytes = orjson.dumps(obj)
        return result

    USING_ORJSON = True

except ImportError:
    import json

    def json_loads(data: bytes | str) -> dict[str, Any]:
        """Parse JSON from bytes or string."""
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        result: dict[str, Any] = json.loads(data)
        return result

    def json_dumps(obj: dict[str, Any]) -> bytes:
        """Serialize dict to compact JSON bytes."""
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")

    USING_ORJSON = False
