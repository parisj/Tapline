"""Unit tests for the JSON compatibility layer.

The src.utils.json_compat module exposes:
- json_loads(data) -> dict
- json_dumps(obj) -> bytes
- USING_ORJSON (bool flag)

Two implementation branches exist:
- orjson available (preferred)
- stdlib json fallback (when orjson import fails)

Both branches must roundtrip dict payloads and accept both bytes and str
inputs in json_loads. The fallback branch is exercised by reloading the
module with orjson masked from sys.modules via monkeypatch.
"""

from __future__ import annotations

import importlib
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest


class TestJsonRoundtrip:
    """Roundtrip dumps/loads with the active implementation."""

    def test_dumps_returns_bytes(self) -> None:
        from src.utils import json_compat

        result = json_compat.json_dumps({"key": "value"})

        assert isinstance(result, bytes)

    def test_loads_accepts_bytes(self) -> None:
        from src.utils import json_compat

        data = b'{"key": "value"}'

        assert json_compat.json_loads(data) == {"key": "value"}

    def test_loads_accepts_str(self) -> None:
        from src.utils import json_compat

        data = '{"key": "value"}'

        assert json_compat.json_loads(data) == {"key": "value"}

    def test_roundtrip_simple_dict(self) -> None:
        from src.utils import json_compat

        original = {"a": 1, "b": "two", "c": True, "d": None}

        serialized = json_compat.json_dumps(original)
        restored = json_compat.json_loads(serialized)

        assert restored == original

    def test_roundtrip_nested_dict(self) -> None:
        from src.utils import json_compat

        original = {
            "outer": {"inner": [1, 2, 3]},
            "list_of_dicts": [{"x": 1}, {"y": 2}],
        }

        serialized = json_compat.json_dumps(original)
        restored = json_compat.json_loads(serialized)

        assert restored == original

    def test_roundtrip_unicode_string(self) -> None:
        from src.utils import json_compat

        original = {"text": "héllo wörld", "emoji": "ok"}

        serialized = json_compat.json_dumps(original)
        restored = json_compat.json_loads(serialized)

        assert restored == original

    def test_roundtrip_numeric_types(self) -> None:
        from src.utils import json_compat

        original = {"int": 42, "float": 3.14, "negative": -7, "zero": 0}

        serialized = json_compat.json_dumps(original)
        restored = json_compat.json_loads(serialized)

        assert restored == original

    def test_roundtrip_empty_dict(self) -> None:
        from src.utils import json_compat

        serialized = json_compat.json_dumps({})

        assert json_compat.json_loads(serialized) == {}

    def test_roundtrip_list_values(self) -> None:
        from src.utils import json_compat

        original = {"items": [1, "two", 3.0, None, True]}

        serialized = json_compat.json_dumps(original)
        restored = json_compat.json_loads(serialized)

        assert restored == original


class TestUsingOrjsonFlag:
    """The module exposes a USING_ORJSON bool indicating the active branch."""

    def test_flag_is_boolean(self) -> None:
        from src.utils import json_compat

        assert isinstance(json_compat.USING_ORJSON, bool)

    def test_flag_matches_orjson_availability(self) -> None:
        from src.utils import json_compat

        try:
            import orjson

            assert json_compat.USING_ORJSON is True
        except ImportError:
            assert json_compat.USING_ORJSON is False


class TestFallbackBranch:
    """Exercise the stdlib fallback by reloading the module with orjson masked."""

    def test_fallback_when_orjson_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Mask orjson so importlib.reload triggers the ImportError branch.
        monkeypatch.setitem(sys.modules, "orjson", None)

        from src.utils import json_compat

        reloaded = importlib.reload(json_compat)
        try:
            assert reloaded.USING_ORJSON is False

            data = {"hello": "world", "n": 1}
            blob = reloaded.json_dumps(data)
            assert isinstance(blob, bytes)
            assert reloaded.json_loads(blob) == data
        finally:
            # Restore the canonical module so other tests see the normal branch.
            monkeypatch.undo()
            importlib.reload(reloaded)

    def test_fallback_dumps_is_compact(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, "orjson", None)

        from src.utils import json_compat

        reloaded = importlib.reload(json_compat)
        try:
            # Compact separators -> no spaces after commas/colons.
            blob = reloaded.json_dumps({"a": 1, "b": 2})

            assert b", " not in blob
            assert b": " not in blob
            assert blob in (b'{"a":1,"b":2}', b'{"b":2,"a":1}')
        finally:
            monkeypatch.undo()
            importlib.reload(reloaded)

    def test_fallback_loads_accepts_bytes_and_str(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, "orjson", None)

        from src.utils import json_compat

        reloaded = importlib.reload(json_compat)
        try:
            assert reloaded.json_loads(b'{"k":"v"}') == {"k": "v"}
            assert reloaded.json_loads('{"k":"v"}') == {"k": "v"}
        finally:
            monkeypatch.undo()
            importlib.reload(reloaded)
