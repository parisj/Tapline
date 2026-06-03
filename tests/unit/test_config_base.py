"""Unit tests for base configuration loader utilities.

Tests for:
- load_toml
- load_toml_or_empty
- get_section
"""

from __future__ import annotations

import tomllib
from typing import TYPE_CHECKING

import pytest

from src.config.base import get_section, load_toml, load_toml_or_empty

if TYPE_CHECKING:
    from pathlib import Path


def _write_toml(tmp_path: Path, text: str, name: str = "config.toml") -> Path:
    p = tmp_path / name
    p.write_text(text, encoding="utf-8")
    return p


class TestLoadToml:
    """Tests for load_toml."""

    def test_load_valid_toml(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [section]
            key = "value"
            number = 42
            """,
        )

        result = load_toml(path)

        assert isinstance(result, dict)
        assert result["section"]["key"] == "value"
        assert result["section"]["number"] == 42

    def test_load_empty_toml(self, tmp_path: Path) -> None:
        path = _write_toml(tmp_path, "")

        result = load_toml(path)

        assert result == {}

    def test_load_toml_with_nested_structures(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [a]
            b = 1
            [a.nested]
            c = "deep"
            """,
        )

        result = load_toml(path)

        assert result["a"]["b"] == 1
        assert result["a"]["nested"]["c"] == "deep"

    def test_load_toml_with_arrays(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            items = [1, 2, 3]
            strings = ["a", "b"]
            """,
        )

        result = load_toml(path)

        assert result["items"] == [1, 2, 3]
        assert result["strings"] == ["a", "b"]

    def test_load_toml_missing_file_raises(self, tmp_path: Path) -> None:
        missing = tmp_path / "missing.toml"

        with pytest.raises(FileNotFoundError):
            load_toml(missing)

    def test_load_toml_invalid_content_raises(self, tmp_path: Path) -> None:
        path = _write_toml(tmp_path, "not a valid [[[ toml")

        with pytest.raises(tomllib.TOMLDecodeError):
            load_toml(path)

    def test_load_toml_utf8(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            'value = "café résumé 日本語"',
        )

        result = load_toml(path)

        assert result["value"] == "café résumé 日本語"


class TestLoadTomlOrEmpty:
    """Tests for load_toml_or_empty."""

    def test_none_returns_empty(self) -> None:
        assert load_toml_or_empty(None) == {}

    def test_nonexistent_file_returns_empty(self, tmp_path: Path) -> None:
        missing = tmp_path / "missing.toml"
        assert load_toml_or_empty(missing) == {}

    def test_existing_file_returns_parsed_content(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [section]
            key = "value"
            """,
        )

        result = load_toml_or_empty(path)

        assert result["section"]["key"] == "value"

    def test_existing_empty_file_returns_empty(self, tmp_path: Path) -> None:
        path = _write_toml(tmp_path, "")

        result = load_toml_or_empty(path)

        assert result == {}


class TestGetSection:
    """Tests for get_section."""

    def test_existing_section_returned(self) -> None:
        doc = {"a": {"x": 1}, "b": {"y": 2}}

        assert get_section(doc, "a") == {"x": 1}
        assert get_section(doc, "b") == {"y": 2}

    def test_missing_key_returns_empty_dict_by_default(self) -> None:
        doc = {"a": {"x": 1}}

        assert get_section(doc, "missing") == {}

    def test_missing_key_returns_explicit_default(self) -> None:
        doc: dict = {}
        default = {"defaulted": True}

        assert get_section(doc, "missing", default=default) == default

    def test_default_none_results_in_empty_dict(self) -> None:
        # default=None triggers the `default or {}` fallback
        doc: dict = {}

        assert get_section(doc, "missing", default=None) == {}

    def test_empty_doc_with_no_default(self) -> None:
        assert get_section({}, "any_key") == {}

    def test_returns_section_as_is_when_present(self) -> None:
        nested = {"inner": {"deeper": True}}
        doc = {"outer": nested}

        result = get_section(doc, "outer")

        # Code returns the same object (not a copy)
        assert result is nested

    def test_falsy_default_dict_returns_empty(self) -> None:
        # `default or {}` means an empty default dict falls back to a new empty {}
        assert get_section({}, "missing", default={}) == {}
