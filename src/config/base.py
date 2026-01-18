"""Base configuration loader utilities.

Provides common patterns for TOML configuration loading.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

T = TypeVar("T")


def load_toml(path: Path) -> dict[str, Any]:
    """Load TOML file and return parsed dict.

    Args:
        path: Path to TOML file

    Returns:
        Parsed TOML as dict

    Raises:
        FileNotFoundError: If file doesn't exist
        tomllib.TOMLDecodeError: If file is invalid TOML

    """
    return tomllib.loads(path.read_text(encoding="utf-8"))


def load_toml_or_empty(path: Path | None) -> dict[str, Any]:
    """Load TOML file or return empty dict if not found.

    Args:
        path: Path to TOML file (or None)

    Returns:
        Parsed TOML as dict, or empty dict

    """
    if path is None or not path.exists():
        return {}
    return load_toml(path)


def get_section(doc: dict[str, Any], key: str, default: dict | None = None) -> dict[str, Any]:
    """Get a section from TOML doc with default fallback.

    Args:
        doc: Parsed TOML dict
        key: Section key
        default: Default value if section missing

    Returns:
        Section dict or default

    """
    return doc.get(key, default or {})
