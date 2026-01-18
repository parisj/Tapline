from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from src.dispatch.dispatcher import DispatchPlan
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from src.algorithms.registry import AlgorithmRegistry

logger = get_logger(__name__)

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


class RoutesConfigError(ValueError):
    pass


@dataclass(frozen=True)
class LoadedRoute:
    directory_key: str
    algorithm: str
    version: str
    settings_relpath: str
    settings: Mapping[str, Any]


def load_routes_toml(
    routes_path: Path,
    *,
    config_root: Path | None = None,
) -> dict[str, LoadedRoute]:
    routes_path = routes_path.resolve()
    root = (config_root or routes_path.parent).resolve()

    doc = _read_toml(routes_path, "routes.toml")
    route_table = doc.get("route")
    if not isinstance(route_table, dict) or not route_table:
        msg = "routes.toml must contain non-empty [route.*] sections."
        raise RoutesConfigError(msg)

    out: dict[str, LoadedRoute] = {}
    for directory_key, entry in route_table.items():
        if not isinstance(entry, dict):
            msg = f"[route.{directory_key}] must be a table."
            raise RoutesConfigError(msg)
        algorithm = _require_str(entry, "algorithm", f"[route.{directory_key}]")
        version = _require_str(entry, "version", f"[route.{directory_key}]")
        settings_rel = _require_str(entry, "settings", f"[route.{directory_key}]")

        settings_path = (root / settings_rel).resolve()
        if not settings_path.exists():
            msg = f"Settings not found for {directory_key}: {settings_path}"
            raise RoutesConfigError(msg)

        settings_doc = _read_toml(settings_path, f"settings for {directory_key}")

        out[directory_key] = LoadedRoute(
            directory_key=directory_key,
            algorithm=algorithm,
            version=version,
            settings_relpath=settings_rel,
            settings=settings_doc,
        )

    return out


def build_dispatch_plans(
    loaded_routes: Mapping[str, LoadedRoute],
    *,
    registry: AlgorithmRegistry,
) -> dict[str, DispatchPlan]:
    """Convert LoadedRoute -> DispatchPlan by instantiating algorithms from registry."""
    plans: dict[str, DispatchPlan] = {}
    for key, lr in loaded_routes.items():
        algo = registry.create(lr.algorithm, lr.version)  # validates existence
        plans[key] = DispatchPlan(algo=algo, settings=lr.settings)
    return plans


def _read_toml(path: Path, what: str) -> dict[str, Any]:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except Exception as ex:
        msg = f"Invalid TOML in {what}: {path} :: {ex}"
        raise RoutesConfigError(msg) from ex


def _require_str(entry: Mapping[str, Any], key: str, where: str) -> str:
    value = entry.get(key)
    if not isinstance(value, str) or not value.strip():
        msg = f"{where}: '{key}' must be a non-empty string."
        raise RoutesConfigError(msg)
    return value.strip()
