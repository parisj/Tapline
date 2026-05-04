from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

    from src.algorithms.base import Processor
    from src.domain.tasks import Task


@dataclass(frozen=True)
class DispatchPlan:
    """Maps an ingest task to one processor + settings for that directory."""

    processor: Processor
    settings: Mapping[str, Any]


class Dispatcher:
    """Maps directory_key -> DispatchPlan."""

    def __init__(self, routes: Mapping[str, DispatchPlan]) -> None:
        self._routes = routes

    def dispatch(self, task: Task) -> DispatchPlan:
        if task.directory_key not in self._routes:
            msg = f"No dispatch route for directory_key={task.directory_key}"
            raise KeyError(msg)
        return self._routes[task.directory_key]
