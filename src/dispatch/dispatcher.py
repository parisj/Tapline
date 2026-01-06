from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

    from src.algorithms.base import Algorithm
    from src.domain.jobs import Job


@dataclass(frozen=True)
class DispatchPlan:
    """Maps an ingest job to one algorithm + settings for that directory."""

    algo: Algorithm
    settings: Mapping[str, Any]


class Dispatcher:
    """Maps directory_key -> DispatchPlan."""

    def __init__(self, routes: Mapping[str, DispatchPlan]) -> None:
        self._routes = routes

    def dispatch(self, job: Job) -> DispatchPlan:
        if job.directory_key not in self._routes:
            msg = f"No dispatch route for directory_key={job.directory_key}"
            raise KeyError(msg)
        return self._routes[job.directory_key]
