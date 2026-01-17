from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, TypedDict

if TYPE_CHECKING:
    from src.domain.results import Artifact


class Analyzer(Protocol):
    """Contract for evaluation analyzers.

    An analyzer consumes a list of values for one metric over a time window and returns
    derived/aggregate data suitable for persistence and later visualization.

    Implementations must be:
    - deterministic (for same input)
    - side-effect free (no DB writes, no file writes)
    - JSON-serializable output
    """

    def run(self, *, values: list[Any], meta: dict[str, Any]) -> AnalyzerResult: ...


class AnalyzerResult(TypedDict, total=False):
    summary: dict[str, Any]
    artifact: Artifact | None

