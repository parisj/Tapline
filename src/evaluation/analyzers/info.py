from __future__ import annotations

from typing import Any

from src.evaluation.analyzers.base import Analyzer, AnalyzerResult


class InfoAnalyzer(Analyzer):
    """Information about categorical metrics.

    Output keys:
      count, missing, unique, top (list of [value, count])
    """

    def run(self, *, values: list[Any], meta: dict[str, Any]) -> AnalyzerResult:
        return {"summary": {"info": dict(meta)}, "artifact": None}
