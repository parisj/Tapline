import io
from typing import Any

import numpy as np

from src.domain.results import Artifact
from src.evaluation.analyzers.base import Analyzer, AnalyzerResult


class HistogramAnalyzer(Analyzer):
    def run(self, *, values: list[Any], meta: dict[str, Any]) -> AnalyzerResult:
        nums = np.array([v for v in values if isinstance(v, (int, float))])

        if nums.size == 0:
            return {
                "summary": {"count": 0, "missing": len(values)},
            }
        missing = len(values) - len(nums)
        bins = int(meta.get("bins", 50))
        counts, edges = np.histogram(nums, bins=bins)

        # ---- binary artifact (for Bokeh) ----
        buf = io.BytesIO()
        np.savez_compressed(buf, edges=edges, counts=counts)

        return {
            "summary": {
                "count": int(nums.size),
                "mean": float(nums.mean()),
                "std": float(nums.std(ddof=1)) if nums.size > 1 else 0.0,
                "bins": bins,
                "missing": missing,
            },
            "artifact": Artifact(
                name="histogram.npz",
                mime="application/x-npz",
                data=buf.getvalue(),
            ),
        }
