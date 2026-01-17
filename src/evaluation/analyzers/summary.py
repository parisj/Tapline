from __future__ import annotations

import io
import math
import statistics
from typing import Any

import numpy as np

from src.evaluation.analyzers.base import Analyzer
from src.domain.results import Artifact


class SummaryAnalyzer(Analyzer):
    """Numeric summary statistics.

    Output keys:
      count, missing, mean, median, min, max, std, variance
    """

    def run(self, *, values: list[Any], meta: dict[str, Any]) -> dict[str, Any]:
        nums: list[float] = []
        missing = 0

        for v in values:
            if v is None:
                missing += 1
                continue
            if isinstance(v, bool):
                # bool is int subclass; treat as non-numeric here to avoid polluting stats
                missing += 1
                continue
            if isinstance(v, (int, float)) and math.isfinite(float(v)):
                nums.append(float(v))
            else:
                missing += 1

        if not nums:
            return {
                "summary": {
                    "count": 0,
                    "missing": 0,
                    "mean": 0,
                    "median": 0,
                    "min": 0,
                    "max": 0,
                    "std": 0,
                    "variance": 0,
                },
                "artifact": None,
            }

        mean = statistics.fmean(nums)
        median = statistics.median(nums)
        vmin = min(nums)
        vmax = max(nums)

        # Use sample variance/std if n>=2 else 0
        if len(nums) >= 2:
            variance = statistics.variance(nums)
            std = statistics.stdev(nums)
        else:
            variance = 0.0
            std = 0.0
        values = np.array(nums)
        buf = io.BytesIO()
        np.savez_compressed(buf, value=values)


        return {
            "summary": {
                "count": len(nums),
                "missing": missing,
                "mean": mean,
                "median": median,
                "min": vmin,
                "max": vmax,
                "std": std,
                "variance": variance,
            },

            "artifact": Artifact(
                name="summary.npz",
                mime="summary/x-npz",
                data=buf.getvalue(),
            ),
        }
