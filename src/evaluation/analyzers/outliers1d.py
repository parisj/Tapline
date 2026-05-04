"""Outlier detection analyzer using IQR method.

Detects outliers in 1D numeric data using the Interquartile Range (IQR) method,
which is robust to extreme values.
"""

from __future__ import annotations

import io
from typing import Any

import numpy as np

from src.domain.results import Artifact
from src.evaluation.analyzers.base import Analyzer, AnalyzerResult

# Default IQR multiplier for outlier detection (1.5 = mild, 3.0 = extreme)
_DEFAULT_IQR_MULTIPLIER = 1.5


class OutliersAnalyzer(Analyzer):
    """Detect outliers using IQR method.

    Output keys:
      count, missing, outlier_count, outlier_indices, q1, q3, iqr, lower_bound, upper_bound
    """

    def run(self, *, values: list[Any], meta: dict[str, Any]) -> AnalyzerResult:
        nums: list[float] = []
        indices: list[int] = []
        missing = 0

        for i, v in enumerate(values):
            if v is None:
                missing += 1
                continue
            if isinstance(v, bool):
                missing += 1
                continue
            if isinstance(v, (int, float)):
                try:
                    fv = float(v)
                    if np.isfinite(fv):
                        nums.append(fv)
                        indices.append(i)
                    else:
                        missing += 1
                except (TypeError, ValueError):
                    missing += 1
            else:
                missing += 1

        if len(nums) < 4:
            # Need at least 4 values for meaningful IQR
            return {
                "summary": {
                    "count": len(nums),
                    "missing": missing,
                    "outlier_count": 0,
                    "outlier_indices": [],
                    "too_few_values": True,
                },
                "artifact": None,
            }

        arr = np.array(nums)
        multiplier = meta.get("iqr_multiplier", _DEFAULT_IQR_MULTIPLIER)

        # Calculate IQR bounds
        q1 = float(np.percentile(arr, 25))
        q3 = float(np.percentile(arr, 75))
        iqr = q3 - q1
        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr

        # Find outliers
        outlier_mask = (arr < lower_bound) | (arr > upper_bound)
        outlier_values = arr[outlier_mask]
        outlier_indices_local = np.where(outlier_mask)[0]

        # Map back to original indices
        outlier_original_indices = [indices[i] for i in outlier_indices_local]

        # Create artifact with outlier data
        buf = io.BytesIO()
        np.savez_compressed(
            buf,
            values=arr,
            outlier_mask=outlier_mask,
            outlier_values=outlier_values,
            outlier_indices=np.array(outlier_original_indices),
        )

        return {
            "summary": {
                "count": len(nums),
                "missing": missing,
                "outlier_count": int(outlier_mask.sum()),
                "outlier_fraction": float(outlier_mask.sum() / len(nums)) if nums else 0.0,
                "q1": q1,
                "q3": q3,
                "iqr": iqr,
                "lower_bound": lower_bound,
                "upper_bound": upper_bound,
                "iqr_multiplier": multiplier,
            },
            "artifact": Artifact(
                name="outliers.npz",
                mime="application/x-npz",
                data=buf.getvalue(),
            ),
        }
