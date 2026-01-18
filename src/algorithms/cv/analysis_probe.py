from __future__ import annotations

import json
import math
from typing import TYPE_CHECKING, Any

from src.algorithms.base import Algorithm
from src.domain.evaluation import AnalysisKind
from src.domain.results import AlgoResult, Artifact, MetricValue

if TYPE_CHECKING:
    from collections.abc import Mapping


class AnalysisProbeAlgo(Algorithm):
    """Test / probe algorithm.

    Purpose:
    - Emit metrics that exercise *all* analysis kinds
    - Provide deterministic outputs for unit & integration tests
    - Validate evaluator routing and persistence
    """

    @property
    def name(self) -> str:
        return "analysis_probe"

    @property
    def version(self) -> str:
        return "0.1.0"

    def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> AlgoResult:
        n_bytes = len(image_bytes)

        # --- 1D numeric metric (summary + histogram + outliers)
        score = (n_bytes % 1000) / 1000.0

        # --- Boolean / counter metric
        passed = score > float(settings.get("pass_threshold", 0.5))

        # --- Rate / trend metric (countable over time)
        event_count = int(settings.get("event_count", 1))

        # --- 2D metric for ellipse / contour
        # Deterministic pseudo-position derived from input size
        x = math.sin(n_bytes) * 10.0
        y = math.cos(n_bytes) * 10.0

        # --- Histogram-style metric (explicit binning later)
        intensity = n_bytes % 256

        return AlgoResult(
            metrics={
                # ============================
                # Scalar numeric
                # ============================
                "score": MetricValue(
                    value=score,
                    analysis=(
                        AnalysisKind.SUMMARY
                        | AnalysisKind.DISTRIBUTION_1D
                        | AnalysisKind.OUTLIERS_1D
                    ),
                    meta={
                        "units": "normalized",
                        "range": [0.0, 1.0],
                    },
                ),

                # ============================
                # Boolean / counter
                # ============================
                "passed": MetricValue(
                    value=passed,
                    analysis=AnalysisKind.COUNTER
                    | AnalysisKind.RATE,
                    meta={
                        "true_label": "pass",
                        "false_label": "fail",
                    },
                ),
                # ============================
                # Explicit count (rate/trend)
                # ============================
                "event_count": MetricValue(
                    value=event_count,
                    analysis=AnalysisKind.RATE
                    | AnalysisKind.SUMMARY,
                ),
                # ============================
                # 1D histogram value
                # ============================
                "intensity": MetricValue(
                    value=intensity,
                    analysis=AnalysisKind.DISTRIBUTION_1D | AnalysisKind.SUMMARY,
                    meta={
                        "bins": 32,
                        "range": [0, 255],
                    },
                ),
                # ============================
                # 2D spatial metric
                # ============================
                "center_xy": MetricValue(
                    value={"x": x, "y": y},
                    analysis=AnalysisKind.ELLIPSE_2D | AnalysisKind.CONTOUR_2D,
                    meta={
                        "coordinate_system": "image",
                        "units": "pixels",
                    },
                ),
            },
            # Generate a JSON report artifact for MinIO storage
            artifacts=(
                Artifact(
                    name="analysis_report.json",
                    mime="application/json",
                    data=json.dumps({
                        "algorithm": "analysis_probe",
                        "version": "0.1.0",
                        "input_size_bytes": n_bytes,
                        "score": score,
                        "passed": passed,
                        "center": {"x": x, "y": y},
                    }, indent=2).encode("utf-8"),
                ),
            ),
        )
