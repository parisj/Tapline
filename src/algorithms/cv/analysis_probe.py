from __future__ import annotations

import json
import math
from typing import TYPE_CHECKING, Any

from src.algorithms.base import Processor
from src.domain.evaluation import AggregationType
from src.domain.results import Artifact, Measurement, ProcessorResult

if TYPE_CHECKING:
    from collections.abc import Mapping


class AnalysisProbeProcessor(Processor):
    """Test / probe processor.

    Purpose:
    - Emit metrics that exercise *all* aggregation types
    - Provide deterministic outputs for unit & integration tests
    - Validate evaluator routing and storage
    """

    @property
    def name(self) -> str:
        return "analysis_probe"

    @property
    def version(self) -> str:
        return "1.0.0"

    def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> ProcessorResult:
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

        return ProcessorResult(
            metrics={
                # ============================
                # Scalar numeric
                # ============================
                "score": Measurement(
                    value=score,
                    aggregation=(AggregationType.STATS | AggregationType.HISTOGRAM | AggregationType.OUTLIERS),
                    meta={
                        "units": "normalized",
                        "range": [0.0, 1.0],
                    },
                ),
                # ============================
                # Boolean / counter
                # ============================
                "passed": Measurement(
                    value=passed,
                    aggregation=AggregationType.TALLY | AggregationType.RATE,
                    meta={
                        "true_label": "pass",
                        "false_label": "fail",
                    },
                ),
                # ============================
                # Explicit count (rate/trend)
                # ============================
                "event_count": Measurement(
                    value=event_count,
                    aggregation=AggregationType.RATE | AggregationType.STATS,
                ),
                # ============================
                # 1D histogram value
                # ============================
                "intensity": Measurement(
                    value=intensity,
                    aggregation=AggregationType.HISTOGRAM | AggregationType.STATS,
                    meta={
                        "bins": 32,
                        "range": [0, 255],
                    },
                ),
                # ============================
                # 2D spatial metric
                # ============================
                "center_xy": Measurement(
                    value={"x": x, "y": y},
                    aggregation=AggregationType.SCATTER_ELLIPSE | AggregationType.DENSITY_MAP,
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
                    data=json.dumps(
                        {
                            "processor": "analysis_probe",
                            "version": "1.0.0",
                            "input_size_bytes": n_bytes,
                            "score": score,
                            "passed": passed,
                            "center": {"x": x, "y": y},
                        },
                        indent=2,
                    ).encode("utf-8"),
                ),
            ),
        )


# Backwards compatibility alias (deprecated)
AnalysisProbeAlgo = AnalysisProbeProcessor
