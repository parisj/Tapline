"""Contour analysis processor.

Detects and analyzes contours (shapes) in images:
- Contour count
- Largest contour area
- Total contour area
- Average circularity
- Centroid positions (for 2D spatial analysis)
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import cv2
import numpy as np

from src.algorithms.base import Processor
from src.domain.evaluation import AggregationType
from src.domain.results import Artifact, Measurement, ProcessorResult
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping

logger = get_logger(__name__)


class ContourAnalysisProcessor(Processor):
    """Contour analysis processor using OpenCV.

    Detects contours and computes shape metrics useful for
    object detection and spatial analysis.
    """

    _min_contour_area: float
    _max_contour_area: float
    _threshold_method: str
    _threshold_value: int
    _min_contour_count: int

    @property
    def name(self) -> str:
        return "contour_analysis"

    @property
    def version(self) -> str:
        return "1.0.0"

    def initialize(self, settings: Mapping[str, Any]) -> None:
        """Initialize with contour detection parameters from settings."""
        detector_cfg = settings.get("detector", {})

        self._min_contour_area = float(detector_cfg.get("min_contour_area", 100))
        self._max_contour_area = float(detector_cfg.get("max_contour_area", 1e7))
        self._threshold_method = str(detector_cfg.get("threshold_method", "otsu")).lower()
        self._threshold_value = int(detector_cfg.get("threshold_value", 127))

        algorithm_cfg = settings.get("algorithm", {})
        self._min_contour_count = int(algorithm_cfg.get("min_contour_count", 1))

        logger.info(
            "ContourAnalysisProcessor initialized: area=[%.1f,%.1f], thresh=%s, min_count=%d",
            self._min_contour_area,
            self._max_contour_area,
            self._threshold_method,
            self._min_contour_count,
        )

    def _threshold_image(self, gray: np.ndarray) -> np.ndarray:
        """Apply thresholding to create binary image."""
        if self._threshold_method == "otsu":
            _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        elif self._threshold_method == "adaptive":
            binary = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2)
        else:  # Simple threshold
            _, binary = cv2.threshold(gray, self._threshold_value, 255, cv2.THRESH_BINARY)

        return binary

    def _compute_circularity(self, contour: np.ndarray) -> float:
        """Compute circularity of a contour.

        Circularity = 4 * pi * Area / Perimeter^2
        Perfect circle = 1.0, less circular shapes < 1.0
        """
        area = cv2.contourArea(contour)
        perimeter = cv2.arcLength(contour, closed=True)

        if perimeter == 0:
            return 0.0

        circularity = 4 * np.pi * area / (perimeter**2)
        return float(min(1.0, circularity))  # Clamp to 1.0 max

    def _compute_centroid(self, contour: np.ndarray) -> tuple[float, float]:
        """Compute centroid of a contour using moments."""
        moments = cv2.moments(contour)
        if moments["m00"] == 0:
            # Fallback to bounding box center
            x, y, w, h = cv2.boundingRect(contour)
            return float(x + w / 2), float(y + h / 2)

        cx = moments["m10"] / moments["m00"]
        cy = moments["m01"] / moments["m00"]
        return float(cx), float(cy)

    def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> ProcessorResult:  # noqa: ARG002
        """Detect and analyze contours."""
        # Decode image
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if img is None:
            return ProcessorResult(
                metrics={
                    "contour_count": Measurement(
                        value=0,
                        aggregation=AggregationType.STATS | AggregationType.HISTOGRAM,
                        meta={"error": "Failed to decode image"},
                    ),
                    "contour_detected": Measurement(
                        value=False,
                        aggregation=AggregationType.TALLY | AggregationType.RATE,
                        meta={"true_label": "detected", "false_label": "none"},
                    ),
                },
            )

        # Convert to grayscale
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        # Apply Gaussian blur to reduce noise
        gray = cv2.GaussianBlur(gray, (5, 5), 0)

        # Threshold to create binary image
        binary = self._threshold_image(gray)

        # Find contours
        contours, _ = cv2.findContours(binary, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        # Filter contours by area
        valid_contours = [c for c in contours if self._min_contour_area <= cv2.contourArea(c) <= self._max_contour_area]

        contour_count = len(valid_contours)
        contour_detected = bool(contour_count >= self._min_contour_count)

        # Compute metrics
        areas = [cv2.contourArea(c) for c in valid_contours]
        circularities = [self._compute_circularity(c) for c in valid_contours]

        largest_contour_area = max(areas) if areas else 0.0
        total_contour_area = sum(areas) if areas else 0.0
        avg_circularity = float(np.mean(circularities)) if circularities else 0.0

        # Compute centroid of largest contour (for 2D spatial analysis)
        if valid_contours:
            largest_idx = np.argmax(areas)
            centroid_x, centroid_y = self._compute_centroid(valid_contours[largest_idx])
        else:
            centroid_x, centroid_y = 0.0, 0.0

        # Build contour data for artifact
        contour_data = []
        for i, contour in enumerate(valid_contours):
            cx, cy = self._compute_centroid(contour)
            contour_data.append(
                {
                    "index": i,
                    "area": float(cv2.contourArea(contour)),
                    "perimeter": float(cv2.arcLength(contour, closed=True)),
                    "circularity": self._compute_circularity(contour),
                    "centroid": {"x": cx, "y": cy},
                    "bounding_box": list(cv2.boundingRect(contour)),
                },
            )

        # Build metrics dict
        metrics: dict[str, Measurement] = {
            "contour_count": Measurement(
                value=contour_count,
                aggregation=AggregationType.STATS | AggregationType.HISTOGRAM,
                meta={
                    "units": "count",
                    "threshold": self._min_contour_count,
                },
            ),
            "largest_contour_area": Measurement(
                value=largest_contour_area,
                aggregation=AggregationType.STATS | AggregationType.HISTOGRAM | AggregationType.OUTLIERS,
                meta={
                    "units": "pixels_squared",
                    "min_area": self._min_contour_area,
                    "max_area": self._max_contour_area,
                },
            ),
            "total_contour_area": Measurement(
                value=total_contour_area,
                aggregation=AggregationType.STATS | AggregationType.HISTOGRAM,
                meta={"units": "pixels_squared"},
            ),
            "avg_contour_circularity": Measurement(
                value=avg_circularity,
                aggregation=AggregationType.STATS | AggregationType.HISTOGRAM,
                meta={
                    "units": "ratio",
                    "range": [0.0, 1.0],
                    "description": "Mean circularity (1.0 = perfect circle)",
                },
            ),
            "contour_centroid_xy": Measurement(
                value={"x": centroid_x, "y": centroid_y},
                aggregation=AggregationType.SCATTER_ELLIPSE | AggregationType.DENSITY_MAP,
                meta={
                    "coordinate_system": "image",
                    "units": "pixels",
                    "contour_count": contour_count,
                    "description": "Centroid of largest contour",
                },
            ),
            "contour_detected": Measurement(
                value=contour_detected,
                aggregation=AggregationType.TALLY | AggregationType.RATE,
                meta={
                    "true_label": "detected",
                    "false_label": "none",
                    "threshold": self._min_contour_count,
                    "contour_count": contour_count,
                    "largest_area": largest_contour_area,
                    "total_area": total_contour_area,
                },
            ),
        }

        # Generate JSON artifact with detailed results
        artifact_data = {
            "algorithm": self.name,
            "version": self.version,
            "metrics": {
                "contour_count": contour_count,
                "largest_contour_area": largest_contour_area,
                "total_contour_area": total_contour_area,
                "avg_contour_circularity": avg_circularity,
            },
            "contour_detected": contour_detected,
            "contours": contour_data,
            "image_shape": list(img.shape),
            "parameters": {
                "min_contour_area": self._min_contour_area,
                "max_contour_area": self._max_contour_area,
                "threshold_method": self._threshold_method,
                "threshold_value": self._threshold_value,
            },
        }

        return ProcessorResult(
            metrics=metrics,
            artifacts=(
                Artifact(
                    name="contour_analysis_report.json",
                    mime="application/json",
                    data=json.dumps(artifact_data, indent=2).encode("utf-8"),
                ),
            ),
        )


# Backwards compatibility alias
ContourAnalysisAlgo = ContourAnalysisProcessor
