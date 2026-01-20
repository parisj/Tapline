"""Edge detection algorithm.

Detects edges in images using various methods (Canny, Sobel, Laplacian)
and returns metrics about edge density, count, and orientation.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import cv2
import numpy as np

from src.algorithms.base import Algorithm
from src.domain.evaluation import AnalysisKind
from src.domain.results import AlgoResult, Artifact, MetricValue
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping

logger = get_logger(__name__)


class EdgeDetectionAlgo(Algorithm):
    """Edge detection algorithm using OpenCV.

    Supports multiple edge detection methods and returns metrics
    about edge characteristics.
    """

    _method: str
    _canny_threshold1: float
    _canny_threshold2: float
    _sobel_ksize: int
    _min_edge_density: float

    @property
    def name(self) -> str:
        return "edge_detection"

    @property
    def version(self) -> str:
        return "1.0.0"

    def initialize(self, settings: Mapping[str, Any]) -> None:
        """Initialize with edge detection parameters from settings."""
        detector_cfg = settings.get("detector", {})

        self._method = str(detector_cfg.get("method", "canny")).lower()
        self._canny_threshold1 = float(detector_cfg.get("canny_threshold1", 100))
        self._canny_threshold2 = float(detector_cfg.get("canny_threshold2", 200))
        self._sobel_ksize = int(detector_cfg.get("sobel_ksize", 3))

        algorithm_cfg = settings.get("algorithm", {})
        self._min_edge_density = float(algorithm_cfg.get("min_edge_density", 0.01))

        logger.info(
            "EdgeDetectionAlgo initialized: method=%s, canny=[%.1f,%.1f], sobel_k=%d",
            self._method,
            self._canny_threshold1,
            self._canny_threshold2,
            self._sobel_ksize,
        )

    def _detect_edges_canny(self, gray: np.ndarray) -> np.ndarray:
        """Detect edges using Canny edge detector."""
        return cv2.Canny(gray, self._canny_threshold1, self._canny_threshold2)

    def _detect_edges_sobel(self, gray: np.ndarray) -> np.ndarray:
        """Detect edges using Sobel operator."""
        sobel_x = cv2.Sobel(gray, cv2.CV_64F, 1, 0, ksize=self._sobel_ksize)
        sobel_y = cv2.Sobel(gray, cv2.CV_64F, 0, 1, ksize=self._sobel_ksize)
        magnitude = np.sqrt(sobel_x**2 + sobel_y**2)
        # Normalize to 0-255 range
        magnitude = np.clip(magnitude / magnitude.max() * 255, 0, 255).astype(np.uint8)
        # Apply threshold to get binary edge map
        _, edges = cv2.threshold(magnitude, 50, 255, cv2.THRESH_BINARY)
        return np.asarray(edges)

    def _detect_edges_laplacian(self, gray: np.ndarray) -> np.ndarray:
        """Detect edges using Laplacian operator."""
        laplacian = cv2.Laplacian(gray, cv2.CV_64F)
        # Take absolute value and normalize
        laplacian = np.abs(laplacian)
        laplacian = np.clip(laplacian / laplacian.max() * 255, 0, 255).astype(np.uint8)
        # Apply threshold
        _, edges = cv2.threshold(laplacian, 30, 255, cv2.THRESH_BINARY)
        return np.asarray(edges)

    def _compute_edge_density(self, edges: np.ndarray) -> float:
        """Compute ratio of edge pixels to total pixels."""
        total_pixels = edges.size
        edge_pixels = np.count_nonzero(edges)
        return float(edge_pixels / total_pixels) if total_pixels > 0 else 0.0

    def _count_contours(self, edges: np.ndarray) -> int:
        """Count distinct contours/edges in the edge map."""
        contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        return len(contours)

    def _compute_dominant_orientation(self, gray: np.ndarray) -> float:
        """Compute dominant edge orientation in degrees (0-180).

        Uses gradient direction histogram to find the most common orientation.
        """
        # Compute gradients
        sobel_x = cv2.Sobel(gray, cv2.CV_64F, 1, 0, ksize=3)
        sobel_y = cv2.Sobel(gray, cv2.CV_64F, 0, 1, ksize=3)

        # Compute gradient magnitude and direction
        magnitude = np.sqrt(sobel_x**2 + sobel_y**2)
        direction = np.arctan2(sobel_y, sobel_x) * 180 / np.pi

        # Convert to 0-180 range (edges are undirected)
        direction = np.mod(direction, 180)

        # Weight directions by magnitude and compute histogram
        # Only consider pixels with significant gradient
        threshold = np.percentile(magnitude, 90)
        mask = magnitude > threshold

        if not np.any(mask):
            return 0.0

        # Create weighted histogram of orientations
        hist, bin_edges = np.histogram(direction[mask], bins=36, range=(0, 180), weights=magnitude[mask])

        # Find dominant orientation
        dominant_bin = np.argmax(hist)
        dominant_angle = (bin_edges[dominant_bin] + bin_edges[dominant_bin + 1]) / 2

        return float(dominant_angle)

    def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> AlgoResult:  # noqa: ARG002
        """Detect edges and return metrics."""
        # Decode image
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if img is None:
            return AlgoResult(
                metrics={
                    "edge_density": MetricValue(
                        value=0.0,
                        analysis=AnalysisKind.SUMMARY | AnalysisKind.DISTRIBUTION_1D,
                        meta={"error": "Failed to decode image"},
                    ),
                    "edge_detection_passed": MetricValue(
                        value=False,
                        analysis=AnalysisKind.COUNTER | AnalysisKind.RATE,
                        meta={"true_label": "pass", "false_label": "fail"},
                    ),
                },
            )

        # Convert to grayscale
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        # Apply Gaussian blur to reduce noise
        gray = cv2.GaussianBlur(gray, (5, 5), 0)

        # Detect edges using selected method
        if self._method == "sobel":
            edges = self._detect_edges_sobel(gray)
        elif self._method == "laplacian":
            edges = self._detect_edges_laplacian(gray)
        else:  # Default to Canny
            edges = self._detect_edges_canny(gray)

        # Compute metrics
        edge_density = self._compute_edge_density(edges)
        edge_count = self._count_contours(edges)
        dominant_orientation = self._compute_dominant_orientation(gray)

        # Pass/fail based on minimum edge density
        edge_detection_passed = bool(edge_density >= self._min_edge_density)

        # Build metrics dict
        metrics: dict[str, MetricValue] = {
            "edge_density": MetricValue(
                value=edge_density,
                analysis=AnalysisKind.SUMMARY | AnalysisKind.DISTRIBUTION_1D,
                meta={
                    "units": "ratio",
                    "range": [0.0, 1.0],
                    "threshold": self._min_edge_density,
                    "method": self._method,
                },
            ),
            "edge_count": MetricValue(
                value=edge_count,
                analysis=AnalysisKind.SUMMARY | AnalysisKind.DISTRIBUTION_1D,
                meta={"units": "count", "method": self._method},
            ),
            "dominant_orientation": MetricValue(
                value=dominant_orientation,
                analysis=AnalysisKind.SUMMARY | AnalysisKind.DISTRIBUTION_1D,
                meta={
                    "units": "degrees",
                    "range": [0.0, 180.0],
                    "description": "Most common edge direction",
                },
            ),
            "edge_detection_passed": MetricValue(
                value=edge_detection_passed,
                analysis=AnalysisKind.COUNTER | AnalysisKind.RATE,
                meta={
                    "true_label": "pass",
                    "false_label": "fail",
                    "threshold": self._min_edge_density,
                    "edge_density": edge_density,
                    "edge_count": edge_count,
                    "dominant_orientation": dominant_orientation,
                },
            ),
        }

        # Encode edge map as PNG for artifact
        _, edge_png = cv2.imencode(".png", edges)

        # Generate JSON report
        report_data = {
            "algorithm": self.name,
            "version": self.version,
            "method": self._method,
            "metrics": {
                "edge_density": edge_density,
                "edge_count": edge_count,
                "dominant_orientation": dominant_orientation,
            },
            "edge_detection_passed": edge_detection_passed,
            "image_shape": list(img.shape),
            "parameters": {
                "method": self._method,
                "canny_threshold1": self._canny_threshold1,
                "canny_threshold2": self._canny_threshold2,
                "sobel_ksize": self._sobel_ksize,
            },
        }

        return AlgoResult(
            metrics=metrics,
            artifacts=(
                Artifact(
                    name="edge_map.png",
                    mime="image/png",
                    data=edge_png.tobytes(),
                ),
                Artifact(
                    name="edge_detection_report.json",
                    mime="application/json",
                    data=json.dumps(report_data, indent=2).encode("utf-8"),
                ),
            ),
        )
