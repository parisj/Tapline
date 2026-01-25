"""Image quality assessment processor.

Computes various image quality metrics useful for filtering or preprocessing decisions:
- Sharpness (Laplacian variance for blur detection)
- Brightness (mean pixel intensity)
- Contrast (standard deviation of intensities)
- Noise estimate (high-frequency content ratio)
- Saturation (color saturation level)
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


class ImageQualityProcessor(Processor):
    """Image quality assessment processor.

    Computes quality metrics for images to support filtering decisions
    and quality control workflows.
    """

    @property
    def name(self) -> str:
        return "image_quality"

    @property
    def version(self) -> str:
        return "1.0.0"

    def initialize(self, settings: Mapping[str, Any]) -> None:
        """Initialize with quality thresholds from settings."""
        thresholds = settings.get("thresholds", {})
        self._min_sharpness = float(thresholds.get("min_sharpness", 100.0))
        self._min_brightness = float(thresholds.get("min_brightness", 30.0))
        self._max_brightness = float(thresholds.get("max_brightness", 225.0))
        self._min_contrast = float(thresholds.get("min_contrast", 20.0))
        self._max_noise = float(thresholds.get("max_noise", 0.3))
        self._min_saturation = float(thresholds.get("min_saturation", 0.1))

        logger.info(
            "ImageQualityProcessor initialized with thresholds: "
            "sharpness>%.1f, brightness=[%.1f,%.1f], contrast>%.1f, noise<%.2f, saturation>%.2f",
            self._min_sharpness,
            self._min_brightness,
            self._max_brightness,
            self._min_contrast,
            self._max_noise,
            self._min_saturation,
        )

    def _compute_sharpness(self, gray: np.ndarray) -> float:
        """Compute sharpness using Laplacian variance.

        Higher values indicate sharper images; low values indicate blur.
        """
        laplacian = cv2.Laplacian(gray, cv2.CV_64F)
        return float(laplacian.var())

    def _compute_brightness(self, gray: np.ndarray) -> float:
        """Compute mean brightness (0-255 scale)."""
        return float(np.mean(gray))

    def _compute_contrast(self, gray: np.ndarray) -> float:
        """Compute contrast as standard deviation of intensities."""
        return float(np.std(gray))

    def _compute_noise_estimate(self, gray: np.ndarray) -> float:
        """Estimate noise level using high-frequency content ratio.

        Uses the ratio of high-frequency energy (from Laplacian) to total energy.
        Higher values indicate more noise.
        """
        # Compute Laplacian (high-frequency content)
        laplacian = cv2.Laplacian(gray, cv2.CV_64F)
        high_freq_energy = np.sum(laplacian**2)

        # Total energy from original image
        total_energy = np.sum(gray.astype(np.float64) ** 2)

        if total_energy == 0:
            return 0.0

        # Normalize to 0-1 range (approximately)
        ratio = high_freq_energy / total_energy
        # Clamp and scale to reasonable range
        return float(min(1.0, ratio / 100.0))

    def _compute_saturation(self, img: np.ndarray) -> float:
        """Compute mean saturation from HSV color space.

        Returns value in 0-1 range. Grayscale images return 0.
        """
        if len(img.shape) < 3 or img.shape[2] < 3:
            return 0.0

        hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
        saturation_channel = hsv[:, :, 1]
        return float(np.mean(saturation_channel.astype(np.float64)) / 255.0)

    def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> ProcessorResult:  # noqa: ARG002
        """Compute image quality metrics."""
        # Decode image
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if img is None:
            return ProcessorResult(
                metrics={
                    "sharpness": Measurement(
                        value=0.0,
                        aggregation=AggregationType.STATS | AggregationType.HISTOGRAM | AggregationType.OUTLIERS,
                        meta={"error": "Failed to decode image"},
                    ),
                    "quality_passed": Measurement(
                        value=False,
                        aggregation=AggregationType.TALLY | AggregationType.RATE,
                        meta={"true_label": "pass", "false_label": "fail"},
                    ),
                },
            )

        # Convert to grayscale for most metrics
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        # Compute all metrics
        sharpness = self._compute_sharpness(gray)
        brightness = self._compute_brightness(gray)
        contrast = self._compute_contrast(gray)
        noise_estimate = self._compute_noise_estimate(gray)
        saturation = self._compute_saturation(img)

        # Determine pass/fail based on thresholds
        quality_passed = bool(
            sharpness >= self._min_sharpness
            and self._min_brightness <= brightness <= self._max_brightness
            and contrast >= self._min_contrast
            and noise_estimate <= self._max_noise
            and saturation >= self._min_saturation,
        )

        # Build metrics dict
        metrics: dict[str, Measurement] = {
            "sharpness": Measurement(
                value=sharpness,
                aggregation=AggregationType.STATS | AggregationType.HISTOGRAM | AggregationType.OUTLIERS,
                meta={
                    "units": "variance",
                    "threshold": self._min_sharpness,
                    "description": "Laplacian variance (higher = sharper)",
                },
            ),
            "brightness": Measurement(
                value=brightness,
                aggregation=AggregationType.STATS | AggregationType.HISTOGRAM,
                meta={
                    "units": "intensity",
                    "range": [0.0, 255.0],
                    "threshold_min": self._min_brightness,
                    "threshold_max": self._max_brightness,
                },
            ),
            "contrast": Measurement(
                value=contrast,
                aggregation=AggregationType.STATS | AggregationType.HISTOGRAM,
                meta={
                    "units": "std_dev",
                    "threshold": self._min_contrast,
                },
            ),
            "noise_estimate": Measurement(
                value=noise_estimate,
                aggregation=AggregationType.STATS | AggregationType.OUTLIERS,
                meta={
                    "units": "ratio",
                    "range": [0.0, 1.0],
                    "threshold": self._max_noise,
                    "description": "High-frequency energy ratio (lower = less noise)",
                },
            ),
            "saturation": Measurement(
                value=saturation,
                aggregation=AggregationType.STATS | AggregationType.HISTOGRAM,
                meta={
                    "units": "ratio",
                    "range": [0.0, 1.0],
                    "threshold": self._min_saturation,
                },
            ),
            "quality_passed": Measurement(
                value=quality_passed,
                aggregation=AggregationType.TALLY | AggregationType.RATE,
                meta={
                    "true_label": "pass",
                    "false_label": "fail",
                    "thresholds": {
                        "min_sharpness": self._min_sharpness,
                        "min_brightness": self._min_brightness,
                        "max_brightness": self._max_brightness,
                        "min_contrast": self._min_contrast,
                        "max_noise": self._max_noise,
                        "min_saturation": self._min_saturation,
                    },
                    "measured": {
                        "sharpness": sharpness,
                        "brightness": brightness,
                        "contrast": contrast,
                        "noise_estimate": noise_estimate,
                        "saturation": saturation,
                    },
                },
            ),
        }

        # Generate JSON artifact with detailed results
        artifact_data = {
            "algorithm": self.name,
            "version": self.version,
            "metrics": {
                "sharpness": sharpness,
                "brightness": brightness,
                "contrast": contrast,
                "noise_estimate": noise_estimate,
                "saturation": saturation,
            },
            "quality_passed": quality_passed,
            "image_shape": list(img.shape),
            "thresholds": {
                "min_sharpness": self._min_sharpness,
                "min_brightness": self._min_brightness,
                "max_brightness": self._max_brightness,
                "min_contrast": self._min_contrast,
                "max_noise": self._max_noise,
                "min_saturation": self._min_saturation,
            },
        }

        return ProcessorResult(
            metrics=metrics,
            artifacts=(
                Artifact(
                    name="image_quality_report.json",
                    mime="application/json",
                    data=json.dumps(artifact_data, indent=2).encode("utf-8"),
                ),
            ),
        )


# Backwards compatibility alias
ImageQualityAlgo = ImageQualityProcessor
