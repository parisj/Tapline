"""Histogram analysis algorithm.

Analyzes color and intensity distributions in images:
- Histogram peaks (number of dominant peaks)
- Histogram spread (distribution width)
- Channel balance (RGB similarity)
- Dynamic range (used intensity range)
- Bimodality detection
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


class HistogramAnalysisAlgo(Algorithm):
    """Histogram analysis algorithm.

    Analyzes intensity and color distributions to characterize
    image content and detect distribution anomalies.
    """

    _num_bins: int
    _peak_threshold: float
    _bimodal_threshold: float

    @property
    def name(self) -> str:
        return "histogram_analysis"

    @property
    def version(self) -> str:
        return "1.0.0"

    def initialize(self, settings: Mapping[str, Any]) -> None:
        """Initialize with histogram parameters from settings."""
        histogram_cfg = settings.get("histogram", {})

        self._num_bins = int(histogram_cfg.get("num_bins", 256))
        self._peak_threshold = float(histogram_cfg.get("peak_threshold", 0.1))
        self._bimodal_threshold = float(histogram_cfg.get("bimodal_threshold", 0.3))

        logger.info(
            "HistogramAnalysisAlgo initialized: bins=%d, peak_thresh=%.2f, bimodal_thresh=%.2f",
            self._num_bins,
            self._peak_threshold,
            self._bimodal_threshold,
        )

    def _compute_histogram(self, channel: np.ndarray) -> np.ndarray:
        """Compute normalized histogram for a single channel."""
        hist = cv2.calcHist([channel], [0], None, [self._num_bins], [0, 256])
        hist = hist.flatten()
        # Normalize to sum to 1
        total = hist.sum()
        if total > 0:
            hist = hist / total
        return hist

    def _count_peaks(self, hist: np.ndarray) -> int:
        """Count significant peaks in histogram.

        A peak is a local maximum above the threshold.
        """
        # Smooth histogram to reduce noise
        kernel_size = max(3, self._num_bins // 32)
        if kernel_size % 2 == 0:
            kernel_size += 1
        smoothed = np.convolve(hist, np.ones(kernel_size) / kernel_size, mode="same")

        # Find local maxima
        peaks = []
        for i in range(1, len(smoothed) - 1):
            is_local_max = smoothed[i] > smoothed[i - 1] and smoothed[i] > smoothed[i + 1]
            if is_local_max and smoothed[i] >= self._peak_threshold * smoothed.max():
                peaks.append(i)

        return len(peaks)

    def _compute_spread(self, hist: np.ndarray) -> float:
        """Compute histogram spread as standard deviation of distribution."""
        bins = np.arange(len(hist))
        mean = np.sum(bins * hist)
        variance = np.sum(((bins - mean) ** 2) * hist)
        return float(np.sqrt(variance))

    def _compute_channel_balance(self, img: np.ndarray) -> float:
        """Compute RGB channel balance (similarity).

        Returns 1.0 for perfectly balanced channels, lower for imbalanced.
        """
        if len(img.shape) < 3 or img.shape[2] < 3:
            return 1.0  # Grayscale is "balanced"

        # Compute mean of each channel
        means = [float(np.mean(img[:, :, i])) for i in range(3)]

        # Compute coefficient of variation (lower = more balanced)
        mean_of_means = np.mean(means)
        if mean_of_means == 0:
            return 1.0

        std_of_means = float(np.std(means))
        mean_val = float(mean_of_means)
        cv = std_of_means / mean_val

        # Convert to 0-1 scale where 1 = perfectly balanced
        # CV of 0.5 or higher is considered very imbalanced
        return max(0.0, 1.0 - cv * 2)

    def _compute_dynamic_range(self, gray: np.ndarray) -> tuple[float, float, float]:
        """Compute dynamic range metrics.

        Returns (min_used, max_used, range_ratio).
        """
        # Find the actual used range (ignoring outliers)
        p1 = float(np.percentile(gray, 1))
        p99 = float(np.percentile(gray, 99))

        range_used = p99 - p1
        range_ratio = range_used / 255.0

        return p1, p99, range_ratio

    def _is_bimodal(self, hist: np.ndarray) -> bool:
        """Detect if histogram is bimodal (two distinct peaks).

        Uses valley detection between peaks.
        """
        # Smooth histogram
        kernel_size = max(3, self._num_bins // 16)
        if kernel_size % 2 == 0:
            kernel_size += 1
        smoothed = np.convolve(hist, np.ones(kernel_size) / kernel_size, mode="same")

        # Find peaks
        peaks = []
        for i in range(1, len(smoothed) - 1):
            is_local_max = smoothed[i] > smoothed[i - 1] and smoothed[i] > smoothed[i + 1]
            if is_local_max and smoothed[i] >= self._peak_threshold * smoothed.max():
                peaks.append((i, smoothed[i]))

        if len(peaks) < 2:
            return False

        # Sort by height and take top 2
        peaks.sort(key=lambda x: x[1], reverse=True)
        peak1_idx, peak1_val = peaks[0]
        peak2_idx, peak2_val = peaks[1]

        # Ensure peak1 is to the left
        if peak1_idx > peak2_idx:
            peak1_idx, peak2_idx = peak2_idx, peak1_idx
            peak1_val, peak2_val = peak2_val, peak1_val

        # Find minimum between peaks
        valley_region = smoothed[peak1_idx:peak2_idx]
        if len(valley_region) == 0:
            return False

        valley_min = valley_region.min()
        smaller_peak = min(peak1_val, peak2_val)

        # Bimodal if valley is significantly lower than both peaks
        if smaller_peak > 0:
            valley_ratio = valley_min / smaller_peak
            return bool(valley_ratio < (1.0 - self._bimodal_threshold))

        return False

    def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> AlgoResult:  # noqa: ARG002
        """Analyze histogram and return metrics."""
        # Decode image
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if img is None:
            return AlgoResult(
                metrics={
                    "histogram_peaks": MetricValue(
                        value=0,
                        analysis=AnalysisKind.SUMMARY | AnalysisKind.COUNTER,
                        meta={"error": "Failed to decode image"},
                    ),
                    "is_bimodal": MetricValue(
                        value=False,
                        analysis=AnalysisKind.COUNTER | AnalysisKind.RATE,
                        meta={"true_label": "bimodal", "false_label": "unimodal"},
                    ),
                },
            )

        # Convert to grayscale for intensity histogram
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        # Compute grayscale histogram
        gray_hist = self._compute_histogram(gray)

        # Compute metrics
        histogram_peaks = self._count_peaks(gray_hist)
        histogram_spread = self._compute_spread(gray_hist)
        channel_balance = self._compute_channel_balance(img)
        min_used, max_used, dynamic_range = self._compute_dynamic_range(gray)
        is_bimodal = self._is_bimodal(gray_hist)

        # Compute per-channel histograms for artifact
        channel_hists = {}
        if len(img.shape) >= 3 and img.shape[2] >= 3:
            for i, name in enumerate(["blue", "green", "red"]):
                channel_hists[name] = self._compute_histogram(img[:, :, i]).tolist()

        # Build metrics dict
        metrics: dict[str, MetricValue] = {
            "histogram_peaks": MetricValue(
                value=histogram_peaks,
                analysis=AnalysisKind.SUMMARY | AnalysisKind.COUNTER,
                meta={
                    "units": "count",
                    "threshold": self._peak_threshold,
                    "description": "Number of significant peaks in intensity histogram",
                },
            ),
            "histogram_spread": MetricValue(
                value=histogram_spread,
                analysis=AnalysisKind.SUMMARY | AnalysisKind.DISTRIBUTION_1D,
                meta={
                    "units": "bins",
                    "range": [0.0, float(self._num_bins / 2)],
                    "description": "Standard deviation of intensity distribution",
                },
            ),
            "channel_balance": MetricValue(
                value=channel_balance,
                analysis=AnalysisKind.SUMMARY | AnalysisKind.OUTLIERS_1D,
                meta={
                    "units": "ratio",
                    "range": [0.0, 1.0],
                    "description": "RGB channel similarity (1.0 = perfectly balanced)",
                },
            ),
            "dynamic_range": MetricValue(
                value=dynamic_range,
                analysis=AnalysisKind.SUMMARY | AnalysisKind.DISTRIBUTION_1D,
                meta={
                    "units": "ratio",
                    "range": [0.0, 1.0],
                    "min_intensity": min_used,
                    "max_intensity": max_used,
                    "description": "Proportion of intensity range used",
                },
            ),
            "is_bimodal": MetricValue(
                value=is_bimodal,
                analysis=AnalysisKind.COUNTER | AnalysisKind.RATE,
                meta={
                    "true_label": "bimodal",
                    "false_label": "unimodal",
                    "threshold": self._bimodal_threshold,
                    "histogram_peaks": histogram_peaks,
                    "histogram_spread": histogram_spread,
                    "dynamic_range": dynamic_range,
                },
            ),
        }

        # Generate JSON artifact with histogram data
        artifact_data = {
            "algorithm": self.name,
            "version": self.version,
            "metrics": {
                "histogram_peaks": histogram_peaks,
                "histogram_spread": histogram_spread,
                "channel_balance": channel_balance,
                "dynamic_range": dynamic_range,
                "is_bimodal": is_bimodal,
            },
            "histograms": {
                "grayscale": gray_hist.tolist(),
                **channel_hists,
            },
            "parameters": {
                "num_bins": self._num_bins,
                "peak_threshold": self._peak_threshold,
                "bimodal_threshold": self._bimodal_threshold,
            },
            "image_shape": list(img.shape),
        }

        return AlgoResult(
            metrics=metrics,
            artifacts=(
                Artifact(
                    name="histogram_analysis_report.json",
                    mime="application/json",
                    data=json.dumps(artifact_data, indent=2).encode("utf-8"),
                ),
            ),
        )
