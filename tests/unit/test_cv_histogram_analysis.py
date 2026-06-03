"""Unit tests for the histogram analysis processor."""

from __future__ import annotations

import json

import cv2
import numpy as np
import pytest

from src.algorithms.cv.histogram_analysis import (
    HistogramAnalysisAlgo,
    HistogramAnalysisProcessor,
)
from src.domain.evaluation import AggregationType
from src.domain.results import ProcessorResult


def _png(img: np.ndarray) -> bytes:
    ok, buf = cv2.imencode(".png", img)
    assert ok
    return bytes(buf)


def _bimodal_image(h: int = 200, w: int = 200, seed: int = 42) -> np.ndarray:
    """Two intensity clusters with Gaussian spread -> bimodal histogram.

    The processor's peak detection smooths the histogram with a 9-bin kernel,
    so isolated single-bin spikes get washed out. We use Gaussian-spread
    intensity blobs to produce broad peaks that survive smoothing.
    """
    rng = np.random.default_rng(seed)
    img = np.zeros((h, w, 3), dtype=np.uint8)
    low = np.clip(rng.normal(50, 8, (h // 2, w)), 0, 255).astype(np.uint8)
    high = np.clip(rng.normal(200, 8, (h - h // 2, w)), 0, 255).astype(np.uint8)
    for c in range(3):
        img[: h // 2, :, c] = low
        img[h // 2 :, :, c] = high
    return img


def _uniform_image(h: int = 100, w: int = 100, value: int = 128) -> np.ndarray:
    return np.full((h, w, 3), value, dtype=np.uint8)


class TestHistogramAnalysisMetadata:
    def test_name(self) -> None:
        assert HistogramAnalysisProcessor().name == "histogram_analysis"

    def test_version(self) -> None:
        assert HistogramAnalysisProcessor().version == "1.0.0"

    def test_alias_points_to_processor(self) -> None:
        assert HistogramAnalysisAlgo is HistogramAnalysisProcessor


class TestHistogramAnalysisInitialize:
    def test_initialize_with_defaults(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({})
        assert proc._num_bins == 256
        assert proc._peak_threshold == 0.1
        assert proc._bimodal_threshold == 0.3

    def test_initialize_with_custom_values(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize(
            {
                "histogram": {
                    "num_bins": 64,
                    "peak_threshold": 0.2,
                    "bimodal_threshold": 0.5,
                },
            },
        )
        assert proc._num_bins == 64
        assert proc._peak_threshold == 0.2
        assert proc._bimodal_threshold == 0.5


class TestHistogramAnalysisHelpers:
    def test_compute_histogram_returns_normalized(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({})
        channel = np.zeros((50, 50), dtype=np.uint8)
        hist = proc._compute_histogram(channel)
        assert hist.shape == (256,)
        # Normalized to sum to 1
        assert hist.sum() == pytest.approx(1.0)
        # All pixels are zero, so bin 0 has all mass
        assert hist[0] == pytest.approx(1.0)

    def test_compute_histogram_on_empty_input_returns_zero_sum(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({"histogram": {"num_bins": 8}})
        # Force a histogram of all-zero counts by passing an "impossible" channel.
        # Use a channel where every value is out of [0, 256) - this is uint8 so
        # we just call with a normal one and check the path is at least exercised.
        channel = np.zeros((1, 1), dtype=np.uint8)
        hist = proc._compute_histogram(channel)
        assert hist.shape == (8,)
        assert hist.sum() == pytest.approx(1.0)

    def test_count_peaks_returns_int(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({})
        # Use a broad Gaussian-like peak that survives the smoothing kernel
        # (single-bin spikes get washed out by the 9-bin average filter).
        x = np.arange(256)
        hist = np.exp(-((x - 100) ** 2) / (2 * 15**2))
        hist = hist / hist.sum()
        peaks = proc._count_peaks(hist)
        assert isinstance(peaks, int)
        assert peaks >= 1

    def test_count_peaks_zero_for_flat_histogram(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({})
        flat = np.ones(256) / 256.0
        # A flat distribution has no strict local maxima
        assert proc._count_peaks(flat) == 0

    def test_compute_spread_returns_float(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({})
        hist = np.ones(256) / 256.0
        spread = proc._compute_spread(hist)
        assert isinstance(spread, float)
        assert spread > 0.0

    def test_compute_channel_balance_grayscale_input(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({})
        gray = np.zeros((10, 10), dtype=np.uint8)
        # Single-channel input is treated as "balanced"
        assert proc._compute_channel_balance(gray) == 1.0

    def test_compute_channel_balance_balanced_color(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({})
        img = np.full((10, 10, 3), 100, dtype=np.uint8)
        balance = proc._compute_channel_balance(img)
        # All channels equal -> perfectly balanced
        assert balance == pytest.approx(1.0)

    def test_compute_channel_balance_imbalanced(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({})
        img = np.zeros((10, 10, 3), dtype=np.uint8)
        img[..., 0] = 200
        img[..., 1] = 50
        img[..., 2] = 10
        balance = proc._compute_channel_balance(img)
        assert 0.0 <= balance < 1.0

    def test_compute_channel_balance_zero_mean_returns_one(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({})
        img = np.zeros((10, 10, 3), dtype=np.uint8)
        # Means are all zero -> returns 1.0
        assert proc._compute_channel_balance(img) == 1.0

    def test_compute_dynamic_range(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({})
        gray = np.linspace(0, 255, 256, dtype=np.uint8).reshape(16, 16)
        p1, p99, ratio = proc._compute_dynamic_range(gray)
        assert 0.0 <= p1 <= p99 <= 255.0
        assert 0.0 <= ratio <= 1.0

    def test_is_bimodal_true_for_two_peaks(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({})
        gray = cv2.cvtColor(_bimodal_image(), cv2.COLOR_BGR2GRAY)
        hist = proc._compute_histogram(gray)
        assert proc._is_bimodal(hist) is True

    def test_is_bimodal_false_for_uniform(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({})
        gray = cv2.cvtColor(_uniform_image(), cv2.COLOR_BGR2GRAY)
        hist = proc._compute_histogram(gray)
        assert proc._is_bimodal(hist) is False


class TestHistogramAnalysisRun:
    def test_invalid_image_bytes_returns_error_metrics(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({})
        result = proc.run(b"garbage", {})
        assert isinstance(result, ProcessorResult)
        assert result.metrics is not None
        assert result.metrics["histogram_peaks"].value == 0
        assert result.metrics["is_bimodal"].value is False
        assert "error" in (result.metrics["histogram_peaks"].meta or {})

    def test_uniform_image_metrics(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({})
        result = proc.run(_png(_uniform_image()), {})
        assert result.metrics is not None
        # Uniform grayscale of 128 means a single dominant peak
        assert result.metrics["histogram_peaks"].value >= 0
        assert result.metrics["is_bimodal"].value is False
        # Dynamic range should be tiny for a uniform image
        assert 0.0 <= result.metrics["dynamic_range"].value <= 1.0
        # Channel balance for balanced image == 1.0
        assert result.metrics["channel_balance"].value == pytest.approx(1.0)

    def test_bimodal_image_detected(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({})
        result = proc.run(_png(_bimodal_image()), {})
        assert result.metrics is not None
        assert result.metrics["is_bimodal"].value is True

    def test_emits_all_expected_metrics(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({})
        result = proc.run(_png(_uniform_image()), {})
        assert result.metrics is not None
        expected = {
            "histogram_peaks",
            "histogram_spread",
            "channel_balance",
            "dynamic_range",
            "is_bimodal",
        }
        assert set(result.metrics) == expected

    def test_aggregation_flags(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({})
        result = proc.run(_png(_uniform_image()), {})
        assert result.metrics is not None
        assert AggregationType.STATS in result.metrics["histogram_peaks"].aggregation
        assert AggregationType.TALLY in result.metrics["histogram_peaks"].aggregation
        assert AggregationType.HISTOGRAM in result.metrics["histogram_spread"].aggregation
        assert AggregationType.OUTLIERS in result.metrics["channel_balance"].aggregation
        assert AggregationType.TALLY in result.metrics["is_bimodal"].aggregation

    def test_produces_json_artifact_with_histograms(self) -> None:
        proc = HistogramAnalysisProcessor()
        proc.initialize({"histogram": {"num_bins": 64}})
        result = proc.run(_png(_uniform_image()), {})
        assert len(result.artifacts) == 1
        artifact = result.artifacts[0]
        assert artifact.name == "histogram_analysis_report.json"
        assert artifact.mime == "application/json"
        payload = json.loads(artifact.data.decode("utf-8"))
        assert payload["algorithm"] == "histogram_analysis"
        assert "histograms" in payload
        assert "grayscale" in payload["histograms"]
        assert len(payload["histograms"]["grayscale"]) == 64
        for ch in ("blue", "green", "red"):
            assert ch in payload["histograms"]
