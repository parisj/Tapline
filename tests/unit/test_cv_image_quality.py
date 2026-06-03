"""Unit tests for the image quality processor."""

from __future__ import annotations

import json

import cv2
import numpy as np
import pytest

from src.algorithms.cv.image_quality import (
    ImageQualityAlgo,
    ImageQualityProcessor,
)
from src.domain.evaluation import AggregationType
from src.domain.results import ProcessorResult


def _png(img: np.ndarray) -> bytes:
    ok, buf = cv2.imencode(".png", img)
    assert ok
    return bytes(buf)


def _uniform_image(h: int = 100, w: int = 100, value: int = 128) -> np.ndarray:
    return np.full((h, w, 3), value, dtype=np.uint8)


def _noisy_image(h: int = 200, w: int = 200, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.integers(0, 256, size=(h, w, 3), dtype=np.uint8)


def _saturated_image(h: int = 100, w: int = 100) -> np.ndarray:
    """An image with high colour saturation (bright red)."""
    img = np.zeros((h, w, 3), dtype=np.uint8)
    img[..., 2] = 255  # BGR -> red channel
    return img


class TestImageQualityMetadata:
    def test_name(self) -> None:
        assert ImageQualityProcessor().name == "image_quality"

    def test_version(self) -> None:
        assert ImageQualityProcessor().version == "1.0.0"

    def test_alias_points_to_processor(self) -> None:
        assert ImageQualityAlgo is ImageQualityProcessor


class TestImageQualityInitialize:
    def test_initialize_with_defaults(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize({})
        assert proc._min_sharpness == 100.0
        assert proc._min_brightness == 30.0
        assert proc._max_brightness == 225.0
        assert proc._min_contrast == 20.0
        assert proc._max_noise == 0.3
        assert proc._min_saturation == 0.1

    def test_initialize_with_custom_thresholds(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize(
            {
                "thresholds": {
                    "min_sharpness": 10,
                    "min_brightness": 5,
                    "max_brightness": 250,
                    "min_contrast": 1,
                    "max_noise": 0.9,
                    "min_saturation": 0.0,
                },
            },
        )
        assert proc._min_sharpness == 10.0
        assert proc._min_brightness == 5.0
        assert proc._max_brightness == 250.0
        assert proc._min_contrast == 1.0
        assert proc._max_noise == 0.9
        assert proc._min_saturation == 0.0


class TestImageQualityHelpers:
    def test_compute_sharpness_uniform_is_zero(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize({})
        gray = np.full((50, 50), 128, dtype=np.uint8)
        assert proc._compute_sharpness(gray) == 0.0

    def test_compute_sharpness_noisy_is_positive(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize({})
        gray = cv2.cvtColor(_noisy_image(), cv2.COLOR_BGR2GRAY)
        assert proc._compute_sharpness(gray) > 0.0

    def test_compute_brightness_matches_mean(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize({})
        gray = np.full((10, 10), 100, dtype=np.uint8)
        assert proc._compute_brightness(gray) == pytest.approx(100.0)

    def test_compute_contrast_uniform_is_zero(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize({})
        gray = np.full((10, 10), 50, dtype=np.uint8)
        assert proc._compute_contrast(gray) == 0.0

    def test_compute_contrast_varied_is_positive(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize({})
        gray = np.array([[0, 255], [255, 0]], dtype=np.uint8)
        assert proc._compute_contrast(gray) > 0.0

    def test_compute_noise_estimate_zero_image_is_zero(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize({})
        gray = np.zeros((10, 10), dtype=np.uint8)
        # Total energy is zero -> returns 0.0
        assert proc._compute_noise_estimate(gray) == 0.0

    def test_compute_noise_estimate_in_unit_range(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize({})
        gray = cv2.cvtColor(_noisy_image(), cv2.COLOR_BGR2GRAY)
        noise = proc._compute_noise_estimate(gray)
        assert 0.0 <= noise <= 1.0

    def test_compute_saturation_grayscale_input(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize({})
        gray = np.zeros((10, 10), dtype=np.uint8)
        assert proc._compute_saturation(gray) == 0.0

    def test_compute_saturation_for_pure_red(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize({})
        sat = proc._compute_saturation(_saturated_image())
        # Pure red is fully saturated -> ~1.0
        assert sat == pytest.approx(1.0, rel=0.01)


class TestImageQualityRun:
    def test_invalid_image_bytes_returns_error_metrics(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize({})
        result = proc.run(b"not-an-image", {})
        assert isinstance(result, ProcessorResult)
        assert result.metrics is not None
        assert result.metrics["sharpness"].value == 0.0
        assert result.metrics["quality_passed"].value is False
        assert "error" in (result.metrics["sharpness"].meta or {})

    def test_uniform_image_has_zero_sharpness_and_contrast(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize({})
        result = proc.run(_png(_uniform_image()), {})
        assert result.metrics is not None
        assert result.metrics["sharpness"].value == 0.0
        assert result.metrics["contrast"].value == 0.0
        assert result.metrics["quality_passed"].value is False

    def test_brightness_matches_input(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize({})
        result = proc.run(_png(_uniform_image(value=100)), {})
        assert result.metrics is not None
        # PNG encode/decode is lossless for uint8 RGB
        assert result.metrics["brightness"].value == pytest.approx(100.0, abs=1.0)

    def test_quality_passes_with_relaxed_thresholds(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize(
            {
                "thresholds": {
                    "min_sharpness": 0.0,
                    "min_brightness": 0.0,
                    "max_brightness": 255.0,
                    "min_contrast": 0.0,
                    "max_noise": 1.0,
                    "min_saturation": 0.0,
                },
            },
        )
        result = proc.run(_png(_noisy_image()), {})
        assert result.metrics is not None
        assert result.metrics["quality_passed"].value is True

    def test_emits_all_expected_metrics(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize({})
        result = proc.run(_png(_uniform_image()), {})
        assert result.metrics is not None
        expected = {
            "sharpness",
            "brightness",
            "contrast",
            "noise_estimate",
            "saturation",
            "quality_passed",
        }
        assert set(result.metrics) == expected

    def test_aggregation_flags(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize({})
        result = proc.run(_png(_uniform_image()), {})
        assert result.metrics is not None
        assert AggregationType.STATS in result.metrics["sharpness"].aggregation
        assert AggregationType.HISTOGRAM in result.metrics["brightness"].aggregation
        assert AggregationType.OUTLIERS in result.metrics["noise_estimate"].aggregation
        assert AggregationType.TALLY in result.metrics["quality_passed"].aggregation

    def test_produces_json_artifact(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize({})
        result = proc.run(_png(_uniform_image()), {})

        assert len(result.artifacts) == 1
        artifact = result.artifacts[0]
        assert artifact.name == "image_quality_report.json"
        assert artifact.mime == "application/json"
        payload = json.loads(artifact.data.decode("utf-8"))
        assert payload["algorithm"] == "image_quality"
        assert payload["version"] == "1.0.0"
        assert "metrics" in payload
        assert "thresholds" in payload
        assert payload["image_shape"][0] == 100  # height

    def test_saturation_zero_for_grayscale_uniform(self) -> None:
        proc = ImageQualityProcessor()
        proc.initialize({})
        # A uniform "white-ish" image still has zero saturation
        result = proc.run(_png(_uniform_image(value=200)), {})
        assert result.metrics is not None
        assert result.metrics["saturation"].value == pytest.approx(0.0, abs=0.01)
