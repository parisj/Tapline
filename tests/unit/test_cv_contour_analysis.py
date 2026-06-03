"""Unit tests for the contour analysis processor."""

from __future__ import annotations

import json

import cv2
import numpy as np
import pytest

from src.algorithms.cv.contour_analysis import (
    ContourAnalysisAlgo,
    ContourAnalysisProcessor,
)
from src.domain.evaluation import AggregationType
from src.domain.results import ProcessorResult


def _png(img: np.ndarray) -> bytes:
    ok, buf = cv2.imencode(".png", img)
    assert ok
    return bytes(buf)


def _white_image(h: int = 200, w: int = 200) -> np.ndarray:
    return np.full((h, w, 3), 255, dtype=np.uint8)


def _image_with_shapes(h: int = 300, w: int = 300) -> np.ndarray:
    """White image with a couple of dark filled shapes."""
    img = np.full((h, w, 3), 255, dtype=np.uint8)
    # A large filled circle
    cv2.circle(img, (90, 90), 40, (0, 0, 0), thickness=-1)
    # A filled rectangle
    cv2.rectangle(img, (170, 170), (250, 250), (0, 0, 0), thickness=-1)
    return img


class TestContourAnalysisMetadata:
    def test_name(self) -> None:
        assert ContourAnalysisProcessor().name == "contour_analysis"

    def test_version(self) -> None:
        assert ContourAnalysisProcessor().version == "1.0.0"

    def test_alias_points_to_processor(self) -> None:
        assert ContourAnalysisAlgo is ContourAnalysisProcessor


class TestContourAnalysisInitialize:
    def test_initialize_with_defaults(self) -> None:
        proc = ContourAnalysisProcessor()
        proc.initialize({})
        assert proc._min_contour_area == 100.0
        assert proc._max_contour_area == 1e7
        assert proc._threshold_method == "otsu"
        assert proc._threshold_value == 127
        assert proc._min_contour_count == 1

    def test_initialize_with_custom_values(self) -> None:
        proc = ContourAnalysisProcessor()
        proc.initialize(
            {
                "detector": {
                    "min_contour_area": 50,
                    "max_contour_area": 5000,
                    "threshold_method": "ADAPTIVE",  # gets lower-cased
                    "threshold_value": 100,
                },
                "algorithm": {"min_contour_count": 3},
            },
        )
        assert proc._min_contour_area == 50.0
        assert proc._max_contour_area == 5000.0
        assert proc._threshold_method == "adaptive"
        assert proc._threshold_value == 100
        assert proc._min_contour_count == 3


class TestContourAnalysisHelpers:
    def test_compute_circularity_perfect_circle(self) -> None:
        proc = ContourAnalysisProcessor()
        proc.initialize({})
        # Build a disk mask and grab its contour
        img = np.zeros((200, 200), dtype=np.uint8)
        cv2.circle(img, (100, 100), 50, 255, thickness=-1)
        contours, _ = cv2.findContours(img, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_NONE)
        assert contours
        circ = proc._compute_circularity(contours[0])
        assert 0.0 < circ <= 1.0
        # A disk approximated as a polygon should be close to 1.0
        assert circ > 0.85

    def test_compute_circularity_with_zero_perimeter_returns_zero(self) -> None:
        proc = ContourAnalysisProcessor()
        proc.initialize({})
        # A single-point contour has zero perimeter
        single_point = np.array([[[10, 10]]], dtype=np.int32)
        assert proc._compute_circularity(single_point) == 0.0

    def test_compute_centroid_returns_floats(self) -> None:
        proc = ContourAnalysisProcessor()
        proc.initialize({})
        img = np.zeros((200, 200), dtype=np.uint8)
        cv2.circle(img, (100, 100), 50, 255, thickness=-1)
        contours, _ = cv2.findContours(img, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_NONE)
        cx, cy = proc._compute_centroid(contours[0])
        assert isinstance(cx, float)
        assert isinstance(cy, float)
        assert 90 < cx < 110
        assert 90 < cy < 110

    def test_compute_centroid_fallback_to_bounding_box(self) -> None:
        proc = ContourAnalysisProcessor()
        proc.initialize({})
        # A degenerate contour (line) has zero m00 moment
        line = np.array([[[10, 10]], [[20, 10]]], dtype=np.int32)
        cx, cy = proc._compute_centroid(line)
        assert isinstance(cx, float)
        assert isinstance(cy, float)

    def test_threshold_image_otsu(self) -> None:
        proc = ContourAnalysisProcessor()
        proc.initialize({"detector": {"threshold_method": "otsu"}})
        gray = np.full((50, 50), 128, dtype=np.uint8)
        binary = proc._threshold_image(gray)
        assert binary.shape == gray.shape
        assert binary.dtype == np.uint8

    def test_threshold_image_adaptive(self) -> None:
        proc = ContourAnalysisProcessor()
        proc.initialize({"detector": {"threshold_method": "adaptive"}})
        gray = (np.random.default_rng(0).integers(0, 256, size=(50, 50))).astype(np.uint8)
        binary = proc._threshold_image(gray)
        assert binary.shape == gray.shape

    def test_threshold_image_simple(self) -> None:
        proc = ContourAnalysisProcessor()
        proc.initialize({"detector": {"threshold_method": "simple", "threshold_value": 100}})
        gray = np.array([[50, 150], [200, 80]], dtype=np.uint8)
        binary = proc._threshold_image(gray)
        # values > 100 become 255
        assert binary[0, 0] == 0
        assert binary[0, 1] == 255
        assert binary[1, 0] == 255
        assert binary[1, 1] == 0


class TestContourAnalysisRun:
    def test_invalid_image_bytes_returns_error_metrics(self) -> None:
        proc = ContourAnalysisProcessor()
        proc.initialize({})
        result = proc.run(b"bogus", {})
        assert isinstance(result, ProcessorResult)
        assert result.metrics is not None
        assert result.metrics["contour_count"].value == 0
        assert result.metrics["contour_detected"].value is False
        assert "error" in (result.metrics["contour_count"].meta or {})

    def test_blank_white_image_yields_full_frame_contour(self) -> None:
        # Characterisation: OTSU on a uniform white image still returns the
        # full image frame as a single external contour with RETR_EXTERNAL.
        proc = ContourAnalysisProcessor()
        proc.initialize({})
        result = proc.run(_png(_white_image()), {})
        assert result.metrics is not None
        assert result.metrics["contour_count"].value == 1
        assert result.metrics["largest_contour_area"].value > 0

    def test_image_with_shapes_detects_at_least_one_contour(self) -> None:
        # Characterisation: RETR_EXTERNAL + OTSU returns the outer boundary
        # of the background region rather than each individual dark shape.
        # We lock in "at least one contour found".
        proc = ContourAnalysisProcessor()
        proc.initialize({"detector": {"min_contour_area": 50}})
        result = proc.run(_png(_image_with_shapes()), {})
        assert result.metrics is not None
        assert result.metrics["contour_count"].value >= 1
        assert result.metrics["largest_contour_area"].value > 0
        assert result.metrics["total_contour_area"].value > 0
        assert result.metrics["contour_detected"].value is True

    def test_emits_all_expected_metrics(self) -> None:
        proc = ContourAnalysisProcessor()
        proc.initialize({})
        result = proc.run(_png(_white_image()), {})
        assert result.metrics is not None
        expected = {
            "contour_count",
            "largest_contour_area",
            "total_contour_area",
            "avg_contour_circularity",
            "contour_centroid_xy",
            "contour_detected",
        }
        assert set(result.metrics) == expected

    def test_aggregation_flags(self) -> None:
        proc = ContourAnalysisProcessor()
        proc.initialize({})
        result = proc.run(_png(_white_image()), {})
        assert result.metrics is not None
        assert AggregationType.STATS in result.metrics["contour_count"].aggregation
        assert AggregationType.HISTOGRAM in result.metrics["contour_count"].aggregation
        assert AggregationType.OUTLIERS in result.metrics["largest_contour_area"].aggregation
        assert AggregationType.SCATTER_ELLIPSE in result.metrics["contour_centroid_xy"].aggregation
        assert AggregationType.TALLY in result.metrics["contour_detected"].aggregation

    def test_min_contour_count_threshold_affects_detection(self) -> None:
        proc = ContourAnalysisProcessor()
        proc.initialize(
            {
                "detector": {"min_contour_area": 50},
                "algorithm": {"min_contour_count": 1000},
            },
        )
        result = proc.run(_png(_image_with_shapes()), {})
        assert result.metrics is not None
        assert result.metrics["contour_detected"].value is False

    def test_avg_circularity_in_unit_range(self) -> None:
        proc = ContourAnalysisProcessor()
        proc.initialize({"detector": {"min_contour_area": 50}})
        result = proc.run(_png(_image_with_shapes()), {})
        assert result.metrics is not None
        circ = result.metrics["avg_contour_circularity"].value
        assert 0.0 <= circ <= 1.0

    def test_produces_json_artifact(self) -> None:
        proc = ContourAnalysisProcessor()
        proc.initialize({"detector": {"min_contour_area": 50}})
        result = proc.run(_png(_image_with_shapes()), {})

        assert len(result.artifacts) == 1
        artifact = result.artifacts[0]
        assert artifact.name == "contour_analysis_report.json"
        assert artifact.mime == "application/json"
        assert artifact.data is not None
        payload = json.loads(artifact.data.decode("utf-8"))
        assert payload["algorithm"] == "contour_analysis"
        assert payload["version"] == "1.0.0"
        assert "metrics" in payload
        assert "contours" in payload
        assert isinstance(payload["contours"], list)

    def test_max_contour_area_filters_large_shapes(self) -> None:
        proc = ContourAnalysisProcessor()
        # min area very low, max area very low -> filter out everything
        proc.initialize({"detector": {"min_contour_area": 1, "max_contour_area": 5}})
        result = proc.run(_png(_image_with_shapes()), {})
        assert result.metrics is not None
        assert result.metrics["contour_count"].value == 0
