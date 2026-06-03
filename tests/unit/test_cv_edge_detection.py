"""Unit tests for the edge detection processor."""

from __future__ import annotations

import json

import cv2
import numpy as np
import pytest

from src.algorithms.cv.edge_detection import (
    EdgeDetectionAlgo,
    EdgeDetectionProcessor,
)
from src.domain.evaluation import AggregationType
from src.domain.results import ProcessorResult


def _png(img: np.ndarray) -> bytes:
    ok, buf = cv2.imencode(".png", img)
    assert ok
    return bytes(buf)


def _striped_image(h: int = 200, w: int = 200) -> np.ndarray:
    """Image with strong vertical edges via alternating bands."""
    img = np.full((h, w, 3), 255, dtype=np.uint8)
    for x in range(0, w, 20):
        cv2.rectangle(img, (x, 0), (x + 10, h), (0, 0, 0), thickness=-1)
    return img


def _solid_image(h: int = 100, w: int = 100, value: int = 128) -> np.ndarray:
    return np.full((h, w, 3), value, dtype=np.uint8)


class TestEdgeDetectionMetadata:
    def test_name(self) -> None:
        assert EdgeDetectionProcessor().name == "edge_detection"

    def test_version(self) -> None:
        assert EdgeDetectionProcessor().version == "1.0.0"

    def test_alias_points_to_processor(self) -> None:
        assert EdgeDetectionAlgo is EdgeDetectionProcessor


class TestEdgeDetectionInitialize:
    def test_initialize_with_defaults(self) -> None:
        proc = EdgeDetectionProcessor()
        proc.initialize({})
        assert proc._method == "canny"
        assert proc._canny_threshold1 == 100.0
        assert proc._canny_threshold2 == 200.0
        assert proc._sobel_ksize == 3
        assert proc._min_edge_density == 0.01

    def test_initialize_lowercases_method(self) -> None:
        proc = EdgeDetectionProcessor()
        proc.initialize({"detector": {"method": "SOBEL"}})
        assert proc._method == "sobel"

    def test_initialize_with_custom_values(self) -> None:
        proc = EdgeDetectionProcessor()
        proc.initialize(
            {
                "detector": {
                    "method": "laplacian",
                    "canny_threshold1": 50,
                    "canny_threshold2": 150,
                    "sobel_ksize": 5,
                },
                "algorithm": {"min_edge_density": 0.05},
            },
        )
        assert proc._method == "laplacian"
        assert proc._canny_threshold1 == 50.0
        assert proc._canny_threshold2 == 150.0
        assert proc._sobel_ksize == 5
        assert proc._min_edge_density == 0.05


class TestEdgeDetectionHelpers:
    def test_compute_edge_density_zero_for_blank_edge_map(self) -> None:
        proc = EdgeDetectionProcessor()
        proc.initialize({})
        edges = np.zeros((100, 100), dtype=np.uint8)
        assert proc._compute_edge_density(edges) == 0.0

    def test_compute_edge_density_one_for_filled_edge_map(self) -> None:
        proc = EdgeDetectionProcessor()
        proc.initialize({})
        edges = np.full((100, 100), 255, dtype=np.uint8)
        assert proc._compute_edge_density(edges) == 1.0

    def test_compute_edge_density_half(self) -> None:
        proc = EdgeDetectionProcessor()
        proc.initialize({})
        edges = np.zeros((100, 100), dtype=np.uint8)
        edges[:, :50] = 255
        assert proc._compute_edge_density(edges) == pytest.approx(0.5)

    def test_count_contours_on_blank(self) -> None:
        proc = EdgeDetectionProcessor()
        proc.initialize({})
        edges = np.zeros((100, 100), dtype=np.uint8)
        assert proc._count_contours(edges) == 0

    def test_count_contours_one_box(self) -> None:
        proc = EdgeDetectionProcessor()
        proc.initialize({})
        edges = np.zeros((100, 100), dtype=np.uint8)
        cv2.rectangle(edges, (10, 10), (50, 50), 255, thickness=1)
        assert proc._count_contours(edges) >= 1

    def test_dominant_orientation_zero_for_uniform_image(self) -> None:
        proc = EdgeDetectionProcessor()
        proc.initialize({})
        gray = np.full((100, 100), 128, dtype=np.uint8)
        # No gradient -> mask is empty -> returns 0.0
        assert proc._compute_dominant_orientation(gray) == 0.0

    def test_dominant_orientation_in_range(self) -> None:
        proc = EdgeDetectionProcessor()
        proc.initialize({})
        gray = cv2.cvtColor(_striped_image(), cv2.COLOR_BGR2GRAY)
        deg = proc._compute_dominant_orientation(gray)
        assert 0.0 <= deg <= 180.0


class TestEdgeDetectionRun:
    def test_invalid_image_bytes_returns_error_metrics(self) -> None:
        proc = EdgeDetectionProcessor()
        proc.initialize({})
        result = proc.run(b"nope", {})
        assert isinstance(result, ProcessorResult)
        assert result.metrics is not None
        assert result.metrics["edge_density"].value == 0.0
        assert result.metrics["edge_detection_passed"].value is False
        assert "error" in (result.metrics["edge_density"].meta or {})

    def test_canny_on_striped_image_has_edges(self) -> None:
        proc = EdgeDetectionProcessor()
        proc.initialize({})  # canny default
        result = proc.run(_png(_striped_image()), {})
        assert result.metrics is not None
        assert result.metrics["edge_density"].value > 0.0
        assert result.metrics["edge_count"].value >= 1
        assert result.metrics["edge_detection_passed"].value is True

    def test_sobel_method(self) -> None:
        proc = EdgeDetectionProcessor()
        proc.initialize({"detector": {"method": "sobel"}})
        result = proc.run(_png(_striped_image()), {})
        assert result.metrics is not None
        assert result.metrics["edge_density"].value > 0.0
        # The "method" metadata should reflect sobel
        assert result.metrics["edge_density"].meta is not None
        assert result.metrics["edge_density"].meta["method"] == "sobel"

    def test_laplacian_method(self) -> None:
        proc = EdgeDetectionProcessor()
        proc.initialize({"detector": {"method": "laplacian"}})
        result = proc.run(_png(_striped_image()), {})
        assert result.metrics is not None
        assert result.metrics["edge_density"].meta is not None
        assert result.metrics["edge_density"].meta["method"] == "laplacian"

    def test_emits_all_expected_metrics(self) -> None:
        proc = EdgeDetectionProcessor()
        proc.initialize({})
        result = proc.run(_png(_striped_image()), {})
        assert result.metrics is not None
        expected = {"edge_density", "edge_count", "dominant_orientation", "edge_detection_passed"}
        assert set(result.metrics) == expected

    def test_aggregation_flags(self) -> None:
        proc = EdgeDetectionProcessor()
        proc.initialize({})
        result = proc.run(_png(_striped_image()), {})
        assert result.metrics is not None
        assert AggregationType.STATS in result.metrics["edge_density"].aggregation
        assert AggregationType.HISTOGRAM in result.metrics["edge_count"].aggregation
        assert AggregationType.TALLY in result.metrics["edge_detection_passed"].aggregation

    def test_produces_two_artifacts_png_and_json(self) -> None:
        proc = EdgeDetectionProcessor()
        proc.initialize({})
        result = proc.run(_png(_striped_image()), {})
        names = {a.name for a in result.artifacts}
        assert names == {"edge_map.png", "edge_detection_report.json"}

        mimes = {a.name: a.mime for a in result.artifacts}
        assert mimes["edge_map.png"] == "image/png"
        assert mimes["edge_detection_report.json"] == "application/json"

        report = next(a for a in result.artifacts if a.name.endswith(".json"))
        payload = json.loads(report.data.decode("utf-8"))
        assert payload["algorithm"] == "edge_detection"
        assert payload["method"] == "canny"
        assert "metrics" in payload

    def test_min_edge_density_threshold_can_fail(self) -> None:
        proc = EdgeDetectionProcessor()
        # Very high threshold so detection fails on a low-edge image
        proc.initialize({"algorithm": {"min_edge_density": 0.99}})
        result = proc.run(_png(_solid_image()), {})
        assert result.metrics is not None
        assert result.metrics["edge_detection_passed"].value is False
