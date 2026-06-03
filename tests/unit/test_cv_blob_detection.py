"""Unit tests for the blob detection processor."""

from __future__ import annotations

import json

import cv2
import numpy as np
import pytest

from src.algorithms.cv.blob_detection import (
    BlobDetectionAlgo,
    BlobDetectionProcessor,
)
from src.domain.evaluation import AggregationType
from src.domain.results import ProcessorResult


def _png_bytes(img: np.ndarray) -> bytes:
    ok, buf = cv2.imencode(".png", img)
    assert ok
    return bytes(buf)


def _white_image(h: int = 200, w: int = 200) -> np.ndarray:
    return np.full((h, w, 3), 255, dtype=np.uint8)


def _image_with_dark_blobs(h: int = 300, w: int = 300, n_blobs: int = 5) -> np.ndarray:
    """Create a white image with `n_blobs` dark circles (blob_color=0 detects these)."""
    img = np.full((h, w, 3), 255, dtype=np.uint8)
    # Distribute blobs along a diagonal-ish layout
    for i in range(n_blobs):
        cx = 40 + (i * 50)
        cy = 40 + (i * 50)
        cv2.circle(img, (cx, cy), 15, (0, 0, 0), thickness=-1)
    return img


class TestBlobDetectionMetadata:
    def test_name(self) -> None:
        assert BlobDetectionProcessor().name == "blob_detection"

    def test_version(self) -> None:
        assert BlobDetectionProcessor().version == "1.0.0"

    def test_alias_points_to_processor(self) -> None:
        assert BlobDetectionAlgo is BlobDetectionProcessor


class TestBlobDetectionInitialize:
    def test_initialize_with_defaults(self) -> None:
        proc = BlobDetectionProcessor()
        proc.initialize({})
        # After initialize, detector and gpu state should be set
        assert proc._detector is not None
        assert proc._gpu_context is not None

    def test_initialize_with_custom_detector_config(self) -> None:
        proc = BlobDetectionProcessor()
        proc.initialize(
            {
                "detector": {
                    "min_threshold": 20,
                    "max_threshold": 180,
                    "min_area": 50,
                    "max_area": 10000,
                    "min_circularity": 0.2,
                    "min_convexity": 0.6,
                    "min_inertia_ratio": 0.2,
                    "filter_by_color": True,
                    "blob_color": 0,
                },
            },
        )
        assert proc._detector is not None


class TestBlobDetectionRun:
    def test_invalid_image_bytes_returns_error_metrics(self) -> None:
        proc = BlobDetectionProcessor()
        proc.initialize({})
        result = proc.run(b"not-an-image", {})

        assert isinstance(result, ProcessorResult)
        assert result.metrics is not None
        assert result.metrics["blob_count"].value == 0
        assert result.metrics["detection_passed"].value is False
        assert "error" in (result.metrics["blob_count"].meta or {})

    def test_blank_image_detects_no_blobs(self) -> None:
        proc = BlobDetectionProcessor()
        proc.initialize({})
        result = proc.run(_png_bytes(_white_image()), {})

        assert result.metrics is not None
        assert result.metrics["blob_count"].value == 0
        assert result.metrics["avg_blob_size"].value == 0.0
        assert result.metrics["detection_passed"].value is False

    def test_image_with_blobs_detects_some(self) -> None:
        proc = BlobDetectionProcessor()
        proc.initialize({"detector": {"min_area": 50, "max_area": 5000}})
        result = proc.run(_png_bytes(_image_with_dark_blobs(n_blobs=5)), {})

        assert result.metrics is not None
        # The detector should find at least one of the dark circles
        assert result.metrics["blob_count"].value >= 1
        assert result.metrics["avg_blob_size"].value > 0.0
        # detection_passed depends on min_blob_count (default 1)
        assert result.metrics["detection_passed"].value is True

    def test_min_blob_count_threshold(self) -> None:
        proc = BlobDetectionProcessor()
        proc.initialize({"detector": {"min_area": 50, "max_area": 5000}})
        # Force a very high min_blob_count so detection fails
        result = proc.run(
            _png_bytes(_image_with_dark_blobs(n_blobs=2)),
            {"algorithm": {"min_blob_count": 100}},
        )
        assert result.metrics is not None
        assert result.metrics["detection_passed"].value is False

    def test_emits_all_expected_metrics(self) -> None:
        proc = BlobDetectionProcessor()
        proc.initialize({})
        result = proc.run(_png_bytes(_white_image()), {})

        assert result.metrics is not None
        expected = {"blob_count", "avg_blob_size", "detection_passed", "blob_centroid_xy"}
        assert set(result.metrics) == expected

    def test_centroid_value_is_xy_dict(self) -> None:
        proc = BlobDetectionProcessor()
        proc.initialize({})
        result = proc.run(_png_bytes(_white_image()), {})

        assert result.metrics is not None
        xy = result.metrics["blob_centroid_xy"].value
        assert "x" in xy
        assert "y" in xy
        assert xy["x"] == 0.0
        assert xy["y"] == 0.0

    def test_aggregation_flags(self) -> None:
        proc = BlobDetectionProcessor()
        proc.initialize({})
        result = proc.run(_png_bytes(_white_image()), {})
        assert result.metrics is not None

        assert AggregationType.STATS in result.metrics["blob_count"].aggregation
        assert AggregationType.TALLY in result.metrics["detection_passed"].aggregation
        assert AggregationType.SCATTER_ELLIPSE in result.metrics["blob_centroid_xy"].aggregation

    def test_produces_json_artifact(self) -> None:
        proc = BlobDetectionProcessor()
        proc.initialize({})
        result = proc.run(_png_bytes(_white_image()), {})

        assert len(result.artifacts) == 1
        artifact = result.artifacts[0]
        assert artifact.name == "blob_detection_report.json"
        assert artifact.mime == "application/json"
        assert artifact.data is not None

        payload = json.loads(artifact.data.decode("utf-8"))
        assert payload["algorithm"] == "blob_detection"
        assert payload["version"] == "1.0.0"
        assert "blob_count" in payload
        assert "blobs" in payload
        assert payload["accelerator"] in {"cpu", "gpu"}
        assert payload["image_shape"][0] == 200  # height

    def test_centroid_reflects_blob_positions(self) -> None:
        proc = BlobDetectionProcessor()
        proc.initialize({"detector": {"min_area": 50, "max_area": 5000}})
        result = proc.run(_png_bytes(_image_with_dark_blobs(n_blobs=3)), {})

        assert result.metrics is not None
        if result.metrics["blob_count"].value > 0:
            xy = result.metrics["blob_centroid_xy"].value
            # Centroid is inside image bounds (300x300)
            assert 0.0 <= xy["x"] <= 300.0
            assert 0.0 <= xy["y"] <= 300.0
