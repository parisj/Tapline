"""Unit tests for artifact viewers.

Tests all viewer implementations for:
- MIME type matching
- Aggregation mask matching
- Data rendering
- Error handling
"""

from __future__ import annotations

import io
import json
from typing import Any

import numpy as np
import pytest

from src.domain.evaluation import AggregationType
from src.visualization.viewers.contour_viewer import ContourViewer
from src.visualization.viewers.histogram_viewer import HistogramViewer
from src.visualization.viewers.image_viewer import ImageViewer
from src.visualization.viewers.json_viewer import JSONViewer
from src.visualization.viewers.scatter_viewer import ScatterViewer
from src.visualization.viewers.stats_viewer import StatsViewer


def create_npz_data(**arrays: np.ndarray) -> bytes:
    """Helper to create NPZ data from arrays."""
    buf = io.BytesIO()
    np.savez_compressed(buf, **arrays)
    return buf.getvalue()


class TestHistogramViewer:
    """Tests for HistogramViewer."""

    def test_viewer_type(self) -> None:
        viewer = HistogramViewer()
        assert viewer.viewer_type == "histogram"

    def test_can_view_npz_with_histogram_mask(self) -> None:
        viewer = HistogramViewer()
        assert viewer.can_view(
            mime="application/x-npz",
            aggregation_mask=AggregationType.HISTOGRAM.value,
        )

    def test_cannot_view_without_histogram_mask(self) -> None:
        viewer = HistogramViewer()
        assert not viewer.can_view(
            mime="application/x-npz",
            aggregation_mask=AggregationType.STATS.value,
        )

    def test_cannot_view_non_npz(self) -> None:
        viewer = HistogramViewer()
        assert not viewer.can_view(
            mime="application/json",
            aggregation_mask=AggregationType.HISTOGRAM.value,
        )

    def test_view_renders_plotly_config(self) -> None:
        viewer = HistogramViewer()
        edges = np.array([0.0, 1.0, 2.0, 3.0, 4.0])
        counts = np.array([5, 10, 8, 3])
        data = create_npz_data(edges=edges, counts=counts)

        result = viewer.view(
            data=data,
            mime="application/x-npz",
            aggregation_mask=AggregationType.HISTOGRAM.value,
            metadata={"metric_name": "latency"},
        )

        assert result.render_type == "plotly"
        assert result.error is None
        assert "data" in result.data
        assert result.data["data"][0]["type"] == "bar"

    def test_view_handles_missing_data(self) -> None:
        viewer = HistogramViewer()
        data = create_npz_data(x=np.array([1, 2, 3]))  # Wrong keys

        result = viewer.view(
            data=data,
            mime="application/x-npz",
            aggregation_mask=AggregationType.HISTOGRAM.value,
            metadata={},
        )

        assert result.render_type == "error"
        assert result.error is not None

    def test_view_handles_invalid_npz(self) -> None:
        viewer = HistogramViewer()

        result = viewer.view(
            data=b"not valid npz",
            mime="application/x-npz",
            aggregation_mask=AggregationType.HISTOGRAM.value,
            metadata={},
        )

        assert result.render_type == "error"


class TestStatsViewer:
    """Tests for StatsViewer."""

    def test_viewer_type(self) -> None:
        viewer = StatsViewer()
        assert viewer.viewer_type == "stats"

    def test_can_view_summary_npz(self) -> None:
        viewer = StatsViewer()
        assert viewer.can_view(mime="summary/x-npz", aggregation_mask=0)

    def test_can_view_npz_with_stats_mask(self) -> None:
        viewer = StatsViewer()
        assert viewer.can_view(
            mime="application/x-npz",
            aggregation_mask=AggregationType.STATS.value,
        )

    def test_view_extracts_values_and_stats(self) -> None:
        viewer = StatsViewer()
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        data = create_npz_data(value=values)

        result = viewer.view(
            data=data,
            mime="summary/x-npz",
            aggregation_mask=AggregationType.STATS.value,
            metadata={},
        )

        assert result.render_type == "stats"
        assert result.error is None
        assert result.data["count"] == 5
        assert result.data["mean"] == pytest.approx(3.0)


class TestScatterViewer:
    """Tests for ScatterViewer."""

    def test_viewer_type(self) -> None:
        viewer = ScatterViewer()
        assert viewer.viewer_type == "scatter"

    def test_can_view_npz_with_scatter_mask(self) -> None:
        viewer = ScatterViewer()
        assert viewer.can_view(
            mime="application/x-npz",
            aggregation_mask=AggregationType.SCATTER_ELLIPSE.value,
        )

    def test_view_renders_scatter_plot(self) -> None:
        viewer = ScatterViewer()
        x = np.array([1.0, 2.0, 3.0, 4.0])
        y = np.array([2.0, 4.0, 6.0, 8.0])
        data = create_npz_data(x=x, y=y)

        result = viewer.view(
            data=data,
            mime="application/x-npz",
            aggregation_mask=AggregationType.SCATTER_ELLIPSE.value,
            metadata={},
        )

        assert result.render_type == "plotly"
        assert result.error is None
        assert result.data["data"][0]["type"] == "scatter"

    def test_view_includes_ellipse_if_present(self) -> None:
        viewer = ScatterViewer()
        x = np.array([1.0, 2.0, 3.0])
        y = np.array([2.0, 4.0, 6.0])
        ellipse_x = np.cos(np.linspace(0, 2 * np.pi, 50))
        ellipse_y = np.sin(np.linspace(0, 2 * np.pi, 50))
        data = create_npz_data(x=x, y=y, ellipse_x=ellipse_x, ellipse_y=ellipse_y)

        result = viewer.view(
            data=data,
            mime="application/x-npz",
            aggregation_mask=AggregationType.SCATTER_ELLIPSE.value,
            metadata={},
        )

        assert result.render_type == "plotly"
        assert len(result.data["data"]) >= 2  # Points + ellipse
        assert result.metadata["has_ellipse"] is True


class TestContourViewer:
    """Tests for ContourViewer."""

    def test_viewer_type(self) -> None:
        viewer = ContourViewer()
        assert viewer.viewer_type == "contour"

    def test_can_view_npz_with_density_mask(self) -> None:
        viewer = ContourViewer()
        assert viewer.can_view(
            mime="application/x-npz",
            aggregation_mask=AggregationType.DENSITY_MAP.value,
        )

    def test_view_renders_heatmap(self) -> None:
        viewer = ContourViewer()
        rng = np.random.default_rng(42)
        grid = rng.random((16, 16))
        x_edges = np.linspace(0, 10, 17)
        y_edges = np.linspace(0, 10, 17)
        data = create_npz_data(grid=grid, x_edges=x_edges, y_edges=y_edges)

        result = viewer.view(
            data=data,
            mime="application/x-npz",
            aggregation_mask=AggregationType.DENSITY_MAP.value,
            metadata={},
        )

        assert result.render_type == "plotly"
        assert result.error is None
        assert result.data["data"][0]["type"] == "heatmap"

    def test_view_handles_missing_grid(self) -> None:
        viewer = ContourViewer()
        data = create_npz_data(x=np.array([1, 2, 3]))

        result = viewer.view(
            data=data,
            mime="application/x-npz",
            aggregation_mask=AggregationType.DENSITY_MAP.value,
            metadata={},
        )

        assert result.render_type == "error"


class TestJSONViewer:
    """Tests for JSONViewer."""

    def test_viewer_type(self) -> None:
        viewer = JSONViewer()
        assert viewer.viewer_type == "json"

    def test_can_view_json(self) -> None:
        viewer = JSONViewer()
        assert viewer.can_view(mime="application/json", aggregation_mask=0)
        assert viewer.can_view(mime="text/json", aggregation_mask=0)

    def test_cannot_view_non_json(self) -> None:
        viewer = JSONViewer()
        assert not viewer.can_view(mime="image/png", aggregation_mask=0)

    def test_view_parses_json_object(self) -> None:
        viewer = JSONViewer()
        json_data = {"key": "value", "number": 42}
        data = json.dumps(json_data).encode("utf-8")

        result = viewer.view(
            data=data,
            mime="application/json",
            aggregation_mask=0,
            metadata={},
        )

        assert result.render_type == "json"
        assert result.error is None
        assert result.data == json_data
        assert result.metadata["keys"] == ["key", "number"]

    def test_view_parses_json_array(self) -> None:
        viewer = JSONViewer()
        json_data = [1, 2, 3, 4, 5]
        data = json.dumps(json_data).encode("utf-8")

        result = viewer.view(
            data=data,
            mime="application/json",
            aggregation_mask=0,
            metadata={},
        )

        assert result.render_type == "json"
        assert result.data == json_data
        assert result.metadata["item_count"] == 5

    def test_view_handles_invalid_json(self) -> None:
        viewer = JSONViewer()

        result = viewer.view(
            data=b"not valid json {",
            mime="application/json",
            aggregation_mask=0,
            metadata={},
        )

        assert result.render_type == "error"
        assert "Invalid JSON" in (result.error or "")

    def test_view_handles_invalid_encoding(self) -> None:
        viewer = JSONViewer()

        result = viewer.view(
            data=b"\xff\xfe",  # Invalid UTF-8
            mime="application/json",
            aggregation_mask=0,
            metadata={},
        )

        assert result.render_type == "error"


class TestImageViewer:
    """Tests for ImageViewer."""

    def test_viewer_type(self) -> None:
        viewer = ImageViewer()
        assert viewer.viewer_type == "image"

    def test_can_view_png(self) -> None:
        viewer = ImageViewer()
        assert viewer.can_view(mime="image/png", aggregation_mask=0)

    def test_can_view_jpeg(self) -> None:
        viewer = ImageViewer()
        assert viewer.can_view(mime="image/jpeg", aggregation_mask=0)
        assert viewer.can_view(mime="image/jpg", aggregation_mask=0)

    def test_cannot_view_non_image(self) -> None:
        viewer = ImageViewer()
        assert not viewer.can_view(mime="application/json", aggregation_mask=0)

    def test_view_encodes_base64(self) -> None:
        viewer = ImageViewer()
        # Minimal 1x1 PNG
        png_data = bytes(
            [
                0x89,
                0x50,
                0x4E,
                0x47,
                0x0D,
                0x0A,
                0x1A,
                0x0A,
                0x00,
                0x00,
                0x00,
                0x0D,
                0x49,
                0x48,
                0x44,
                0x52,
                0x00,
                0x00,
                0x00,
                0x01,
                0x00,
                0x00,
                0x00,
                0x01,
                0x08,
                0x02,
                0x00,
                0x00,
                0x00,
                0x90,
                0x77,
                0x53,
                0xDE,
            ],
        )

        result = viewer.view(
            data=png_data,
            mime="image/png",
            aggregation_mask=0,
            metadata={},
        )

        assert result.render_type == "image"
        assert result.error is None
        assert "data_uri" in result.data
        assert result.data["data_uri"].startswith("data:image/png;base64,")
        assert result.data["mime"] == "image/png"
        assert result.data["size_bytes"] == len(png_data)


class TestViewerIntegration:
    """Integration tests for the viewer system."""

    def test_all_viewers_follow_protocol(self) -> None:
        viewers = [
            HistogramViewer(),
            StatsViewer(),
            ScatterViewer(),
            ContourViewer(),
            JSONViewer(),
            ImageViewer(),
        ]

        for viewer in viewers:
            # Check protocol properties
            assert hasattr(viewer, "viewer_type")
            assert isinstance(viewer.viewer_type, str)

            # Check protocol methods
            assert hasattr(viewer, "can_view")
            assert hasattr(viewer, "view")

    def test_viewers_return_valid_results(self) -> None:
        from src.visualization.viewers.base import ViewerResult

        json_viewer = JSONViewer()
        result = json_viewer.view(
            data=b'{"test": true}',
            mime="application/json",
            aggregation_mask=0,
            metadata={"source": "test"},
        )

        assert isinstance(result, ViewerResult)
        assert result.viewer_type == "json"
        assert result.render_type in ("json", "plotly", "image", "stats", "error")
