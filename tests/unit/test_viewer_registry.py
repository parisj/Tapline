"""Unit tests for the viewer registry.

Tests the ViewerRegistry class for:
- Viewer registration and priority-based lookup
- Viewer matching by MIME type and aggregation mask
- Default registry configuration
"""

from __future__ import annotations

from typing import Any

import pytest

from src.domain.evaluation import AggregationType
from src.visualization.viewers.base import ViewerResult
from src.visualization.viewers.registry import (
    ViewerRegistry,
    build_default_registry,
    get_default_viewer_registry,
)


class MockViewer:
    """Mock viewer for testing."""

    def __init__(self, name: str, mime_pattern: str, mask: int = 0) -> None:
        self._name = name
        self._mime_pattern = mime_pattern
        self._mask = mask

    @property
    def viewer_type(self) -> str:
        return self._name

    def can_view(self, *, mime: str, aggregation_mask: int) -> bool:
        mime_match = self._mime_pattern == "*" or mime == self._mime_pattern
        mask_match = self._mask == 0 or bool(aggregation_mask & self._mask)
        return mime_match and mask_match

    def view(
        self,
        *,
        data: bytes,
        mime: str,
        aggregation_mask: int,
        metadata: dict[str, Any],
    ) -> ViewerResult:
        return ViewerResult(
            viewer_type=self._name,
            render_type="test",
            data={"viewed_by": self._name},
        )


class TestViewerRegistry:
    """Tests for ViewerRegistry class."""

    def test_register_viewer(self) -> None:
        registry = ViewerRegistry()
        viewer = MockViewer("test", "application/json")

        registry.register(viewer)

        assert "test" in registry.viewer_types

    def test_find_viewer_by_mime(self) -> None:
        registry = ViewerRegistry()
        json_viewer = MockViewer("json", "application/json")
        image_viewer = MockViewer("image", "image/png")

        registry.register(json_viewer)
        registry.register(image_viewer)

        found = registry.find_viewer(mime="application/json", aggregation_mask=0)
        assert found is json_viewer

    def test_find_viewer_returns_none_when_no_match(self) -> None:
        registry = ViewerRegistry()
        viewer = MockViewer("json", "application/json")
        registry.register(viewer)

        found = registry.find_viewer(mime="image/png", aggregation_mask=0)
        assert found is None

    def test_priority_ordering(self) -> None:
        registry = ViewerRegistry()
        low_priority = MockViewer("low", "*")
        high_priority = MockViewer("high", "*")

        registry.register(low_priority, priority=100)
        registry.register(high_priority, priority=10)

        # High priority should be found first
        found = registry.find_viewer(mime="anything", aggregation_mask=0)
        assert found is high_priority

    def test_view_uses_matching_viewer(self) -> None:
        registry = ViewerRegistry()
        viewer = MockViewer("json", "application/json")
        registry.register(viewer)

        result = registry.view(
            data=b"{}",
            mime="application/json",
            aggregation_mask=0,
            metadata={},
        )

        assert result.viewer_type == "json"
        assert result.render_type == "test"
        assert result.data == {"viewed_by": "json"}

    def test_view_returns_error_when_no_viewer(self) -> None:
        registry = ViewerRegistry()

        result = registry.view(
            data=b"data",
            mime="unknown/type",
            aggregation_mask=0,
            metadata={},
        )

        assert result.viewer_type == "unknown"
        assert result.render_type == "error"
        assert result.error is not None

    def test_find_viewer_by_aggregation_mask(self) -> None:
        registry = ViewerRegistry()
        stats_viewer = MockViewer("stats", "application/x-npz", AggregationType.STATS.value)
        histogram_viewer = MockViewer(
            "histogram",
            "application/x-npz",
            AggregationType.HISTOGRAM.value,
        )

        registry.register(histogram_viewer, priority=10)
        registry.register(stats_viewer, priority=20)

        # With HISTOGRAM mask, should find histogram viewer
        found = registry.find_viewer(
            mime="application/x-npz",
            aggregation_mask=AggregationType.HISTOGRAM.value,
        )
        assert found is histogram_viewer

    def test_viewer_types_property(self) -> None:
        registry = ViewerRegistry()
        registry.register(MockViewer("a", "*"))
        registry.register(MockViewer("b", "*"))
        registry.register(MockViewer("c", "*"))

        types = registry.viewer_types
        assert set(types) == {"a", "b", "c"}


class TestBuildDefaultRegistry:
    """Tests for build_default_registry function."""

    def test_returns_registry_with_all_viewers(self) -> None:
        registry = build_default_registry()

        expected_types = ["histogram", "stats", "scatter", "contour", "image", "json"]
        actual_types = registry.viewer_types

        for vtype in expected_types:
            assert vtype in actual_types, f"Missing viewer: {vtype}"

    def test_image_viewer_matches_png(self) -> None:
        registry = build_default_registry()

        found = registry.find_viewer(mime="image/png", aggregation_mask=0)
        assert found is not None
        assert found.viewer_type == "image"

    def test_json_viewer_matches_json(self) -> None:
        registry = build_default_registry()

        found = registry.find_viewer(mime="application/json", aggregation_mask=0)
        assert found is not None
        assert found.viewer_type == "json"

    def test_histogram_viewer_matches_npz_with_histogram_mask(self) -> None:
        registry = build_default_registry()

        found = registry.find_viewer(
            mime="application/x-npz",
            aggregation_mask=AggregationType.HISTOGRAM.value,
        )
        assert found is not None
        assert found.viewer_type == "histogram"


class TestGetDefaultViewerRegistry:
    """Tests for get_default_viewer_registry singleton function."""

    def test_returns_same_instance(self) -> None:
        registry1 = get_default_viewer_registry()
        registry2 = get_default_viewer_registry()

        assert registry1 is registry2

    def test_registry_is_fully_configured(self) -> None:
        registry = get_default_viewer_registry()

        # Should have at least 6 default viewers
        assert len(registry.viewer_types) >= 6
