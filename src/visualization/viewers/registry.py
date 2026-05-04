"""Registry for artifact viewers with priority-based lookup.

Viewers are registered with priorities and matched against artifacts
based on MIME type and aggregation mask.
"""

from __future__ import annotations

from typing import Any

from src.visualization.viewers.base import ViewerResult


class ViewerRegistry:
    """Registry for artifact viewers with priority-based matching.

    Viewers are registered with a priority (lower = higher priority).
    When matching an artifact, viewers are tried in priority order
    and the first matching viewer is used.
    """

    def __init__(self) -> None:
        self._viewers: list[tuple[int, Any]] = []

    def register(self, viewer: Any, priority: int = 100) -> None:
        """Register a viewer with the given priority.

        Args:
            viewer: Viewer instance to register
            priority: Priority for matching (lower = tried first)

        """
        self._viewers.append((priority, viewer))
        self._viewers.sort(key=lambda x: x[0])

    def find_viewer(
        self,
        *,
        mime: str,
        aggregation_mask: int,
    ) -> Any:
        """Find a viewer that can handle the given artifact.

        Args:
            mime: MIME type of the artifact
            aggregation_mask: Bitmask of AggregationType values

        Returns:
            First matching Viewer, or None if no match

        """
        for _, viewer in self._viewers:
            if viewer.can_view(mime=mime, aggregation_mask=aggregation_mask):
                return viewer
        return None

    def view(
        self,
        *,
        data: bytes,
        mime: str,
        aggregation_mask: int,
        metadata: dict[str, Any],
    ) -> ViewerResult:
        """Find a viewer and render the artifact.

        Args:
            data: Raw artifact bytes
            mime: MIME type of the artifact
            aggregation_mask: Bitmask of AggregationType values
            metadata: Additional metadata from storage

        Returns:
            ViewerResult from the matched viewer, or error result

        """
        viewer = self.find_viewer(mime=mime, aggregation_mask=aggregation_mask)
        if viewer is None:
            return ViewerResult(
                viewer_type="unknown",
                render_type="error",
                data=None,
                error=f"No viewer found for mime={mime}, mask={aggregation_mask}",
            )
        result: ViewerResult = viewer.view(
            data=data,
            mime=mime,
            aggregation_mask=aggregation_mask,
            metadata=metadata,
        )
        return result

    @property
    def viewer_types(self) -> list[str]:
        """Get list of all registered viewer types."""
        return [v.viewer_type for _, v in self._viewers]


def _lazy_histogram_viewer() -> Any:
    from src.visualization.viewers.histogram_viewer import HistogramViewer

    return HistogramViewer()


def _lazy_stats_viewer() -> Any:
    from src.visualization.viewers.stats_viewer import StatsViewer

    return StatsViewer()


def _lazy_scatter_viewer() -> Any:
    from src.visualization.viewers.scatter_viewer import ScatterViewer

    return ScatterViewer()


def _lazy_contour_viewer() -> Any:
    from src.visualization.viewers.contour_viewer import ContourViewer

    return ContourViewer()


def _lazy_json_viewer() -> Any:
    from src.visualization.viewers.json_viewer import JSONViewer

    return JSONViewer()


def _lazy_image_viewer() -> Any:
    from src.visualization.viewers.image_viewer import ImageViewer

    return ImageViewer()


def build_default_registry() -> ViewerRegistry:
    """Build the default viewer registry with all built-in viewers.

    Returns:
        ViewerRegistry with all default viewers registered

    """
    registry = ViewerRegistry()

    # Register viewers in priority order (lower = higher priority)
    # Specialized viewers first, then generic fallbacks
    registry.register(_lazy_histogram_viewer(), priority=10)
    registry.register(_lazy_stats_viewer(), priority=20)
    registry.register(_lazy_scatter_viewer(), priority=30)
    registry.register(_lazy_contour_viewer(), priority=40)
    registry.register(_lazy_image_viewer(), priority=50)
    registry.register(_lazy_json_viewer(), priority=90)  # Generic JSON last

    return registry


_default_registry: ViewerRegistry | None = None


def get_default_viewer_registry() -> ViewerRegistry:
    """Get or create the default viewer registry singleton.

    Returns:
        The default ViewerRegistry instance

    """
    global _default_registry
    if _default_registry is None:
        _default_registry = build_default_registry()
    return _default_registry
