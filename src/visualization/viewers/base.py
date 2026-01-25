"""Base types for the artifact viewer system.

Defines the Viewer protocol and ViewerResult dataclass used by all viewer
implementations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class ViewerResult:
    """Result from a viewer rendering operation.

    Attributes:
        viewer_type: Identifier for the type of viewer that produced this result
        render_type: How to render in frontend ('plotly', 'image', 'json', 'html')
        data: The rendered data (Plotly config, base64 image, JSON, or HTML string)
        metadata: Additional metadata about the artifact
        error: Error message if rendering failed

    """

    viewer_type: str
    render_type: str
    data: Any
    metadata: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class Viewer(Protocol):
    """Protocol for artifact viewers.

    A viewer knows how to render specific types of artifacts for visualization
    in the dashboard. Viewers are matched to artifacts based on MIME type
    and/or aggregation mask.
    """

    @property
    def viewer_type(self) -> str:
        """Unique identifier for this viewer type."""
        ...

    def can_view(
        self,
        *,
        mime: str,
        aggregation_mask: int,
    ) -> bool:
        """Check if this viewer can handle the given artifact.

        Args:
            mime: MIME type of the artifact
            aggregation_mask: Bitmask of AggregationType values

        Returns:
            True if this viewer can render the artifact

        """
        ...

    def view(
        self,
        *,
        data: bytes,
        mime: str,
        aggregation_mask: int,
        metadata: dict[str, Any],
    ) -> ViewerResult:
        """Render the artifact data for visualization.

        Args:
            data: Raw artifact bytes
            mime: MIME type of the artifact
            aggregation_mask: Bitmask of AggregationType values
            metadata: Additional metadata from storage

        Returns:
            ViewerResult with rendered data

        """
        ...
