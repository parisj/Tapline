"""Artifact viewers for the Tapline dashboard.

This module provides a registry-based system for viewing different types
of artifacts in the dashboard. Viewers convert raw artifact data into
frontend-friendly formats (Plotly configs, base64 images, JSON).

Usage:
    from src.visualization.viewers import get_default_viewer_registry

    registry = get_default_viewer_registry()
    result = registry.view(
        data=artifact_bytes,
        mime="application/x-npz",
        aggregation_mask=2,  # HISTOGRAM
        metadata={"metric_name": "latency"},
    )
"""

from src.visualization.viewers.base import Viewer, ViewerResult
from src.visualization.viewers.registry import (
    ViewerRegistry,
    build_default_registry,
    get_default_viewer_registry,
)

__all__ = [
    "Viewer",
    "ViewerRegistry",
    "ViewerResult",
    "build_default_registry",
    "get_default_viewer_registry",
]
