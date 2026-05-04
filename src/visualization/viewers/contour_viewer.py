"""Contour viewer for density map/heatmap artifacts.

Converts 2D density grid data to Plotly heatmap/contour visualization.
"""

from __future__ import annotations

import io
from typing import Any

import numpy as np

from src.domain.evaluation import AggregationType
from src.visualization.viewers.base import ViewerResult


class ContourViewer:
    """Viewer for density map/contour NPZ artifacts.

    Handles artifacts with DENSITY_MAP aggregation type.
    Produces Plotly heatmap/contour configuration.
    """

    @property
    def viewer_type(self) -> str:
        return "contour"

    def can_view(self, *, mime: str, aggregation_mask: int) -> bool:
        """Check if this is a contour/density artifact."""
        is_npz = mime in ("application/x-npz", "contour/x-npz")
        has_density = bool(aggregation_mask & AggregationType.DENSITY_MAP.value)
        return is_npz and has_density

    def view(
        self,
        *,
        data: bytes,
        mime: str,
        aggregation_mask: int,
        metadata: dict[str, Any],
    ) -> ViewerResult:
        """Render density data as Plotly heatmap config."""
        try:
            buf = io.BytesIO(data)
            npz = np.load(buf)

            # Get density grid
            grid = npz.get("grid", npz.get("density", npz.get("z")))
            x_edges = npz.get("x_edges", npz.get("x"))
            y_edges = npz.get("y_edges", npz.get("y"))

            if grid is None:
                return ViewerResult(
                    viewer_type=self.viewer_type,
                    render_type="error",
                    data=None,
                    error="Invalid contour NPZ: missing grid data",
                )

            grid_list = grid.tolist()

            # Calculate axis values from edges or use indices
            x_vals = x_edges.tolist() if x_edges is not None else list(range(len(grid_list[0]) if grid_list else 0))
            y_vals = y_edges.tolist() if y_edges is not None else list(range(len(grid_list)))

            plotly_config = {
                "data": [
                    {
                        "type": "heatmap",
                        "z": grid_list,
                        "x": x_vals,
                        "y": y_vals,
                        "colorscale": "Viridis",
                        "colorbar": {"title": "Density"},
                    },
                ],
                "layout": {
                    "title": metadata.get("metric_name", "Density Map"),
                    "xaxis": {"title": "X"},
                    "yaxis": {"title": "Y"},
                },
            }

            return ViewerResult(
                viewer_type=self.viewer_type,
                render_type="plotly",
                data=plotly_config,
                metadata={
                    "grid_shape": list(grid.shape),
                    "max_density": float(np.max(grid)),
                },
            )
        except Exception as e:
            return ViewerResult(
                viewer_type=self.viewer_type,
                render_type="error",
                data=None,
                error=f"Failed to parse contour data: {e}",
            )
