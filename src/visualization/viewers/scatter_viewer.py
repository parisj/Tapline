"""Scatter viewer for 2D point data with ellipse overlay.

Converts scatter point data with covariance ellipse to Plotly scatter plot.
"""

from __future__ import annotations

import io
from typing import Any

import numpy as np

from src.domain.evaluation import AggregationType
from src.visualization.viewers.base import ViewerResult


class ScatterViewer:
    """Viewer for scatter plot NPZ artifacts with ellipse overlay.

    Handles artifacts with SCATTER_ELLIPSE aggregation type.
    Produces Plotly scatter chart configuration with optional ellipse.
    """

    @property
    def viewer_type(self) -> str:
        return "scatter"

    def can_view(self, *, mime: str, aggregation_mask: int) -> bool:
        """Check if this is a scatter/ellipse artifact."""
        is_npz = mime in ("application/x-npz", "ellipse/x-npz")
        has_scatter = bool(aggregation_mask & AggregationType.SCATTER_ELLIPSE.value)
        return is_npz and has_scatter

    def view(
        self,
        *,
        data: bytes,
        mime: str,
        aggregation_mask: int,
        metadata: dict[str, Any],
    ) -> ViewerResult:
        """Render scatter data with ellipse as Plotly chart config."""
        try:
            buf = io.BytesIO(data)
            npz = np.load(buf)

            traces: list[dict[str, Any]] = []

            # Get scatter points
            x_vals = npz.get("x")
            y_vals = npz.get("y")

            if x_vals is not None and y_vals is not None:
                traces.append(
                    {
                        "type": "scatter",
                        "mode": "markers",
                        "x": x_vals.tolist(),
                        "y": y_vals.tolist(),
                        "marker": {"color": "#3b82f6", "size": 6, "opacity": 0.7},
                        "name": "Data Points",
                    },
                )

            # Get ellipse outline if present
            ellipse_x = npz.get("ellipse_x")
            ellipse_y = npz.get("ellipse_y")

            if ellipse_x is not None and ellipse_y is not None:
                traces.append(
                    {
                        "type": "scatter",
                        "mode": "lines",
                        "x": ellipse_x.tolist(),
                        "y": ellipse_y.tolist(),
                        "line": {"color": "#ef4444", "width": 2},
                        "name": "95% Confidence Ellipse",
                    },
                )

            if not traces:
                return ViewerResult(
                    viewer_type=self.viewer_type,
                    render_type="error",
                    data=None,
                    error="Invalid scatter NPZ: missing x/y data",
                )

            # Get center point if available
            center_x = npz.get("center_x")
            center_y = npz.get("center_y")
            if center_x is not None and center_y is not None:
                traces.append(
                    {
                        "type": "scatter",
                        "mode": "markers",
                        "x": [float(center_x)],
                        "y": [float(center_y)],
                        "marker": {
                            "color": "#22c55e",
                            "size": 12,
                            "symbol": "cross",
                        },
                        "name": "Center",
                    },
                )

            plotly_config = {
                "data": traces,
                "layout": {
                    "title": metadata.get("metric_name", "Scatter Plot"),
                    "xaxis": {"title": "X"},
                    "yaxis": {"title": "Y", "scaleanchor": "x", "scaleratio": 1},
                    "showlegend": True,
                },
            }

            return ViewerResult(
                viewer_type=self.viewer_type,
                render_type="plotly",
                data=plotly_config,
                metadata={
                    "point_count": len(x_vals) if x_vals is not None else 0,
                    "has_ellipse": ellipse_x is not None,
                },
            )
        except Exception as e:
            return ViewerResult(
                viewer_type=self.viewer_type,
                render_type="error",
                data=None,
                error=f"Failed to parse scatter data: {e}",
            )
