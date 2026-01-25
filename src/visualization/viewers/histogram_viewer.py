"""Histogram viewer for NPZ histogram artifacts.

Converts histogram data (bin edges + counts) to Plotly bar chart configuration.
"""

from __future__ import annotations

import io
from typing import Any

import numpy as np

from src.domain.evaluation import AggregationType
from src.visualization.viewers.base import ViewerResult


class HistogramViewer:
    """Viewer for histogram NPZ artifacts.

    Handles artifacts with MIME type 'application/x-npz' and HISTOGRAM
    aggregation type. Produces Plotly bar chart configuration.
    """

    @property
    def viewer_type(self) -> str:
        return "histogram"

    def can_view(self, *, mime: str, aggregation_mask: int) -> bool:
        """Check if this is a histogram artifact."""
        is_npz = mime in ("application/x-npz", "application/octet-stream")
        has_histogram = bool(aggregation_mask & AggregationType.HISTOGRAM.value)
        return is_npz and has_histogram

    def view(
        self,
        *,
        data: bytes,
        mime: str,
        aggregation_mask: int,
        metadata: dict[str, Any],
    ) -> ViewerResult:
        """Render histogram data as Plotly bar chart config."""
        try:
            buf = io.BytesIO(data)
            npz = np.load(buf)

            edges = npz.get("edges", npz.get("bin_edges"))
            counts = npz.get("counts")

            if edges is None or counts is None:
                return ViewerResult(
                    viewer_type=self.viewer_type,
                    render_type="error",
                    data=None,
                    error="Invalid histogram NPZ: missing edges or counts",
                )

            edges_list = edges.tolist()
            counts_list = counts.tolist()

            # Calculate bin centers for x-axis
            bin_centers = [(edges_list[i] + edges_list[i + 1]) / 2 for i in range(len(edges_list) - 1)]

            plotly_config = {
                "data": [
                    {
                        "type": "bar",
                        "x": bin_centers,
                        "y": counts_list,
                        "marker": {"color": "#3b82f6"},
                    },
                ],
                "layout": {
                    "title": metadata.get("metric_name", "Histogram"),
                    "xaxis": {"title": "Value"},
                    "yaxis": {"title": "Count"},
                    "bargap": 0.05,
                },
            }

            return ViewerResult(
                viewer_type=self.viewer_type,
                render_type="plotly",
                data=plotly_config,
                metadata={
                    "bin_count": len(counts_list),
                    "total_count": int(sum(counts_list)),
                    "bin_edges": edges_list,
                },
            )
        except Exception as e:
            return ViewerResult(
                viewer_type=self.viewer_type,
                render_type="error",
                data=None,
                error=f"Failed to parse histogram: {e}",
            )
