"""Histogram plot for DISTRIBUTION_1D analysis kind."""

from __future__ import annotations

from typing import Any

import numpy as np
from bokeh.models import ColumnDataSource, HoverTool, Model

from src.visualization.plots.common import (
    THEME_COLORS,
    create_figure,
    create_no_data_placeholder,
)


def create_histogram_plot(
    npz_data: dict[str, np.ndarray],
    *,
    title: str = "Distribution",
    summary: dict[str, Any] | None = None,
) -> Model:
    """Create a histogram plot from histogram.npz data.

    Expected NPZ keys:
        - edges: Array of bin edges (n+1 elements)
        - counts: Array of counts per bin (n elements)

    Args:
        npz_data: Parsed NPZ data with 'edges' and 'counts' arrays
        title: Plot title
        summary: Optional summary dict to display in title

    Returns:
        Bokeh figure or placeholder widget

    """
    if npz_data is None or "edges" not in npz_data or "counts" not in npz_data:
        return create_no_data_placeholder("Histogram data not available")

    edges = npz_data["edges"]
    counts = npz_data["counts"]

    if len(edges) < 2 or len(counts) == 0:
        return create_no_data_placeholder("Histogram data is empty")

    left = edges[:-1]
    right = edges[1:]

    source = ColumnDataSource(
        data={
            "left": left,
            "right": right,
            "top": counts,
            "count": counts,
            "range_start": [f"{val:.4g}" for val in left],
            "range_end": [f"{r:.4g}" for r in right],
        },
    )

    full_title = title
    if summary:
        mean = summary.get("mean")
        std = summary.get("std")
        count = summary.get("count")
        if mean is not None and std is not None:
            full_title = f"{title} (n={count}, mean={mean:.4g}, std={std:.4g})"

    p = create_figure(
        title=full_title,
        x_axis_label="Value",
        y_axis_label="Count",
    )

    p.quad(
        source=source,
        left="left",
        right="right",
        top="top",
        bottom=0,
        fill_color=THEME_COLORS["primary"],
        line_color="white",
        alpha=0.7,
        hover_fill_color=THEME_COLORS["secondary"],
        hover_alpha=0.9,
    )

    hover = HoverTool(
        tooltips=[
            ("Range", "@range_start to @range_end"),
            ("Count", "@count"),
        ],
        mode="vline",
    )
    p.add_tools(hover)

    return p
