"""Counter horizontal bar chart for COUNTER analysis kind."""

from __future__ import annotations

from typing import Any

import numpy as np
from bokeh.models import ColumnDataSource, HoverTool, Model

from src.visualization.plots.common import (
    PALETTE,
    create_figure,
    create_no_data_placeholder,
)


def create_counter_chart(
    npz_data: dict[str, np.ndarray] | None = None,
    summary: dict[str, Any] | None = None,
    *,
    title: str = "Category Counts",
    max_categories: int = 20,
) -> Model:
    """Create a horizontal bar chart from counter.npz data.

    Expected NPZ keys:
        - labels: Array of category labels (object array)
        - counts: Array of counts per category (int64 array)

    Or from summary:
        - top_k: List of (label, count) tuples

    Args:
        npz_data: Parsed NPZ data with 'labels' and 'counts' arrays
        summary: Optional summary dict with 'top_k' key
        title: Plot title
        max_categories: Maximum number of categories to display

    Returns:
        Bokeh figure or placeholder widget

    """
    labels = None
    counts = None

    if npz_data is not None and "labels" in npz_data and "counts" in npz_data:
        labels = list(npz_data["labels"])
        counts = list(npz_data["counts"])
    elif summary is not None and "top_k" in summary:
        top_k = summary["top_k"]
        if isinstance(top_k, list) and top_k:
            labels = [str(item[0]) for item in top_k]
            counts = [int(item[1]) for item in top_k]

    if not labels or not counts:
        return create_no_data_placeholder("Counter data not available")

    if len(labels) > max_categories:
        labels = labels[:max_categories]
        counts = counts[:max_categories]

    labels_reversed = labels[::-1]
    counts_reversed = counts[::-1]

    colors = [PALETTE[i % len(PALETTE)] for i in range(len(labels_reversed))]

    source = ColumnDataSource(
        data={
            "labels": labels_reversed,
            "counts": counts_reversed,
            "colors": colors,
        },
    )

    unique_count = summary.get("unique", len(labels)) if summary else len(labels)
    full_title = f"{title} (showing {len(labels)} of {unique_count} unique)"

    height = max(300, len(labels) * 25 + 100)

    p = create_figure(
        title=full_title,
        y_range=labels_reversed,
        x_axis_label="Count",
        height=height,
    )

    p.hbar(
        y="labels",
        right="counts",
        height=0.7,
        source=source,
        fill_color="colors",
        line_color="white",
        alpha=0.8,
    )

    hover = HoverTool(
        tooltips=[
            ("Category", "@labels"),
            ("Count", "@counts"),
        ],
    )
    p.add_tools(hover)

    p.ygrid.grid_line_color = None

    return p
