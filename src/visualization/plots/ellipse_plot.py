"""2D scatter plot with covariance ellipse for ELLIPSE_2D analysis kind."""

from __future__ import annotations

from typing import Any

import numpy as np
from bokeh.models import ColumnDataSource, HoverTool, Model

from src.visualization.plots.common import (
    PALETTE,
    THEME_COLORS,
    create_figure,
    create_no_data_placeholder,
)


def create_ellipse_plot(
    npz_data: dict[str, np.ndarray],
    *,
    title: str = "2D Covariance Ellipse",
    summary: dict[str, Any] | None = None,
) -> Model:
    """Create a scatter plot with ellipse overlays from ellipse.npz.

    Expected NPZ keys:
        - xs: Object array of polyline x-coordinates (one per sigma level)
        - ys: Object array of polyline y-coordinates (one per sigma level)
        - k: Array of sigma multipliers (e.g., [1, 2, 3])
        - center: Optional 2-element array with center coordinates

    Args:
        npz_data: Parsed NPZ data with ellipse polylines
        title: Plot title
        summary: Optional summary dict with statistics

    Returns:
        Bokeh figure or placeholder widget

    """
    if npz_data is None:
        return create_no_data_placeholder("Ellipse data not available")

    xs_list = npz_data.get("xs")
    ys_list = npz_data.get("ys")
    k_values = npz_data.get("k")

    if xs_list is None or ys_list is None:
        return create_no_data_placeholder("Ellipse data is incomplete")

    center = npz_data.get("center")

    full_title = title
    if summary:
        count = summary.get("count")
        if count is not None:
            full_title = f"{title} (n={count})"

    p = create_figure(
        title=full_title,
        x_axis_label="X",
        y_axis_label="Y",
        match_aspect=True,
    )

    for i, (xs, ys) in enumerate(zip(xs_list, ys_list)):
        if xs is None or ys is None:
            continue

        xs_arr = np.asarray(xs)
        ys_arr = np.asarray(ys)

        if len(xs_arr) == 0 or len(ys_arr) == 0:
            continue

        k = k_values[i] if k_values is not None and i < len(k_values) else i + 1
        color = PALETTE[i % len(PALETTE)]
        legend_label = f"{k}σ"

        p.line(
            xs_arr,
            ys_arr,
            line_color=color,
            line_width=2,
            alpha=0.8,
            legend_label=legend_label,
        )

    if center is not None and len(center) >= 2:
        center_source = ColumnDataSource(
            data={
                "x": [float(center[0])],
                "y": [float(center[1])],
            },
        )
        p.circle(
            x="x",
            y="y",
            size=10,
            source=center_source,
            color=THEME_COLORS["text"],
            legend_label="Center",
        )

        hover = HoverTool(
            tooltips=[
                ("X", "@x{0.4f}"),
                ("Y", "@y{0.4f}"),
            ],
            renderers=[p.renderers[-1]],
        )
        p.add_tools(hover)

    p.legend.click_policy = "hide"
    p.legend.location = "top_right"

    return p
