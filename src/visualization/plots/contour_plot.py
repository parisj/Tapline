"""2D density contour/heatmap plot for CONTOUR_2D analysis kind."""

from __future__ import annotations

from typing import Any

import numpy as np
from bokeh.models import ColorBar, LinearColorMapper, Model
from bokeh.palettes import Viridis256

from src.visualization.plots.common import create_figure, create_no_data_placeholder


def create_contour_plot(
    npz_data: dict[str, np.ndarray],
    *,
    title: str = "2D Density",
    summary: dict[str, Any] | None = None,
) -> Model:
    """Create a density heatmap from contour.npz data.

    Note: Bokeh doesn't have native contour support, so we render
    the density as a heatmap using image().

    Expected NPZ keys:
        - x: 2D meshgrid of X coordinates
        - y: 2D meshgrid of Y coordinates
        - z: 2D array of density values
        - levels: Optional array of contour levels (for reference)

    Args:
        npz_data: Parsed NPZ data with density grid
        title: Plot title
        summary: Optional summary dict with statistics

    Returns:
        Bokeh figure or placeholder widget

    """
    if npz_data is None:
        return create_no_data_placeholder("Contour data not available")

    x = npz_data.get("x")
    y = npz_data.get("y")
    z = npz_data.get("z")

    if x is None or y is None or z is None:
        return create_no_data_placeholder("Contour data is incomplete")

    x = np.asarray(x)
    y = np.asarray(y)
    z = np.asarray(z)

    if z.ndim != 2 or z.size == 0:
        return create_no_data_placeholder("Contour density grid is invalid")

    if x.ndim == 2:
        xmin, xmax = float(x.min()), float(x.max())
        ymin, ymax = float(y.min()), float(y.max())
    else:
        xmin, xmax = float(x[0]), float(x[-1])
        ymin, ymax = float(y[0]), float(y[-1])

    full_title = title
    if summary:
        count = summary.get("count")
        grid_size = summary.get("grid_size")
        if count is not None:
            full_title = f"{title} (n={count}"
            if grid_size:
                full_title += f", grid={grid_size}x{grid_size}"
            full_title += ")"

    p = create_figure(
        title=full_title,
        x_axis_label="X",
        y_axis_label="Y",
        x_range=(xmin, xmax),
        y_range=(ymin, ymax),
        match_aspect=True,
    )

    z_min = float(z.min())
    z_max = float(z.max())
    if z_min == z_max:
        z_max = z_min + 1.0

    color_mapper = LinearColorMapper(
        palette=Viridis256,
        low=z_min,
        high=z_max,
    )

    p.image(
        image=[z],
        x=xmin,
        y=ymin,
        dw=xmax - xmin,
        dh=ymax - ymin,
        color_mapper=color_mapper,
    )

    color_bar = ColorBar(
        color_mapper=color_mapper,
        label_standoff=12,
        location=(0, 0),
        title="Density",
    )
    p.add_layout(color_bar, "right")

    return p
