"""Time-series chart components for the dashboard."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from bokeh.models import ColumnDataSource, DatetimeTickFormatter, HoverTool, Div
from bokeh.plotting import figure

from src.visualization.plots.common import (
    PALETTE,
    PLOT_HEIGHT,
    PLOT_WIDTH,
    THEME_COLORS,
    create_figure,
)

if TYPE_CHECKING:
    from bokeh.models import Model


def create_throughput_chart(
    timestamps: list[datetime],
    values: list[float],
    *,
    title: str = "Processing Throughput",
    y_label: str = "Jobs/sec",
    width: int = PLOT_WIDTH,
    height: int = PLOT_HEIGHT,
    show_area: bool = True,
) -> Model:
    """Create a time-series line chart with gradient fill.

    Args:
        timestamps: List of datetime timestamps
        values: List of values corresponding to timestamps
        title: Chart title
        y_label: Y-axis label
        width: Chart width
        height: Chart height
        show_area: Whether to show area fill under the line

    Returns:
        Bokeh figure

    """
    if not timestamps or not values:
        return create_no_data_time_series(title=title, width=width, height=height)

    p = create_figure(
        title=title,
        x_axis_label="Time",
        y_axis_label=y_label,
        width=width,
        height=height,
        x_axis_type="datetime",
    )

    source = ColumnDataSource(data={
        "timestamp": timestamps,
        "value": values,
    })

    # Area fill under the line
    if show_area:
        p.varea(
            x="timestamp",
            y1=0,
            y2="value",
            source=source,
            fill_color=THEME_COLORS["primary"],
            fill_alpha=0.15,
        )

    # Main line
    line = p.line(
        x="timestamp",
        y="value",
        source=source,
        line_color=THEME_COLORS["primary"],
        line_width=2.5,
        line_alpha=0.9,
    )

    # Data points
    p.scatter(
        x="timestamp",
        y="value",
        source=source,
        size=6,
        color=THEME_COLORS["primary"],
        alpha=0.8,
    )

    # Hover tool
    hover = HoverTool(
        tooltips=[
            ("Time", "@timestamp{%Y-%m-%d %H:%M:%S}"),
            ("Value", "@value{0.00}"),
        ],
        formatters={"@timestamp": "datetime"},
        mode="vline",
        renderers=[line],
    )
    p.add_tools(hover)

    # Format x-axis for datetime
    p.xaxis.formatter = DatetimeTickFormatter(
        hours="%H:%M",
        minutes="%H:%M",
        hourmin="%H:%M",
    )

    return p


def create_multi_line_chart(
    data: dict[str, list[tuple[datetime, float]]],
    *,
    title: str = "Metrics Over Time",
    y_label: str = "Value",
    width: int = PLOT_WIDTH,
    height: int = PLOT_HEIGHT,
) -> Model:
    """Create a multi-line time-series chart.

    Args:
        data: Dict mapping series name to list of (timestamp, value) tuples
        title: Chart title
        y_label: Y-axis label
        width: Chart width
        height: Chart height

    Returns:
        Bokeh figure

    """
    if not data:
        return create_no_data_time_series(title=title, width=width, height=height)

    p = create_figure(
        title=title,
        x_axis_label="Time",
        y_axis_label=y_label,
        width=width,
        height=height,
        x_axis_type="datetime",
    )

    colors = PALETTE[:len(data)]

    for (name, series), color in zip(data.items(), colors):
        if not series:
            continue

        timestamps = [point[0] for point in series]
        values = [point[1] for point in series]

        source = ColumnDataSource(data={
            "timestamp": timestamps,
            "value": values,
            "name": [name] * len(timestamps),
        })

        line = p.line(
            x="timestamp",
            y="value",
            source=source,
            line_color=color,
            line_width=2,
            legend_label=name,
        )

    # Configure legend
    p.legend.location = "top_left"
    p.legend.background_fill_color = THEME_COLORS["surface"]
    p.legend.background_fill_alpha = 0.8
    p.legend.label_text_color = THEME_COLORS["text"]
    p.legend.border_line_color = THEME_COLORS["border"]

    # Format x-axis
    p.xaxis.formatter = DatetimeTickFormatter(
        hours="%H:%M",
        minutes="%H:%M",
    )

    return p


def create_sparkline(
    values: list[float],
    *,
    color: str = "primary",
    width: int = 120,
    height: int = 40,
) -> Model:
    """Create a minimal sparkline chart.

    Args:
        values: List of values
        color: Theme color key
        width: Chart width
        height: Chart height

    Returns:
        Bokeh figure

    """
    if not values:
        return Div(text="", width=width, height=height)

    p = figure(
        width=width,
        height=height,
        toolbar_location=None,
        tools="",
    )

    # Hide all axes and grid
    p.xaxis.visible = False
    p.yaxis.visible = False
    p.xgrid.visible = False
    p.ygrid.visible = False
    p.outline_line_color = None
    p.background_fill_color = "transparent"
    p.border_fill_color = "transparent"

    x = list(range(len(values)))
    line_color = THEME_COLORS.get(color, THEME_COLORS["primary"])

    # Area fill
    p.varea(
        x=x,
        y1=[0] * len(values),
        y2=values,
        fill_color=line_color,
        fill_alpha=0.2,
    )

    # Line
    p.line(
        x=x,
        y=values,
        line_color=line_color,
        line_width=1.5,
    )

    return p


def create_no_data_time_series(
    *,
    title: str = "No Data Available",
    width: int = PLOT_WIDTH,
    height: int = PLOT_HEIGHT,
) -> Div:
    """Create a placeholder for when no time-series data is available.

    Args:
        title: Placeholder title
        width: Width in pixels
        height: Height in pixels

    Returns:
        Bokeh Div widget

    """
    html = f"""
    <div style="
        width: {width}px;
        height: {height}px;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        background: {THEME_COLORS['surface']};
        border: 1px dashed {THEME_COLORS['border']};
        border-radius: 12px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    ">
        <div style="
            font-size: 16px;
            font-weight: 600;
            color: {THEME_COLORS['text']};
            margin-bottom: 8px;
        ">{title}</div>
        <div style="
            font-size: 48px;
            margin-bottom: 12px;
            opacity: 0.5;
        ">📈</div>
        <div style="
            color: {THEME_COLORS['text_muted']};
            font-size: 14px;
        ">Waiting for metrics data...</div>
        <div style="
            color: {THEME_COLORS['text_muted']};
            font-size: 12px;
            margin-top: 8px;
        ">Check Prometheus connection</div>
    </div>
    """

    return Div(text=html, width=width, height=height)
