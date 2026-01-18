"""Plot generation utilities for the dashboard."""

from src.visualization.plots.common import (
    PALETTE,
    PLOT_HEIGHT,
    PLOT_WIDTH,
    THEME_COLORS,
    add_hover_tool,
    create_error_placeholder,
    create_figure,
    create_loading_placeholder,
    create_no_data_placeholder,
    create_stat_card,
    format_number,
)
from src.visualization.plots.contour_plot import create_contour_plot
from src.visualization.plots.counter_chart import create_counter_chart
from src.visualization.plots.ellipse_plot import create_ellipse_plot
from src.visualization.plots.histogram import create_histogram_plot
from src.visualization.plots.rate_chart import create_rate_chart
from src.visualization.plots.time_series import (
    create_multi_line_chart,
    create_no_data_time_series,
    create_sparkline,
    create_throughput_chart,
)

__all__ = [
    "PALETTE",
    "PLOT_HEIGHT",
    "PLOT_WIDTH",
    "THEME_COLORS",
    "add_hover_tool",
    "create_contour_plot",
    "create_counter_chart",
    "create_ellipse_plot",
    "create_error_placeholder",
    "create_figure",
    "create_histogram_plot",
    "create_loading_placeholder",
    "create_multi_line_chart",
    "create_no_data_placeholder",
    "create_no_data_time_series",
    "create_rate_chart",
    "create_sparkline",
    "create_stat_card",
    "create_throughput_chart",
    "format_number",
]
