"""Shared plot utilities and theming for modern dashboard."""

from __future__ import annotations

from bokeh.models import Div, HoverTool
from bokeh.plotting import figure

PLOT_WIDTH = 700
PLOT_HEIGHT = 450

# Dark theme color palette - elegant minimal style
THEME_COLORS = {
    # Primary colors
    "primary": "#6366f1",      # Indigo
    "primary_light": "#818cf8",
    "primary_dark": "#4f46e5",
    "secondary": "#06b6d4",    # Cyan
    "secondary_light": "#22d3ee",
    "accent": "#ec4899",       # Pink

    # Semantic colors
    "warning": "#f59e0b",      # Amber
    "info": "#3b82f6",         # Blue
    "neutral": "#6b7280",      # Gray
    "success": "#10b981",      # Emerald
    "error": "#ef4444",        # Red

    # Backgrounds - very dark
    "background": "#0a0a0f",   # Near black
    "surface": "#12121a",      # Dark card
    "surface_elevated": "#1a1a24",  # Slightly lighter

    # Borders - subtle
    "border": "#2a2a35",
    "border_light": "#1f1f28",

    # Text
    "text": "#e4e4e7",         # Light gray
    "text_muted": "#71717a",   # Muted gray

    # Trend indicators
    "trend_up": "#10b981",
    "trend_down": "#ef4444",
    "trend_neutral": "#6b7280",

    # Card styling
    "card_bg": "#12121a",
    "card_border": "#1f1f28",

    # Glow effects
    "glow_primary": "rgba(99, 102, 241, 0.2)",
    "glow_success": "rgba(16, 185, 129, 0.2)",
    "glow_error": "rgba(239, 68, 68, 0.2)",
    "glow_warning": "rgba(245, 158, 11, 0.2)",
}

# Vibrant gradient palette for dark mode charts
PALETTE = [
    "#818cf8",  # Indigo
    "#a78bfa",  # Violet
    "#f472b6",  # Pink
    "#fb7185",  # Rose
    "#fb923c",  # Orange
    "#facc15",  # Yellow
    "#4ade80",  # Green
    "#2dd4bf",  # Teal
    "#22d3ee",  # Cyan
    "#60a5fa",  # Blue
]


def create_figure(
    *,
    title: str = "",
    x_axis_label: str = "",
    y_axis_label: str = "",
    width: int = PLOT_WIDTH,
    height: int = PLOT_HEIGHT,
    tools: str = "pan,wheel_zoom,box_zoom,reset,save",
    **kwargs: object,
) -> figure:
    """Create a styled Bokeh figure with modern defaults.

    Args:
        title: Plot title
        x_axis_label: X-axis label
        y_axis_label: Y-axis label
        width: Plot width in pixels
        height: Plot height in pixels
        tools: Bokeh tools string
        **kwargs: Additional figure arguments

    Returns:
        Configured Bokeh figure

    """
    p = figure(
        title=title,
        x_axis_label=x_axis_label,
        y_axis_label=y_axis_label,
        width=width,
        height=height,
        tools=tools,
        **kwargs,
    )

    # Modern title styling
    p.title.text_font_size = "16pt"
    p.title.text_font = "Helvetica"
    p.title.text_font_style = "bold"
    p.title.text_color = THEME_COLORS["text"]

    # Axis label styling
    p.xaxis.axis_label_text_font_size = "12pt"
    p.yaxis.axis_label_text_font_size = "12pt"
    p.xaxis.axis_label_text_color = THEME_COLORS["text_muted"]
    p.yaxis.axis_label_text_color = THEME_COLORS["text_muted"]

    # Tick label styling
    p.xaxis.major_label_text_font_size = "10pt"
    p.yaxis.major_label_text_font_size = "10pt"
    p.xaxis.major_label_text_color = THEME_COLORS["text_muted"]
    p.yaxis.major_label_text_color = THEME_COLORS["text_muted"]

    # Background and border
    p.background_fill_color = THEME_COLORS["background"]
    p.border_fill_color = THEME_COLORS["background"]
    p.outline_line_color = THEME_COLORS["border"]

    # Grid styling
    p.grid.grid_line_color = THEME_COLORS["border"]
    p.grid.grid_line_alpha = 0.5
    p.grid.grid_line_dash = [4, 4]

    # Axis line styling
    p.xaxis.axis_line_color = THEME_COLORS["border"]
    p.yaxis.axis_line_color = THEME_COLORS["border"]
    p.xaxis.minor_tick_line_color = None
    p.yaxis.minor_tick_line_color = None
    p.xaxis.major_tick_line_color = THEME_COLORS["border"]
    p.yaxis.major_tick_line_color = THEME_COLORS["border"]

    return p


def add_hover_tool(
    p: figure,
    tooltips: list[tuple[str, str]],
    renderers: list | None = None,
) -> None:
    """Add a styled hover tool to a figure.

    Args:
        p: Bokeh figure
        tooltips: List of (label, @field) tuples
        renderers: Optional list of renderers to attach to

    """
    hover = HoverTool(
        tooltips=tooltips,
        mode="mouse",
        renderers=renderers or [],
    )
    p.add_tools(hover)


def create_no_data_placeholder(
    message: str = "No data available",
    *,
    width: int = PLOT_WIDTH,
    height: int = PLOT_HEIGHT,
) -> Div:
    """Create a placeholder div for when no data is available.

    Args:
        message: Message to display
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
        align-items: center;
        justify-content: center;
        background: {THEME_COLORS['surface']};
        border: 1px dashed {THEME_COLORS['border']};
        border-radius: 8px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    ">
        <p style="
            margin: 0;
            color: {THEME_COLORS['text_muted']};
            font-size: 14px;
        ">{message}</p>
    </div>
    """
    return Div(text=html, width=width, height=height)


def create_error_placeholder(
    error: str,
    *,
    width: int = PLOT_WIDTH,
    height: int = PLOT_HEIGHT,
) -> Div:
    """Create a placeholder div for error states.

    Args:
        error: Error message to display
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
        align-items: center;
        justify-content: center;
        background: rgba(248, 113, 113, 0.1);
        border: 1px solid {THEME_COLORS['error']};
        border-radius: 8px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    ">
        <div style="text-align: center;">
            <p style="
                margin: 0 0 4px 0;
                color: {THEME_COLORS['error']};
                font-size: 14px;
                font-weight: 600;
            ">Error</p>
            <p style="
                margin: 0;
                color: {THEME_COLORS['text_muted']};
                font-size: 13px;
            ">{error}</p>
        </div>
    </div>
    """
    return Div(text=html, width=width, height=height)


def create_loading_placeholder(
    message: str = "Loading...",
    *,
    width: int = PLOT_WIDTH,
    height: int = PLOT_HEIGHT,
) -> Div:
    """Create a loading state placeholder.

    Args:
        message: Loading message
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
        align-items: center;
        justify-content: center;
        background: {THEME_COLORS['surface']};
        border-radius: 12px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    ">
        <div style="text-align: center;">
            <div style="
                width: 40px;
                height: 40px;
                border: 4px solid {THEME_COLORS['border']};
                border-top-color: {THEME_COLORS['primary']};
                border-radius: 50%;
                animation: spin 1s linear infinite;
                margin: 0 auto 16px auto;
            "></div>
            <p style="
                margin: 0;
                color: {THEME_COLORS['text_muted']};
                font-size: 14px;
            ">{message}</p>
        </div>
    </div>
    <style>
        @keyframes spin {{
            to {{ transform: rotate(360deg); }}
        }}
    </style>
    """
    return Div(text=html, width=width, height=height)


def create_stat_card(
    label: str,
    value: str | float,
    *,
    color: str = "primary",
    width: int = 160,
) -> Div:
    """Create a stat card widget.

    Args:
        label: Stat label
        value: Stat value
        color: Theme color key
        width: Card width in pixels

    Returns:
        Bokeh Div widget

    """
    accent_color = THEME_COLORS.get(color, THEME_COLORS["primary"])
    value_str = f"{value:.4f}" if isinstance(value, float) else str(value)

    html = f"""
    <div style="
        padding: 12px 16px;
        background: {THEME_COLORS['surface']};
        border: 1px solid {THEME_COLORS['border_light']};
        border-radius: 6px;
        border-left: 3px solid {accent_color};
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    ">
        <div style="
            color: {THEME_COLORS['text_muted']};
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 4px;
        ">{label}</div>
        <div style="
            color: {THEME_COLORS['text']};
            font-size: 20px;
            font-weight: 700;
        ">{value_str}</div>
    </div>
    """
    return Div(text=html, width=width, height=70)


def format_number(value: float, precision: int = 2) -> str:
    """Format a number for display with appropriate precision.

    Args:
        value: Number to format
        precision: Decimal places

    Returns:
        Formatted string

    """
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.{precision}f}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.{precision}f}K"
    if abs(value) < 0.01 and value != 0:
        return f"{value:.2e}"
    return f"{value:.{precision}f}"
