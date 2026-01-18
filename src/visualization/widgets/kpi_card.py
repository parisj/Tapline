"""KPI card widgets with clean minimal design."""

from __future__ import annotations

from bokeh.models import Div

from src.visualization.plots.common import THEME_COLORS, format_number


def create_kpi_card(
    title: str,
    value: float | str,
    *,
    trend_percent: float | None = None,
    subtitle: str = "",
    color: str = "primary",
    width: int = 200,
    height: int = 100,
) -> Div:
    """Create a minimal KPI card.

    Args:
        title: Card title
        value: Main value
        trend_percent: Optional percentage change
        subtitle: Optional subtitle
        color: Accent color key
        width: Card width
        height: Card height

    Returns:
        Bokeh Div widget

    """
    accent_color = THEME_COLORS.get(color, THEME_COLORS["primary"])

    if isinstance(value, float):
        display_value = format_number(value)
    else:
        display_value = str(value)

    trend_html = ""
    if trend_percent is not None:
        if trend_percent > 0:
            trend_color = THEME_COLORS["success"]
            sign = "+"
        elif trend_percent < 0:
            trend_color = THEME_COLORS["error"]
            sign = ""
        else:
            trend_color = THEME_COLORS["text_muted"]
            sign = ""

        trend_html = f"""
        <span style="
            color: {trend_color};
            font-size: 11px;
            font-weight: 500;
            margin-left: 8px;
        ">{sign}{abs(trend_percent):.1f}%</span>
        """

    html = f"""
    <div style="
        padding: 16px 20px;
        background: {THEME_COLORS['surface']};
        border: 1px solid {THEME_COLORS['border_light']};
        border-radius: 12px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    ">
        <div style="
            color: {THEME_COLORS['text_muted']};
            font-size: 11px;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        ">{title}</div>
        <div style="display: flex; align-items: baseline;">
            <span style="
                color: {accent_color};
                font-size: 24px;
                font-weight: 600;
            ">{display_value}</span>
            {trend_html}
        </div>
        {f'<div style="color: {THEME_COLORS["text_muted"]}; font-size: 10px; margin-top: 4px;">{subtitle}</div>' if subtitle else ''}
    </div>
    """

    return Div(text=html, width=width, height=height)


def create_metric_card(
    title: str,
    value: str,
    *,
    subtitle: str = "",
    width: int = 280,
    height: int = 140,
) -> Div:
    """Create a larger metric display card.

    Args:
        title: Card title
        value: Main value (formatted string)
        subtitle: Optional subtitle
        width: Card width
        height: Card height

    Returns:
        Bokeh Div widget

    """
    html = f"""
    <div style="
        padding: 20px 24px;
        background: {THEME_COLORS['surface']};
        border: 1px solid {THEME_COLORS['border_light']};
        border-radius: 12px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        height: {height - 40}px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    ">
        <div style="
            color: {THEME_COLORS['text_muted']};
            font-size: 12px;
            font-weight: 500;
        ">{title}</div>
        <div style="
            color: {THEME_COLORS['text']};
            font-size: 36px;
            font-weight: 600;
            letter-spacing: -1px;
        ">{value}</div>
        {f'<div style="color: {THEME_COLORS["text_muted"]}; font-size: 11px;">{subtitle}</div>' if subtitle else '<div></div>'}
    </div>
    """

    return Div(text=html, width=width, height=height)


def create_status_indicator(
    status: str,
    *,
    is_healthy: bool = True,
) -> Div:
    """Create a minimal status indicator.

    Args:
        status: Status text
        is_healthy: Whether healthy

    Returns:
        Bokeh Div widget

    """
    color = THEME_COLORS["success"] if is_healthy else THEME_COLORS["error"]

    html = f"""
    <div style="
        display: inline-flex;
        align-items: center;
        gap: 6px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    ">
        <div style="
            width: 6px;
            height: 6px;
            background: {color};
            border-radius: 50%;
        "></div>
        <span style="
            color: {THEME_COLORS['text_muted']};
            font-size: 11px;
        ">{status}</span>
    </div>
    """

    return Div(text=html, width=150, height=24)
