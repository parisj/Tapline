"""KPI card widgets with clean light design."""

from __future__ import annotations

from bokeh.models import Div

from src.visualization.plots.common import THEME_COLORS, format_number

# Card styling constants
SHADOW = "0 4px 12px rgba(0, 0, 0, 0.08)"
TRANSITION = "all 0.3s cubic-bezier(0.25, 0.8, 0.25, 1)"


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
    """Create a KPI card with light theme design.

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
            trend_icon = """<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14"
                viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <polyline points="18 15 12 9 6 15"></polyline></svg>"""
            sign = "+"
        elif trend_percent < 0:
            trend_color = THEME_COLORS["error"]
            trend_icon = """<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14"
                viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <polyline points="6 9 12 15 18 9"></polyline></svg>"""
            sign = ""
        else:
            trend_color = THEME_COLORS["text_muted"]
            trend_icon = ""
            sign = ""

        trend_html = f"""
        <div style="
            display: flex;
            align-items: center;
            color: {trend_color};
            font-size: 12px;
            font-weight: 500;
            margin-top: 4px;
        ">{trend_icon} {sign}{abs(trend_percent):.1f}%</div>
        """

    html = f"""
    <div class="kpi-card" style="
        padding: 16px;
        background: {THEME_COLORS['surface']};
        border-radius: 12px;
        box-shadow: {SHADOW};
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        transition: {TRANSITION};
    ">
        <div style="
            color: {THEME_COLORS['text_muted']};
            font-size: 12px;
            font-weight: 500;
            margin-bottom: 8px;
        ">{title}</div>
        <div style="
            color: {THEME_COLORS['text']};
            font-size: 28px;
            font-weight: 700;
        ">{display_value}</div>
        {trend_html}
    </div>
    """

    if subtitle:
        subtitle_html = f"""
        <div style="color: {THEME_COLORS['text_muted']}; font-size: 11px; margin-top: 4px;">
            {subtitle}
        </div>
        """
        html = html.replace("</div>\n    ", subtitle_html + "</div>\n    ", 1)

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
        border-radius: 12px;
        box-shadow: {SHADOW};
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        height: {height - 40}px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        transition: {TRANSITION};
    ">
        <div style="
            color: {THEME_COLORS['text_muted']};
            font-size: 12px;
            font-weight: 500;
        ">{title}</div>
        <div style="
            color: {THEME_COLORS['text']};
            font-size: 36px;
            font-weight: 700;
            letter-spacing: -1px;
        ">{value}</div>
    </div>
    """

    if subtitle:
        subtitle_div = f'<div style="color: {THEME_COLORS["text_muted"]}; font-size: 11px;">'
        subtitle_div += f"{subtitle}</div>"
        html = html.replace("</div>\n    ", subtitle_div + "</div>\n    ", 1)

    return Div(text=html, width=width, height=height)


def create_status_indicator(
    status: str,
    *,
    is_healthy: bool = True,
) -> Div:
    """Create a status indicator.

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
        gap: 8px;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    ">
        <div style="
            width: 8px;
            height: 8px;
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
