"""Sidebar navigation widget for Dashdark X style dashboard."""

from __future__ import annotations

from bokeh.models import Div

from src.visualization.plots.common import THEME_COLORS


def create_sidebar(
    *,
    active_section: str = "dashboard",
    width: int = 72,
) -> Div:
    """Create a collapsible sidebar with navigation items.

    Args:
        active_section: Currently active section key
        width: Sidebar width in pixels

    Returns:
        Bokeh Div widget

    """
    nav_items = [
        ("dashboard", "📊", "Dashboard"),
        ("reports", "📈", "Reports"),
        ("algorithms", "🔧", "Algorithms"),
        ("logs", "📋", "Logs"),
        ("settings", "⚙️", "Settings"),
    ]

    nav_html_parts = []
    for key, icon, label in nav_items:
        is_active = key == active_section
        bg_color = THEME_COLORS["primary"] if is_active else "transparent"
        text_color = THEME_COLORS["text"] if is_active else THEME_COLORS["text_muted"]

        nav_html_parts.append(f"""
        <div class="nav-item" data-section="{key}" style="
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            padding: 14px 8px;
            margin: 4px 8px;
            border-radius: 12px;
            background: {bg_color};
            cursor: pointer;
            transition: all 0.2s ease;
        " title="{label}">
            <span style="font-size: 22px;">{icon}</span>
            <span style="
                font-size: 10px;
                color: {text_color};
                margin-top: 4px;
                font-weight: 500;
            ">{label}</span>
        </div>
        """)

    nav_html = "\n".join(nav_html_parts)

    html = f"""
    <div style="
        width: {width}px;
        height: 100%;
        min-height: 600px;
        background: {THEME_COLORS['surface']};
        border-right: 1px solid {THEME_COLORS['border_light']};
        display: flex;
        flex-direction: column;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    ">
        <!-- Logo -->
        <div style="
            padding: 20px 12px;
            text-align: center;
            border-bottom: 1px solid {THEME_COLORS['border_light']};
        ">
            <div style="
                font-size: 28px;
                font-weight: 700;
                color: {THEME_COLORS['primary']};
            ">V</div>
            <div style="
                font-size: 9px;
                color: {THEME_COLORS['text_muted']};
                letter-spacing: 1px;
                margin-top: 2px;
            ">VISIO</div>
        </div>

        <!-- Navigation -->
        <div style="flex: 1; padding: 12px 0;">
            {nav_html}
        </div>

        <!-- Bottom section -->
        <div style="
            padding: 16px;
            border-top: 1px solid {THEME_COLORS['border_light']};
        ">
            <div style="
                width: 36px;
                height: 36px;
                background: {THEME_COLORS['primary']};
                border-radius: 50%;
                display: flex;
                align-items: center;
                justify-content: center;
                margin: 0 auto;
                font-size: 14px;
                font-weight: 600;
                color: white;
            ">U</div>
        </div>
    </div>
    <style>
        .nav-item:hover {{
            background: {THEME_COLORS['surface_elevated']} !important;
        }}
    </style>
    """

    return Div(text=html, width=width, height=600)


def create_section_header(
    title: str,
    *,
    icon: str = "",
    show_collapse: bool = True,
) -> Div:
    """Create a section header with optional collapse button.

    Args:
        title: Section title
        icon: Optional emoji icon
        show_collapse: Whether to show collapse toggle

    Returns:
        Bokeh Div widget

    """
    collapse_html = ""
    if show_collapse:
        collapse_html = f"""
        <div style="
            color: {THEME_COLORS['text_muted']};
            cursor: pointer;
            font-size: 18px;
            padding: 4px 8px;
            border-radius: 6px;
            transition: background 0.2s ease;
        " class="collapse-btn">▼</div>
        """

    html = f"""
    <div style="
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 16px 0;
        border-bottom: 1px solid {THEME_COLORS['border_light']};
        margin-bottom: 16px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    ">
        <div style="display: flex; align-items: center; gap: 10px;">
            {f'<span style="font-size: 20px;">{icon}</span>' if icon else ''}
            <span style="
                font-size: 18px;
                font-weight: 600;
                color: {THEME_COLORS['text']};
            ">{title}</span>
        </div>
        {collapse_html}
    </div>
    <style>
        .collapse-btn:hover {{
            background: {THEME_COLORS['surface_elevated']};
        }}
    </style>
    """

    return Div(text=html, width=800, height=60)


def create_breadcrumb(
    *items: str,
    separator: str = "/",
) -> Div:
    """Create a breadcrumb navigation.

    Args:
        *items: Breadcrumb items
        separator: Separator between items

    Returns:
        Bokeh Div widget

    """
    parts = []
    for i, item in enumerate(items):
        is_last = i == len(items) - 1
        color = THEME_COLORS["text"] if is_last else THEME_COLORS["text_muted"]
        weight = "600" if is_last else "400"

        parts.append(f"""
        <span style="color: {color}; font-weight: {weight};">{item}</span>
        """)

        if not is_last:
            parts.append(f"""
            <span style="color: {THEME_COLORS['border']}; margin: 0 8px;">{separator}</span>
            """)

    html = f"""
    <div style="
        display: flex;
        align-items: center;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        font-size: 14px;
    ">
        {''.join(parts)}
    </div>
    """

    return Div(text=html, width=400, height=30)
