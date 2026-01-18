"""Real-time clock and datetime widgets for the dashboard."""

from __future__ import annotations

from datetime import datetime

from bokeh.models import Div

from src.visualization.plots.common import THEME_COLORS


def create_clock_widget(
    *,
    show_seconds: bool = True,
    width: int = 200,
) -> Div:
    """Create a real-time clock widget.

    Note: The clock updates via JavaScript in the browser.

    Args:
        show_seconds: Whether to show seconds
        width: Widget width

    Returns:
        Bokeh Div widget with auto-updating clock

    """
    time_format = "HH:mm:ss" if show_seconds else "HH:mm"

    html = f"""
    <div id="clock-widget" style="
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        color: {THEME_COLORS['text']};
        text-align: right;
    ">
        <div id="clock-time" style="
            font-size: 28px;
            font-weight: 700;
            letter-spacing: 1px;
            font-variant-numeric: tabular-nums;
        ">--:--:--</div>
        <div id="clock-date" style="
            font-size: 13px;
            color: {THEME_COLORS['text_muted']};
            margin-top: 2px;
        ">Loading...</div>
    </div>
    <script>
        (function() {{
            function updateClock() {{
                const now = new Date();
                const timeEl = document.getElementById('clock-time');
                const dateEl = document.getElementById('clock-date');

                if (timeEl) {{
                    const hours = String(now.getHours()).padStart(2, '0');
                    const minutes = String(now.getMinutes()).padStart(2, '0');
                    const seconds = String(now.getSeconds()).padStart(2, '0');
                    timeEl.textContent = {'`${hours}:${minutes}:${seconds}`' if show_seconds else '`${hours}:${minutes}`'};
                }}

                if (dateEl) {{
                    const options = {{ weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' }};
                    dateEl.textContent = now.toLocaleDateString('en-US', options);
                }}
            }}

            updateClock();
            setInterval(updateClock, 1000);
        }})();
    </script>
    """

    return Div(text=html, width=width, height=60)


def create_datetime_header(
    *,
    last_refresh: datetime | None = None,
    next_refresh_sec: int = 30,
    width: int = 400,
) -> Div:
    """Create a header with date, time, and refresh information.

    Args:
        last_refresh: Last refresh timestamp
        next_refresh_sec: Seconds until next refresh
        width: Widget width

    Returns:
        Bokeh Div widget

    """
    refresh_text = ""
    if last_refresh:
        refresh_text = f"Last refresh: {last_refresh.strftime('%H:%M:%S')}"

    html = f"""
    <div style="
        display: flex;
        align-items: center;
        justify-content: space-between;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        padding: 12px 0;
    ">
        <div>
            <div id="header-date" style="
                font-size: 14px;
                color: {THEME_COLORS['text_muted']};
            ">Loading...</div>
            <div id="header-time" style="
                font-size: 24px;
                font-weight: 700;
                color: {THEME_COLORS['text']};
                font-variant-numeric: tabular-nums;
            ">--:--:--</div>
        </div>
        <div style="text-align: right;">
            <div id="refresh-info" style="
                font-size: 12px;
                color: {THEME_COLORS['text_muted']};
            ">{refresh_text}</div>
            <div id="refresh-countdown" style="
                font-size: 13px;
                color: {THEME_COLORS['primary']};
                font-weight: 500;
            ">Next refresh in {next_refresh_sec}s</div>
        </div>
    </div>
    <script>
        (function() {{
            let countdown = {next_refresh_sec};

            function updateHeader() {{
                const now = new Date();
                const dateEl = document.getElementById('header-date');
                const timeEl = document.getElementById('header-time');
                const countdownEl = document.getElementById('refresh-countdown');

                if (dateEl) {{
                    const options = {{ weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' }};
                    dateEl.textContent = now.toLocaleDateString('en-US', options);
                }}

                if (timeEl) {{
                    const hours = String(now.getHours()).padStart(2, '0');
                    const minutes = String(now.getMinutes()).padStart(2, '0');
                    const seconds = String(now.getSeconds()).padStart(2, '0');
                    timeEl.textContent = `${{hours}}:${{minutes}}:${{seconds}}`;
                }}

                if (countdownEl) {{
                    countdown = Math.max(0, countdown - 1);
                    if (countdown === 0) {{
                        countdown = {next_refresh_sec};
                    }}
                    countdownEl.textContent = `Next refresh in ${{countdown}}s`;
                }}
            }}

            updateHeader();
            setInterval(updateHeader, 1000);
        }})();
    </script>
    """

    return Div(text=html, width=width, height=70)


def create_refresh_indicator(
    *,
    interval_sec: int = 30,
    last_refresh: datetime | None = None,
) -> Div:
    """Create a compact refresh indicator.

    Args:
        interval_sec: Refresh interval in seconds
        last_refresh: Last refresh timestamp

    Returns:
        Bokeh Div widget

    """
    last_text = last_refresh.strftime("%H:%M:%S") if last_refresh else "Never"

    html = f"""
    <div style="
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 8px 14px;
        background: {THEME_COLORS['surface']};
        border: 1px solid {THEME_COLORS['border_light']};
        border-radius: 8px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    ">
        <div id="refresh-spinner" style="
            width: 16px;
            height: 16px;
            border: 2px solid {THEME_COLORS['border']};
            border-top-color: {THEME_COLORS['primary']};
            border-radius: 50%;
            animation: spin 1s linear infinite;
        "></div>
        <div>
            <div style="
                font-size: 11px;
                color: {THEME_COLORS['text_muted']};
                text-transform: uppercase;
                letter-spacing: 0.5px;
            ">Auto-refresh</div>
            <div id="refresh-status" style="
                font-size: 13px;
                color: {THEME_COLORS['text']};
                font-weight: 500;
            ">Every {interval_sec}s</div>
        </div>
        <div style="
            font-size: 11px;
            color: {THEME_COLORS['text_muted']};
            margin-left: 8px;
        ">Last: {last_text}</div>
    </div>
    <style>
        @keyframes spin {{
            to {{ transform: rotate(360deg); }}
        }}
    </style>
    """

    return Div(text=html, width=280, height=50)
