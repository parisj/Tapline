"""Bokeh server entry point for VisioEval dashboard.

Run with: pixi run dashboard
Or: bokeh serve src/visualization/server.py --port 5006
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root to path when run via bokeh serve
_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bokeh.io import curdoc  # noqa: E402
from bokeh.models import Div  # noqa: E402
from bokeh.themes import Theme  # noqa: E402

from src.config.loader import load_runtime_config  # noqa: E402
from src.dispatch.routes import load_routes_toml  # noqa: E402
from src.storage.config import load_minio_config  # noqa: E402
from src.storage.minio_service import MinioStorageService  # noqa: E402
from src.utils.logging import get_logger  # noqa: E402
from src.visualization.discovery import DiscoveryService  # noqa: E402
from src.visualization.layouts import DashboardLayout  # noqa: E402
from src.visualization.readers import MinioArtifactReader  # noqa: E402

logger = get_logger(__name__)

CONFIG_ROOT = Path("src/config")

# Color constants - Light theme
BG_LIGHT = "#ebf3ff"
SURFACE = "#ffffff"
BORDER = "rgba(74, 144, 226, 0.2)"
TEXT = "#2c3e50"
TEXT_MUTED = "#7f8c8d"
PRIMARY = "#4a90e2"
SECONDARY = "#1c62b3"
SUCCESS = "#00d4b1"
ERROR = "#ff6b6b"
WARNING = "#ffca58"
ACCENT = "#9d65c9"

# Bokeh native theme for consistent plot styling - Light theme
LIGHT_THEME = Theme(json={
    "attrs": {
        "Figure": {
            "background_fill_color": SURFACE,
            "border_fill_color": BG_LIGHT,
            "outline_line_color": "rgba(74, 144, 226, 0.2)",
        },
        "Plot": {
            "background_fill_color": SURFACE,
            "border_fill_color": BG_LIGHT,
            "outline_line_color": "rgba(74, 144, 226, 0.2)",
        },
        "Grid": {
            "grid_line_color": "rgba(74, 144, 226, 0.1)",
            "grid_line_alpha": 1.0,
        },
        "Axis": {
            "major_label_text_color": TEXT_MUTED,
            "axis_label_text_color": TEXT_MUTED,
            "axis_line_color": "rgba(74, 144, 226, 0.2)",
            "major_tick_line_color": "rgba(74, 144, 226, 0.2)",
            "minor_tick_line_color": None,
        },
        "Title": {
            "text_color": TEXT,
        },
        "Legend": {
            "background_fill_color": SURFACE,
            "border_line_color": "rgba(74, 144, 226, 0.2)",
            "label_text_color": TEXT,
        },
    },
})


def create_dashboard(doc) -> DashboardLayout:  # noqa: ANN001
    """Create and configure the dashboard.

    Args:
        doc: Bokeh document for periodic callbacks

    Returns:
        Configured DashboardLayout instance

    """
    logger.info("Initializing VisioEval Dashboard")

    runtime_cfg = load_runtime_config(CONFIG_ROOT / "pipeline.toml")
    logger.info("Loaded runtime config with %d directories", len(runtime_cfg.directories))

    routes = load_routes_toml(
        CONFIG_ROOT / "routes.toml",
        config_root=CONFIG_ROOT,
    )
    logger.info("Loaded %d routes", len(routes))

    minio_cfg = load_minio_config(CONFIG_ROOT / "minio.toml")
    logger.info("Connecting to MinIO at %s", minio_cfg.endpoint)

    try:
        storage = MinioStorageService(minio_cfg)
        logger.info("MinIO connection established")
    except Exception as e:
        logger.warning("MinIO connection failed: %s. Dashboard will have limited functionality.", e)
        storage = None

    discovery = DiscoveryService(runtime_cfg, routes, storage)
    reader = MinioArtifactReader(storage) if storage else None

    layout = DashboardLayout(discovery, reader)

    # Enable auto-refresh with the document
    layout.set_document(doc)

    logger.info("Dashboard initialized with auto-refresh enabled")

    return layout


def _create_global_styles() -> Div:
    """Create a Div that injects global CSS styles."""
    return Div(
        text=f"""
        <style>
            /* Light theme page background */
            html, body {{
                background-color: {BG_LIGHT} !important;
                margin: 0 !important;
                padding: 0 !important;
                min-height: 100vh !important;
            }}

            /* Light theme Bokeh root container */
            .bk-root {{
                background-color: {BG_LIGHT} !important;
                padding: 20px !important;
                max-width: 900px;
                margin: 0 auto !important;
                min-height: 100vh;
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            }}

            /* Column and Row should be transparent */
            .bk-Column, .bk-Row {{
                background-color: transparent !important;
            }}

            /* Form inputs - light style */
            .bk-input {{
                background-color: {SURFACE} !important;
                border: 1px solid rgba(74, 144, 226, 0.2) !important;
                border-radius: 6px !important;
                color: {TEXT} !important;
                padding: 8px 12px !important;
                font-size: 13px !important;
                box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06) !important;
                transition: all 0.3s ease !important;
            }}
            .bk-input:focus {{
                border-color: {PRIMARY} !important;
                outline: none !important;
                box-shadow: 0 4px 12px rgba(74, 144, 226, 0.15) !important;
            }}
            .bk-input-group {{
                display: flex;
                flex-direction: column;
            }}
            .bk-input-group > label {{
                color: {TEXT_MUTED} !important;
                font-size: 11px !important;
                font-weight: 500 !important;
                text-transform: uppercase !important;
                letter-spacing: 0.5px !important;
                margin-bottom: 6px !important;
            }}
            select.bk-input {{
                cursor: pointer;
                appearance: none;
                -webkit-appearance: none;
                background-repeat: no-repeat;
                background-position: right 10px center;
                padding-right: 32px !important;
            }}
            select.bk-input option {{
                background-color: {SURFACE} !important;
                color: {TEXT} !important;
            }}

            /* Buttons */
            .bk-btn {{
                border-radius: 6px !important;
                font-weight: 500 !important;
                font-size: 12px !important;
                padding: 8px 14px !important;
                border: 1px solid transparent !important;
                cursor: pointer;
                box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08) !important;
                transition: all 0.3s cubic-bezier(0.25, 0.8, 0.25, 1) !important;
            }}
            .bk-btn:hover {{
                transform: translateY(-2px) !important;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.12) !important;
            }}
            .bk-btn-primary {{
                background-color: {PRIMARY} !important;
                color: #fff !important;
            }}
            .bk-btn-success {{
                background-color: {SUCCESS} !important;
                color: #fff !important;
            }}
            .bk-btn-default {{
                background-color: {SURFACE} !important;
                border: 1px solid rgba(74, 144, 226, 0.2) !important;
                color: {TEXT_MUTED} !important;
            }}
            input[type="number"].bk-input {{
                width: 50px !important;
            }}

            /* Spinner */
            .bk-spin-wrapper {{
                background-color: {SURFACE} !important;
                border: 1px solid rgba(74, 144, 226, 0.2) !important;
                border-radius: 6px !important;
                box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06) !important;
            }}
            .bk-spin-wrapper input {{
                background-color: transparent !important;
                color: {TEXT} !important;
                border: none !important;
            }}
            .bk-spin-btn {{
                background-color: {SURFACE} !important;
                color: {TEXT_MUTED} !important;
                border: none !important;
            }}

            /* Data table */
            .slick-header-column {{
                background-color: {SURFACE} !important;
                color: {TEXT} !important;
            }}
            .slick-row {{
                background-color: {SURFACE} !important;
                color: {TEXT} !important;
            }}
            .slick-cell {{
                border-color: rgba(74, 144, 226, 0.1) !important;
            }}

            /* Custom scrollbar */
            ::-webkit-scrollbar {{
                width: 6px;
                height: 6px;
            }}
            ::-webkit-scrollbar-track {{
                background: {BG_LIGHT};
                border-radius: 3px;
            }}
            ::-webkit-scrollbar-thumb {{
                background: {PRIMARY};
                border-radius: 3px;
            }}

            /* Section content styling */
            .section-content {{
                background: {SURFACE} !important;
                padding: 12px 20px !important;
            }}

            /* Remove default spacing in section content */
            .section-content > .bk-Row {{
                margin-bottom: 12px !important;
            }}
        </style>
        """,
        width=1,
        height=1,
        visible=False,
    )


LIGHT_PAGE_TEMPLATE = """
{% block postamble %}
<style>
html, body {
    background-color: #ebf3ff !important;
    margin: 0 !important;
    padding: 0 !important;
    min-height: 100vh !important;
}
</style>
{% endblock %}
"""


def main() -> None:
    """Main entry point for Bokeh server."""
    doc = curdoc()
    doc.title = "VisioEval Dashboard"

    # Apply native Bokeh theme for plot styling
    doc.theme = LIGHT_THEME

    # Apply dark page background template
    doc.template = LIGHT_PAGE_TEMPLATE

    # Add global CSS styles
    doc.add_root(_create_global_styles())

    try:
        dashboard = create_dashboard(doc)
        doc.add_root(dashboard.build_layout())
        logger.info("Dashboard ready at http://localhost:5006")
    except Exception as e:
        logger.exception("Failed to initialize dashboard")

        error_div = Div(
            text=f"""
            <div style="
                padding: 60px;
                text-align: center;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background: {SURFACE};
                border-radius: 8px;
                border: 1px solid {ERROR};
            ">
                <div style="font-size: 48px; margin-bottom: 24px;">!</div>
                <h2 style="
                    color: {ERROR};
                    font-size: 20px;
                    font-weight: 600;
                    margin: 0 0 16px 0;
                ">Dashboard Initialization Failed</h2>
                <p style="
                    color: {TEXT_MUTED};
                    font-size: 14px;
                    margin: 0 0 24px 0;
                ">{e}</p>
                <div style="
                    background: {BG_LIGHT};
                    border-radius: 6px;
                    padding: 16px;
                    text-align: left;
                    font-size: 13px;
                    color: {TEXT_MUTED};
                ">
                    <p style="margin: 0 0 8px 0; font-weight: 600; color: {TEXT};">Troubleshooting:</p>
                    <ul style="margin: 0; padding-left: 20px; line-height: 1.8;">
                        <li>Check MinIO: <code>docker-compose up -d minio</code></li>
                        <li>Check Kafka: <code>docker-compose up -d kafka</code></li>
                        <li>Run pipeline: <code>pixi run pipeline</code></li>
                        <li>Check logs for details</li>
                    </ul>
                </div>
            </div>
            """,
            width=600,
            height=350,
        )
        doc.add_root(error_div)


main()
