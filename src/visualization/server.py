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

from src.config.loader import load_runtime_config  # noqa: E402
from src.dispatch.routes import load_routes_toml  # noqa: E402
from src.storage.config import load_minio_config  # noqa: E402
from src.storage.minio_service import MinioStorageService  # noqa: E402
from src.utils.logging import get_logger  # noqa: E402
from src.visualization.discovery import DiscoveryService  # noqa: E402
from src.visualization.layouts import DashboardLayout  # noqa: E402
from src.visualization.plots import THEME_COLORS  # noqa: E402
from src.visualization.readers import MinioArtifactReader  # noqa: E402

logger = get_logger(__name__)

CONFIG_ROOT = Path("src/config")


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
    bg = THEME_COLORS['background']
    surface = THEME_COLORS['surface']
    surface_el = THEME_COLORS['surface_elevated']
    border = THEME_COLORS['border']
    border_lt = THEME_COLORS['border_light']
    text = THEME_COLORS['text']
    text_m = THEME_COLORS['text_muted']
    primary = THEME_COLORS['primary']
    primary_lt = THEME_COLORS['primary_light']
    success = THEME_COLORS['success']
    glow = THEME_COLORS['glow_primary']

    return Div(
        text=f"""
        <style>
            html, body {{
                background-color: {bg} !important;
                color: {text} !important;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                margin: 0 !important;
                padding: 0 !important;
                min-height: 100vh;
            }}
            .bk-root {{
                background-color: {bg} !important;
                max-width: 900px;
                margin: 0 auto !important;
                padding: 24px 32px !important;
            }}
            .bk-input {{
                background-color: {surface} !important;
                border: 1px solid {border} !important;
                border-radius: 6px !important;
                color: {text} !important;
                padding: 8px 12px !important;
                font-size: 13px !important;
            }}
            .bk-input:focus {{
                border-color: {primary} !important;
                box-shadow: 0 0 0 2px {glow} !important;
                outline: none !important;
            }}
            .bk-input-group {{
                display: flex;
                flex-direction: column;
            }}
            .bk-input-group > label {{
                color: {text_m} !important;
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
                background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'%3E%3Cpath fill='%2371717a' d='M3 4.5L6 7.5L9 4.5'/%3E%3C/svg%3E");
                background-repeat: no-repeat;
                background-position: right 10px center;
                padding-right: 32px !important;
            }}
            select.bk-input option {{
                background-color: {surface} !important;
                color: {text} !important;
            }}
            .bk-btn {{
                border-radius: 6px !important;
                font-weight: 500 !important;
                font-size: 12px !important;
                padding: 8px 14px !important;
                border: 1px solid transparent !important;
                cursor: pointer;
            }}
            .bk-btn-primary {{
                background-color: {primary} !important;
                color: #fff !important;
            }}
            .bk-btn-primary:hover {{
                background-color: {primary_lt} !important;
            }}
            .bk-btn-success {{
                background-color: {success} !important;
                color: #fff !important;
            }}
            .bk-btn-default {{
                background-color: {surface} !important;
                border-color: {border} !important;
                color: {text_m} !important;
            }}
            .bk-btn-default:hover {{
                background-color: {surface_el} !important;
            }}
            input[type="number"].bk-input {{
                width: 50px !important;
            }}
            .bk-Column, .bk-Row {{
                background-color: transparent !important;
            }}
            .bk-spin-wrapper {{
                background-color: {surface} !important;
                border: 1px solid {border} !important;
                border-radius: 6px !important;
            }}
            .bk-spin-wrapper input {{
                background-color: transparent !important;
                color: {text} !important;
                border: none !important;
            }}
            .bk-spin-btn {{
                background-color: {surface_el} !important;
                color: {text_m} !important;
                border: none !important;
            }}
            .slick-header-column {{
                background-color: {surface} !important;
                color: {text} !important;
            }}
            .slick-row {{
                background-color: {bg} !important;
                color: {text} !important;
            }}
            .slick-cell {{
                border-color: {border_lt} !important;
            }}
        </style>
        """,
        width=0,
        height=0,
    )


def main() -> None:
    """Main entry point for Bokeh server."""
    doc = curdoc()
    doc.title = "VisioEval Dashboard"

    # Add global styles
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
                background: {THEME_COLORS['surface']};
                border-radius: 16px;
                border: 1px solid {THEME_COLORS['error']};
            ">
                <div style="font-size: 64px; margin-bottom: 24px;">⚠️</div>
                <h2 style="
                    color: {THEME_COLORS['error']};
                    font-size: 24px;
                    font-weight: 700;
                    margin: 0 0 16px 0;
                ">Dashboard Initialization Failed</h2>
                <p style="
                    color: {THEME_COLORS['text_muted']};
                    font-size: 16px;
                    margin: 0 0 24px 0;
                ">{e}</p>
                <div style="
                    background: {THEME_COLORS['background']};
                    border-radius: 8px;
                    padding: 16px;
                    text-align: left;
                    font-size: 14px;
                    color: {THEME_COLORS['text_muted']};
                ">
                    <p style="margin: 0 0 8px 0; font-weight: 600; color: {THEME_COLORS['text']};">Troubleshooting:</p>
                    <ul style="margin: 0; padding-left: 20px; line-height: 1.8;">
                        <li>Check MinIO: <code>docker-compose up -d minio</code></li>
                        <li>Check Kafka: <code>docker-compose up -d kafka</code></li>
                        <li>Run pipeline: <code>pixi run pipeline</code></li>
                        <li>Check logs for details</li>
                    </ul>
                </div>
            </div>
            """,
            width=800,
            height=400,
        )
        doc.add_root(error_div)


main()
