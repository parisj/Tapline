"""Bokeh application module.

The main dashboard is served via the Bokeh server. To start:
    pixi run dashboard

Or directly:
    bokeh serve src/visualization/server.py --port 5006

This module provides a programmatic interface for standalone usage.
"""

from __future__ import annotations

from src.utils.logging import get_logger

logger = get_logger("pipeline.visualization.bokeh")


def start_bokeh_server(*, port: int = 5006, show: bool = True) -> None:
    """Start the Bokeh dashboard server programmatically.

    Args:
        port: Port to serve on (default: 5006)
        show: Open browser automatically (default: True)

    """
    from bokeh.server.server import Server

    from src.visualization.server import create_dashboard

    def modify_doc(doc) -> None:  # noqa: ANN001
        dashboard = create_dashboard()
        doc.add_root(dashboard.build_layout())
        doc.title = "VisioEval Dashboard"

    server = Server(
        {"/": modify_doc},
        port=port,
        allow_websocket_origin=[f"localhost:{port}"],
    )

    logger.info("Starting Bokeh server on http://localhost:%d", port)
    server.start()

    if show:
        server.io_loop.add_callback(server.show, "/")

    try:
        server.io_loop.start()
    except KeyboardInterrupt:
        logger.info("Shutting down Bokeh server")


def get_dashboard_url(port: int = 5006) -> str:
    """Get the dashboard URL.

    Args:
        port: Server port

    Returns:
        Dashboard URL string

    """
    return f"http://localhost:{port}"


if __name__ == "__main__":
    start_bokeh_server()
