from __future__ import annotations

from src.utils.logging import get_logger

logger = get_logger("pipeline.visualization.bokeh")


def start_bokeh_placeholder() -> None:
    """
    Placeholder for Bokeh server integration.
    Correct pattern:
    - reads aggregates table (read-only)
    - no worker thread calls into Bokeh
    """
    logger.info("Bokeh placeholder started (implement separately).")
