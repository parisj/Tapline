"""Aggregate sink entry point.

When using Flink SQL mode:
- Consumes raw aggregate records from Kafka (produced by Flink SQL)
- Writes them to MinIO for dashboard access

When using Python aggregation mode (default):
- Python aggregation writes directly to MinIO, so no Kafka consumption needed
- Only runs the metric values collector

Also runs a metric values collector that stores raw metric values
for AnalysisKind-specific computations (histograms, ellipses, etc.).

Usage:
    pixi run aggregate-sink

This should be run alongside:
    - pixi run run (main pipeline)
    - pixi run pipeline (includes Python aggregation by default)

Set VISIOEVAL_USE_FLINK_SQL=true to use Flink SQL mode.
"""

from __future__ import annotations

import atexit
import os
import signal
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from dotenv import load_dotenv

if TYPE_CHECKING:
    from types import FrameType

from src.app.metric_values_collector import run_metric_values_collector
from src.flink.config import load_flink_config
from src.observability.config import load_observability_config
from src.observability.tracing import configure_tracing, shutdown_tracing
from src.storage.config import load_minio_config
from src.storage.minio_service import MinioStorageService
from src.streaming.config import load_kafka_config
from src.utils.logging import configure_logging, get_logger

logger = get_logger("pipeline.app.aggregate_sink")


def main() -> None:
    """Main entry point for aggregate sink."""
    load_dotenv()

    # Check if using Flink SQL mode
    use_flink_sql = os.environ.get("VISIOEVAL_USE_FLINK_SQL", "").lower() == "true"

    # Load configs
    obs_config = load_observability_config(Path("src/config/observability.toml"))
    flink_config = load_flink_config(Path("src/config/flink.toml"))
    kafka_config = load_kafka_config(Path("src/config/kafka.toml"))
    minio_config = load_minio_config(Path("src/config/minio.toml"))

    # Configure logging and tracing
    configure_logging(obs_config)
    configure_tracing(obs_config)
    atexit.register(shutdown_tracing)

    mode = "Flink SQL" if use_flink_sql else "Python aggregation"
    logger.info("Aggregate sink starting (mode: %s)...", mode)

    # Initialize storage
    storage = MinioStorageService(minio_config)

    # Setup stop event and signal handlers
    stop_event = threading.Event()

    def handle_signal(signum: int, _frame: FrameType | None) -> None:
        logger.info("Received signal %d, stopping...", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Start metric values collector in a separate thread
    values_thread = threading.Thread(
        target=run_metric_values_collector,
        args=(flink_config, kafka_config, storage, stop_event),
        name="metric-values-collector",
        daemon=True,
    )
    values_thread.start()
    logger.info("Metric values collector started in background thread")

    if use_flink_sql:
        # Flink SQL mode: consume from visio.aggregates topic
        from src.app.aggregate_consumer import run_aggregate_consumer

        logger.info("Running Flink SQL aggregate consumer (visio.aggregates topic)")
        run_aggregate_consumer(flink_config, kafka_config, storage, stop_event)
    else:
        # Python aggregation mode: aggregates written directly to MinIO
        # Just keep the values collector running
        logger.info(
            "Python aggregation mode: aggregates written directly to MinIO by flink_aggregation.py",
        )
        logger.info("Running metric values collector only...")

        # Wait for stop signal
        while not stop_event.is_set():
            stop_event.wait(timeout=1.0)

    # Wait for values collector to finish
    values_thread.join(timeout=5.0)

    logger.info("Aggregate sink stopped.")


if __name__ == "__main__":
    main()
