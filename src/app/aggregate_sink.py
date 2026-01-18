"""Aggregate sink entry point.

Consumes AGGREGATE_COMPUTED events from Kafka (produced by Flink)
and writes them to MinIO for dashboard access.

Usage:
    pixi run aggregate-sink

This should be run alongside:
    - pixi run run (main pipeline)
    - pixi run run-flink-job (Flink aggregation)

Or use `pixi run pipeline` to start all three together.
"""

from __future__ import annotations

import atexit
import signal
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from dotenv import load_dotenv

if TYPE_CHECKING:
    from types import FrameType

from src.app.aggregate_consumer import run_aggregate_consumer
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

    # Load configs
    obs_config = load_observability_config(Path("src/config/observability.toml"))
    flink_config = load_flink_config(Path("src/config/flink.toml"))
    kafka_config = load_kafka_config(Path("src/config/kafka.toml"))
    minio_config = load_minio_config(Path("src/config/minio.toml"))

    # Configure logging and tracing
    configure_logging(obs_config)
    configure_tracing(obs_config)
    atexit.register(shutdown_tracing)

    logger.info("Aggregate sink starting...")

    # Initialize storage
    storage = MinioStorageService(minio_config)

    # Setup stop event and signal handlers
    stop_event = threading.Event()

    def handle_signal(signum: int, _frame: FrameType | None) -> None:
        logger.info("Received signal %d, stopping...", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Run the consumer (blocks until stop_event is set)
    run_aggregate_consumer(flink_config, kafka_config, storage, stop_event)

    logger.info("Aggregate sink stopped.")


if __name__ == "__main__":
    main()
