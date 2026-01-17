"""Streaming pipeline entry point using Kafka/Flink/MinIO architecture.

Replaces PostgreSQL-based pipeline with streaming infrastructure:
- Kafka for event streaming and job queue
- Flink for stateful stream processing
- MinIO for artifact storage
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from src.algorithms.registry import build_default_registry
from src.app.signals import install_signal_handlers
from src.config.loader import RuntimeConfig, load_runtime_config
from src.dispatch.dispatcher import Dispatcher
from src.dispatch.routes import RoutesConfigError, build_dispatch_plans, load_routes_toml
from src.ingest.dedup import DedupCache
from src.ingest.kafka_observer import KafkaDirectoryObserver
from src.storage.config import load_minio_config
from src.storage.minio_service import MinioStorageService
from src.streaming.config import load_kafka_config
from src.streaming.producer import EventProducer
from src.utils.logging import configure_logging, get_logger
from src.workers.kafka_pool import KafkaWorkerPool

logger = get_logger("pipeline.app.streaming_main")


def build_streaming_app(
    cfg: RuntimeConfig,
) -> tuple[KafkaDirectoryObserver, KafkaWorkerPool, EventProducer, MinioStorageService]:
    """Build the streaming pipeline components.

    Returns:
        Tuple of (observer, worker_pool, producer, storage)
    """
    # Load streaming configs
    kafka_config = load_kafka_config(Path("src/config/kafka.toml"))
    minio_config = load_minio_config(Path("src/config/minio.toml"))

    # Initialize storage
    storage = MinioStorageService(minio_config)

    # Initialize Kafka producer (shared by observer and workers)
    producer = EventProducer(kafka_config)

    # Build algorithm registry and dispatcher
    registry = build_default_registry()

    try:
        loaded_routes = load_routes_toml(
            Path("src/config/routes.toml"),
            config_root=Path("src/config"),
        )
    except RoutesConfigError as e:
        logger.error("Failed to load routes.toml: %s", e)
        raise

    plans = build_dispatch_plans(loaded_routes, registry=registry)
    dispatcher = Dispatcher(routes=plans)

    # Initialize dedup cache
    dedup = DedupCache(ttl_sec=3600.0)

    # Create Kafka-based observer
    observer = KafkaDirectoryObserver(
        directories=cfg.directories,
        producer=producer,
        dedup=dedup,
        poll_interval_sec=cfg.ingest_poll_interval_sec,
        allowed_exts=set(cfg.allowed_image_exts),
        max_wait_sec=cfg.readiness_max_wait_sec,
        stable_window_sec=cfg.readiness_stable_window_sec,
    )

    # Create Kafka-based worker pool
    worker_pool = KafkaWorkerPool(
        kafka_config=kafka_config,
        dispatcher=dispatcher,
        storage=storage,
        producer=producer,
        max_workers=cfg.workers_max,
    )

    return observer, worker_pool, producer, storage


def main() -> None:
    """Main entry point for streaming pipeline."""
    configure_logging()

    logger.info("Starting VisioEval streaming pipeline...")

    cfg = load_runtime_config(Path("src/config/pipeline.toml"))
    observer, worker_pool, producer, storage = build_streaming_app(cfg=cfg)

    stop = threading.Event()
    install_signal_handlers(stop)

    # Start components
    worker_pool.start()
    observer.start()

    logger.info("Streaming pipeline started. Ctrl+C to stop.")
    logger.info(
        "Workers: %d, Directories: %d",
        cfg.workers_max,
        len(cfg.directories),
    )

    # Main loop - just wait for stop signal
    # Aggregation is handled by Flink jobs separately
    while not stop.is_set():
        time.sleep(1.0)

        # Periodically flush producer to ensure delivery
        producer.poll(0)

    logger.info("Stopping streaming pipeline...")

    # Graceful shutdown
    observer.stop()
    worker_pool.stop()
    producer.close()

    logger.info("Streaming pipeline stopped.")


if __name__ == "__main__":
    main()
