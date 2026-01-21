"""VisioEval pipeline entry point.

Streaming mode pipeline using Kafka/Flink/MinIO architecture.
"""

from __future__ import annotations

import atexit
import threading
import time
from pathlib import Path

from dotenv import load_dotenv

from src.algorithms.registry import build_default_registry
from src.app.signals import install_signal_handlers
from src.config.loader import RuntimeConfig, load_runtime_config
from src.dispatch.dispatcher import Dispatcher
from src.dispatch.routes import RoutesConfigError, build_dispatch_plans, load_routes_toml
from src.ingest.dedup import DedupCache
from src.ingest.kafka_observer import KafkaDirectoryObserver
from src.observability.config import ObservabilityConfig, load_observability_config
from src.observability.metrics import (
    PIPELINE_UP,
    WORKER_POOL_SIZE,
    configure_metrics,
    set_build_info,
)
from src.observability.tracing import configure_tracing, shutdown_tracing
from src.storage.config import load_minio_config
from src.storage.minio_service import MinioStorageService
from src.streaming.config import load_kafka_config
from src.streaming.producer import EventProducer
from src.utils.logging import configure_logging, get_logger
from src.workers.kafka_pool import KafkaWorkerPool

logger = get_logger("pipeline.app.main")

# Version
__version__ = "0.1.0"


def build_app(
    cfg: RuntimeConfig,
) -> tuple[KafkaDirectoryObserver, KafkaWorkerPool, EventProducer, MinioStorageService]:
    """Build streaming Kafka/Flink/MinIO-based pipeline components."""
    kafka_config = load_kafka_config(Path("src/config/kafka.toml"))
    minio_config = load_minio_config(Path("src/config/minio.toml"))

    storage = MinioStorageService(minio_config)
    producer = EventProducer(kafka_config)

    registry = build_default_registry()

    try:
        loaded_routes = load_routes_toml(
            Path("src/config/routes.toml"),
            config_root=Path("src/config"),
        )
    except RoutesConfigError as e:
        logger.exception("Failed to load routes.toml: %s", e)
        raise

    plans = build_dispatch_plans(loaded_routes, registry=registry)
    dispatcher = Dispatcher(routes=plans)

    dedup = DedupCache(ttl_sec=3600.0)

    observer = KafkaDirectoryObserver(
        directories=cfg.directories,
        producer=producer,
        dedup=dedup,
        poll_interval_sec=cfg.ingest_poll_interval_sec,
        allowed_exts=set(cfg.allowed_image_exts),
        max_wait_sec=cfg.readiness_max_wait_sec,
        stable_window_sec=cfg.readiness_stable_window_sec,
    )

    worker_pool = KafkaWorkerPool(
        kafka_config=kafka_config,
        dispatcher=dispatcher,
        storage=storage,
        producer=producer,
        max_workers=cfg.workers_max,
    )

    return observer, worker_pool, producer, storage


def run(cfg: RuntimeConfig, obs_config: ObservabilityConfig) -> None:
    """Run the streaming pipeline.

    Note: This runs only the core pipeline (ingest, workers).
    For full functionality with aggregation, use `pixi run pipeline` which
    also starts the Flink job and aggregate sink.
    """
    observer, worker_pool, producer, _storage = build_app(cfg=cfg)

    stop = threading.Event()
    install_signal_handlers(stop)

    # Set pipeline metrics
    PIPELINE_UP.set(1)
    WORKER_POOL_SIZE.set(cfg.workers_max)
    # Note: WORKERS_ACTIVE is now labeled per worker_id and managed by KafkaWorkerPool

    worker_pool.start()

    # Give workers a moment to stabilize after group join
    time.sleep(0.5)

    observer.start()

    logger.info("Pipeline started. Ctrl+C to stop.")
    logger.info("For full pipeline with Flink aggregation: pixi run pipeline")
    logger.info("Dashboard available separately via: pixi run dashboard")
    logger.info(
        "Workers: %d, Directories: %d",
        cfg.workers_max,
        len(cfg.directories),
    )
    logger.info(
        "Observability: tracing=%s, metrics=%s",
        obs_config.tracing_enabled,
        obs_config.metrics_enabled,
    )

    while not stop.is_set():
        time.sleep(1.0)
        producer.poll(0)

    logger.info("Stopping pipeline...")

    # Mark pipeline as down
    PIPELINE_UP.set(0)

    observer.stop()
    worker_pool.stop()
    producer.close()
    logger.info("Pipeline stopped.")


def main() -> None:
    """Main entry point."""
    # Load environment variables
    load_dotenv()

    # Load observability config
    obs_config = load_observability_config(Path("src/config/observability.toml"))

    # Configure logging with observability support
    configure_logging(obs_config)

    logger.info("VisioEval starting...")

    # Initialize tracing
    configure_tracing(obs_config)
    atexit.register(shutdown_tracing)

    # Initialize metrics server
    metrics_started = configure_metrics(obs_config)
    if metrics_started:
        logger.info("Metrics server started on port %d", obs_config.metrics_port)

    # Set build info metric
    set_build_info(version=__version__, mode="streaming")

    cfg = load_runtime_config(Path("src/config/pipeline.toml"))
    run(cfg, obs_config)


if __name__ == "__main__":
    main()
