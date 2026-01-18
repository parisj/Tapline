"""VisioEval pipeline entry point.

Supports two modes:
- Legacy mode (PIPELINE_MODE=legacy): PostgreSQL-based persistence
- Streaming mode (PIPELINE_MODE=streaming): Kafka/Flink/MinIO architecture

Set PIPELINE_MODE environment variable to choose mode.
Default is 'legacy' for backward compatibility.
"""

from __future__ import annotations

import atexit
import os
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
from src.evaluation.evaluator import EvaluationModule
from src.ingest.dedup import DedupCache
from src.ingest.kafka_observer import KafkaDirectoryObserver
from src.ingest.observer import DirectoryObserver
from src.ingest.queue import IngestQueue
from src.persistence.service import PersistenceService
from src.storage.config import load_minio_config
from src.storage.minio_service import MinioStorageService
from src.streaming.config import load_kafka_config
from src.streaming.producer import EventProducer
from src.utils.logging import configure_logging, get_logger
from src.visualization.bokeh_app import start_bokeh_placeholder
from src.workers.kafka_pool import KafkaWorkerPool
from src.workers.pool import WorkerPool

# Import observability
from src.observability.config import load_observability_config
from src.observability.tracing import configure_tracing, shutdown_tracing
from src.observability.metrics import (
    configure_metrics,
    set_build_info,
    PIPELINE_UP,
    WORKERS_ACTIVE,
    WORKER_POOL_SIZE,
)

logger = get_logger("pipeline.app.main")

# Pipeline mode: 'legacy' (PostgreSQL) or 'streaming' (Kafka/Flink/MinIO)
PIPELINE_MODE = os.environ.get("PIPELINE_MODE", "legacy")

# Version
__version__ = "0.1.0"


def build_legacy_app(
    cfg: RuntimeConfig,
) -> tuple:
    """Build legacy PostgreSQL-based pipeline components."""
    persistence = PersistenceService()
    ingest = IngestQueue(
        maxsize=cfg.ingest_queue_maxsize,
        timeout_sec=cfg.ingest_poll_interval_sec,
    )
    dedup = DedupCache(ttl_sec=3600.0)

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

    observer = DirectoryObserver(
        directories=cfg.directories,
        ingest=ingest,
        dedup=dedup,
        poll_interval_sec=cfg.ingest_poll_interval_sec,
        allowed_exts=set(cfg.allowed_image_exts),
        max_wait_sec=cfg.readiness_max_wait_sec,
        stable_window_sec=cfg.readiness_stable_window_sec,
    )

    worker_pool = WorkerPool(
        ingest=ingest,
        dispatcher=dispatcher,
        persistence=persistence,
        max_workers=cfg.workers_max,
    )

    evaluator = EvaluationModule(persistence=persistence)
    return observer, worker_pool, evaluator, start_bokeh_placeholder


def build_streaming_app(
    cfg: RuntimeConfig,
) -> tuple:
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


def run_legacy_mode(cfg: RuntimeConfig) -> None:
    """Run pipeline in legacy PostgreSQL mode."""
    observer, worker_pool, evaluator, start_bokeh = build_legacy_app(cfg=cfg)

    stop = threading.Event()
    install_signal_handlers(stop)

    worker_pool.start()
    observer.start()
    start_bokeh()

    logger.info("Legacy pipeline started. Ctrl+C to stop.")

    last_eval = time.time()
    eval_interval = cfg.evaluation_interval_sec

    while not stop.is_set():
        time.sleep(0.2)
        now = time.time()
        if now - last_eval >= eval_interval:
            try:
                evaluator.run_window(window_start_unix=last_eval, window_end_unix=now)
                logger.info("Evaluation completed for window [%s, %s]", last_eval, now)
            except Exception:
                logger.exception("Evaluation failed.")
            last_eval = now

    logger.info("Stopping legacy pipeline...")
    observer.stop()
    worker_pool.stop()
    logger.info("Legacy pipeline stopped.")


def run_streaming_mode(cfg: RuntimeConfig, obs_config) -> None:
    """Run pipeline in streaming Kafka/Flink/MinIO mode."""
    observer, worker_pool, producer, storage = build_streaming_app(cfg=cfg)

    stop = threading.Event()
    install_signal_handlers(stop)

    # Set pipeline metrics
    PIPELINE_UP.set(1)
    WORKER_POOL_SIZE.set(cfg.workers_max)
    WORKERS_ACTIVE.set(0)

    worker_pool.start()

    # Give workers a moment to stabilize after group join
    time.sleep(0.5)

    observer.start()

    logger.info("Streaming pipeline started. Ctrl+C to stop.")
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

    logger.info("Stopping streaming pipeline...")

    # Mark pipeline as down
    PIPELINE_UP.set(0)

    observer.stop()
    worker_pool.stop()
    producer.close()
    logger.info("Streaming pipeline stopped.")


def main() -> None:
    """General main entry point - runs in configured mode."""
    # Load observability config
    obs_config = load_observability_config(Path("src/config/observability.toml"))

    # Configure logging with observability support
    configure_logging(obs_config)

    logger.info("VisioEval starting in %s mode...", PIPELINE_MODE)

    # Initialize tracing
    configure_tracing(obs_config)
    atexit.register(shutdown_tracing)

    # Initialize metrics server
    metrics_started = configure_metrics(obs_config)
    if metrics_started:
        logger.info("Metrics server started on port %d", obs_config.metrics_port)

    # Set build info metric
    set_build_info(version=__version__, mode=PIPELINE_MODE)

    cfg = load_runtime_config(Path("src/config/pipeline.toml"))

    if PIPELINE_MODE == "streaming":
        run_streaming_mode(cfg, obs_config)
    else:
        run_legacy_mode(cfg)


if __name__ == "__main__":
    main()
