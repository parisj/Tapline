"""Prometheus metrics for VisioEval pipeline."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from prometheus_client import (
    GC_COLLECTOR,
    PLATFORM_COLLECTOR,
    PROCESS_COLLECTOR,
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
    Info,
    start_http_server,
)

from src.utils.logging import get_logger

if TYPE_CHECKING:
    from src.observability.config import ObservabilityConfig

logger = get_logger(__name__)

# Global state
_metrics_server_started = False
_metrics_lock = threading.Lock()

# Default histogram buckets for latency metrics (in seconds)
DEFAULT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

# =============================================================================
# Job Metrics
# =============================================================================

JOBS_CREATED = Counter(
    "visioeval_jobs_created_total",
    "Total number of jobs created",
    ["directory_key"],
)

JOBS_COMPLETED = Counter(
    "visioeval_jobs_completed_total",
    "Total number of jobs completed successfully",
    ["algo_name", "algo_version"],
)

JOBS_FAILED = Counter(
    "visioeval_jobs_failed_total",
    "Total number of jobs failed",
    ["algo_name", "error_type"],
)

JOB_DURATION = Histogram(
    "visioeval_job_duration_seconds",
    "Job processing duration in seconds",
    ["algo_name"],
    buckets=DEFAULT_BUCKETS,
)

JOBS_IN_PROGRESS = Gauge(
    "visioeval_jobs_in_progress",
    "Number of jobs currently being processed",
    ["worker_id"],
)

# =============================================================================
# Kafka Metrics
# =============================================================================

KAFKA_MESSAGES_PRODUCED = Counter(
    "visioeval_kafka_messages_produced_total",
    "Total Kafka messages produced",
    ["topic"],
)

KAFKA_MESSAGES_CONSUMED = Counter(
    "visioeval_kafka_messages_consumed_total",
    "Total Kafka messages consumed",
    ["topic", "consumer_group"],
)

KAFKA_PRODUCE_ERRORS = Counter(
    "visioeval_kafka_produce_errors_total",
    "Kafka producer errors",
    ["topic", "error_type"],
)

KAFKA_CONSUME_ERRORS = Counter(
    "visioeval_kafka_consume_errors_total",
    "Kafka consumer errors",
    ["topic", "error_type"],
)

KAFKA_CONSUMER_LAG = Gauge(
    "visioeval_kafka_consumer_lag",
    "Kafka consumer lag (messages behind)",
    ["topic", "partition", "consumer_group"],
)

KAFKA_PRODUCE_LATENCY = Histogram(
    "visioeval_kafka_produce_latency_seconds",
    "Kafka message produce latency",
    ["topic"],
    buckets=DEFAULT_BUCKETS,
)

# =============================================================================
# MinIO Metrics
# =============================================================================

MINIO_STORE_DURATION = Histogram(
    "visioeval_minio_store_duration_seconds",
    "MinIO store operation duration",
    ["bucket"],
    buckets=DEFAULT_BUCKETS,
)

MINIO_RETRIEVE_DURATION = Histogram(
    "visioeval_minio_retrieve_duration_seconds",
    "MinIO retrieve operation duration",
    ["bucket"],
    buckets=DEFAULT_BUCKETS,
)

MINIO_OBJECTS_STORED = Counter(
    "visioeval_minio_objects_stored_total",
    "Total objects stored in MinIO",
    ["bucket"],
)

MINIO_BYTES_STORED = Counter(
    "visioeval_minio_bytes_stored_total",
    "Total bytes stored in MinIO",
    ["bucket"],
)

MINIO_DEDUP_HITS = Counter(
    "visioeval_minio_dedup_hits_total",
    "Objects deduplicated (already existed)",
    ["bucket"],
)

MINIO_ERRORS = Counter(
    "visioeval_minio_errors_total",
    "MinIO operation errors",
    ["bucket", "operation", "error_type"],
)

# =============================================================================
# Flink/Aggregation Metrics
# =============================================================================

FLINK_AGGREGATIONS_PRODUCED = Counter(
    "visioeval_flink_aggregations_produced_total",
    "Total metric aggregations produced",
)

FLINK_EVENTS_PROCESSED = Counter(
    "visioeval_flink_events_processed_total",
    "Total events processed by Flink aggregation",
)

# =============================================================================
# Pipeline Health Metrics
# =============================================================================

PIPELINE_UP = Gauge(
    "visioeval_pipeline_up",
    "Pipeline health status (1=up, 0=down)",
)

WORKERS_ACTIVE = Gauge(
    "visioeval_workers_active",
    "Number of active worker threads",
    ["worker_id"],
)

WORKER_POOL_SIZE = Gauge(
    "visioeval_worker_pool_size",
    "Configured worker pool size",
)

# =============================================================================
# Ingest Metrics
# =============================================================================

FILES_DISCOVERED = Counter(
    "visioeval_files_discovered_total",
    "Total files discovered by observer",
    ["directory_key"],
)

FILES_READY = Counter(
    "visioeval_files_ready_total",
    "Files that passed readiness check",
    ["directory_key"],
)

INGEST_QUEUE_SIZE = Gauge(
    "visioeval_ingest_queue_size",
    "Current size of the ingest queue",
)

# =============================================================================
# Build Info
# =============================================================================

BUILD_INFO = Info(
    "visioeval_build",
    "Build information",
)


def configure_metrics(config: ObservabilityConfig) -> bool:
    """Configure and start Prometheus metrics server.

    Args:
        config: Observability configuration

    Returns:
        True if metrics server started, False otherwise

    """
    global _metrics_server_started

    with _metrics_lock:
        if _metrics_server_started:
            return True

        if not config.metrics_enabled:
            logger.info("Metrics disabled by configuration")
            return False

        # Optionally disable default collectors
        if not config.include_runtime_metrics:
            try:
                REGISTRY.unregister(GC_COLLECTOR)
                REGISTRY.unregister(PLATFORM_COLLECTOR)
                REGISTRY.unregister(PROCESS_COLLECTOR)
            except Exception:
                pass  # Already unregistered

        try:
            start_http_server(config.metrics_port)
            _metrics_server_started = True
            logger.info(
                "Prometheus metrics server started: port=%d, path=%s",
                config.metrics_port,
                config.metrics_path,
            )
            return True
        except OSError as e:
            logger.exception("Failed to start metrics server: %s", e)
            return False


def set_build_info(version: str, mode: str, **extra: str) -> None:
    """Set build information metric.

    Args:
        version: Application version
        mode: Pipeline mode
        **extra: Additional info fields

    """
    BUILD_INFO.info({"version": version, "mode": mode, **extra})
