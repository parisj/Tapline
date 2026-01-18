"""Observability module for tracing, metrics, and structured logging.

This module provides:
- OpenTelemetry distributed tracing
- Prometheus metrics collection
- Correlation ID propagation
- Structured JSON logging with trace context
"""

from src.observability.config import ObservabilityConfig, load_observability_config
from src.observability.tracing import configure_tracing, get_tracer, traced
from src.observability.metrics import (
    configure_metrics,
    JOBS_CREATED,
    JOBS_COMPLETED,
    JOBS_FAILED,
    JOB_DURATION,
    JOBS_IN_PROGRESS,
    KAFKA_MESSAGES_PRODUCED,
    KAFKA_MESSAGES_CONSUMED,
    KAFKA_PRODUCE_ERRORS,
    MINIO_STORE_DURATION,
    MINIO_RETRIEVE_DURATION,
    MINIO_OBJECTS_STORED,
    MINIO_DEDUP_HITS,
    PIPELINE_UP,
)
from src.observability.correlation import (
    get_correlation_id,
    set_correlation_id,
    generate_correlation_id,
    correlation_context,
)

__all__ = [
    # Config
    "ObservabilityConfig",
    "load_observability_config",
    # Tracing
    "configure_tracing",
    "get_tracer",
    "traced",
    # Metrics
    "configure_metrics",
    "JOBS_CREATED",
    "JOBS_COMPLETED",
    "JOBS_FAILED",
    "JOB_DURATION",
    "JOBS_IN_PROGRESS",
    "KAFKA_MESSAGES_PRODUCED",
    "KAFKA_MESSAGES_CONSUMED",
    "KAFKA_PRODUCE_ERRORS",
    "MINIO_STORE_DURATION",
    "MINIO_RETRIEVE_DURATION",
    "MINIO_OBJECTS_STORED",
    "MINIO_DEDUP_HITS",
    "PIPELINE_UP",
    # Correlation
    "get_correlation_id",
    "set_correlation_id",
    "generate_correlation_id",
    "correlation_context",
]
