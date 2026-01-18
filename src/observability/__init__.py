"""Observability module for tracing, metrics, and structured logging.

This module provides:
- OpenTelemetry distributed tracing
- Prometheus metrics collection
- Correlation ID propagation
- Structured JSON logging with trace context
"""

from src.observability.config import ObservabilityConfig, load_observability_config
from src.observability.correlation import (
    correlation_context,
    generate_correlation_id,
    get_correlation_id,
    set_correlation_id,
)
from src.observability.metrics import (
    JOB_DURATION,
    JOBS_COMPLETED,
    JOBS_CREATED,
    JOBS_FAILED,
    JOBS_IN_PROGRESS,
    KAFKA_MESSAGES_CONSUMED,
    KAFKA_MESSAGES_PRODUCED,
    KAFKA_PRODUCE_ERRORS,
    MINIO_DEDUP_HITS,
    MINIO_OBJECTS_STORED,
    MINIO_RETRIEVE_DURATION,
    MINIO_STORE_DURATION,
    PIPELINE_UP,
    configure_metrics,
)
from src.observability.tracing import configure_tracing, get_tracer, traced

__all__ = [
    "JOBS_COMPLETED",
    "JOBS_CREATED",
    "JOBS_FAILED",
    "JOBS_IN_PROGRESS",
    "JOB_DURATION",
    "KAFKA_MESSAGES_CONSUMED",
    "KAFKA_MESSAGES_PRODUCED",
    "KAFKA_PRODUCE_ERRORS",
    "MINIO_DEDUP_HITS",
    "MINIO_OBJECTS_STORED",
    "MINIO_RETRIEVE_DURATION",
    "MINIO_STORE_DURATION",
    "PIPELINE_UP",
    # Config
    "ObservabilityConfig",
    # Metrics
    "configure_metrics",
    # Tracing
    "configure_tracing",
    "correlation_context",
    "generate_correlation_id",
    # Correlation
    "get_correlation_id",
    "get_tracer",
    "load_observability_config",
    "set_correlation_id",
    "traced",
]
