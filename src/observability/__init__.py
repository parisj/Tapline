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
    KAFKA_MESSAGES_CONSUMED,
    KAFKA_MESSAGES_PRODUCED,
    KAFKA_PRODUCE_ERRORS,
    MINIO_DEDUP_HITS,
    MINIO_OBJECTS_STORED,
    MINIO_RETRIEVE_DURATION,
    MINIO_STORE_DURATION,
    PIPELINE_UP,
    TASK_DURATION,
    TASKS_COMPLETED,
    TASKS_CREATED,
    TASKS_FAILED,
    TASKS_IN_PROGRESS,
    configure_metrics,
)
from src.observability.tracing import configure_tracing, get_tracer, traced

__all__ = [
    "KAFKA_MESSAGES_CONSUMED",
    "KAFKA_MESSAGES_PRODUCED",
    "KAFKA_PRODUCE_ERRORS",
    "MINIO_DEDUP_HITS",
    "MINIO_OBJECTS_STORED",
    "MINIO_RETRIEVE_DURATION",
    "MINIO_STORE_DURATION",
    "PIPELINE_UP",
    "TASKS_COMPLETED",
    "TASKS_CREATED",
    "TASKS_FAILED",
    "TASKS_IN_PROGRESS",
    "TASK_DURATION",
    "ObservabilityConfig",
    "configure_metrics",
    "configure_tracing",
    "correlation_context",
    "generate_correlation_id",
    "get_correlation_id",
    "get_tracer",
    "load_observability_config",
    "set_correlation_id",
    "traced",
]
