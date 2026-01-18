"""Observability configuration loader."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.config.base import get_section, load_toml_or_empty


@dataclass(frozen=True)
class ObservabilityConfig:
    """Observability configuration settings.

    Controls tracing, metrics, logging, and correlation ID behavior.
    """

    # Tracing settings
    tracing_enabled: bool
    service_name: str
    tracing_exporter: str
    otlp_endpoint: str
    sample_rate: float

    # Metrics settings
    metrics_enabled: bool
    metrics_port: int
    metrics_path: str
    include_runtime_metrics: bool
    histogram_buckets: tuple[float, ...]

    # Logging settings
    log_format: str
    include_trace_context: bool
    include_correlation_id: bool
    include_caller_info: bool

    # Correlation settings
    correlation_header_name: str
    kafka_correlation_header: str
    propagate_correlation_to_kafka: bool


def load_observability_config(path: Path | None = None) -> ObservabilityConfig:
    """Load observability configuration from TOML file.

    Args:
        path: Path to config file. If None, uses default path.

    Returns:
        ObservabilityConfig with loaded settings

    """
    if path is None:
        path = Path(__file__).parent.parent / "config" / "observability.toml"

    doc = load_toml_or_empty(path)

    tracing = get_section(doc, "tracing")
    metrics = get_section(doc, "metrics")
    logging_cfg = get_section(doc, "logging")
    correlation = get_section(doc, "correlation")

    return ObservabilityConfig(
        # Tracing
        tracing_enabled=tracing.get("enabled", True),
        service_name=tracing.get("service_name", "visioeval"),
        tracing_exporter=tracing.get("exporter", "otlp"),
        otlp_endpoint=tracing.get("otlp_endpoint", "http://localhost:4317"),
        sample_rate=tracing.get("sample_rate", 1.0),
        # Metrics
        metrics_enabled=metrics.get("enabled", True),
        metrics_port=metrics.get("port", 8000),
        metrics_path=metrics.get("path", "/metrics"),
        include_runtime_metrics=metrics.get("include_runtime_metrics", True),
        histogram_buckets=tuple(
            metrics.get(
                "histogram_buckets",
                [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
            ),
        ),
        # Logging
        log_format=logging_cfg.get("format", "json"),
        include_trace_context=logging_cfg.get("include_trace_context", True),
        include_correlation_id=logging_cfg.get("include_correlation_id", True),
        include_caller_info=logging_cfg.get("include_caller_info", False),
        # Correlation
        correlation_header_name=correlation.get("header_name", "X-Correlation-ID"),
        kafka_correlation_header=correlation.get("kafka_header_name", "x-correlation-id"),
        propagate_correlation_to_kafka=correlation.get("propagate_to_kafka", True),
    )
