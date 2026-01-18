"""OpenTelemetry tracing setup and utilities."""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased

from src.utils.logging import get_logger

if TYPE_CHECKING:
    from src.observability.config import ObservabilityConfig

logger = get_logger(__name__)

# Global state
_tracer_provider: TracerProvider | None = None
_initialized = False

F = TypeVar("F", bound=Callable[..., Any])


def configure_tracing(config: ObservabilityConfig) -> TracerProvider | None:
    """Initialize OpenTelemetry tracing.

    Args:
        config: Observability configuration

    Returns:
        TracerProvider if enabled, None otherwise
    """
    global _tracer_provider, _initialized

    if _initialized:
        return _tracer_provider

    if not config.tracing_enabled:
        logger.info("Tracing disabled by configuration")
        _initialized = True
        return None

    # Create resource with service name
    resource = Resource.create({SERVICE_NAME: config.service_name})

    # Create sampler based on sample rate
    sampler = TraceIdRatioBased(config.sample_rate)

    # Create tracer provider
    _tracer_provider = TracerProvider(resource=resource, sampler=sampler)

    # Configure exporter based on config
    if config.tracing_exporter == "otlp":
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter,
            )

            exporter = OTLPSpanExporter(endpoint=config.otlp_endpoint, insecure=True)
            _tracer_provider.add_span_processor(BatchSpanProcessor(exporter))
            logger.info(
                "OTLP tracing exporter configured: endpoint=%s", config.otlp_endpoint
            )
        except ImportError:
            logger.warning("OTLP exporter not available, falling back to console")
            _tracer_provider.add_span_processor(
                BatchSpanProcessor(ConsoleSpanExporter())
            )
    elif config.tracing_exporter == "console":
        _tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        logger.info("Console tracing exporter configured")
    elif config.tracing_exporter == "none":
        logger.info("No tracing exporter configured")
    else:
        logger.warning("Unknown tracing exporter: %s", config.tracing_exporter)

    # Set as global tracer provider
    trace.set_tracer_provider(_tracer_provider)

    _initialized = True
    logger.info(
        "Tracing initialized: service=%s, sample_rate=%s",
        config.service_name,
        config.sample_rate,
    )

    return _tracer_provider


def get_tracer(name: str) -> trace.Tracer:
    """Get a tracer for the given module.

    Args:
        name: Module or component name (usually __name__)

    Returns:
        Tracer instance
    """
    return trace.get_tracer(name)


def traced(
    span_name: str | None = None,
    attributes: dict[str, Any] | None = None,
) -> Callable[[F], F]:
    """Decorator to trace a function.

    Args:
        span_name: Custom span name (defaults to function name)
        attributes: Static attributes to add to span

    Returns:
        Decorated function

    Example:
        @traced("process_job")
        def process_job(job_id: str) -> None:
            ...

        @traced(attributes={"component": "worker"})
        def do_work() -> None:
            ...
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            tracer = get_tracer(func.__module__)
            name = span_name or func.__name__

            with tracer.start_as_current_span(name) as span:
                span.set_attribute("function", func.__name__)
                span.set_attribute("module", func.__module__)

                if attributes:
                    for key, value in attributes.items():
                        span.set_attribute(key, value)

                return func(*args, **kwargs)

        return wrapper  # type: ignore

    return decorator


def get_current_span() -> trace.Span:
    """Get the current active span."""
    return trace.get_current_span()


def get_current_trace_context() -> dict[str, str]:
    """Get current trace context as dictionary.

    Returns:
        Dict with trace_id and span_id if available
    """
    span = trace.get_current_span()
    ctx = span.get_span_context()

    if ctx.is_valid:
        return {
            "trace_id": format(ctx.trace_id, "032x"),
            "span_id": format(ctx.span_id, "016x"),
        }
    return {}


def shutdown_tracing() -> None:
    """Shutdown tracing and flush pending spans."""
    global _tracer_provider, _initialized

    if _tracer_provider is not None:
        _tracer_provider.force_flush()
        _tracer_provider.shutdown()
        logger.info("Tracing shutdown complete")

    _tracer_provider = None
    _initialized = False
