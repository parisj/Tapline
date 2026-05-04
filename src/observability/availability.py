"""Observability availability utilities.

Provides safe imports for observability components that gracefully
degrade when dependencies are not available.
"""

from __future__ import annotations

# Check if observability dependencies are available
_AVAILABLE = False
_tracer = None
_metrics = None
_correlation = None

try:
    from src.observability import correlation as _correlation_module
    from src.observability import metrics as _metrics_module
    from src.observability import tracing as _tracing_module

    _AVAILABLE = True
    _metrics = _metrics_module
    _tracer = _tracing_module
    _correlation = _correlation_module
except ImportError:
    pass


def is_available() -> bool:
    """Check if observability components are available."""
    return _AVAILABLE


def get_tracer(name: str) -> object | None:
    """Get a tracer if available, otherwise return None."""
    if _tracer:
        result: object = _tracer.get_tracer(name)
        return result
    return None


def get_correlation_id() -> str | None:
    """Get current correlation ID if available."""
    if _correlation:
        result: str = _correlation.get_correlation_id()
        return result
    return None


# Re-export metrics for convenience
class MetricsProxy:
    """Proxy for metrics that no-ops when unavailable."""

    def __getattr__(self, name: str) -> object:
        if _metrics:
            return getattr(_metrics, name)
        # Return a no-op counter/gauge/histogram
        return _NoOpMetric()


class _NoOpMetric:
    """No-op metric that ignores all operations."""

    def inc(self, *args: object, **kwargs: object) -> None:
        pass

    def dec(self, *args: object, **kwargs: object) -> None:
        pass

    def set(self, *args: object, **kwargs: object) -> None:
        pass

    def observe(self, *args: object, **kwargs: object) -> None:
        pass

    def labels(self, *_args: object, **_kwargs: object) -> _NoOpMetric:
        return self


metrics = MetricsProxy()
