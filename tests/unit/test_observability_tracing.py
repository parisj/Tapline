"""Unit tests for src.observability.tracing.

Uses OpenTelemetry's InMemorySpanExporter to capture spans without
network exporters. Tracer provider global state is reset between
tests via the shutdown_tracing helper.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import src.observability.tracing as tracing_module
from src.observability.config import ObservabilityConfig
from src.observability.tracing import (
    configure_tracing,
    get_current_span,
    get_current_trace_context,
    get_tracer,
    shutdown_tracing,
    traced,
)


def _make_config(**overrides: object) -> ObservabilityConfig:
    defaults: dict[str, object] = {
        "tracing_enabled": True,
        "service_name": "unit-svc",
        "tracing_exporter": "none",
        "otlp_endpoint": "http://localhost:4317",
        "sample_rate": 1.0,
        "metrics_enabled": False,
        "metrics_port": 8000,
        "metrics_path": "/metrics",
        "include_runtime_metrics": False,
        "histogram_buckets": (0.1, 1.0),
        "log_format": "json",
        "include_trace_context": False,
        "include_correlation_id": False,
        "include_caller_info": False,
        "correlation_header_name": "X-Correlation-ID",
        "kafka_correlation_header": "x-correlation-id",
        "propagate_correlation_to_kafka": True,
    }
    defaults.update(overrides)
    return ObservabilityConfig(**defaults)  # type: ignore[arg-type]


@pytest.fixture
def reset_tracing():  # type: ignore[no-untyped-def]
    """Reset tracing module state before and after each test."""
    shutdown_tracing()
    # Also reset the global tracer provider — OTel uses a sentinel
    # ProxyTracerProvider by default; reassigning a fresh real one is OK
    # for tests, but we must not leak between tests.
    yield
    shutdown_tracing()


@pytest.fixture
def in_memory_tracer(monkeypatch):  # type: ignore[no-untyped-def]
    """Install an in-memory tracer provider and patch get_tracer to use it.

    OpenTelemetry's global tracer provider can only be set once per
    process. Since other tests/modules may have already initialised one,
    we instead monkey-patch the module's `get_tracer` to delegate to our
    isolated provider, so every span goes through the in-memory exporter.
    """
    shutdown_tracing()
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    def fake_get_tracer(name: str):  # type: ignore[no-untyped-def]
        return provider.get_tracer(name)

    monkeypatch.setattr(tracing_module, "get_tracer", fake_get_tracer)

    # Also bind into this test module's namespace so module-level
    # `get_tracer` references resolve to the patched version.
    monkeypatch.setattr(
        "tests.unit.test_observability_tracing.get_tracer",
        fake_get_tracer,
        raising=False,
    )

    yield exporter, provider
    shutdown_tracing()


class TestConfigureTracingDisabled:
    def test_disabled_returns_none(self, reset_tracing: None) -> None:
        cfg = _make_config(tracing_enabled=False)

        result = configure_tracing(cfg)

        assert result is None
        assert tracing_module._initialized is True

    def test_disabled_second_call_returns_none(self, reset_tracing: None) -> None:
        cfg = _make_config(tracing_enabled=False)

        first = configure_tracing(cfg)
        second = configure_tracing(cfg)

        assert first is None
        assert second is None


class TestConfigureTracingEnabled:
    def test_enabled_creates_provider(self, reset_tracing: None) -> None:
        cfg = _make_config(
            tracing_enabled=True,
            tracing_exporter="none",
            service_name="unit-svc",
            sample_rate=1.0,
        )

        provider = configure_tracing(cfg)

        assert provider is not None
        assert isinstance(provider, TracerProvider)

    def test_enabled_console_exporter(self, reset_tracing: None) -> None:
        cfg = _make_config(tracing_enabled=True, tracing_exporter="console")

        provider = configure_tracing(cfg)

        assert provider is not None

    def test_enabled_unknown_exporter_still_returns_provider(self, reset_tracing: None) -> None:
        cfg = _make_config(tracing_enabled=True, tracing_exporter="weird-thing")

        provider = configure_tracing(cfg)

        # Provider is still set up; warning is logged but no exception
        assert provider is not None

    def test_idempotent_second_call(self, reset_tracing: None) -> None:
        cfg = _make_config(tracing_enabled=True, tracing_exporter="none")

        first = configure_tracing(cfg)
        second = configure_tracing(cfg)

        assert first is second
        assert tracing_module._initialized is True

    def test_otlp_falls_back_when_import_fails(self, reset_tracing: None) -> None:
        cfg = _make_config(tracing_enabled=True, tracing_exporter="otlp")

        # Patch the inner import of OTLPSpanExporter to raise ImportError.
        # The function does a local import inside configure_tracing.
        import builtins

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):  # type: ignore[no-untyped-def]
            if "otlp" in name:
                msg = "simulated missing otlp"
                raise ImportError(msg)
            return real_import(name, *args, **kwargs)

        with patch.object(builtins, "__import__", side_effect=fake_import):
            provider = configure_tracing(cfg)

        # Falls back without raising
        assert provider is not None


class TestGetTracer:
    def test_returns_tracer(self) -> None:
        tracer = get_tracer("my.module")

        # Should be a Tracer-like object (OTel's API returns one regardless
        # of whether SDK is configured).
        assert tracer is not None
        assert hasattr(tracer, "start_as_current_span")


class TestSpanCreation:
    def test_span_is_recorded_by_exporter(self, in_memory_tracer) -> None:  # type: ignore[no-untyped-def]
        exporter, provider = in_memory_tracer

        tracer = provider.get_tracer("test.module")
        with tracer.start_as_current_span("my-span") as span:
            span.set_attribute("key", "value")

        spans = exporter.get_finished_spans()
        names = [s.name for s in spans]
        assert "my-span" in names

    def test_span_attributes_are_set(self, in_memory_tracer) -> None:  # type: ignore[no-untyped-def]
        exporter, provider = in_memory_tracer

        tracer = provider.get_tracer("test.module")
        with tracer.start_as_current_span("attr-span") as span:
            span.set_attribute("attr1", "val1")
            span.set_attribute("count", 42)

        spans = exporter.get_finished_spans()
        attr_span = next(s for s in spans if s.name == "attr-span")
        assert attr_span.attributes["attr1"] == "val1"
        assert attr_span.attributes["count"] == 42

    def test_nested_spans_have_parent_relationship(self, in_memory_tracer) -> None:  # type: ignore[no-untyped-def]
        exporter, provider = in_memory_tracer
        tracer = provider.get_tracer("test.module")

        with tracer.start_as_current_span("parent") as parent, tracer.start_as_current_span("child"):
            pass

        spans = exporter.get_finished_spans()
        parent_span = next(s for s in spans if s.name == "parent")
        child_span = next(s for s in spans if s.name == "child")

        assert child_span.parent is not None
        assert child_span.parent.span_id == parent_span.context.span_id


class TestTracedDecorator:
    def test_traced_creates_span_with_function_name(self, in_memory_tracer) -> None:  # type: ignore[no-untyped-def]
        exporter, _ = in_memory_tracer

        @traced()
        def my_func(x: int) -> int:
            return x * 2

        result = my_func(5)

        assert result == 10
        spans = exporter.get_finished_spans()
        names = [s.name for s in spans]
        assert "my_func" in names

    def test_traced_uses_custom_span_name(self, in_memory_tracer) -> None:  # type: ignore[no-untyped-def]
        exporter, _ = in_memory_tracer

        @traced(span_name="custom.operation")
        def my_func() -> str:
            return "done"

        my_func()

        spans = exporter.get_finished_spans()
        names = [s.name for s in spans]
        assert "custom.operation" in names

    def test_traced_sets_function_and_module_attributes(self, in_memory_tracer) -> None:  # type: ignore[no-untyped-def]
        exporter, _ = in_memory_tracer

        @traced(span_name="attrs-test")
        def my_func() -> None:
            pass

        my_func()

        spans = exporter.get_finished_spans()
        span = next(s for s in spans if s.name == "attrs-test")
        assert span.attributes["function"] == "my_func"
        assert span.attributes["module"] == my_func.__module__

    def test_traced_attaches_custom_attributes(self, in_memory_tracer) -> None:  # type: ignore[no-untyped-def]
        exporter, _ = in_memory_tracer

        @traced(span_name="with-attrs", attributes={"layer": "test", "weight": 7})
        def my_func() -> None:
            pass

        my_func()

        spans = exporter.get_finished_spans()
        span = next(s for s in spans if s.name == "with-attrs")
        assert span.attributes["layer"] == "test"
        assert span.attributes["weight"] == 7

    def test_traced_preserves_return_value_and_args(self, in_memory_tracer) -> None:  # type: ignore[no-untyped-def]
        @traced()
        def add(a: int, b: int) -> int:
            return a + b

        assert add(2, 3) == 5
        assert add(a=10, b=20) == 30

    def test_traced_preserves_function_metadata(self) -> None:
        @traced()
        def documented() -> None:
            """Doc string."""

        # functools.wraps preserves __name__ and __doc__
        assert documented.__name__ == "documented"
        assert documented.__doc__ == "Doc string."


class TestGetCurrentSpan:
    def test_outside_span_returns_invalid_span(self) -> None:
        span = get_current_span()
        # Outside of any span, OTel returns the NonRecordingSpan / INVALID_SPAN
        assert span is not None
        ctx = span.get_span_context()
        # Either invalid context or zero IDs
        assert ctx.trace_id == 0 or not ctx.is_valid

    def test_inside_span_returns_active_span(self, in_memory_tracer) -> None:  # type: ignore[no-untyped-def]
        tracer = get_tracer("test.current")

        with tracer.start_as_current_span("active") as expected_span:
            current = get_current_span()
            assert current.get_span_context().span_id == expected_span.get_span_context().span_id


class TestGetCurrentTraceContext:
    def test_outside_span_returns_empty_dict(self) -> None:
        ctx = get_current_trace_context()
        # An invalid span context yields an empty dict
        assert ctx == {}

    def test_inside_span_returns_ids(self, in_memory_tracer) -> None:  # type: ignore[no-untyped-def]
        tracer = get_tracer("test.ctx")

        with tracer.start_as_current_span("ctx-span"):
            ctx = get_current_trace_context()

        assert "trace_id" in ctx
        assert "span_id" in ctx
        # Hex strings of correct length
        assert len(ctx["trace_id"]) == 32
        assert len(ctx["span_id"]) == 16
        int(ctx["trace_id"], 16)
        int(ctx["span_id"], 16)


class TestShutdownTracing:
    def test_shutdown_clears_state(self, reset_tracing: None) -> None:
        cfg = _make_config(tracing_enabled=True, tracing_exporter="none")
        provider = configure_tracing(cfg)
        assert provider is not None
        assert tracing_module._initialized is True

        shutdown_tracing()

        assert tracing_module._tracer_provider is None
        assert tracing_module._initialized is False

    def test_shutdown_when_not_initialized_is_safe(self, reset_tracing: None) -> None:
        # Should not raise even if nothing was initialized
        shutdown_tracing()
        assert tracing_module._tracer_provider is None
