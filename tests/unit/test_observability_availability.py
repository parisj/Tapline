"""Unit tests for src.observability.availability.

The availability module provides safe fallbacks when observability
dependencies are missing. We test:
- is_available()
- get_tracer() pass-through
- get_correlation_id() pass-through
- MetricsProxy attribute resolution
- _NoOpMetric methods
"""

from __future__ import annotations

import src.observability.availability as availability_module
from src.observability.availability import (
    MetricsProxy,
    _NoOpMetric,
    get_correlation_id,
    get_tracer,
    is_available,
    metrics,
)


class TestIsAvailable:
    def test_returns_true_when_modules_imported(self) -> None:
        # In the test environment, observability modules import cleanly
        assert is_available() is True

    def test_returns_module_flag(self) -> None:
        # Direct inspection of the underlying flag
        assert is_available() == availability_module._AVAILABLE


class TestGetTracer:
    def test_returns_tracer_when_available(self) -> None:
        result = get_tracer("test.module")

        # Should delegate to tracing module's get_tracer
        assert result is not None

    def test_returns_none_when_tracer_module_missing(self) -> None:
        original = availability_module._tracer
        try:
            availability_module._tracer = None
            assert get_tracer("anything") is None
        finally:
            availability_module._tracer = original


class TestGetCorrelationId:
    def test_pass_through_when_available(self) -> None:
        from src.observability.correlation import (
            reset_correlation_id,
            set_correlation_id,
        )

        token = set_correlation_id("avail-test-id")
        try:
            assert get_correlation_id() == "avail-test-id"
        finally:
            reset_correlation_id(token)

    def test_returns_none_when_correlation_module_missing(self) -> None:
        original = availability_module._correlation
        try:
            availability_module._correlation = None
            assert get_correlation_id() is None
        finally:
            availability_module._correlation = original


class TestMetricsProxy:
    def test_returns_real_metric_when_available(self) -> None:
        proxy = MetricsProxy()

        # The metrics module exposes TASKS_CREATED Counter
        attr = proxy.TASKS_CREATED

        # It should be the real Counter, not a no-op
        from prometheus_client import Counter

        assert isinstance(attr, Counter)

    def test_returns_noop_when_metrics_module_missing(self) -> None:
        original = availability_module._metrics
        try:
            availability_module._metrics = None
            proxy = MetricsProxy()

            attr = proxy.ANY_NAME_AT_ALL
            assert isinstance(attr, _NoOpMetric)
        finally:
            availability_module._metrics = original

    def test_module_level_metrics_singleton(self) -> None:
        # The module exports a `metrics` instance
        assert isinstance(metrics, MetricsProxy)


class TestNoOpMetric:
    def test_inc_does_not_raise(self) -> None:
        m = _NoOpMetric()
        # No return; should be silent
        assert m.inc() is None
        assert m.inc(1.0) is None
        assert m.inc(amount=5) is None

    def test_dec_does_not_raise(self) -> None:
        m = _NoOpMetric()
        assert m.dec() is None
        assert m.dec(2.0) is None

    def test_set_does_not_raise(self) -> None:
        m = _NoOpMetric()
        assert m.set(42) is None

    def test_observe_does_not_raise(self) -> None:
        m = _NoOpMetric()
        assert m.observe(0.123) is None

    def test_labels_returns_self(self) -> None:
        m = _NoOpMetric()
        labelled = m.labels("a", "b", key="value")

        # labels() returns self so chained calls continue to no-op
        assert labelled is m

    def test_labels_then_inc_chain(self) -> None:
        m = _NoOpMetric()
        # Should be silent end-to-end
        m.labels("topic").inc()
        m.labels("topic", "group").inc(3)


class TestNoOpProxyChain:
    def test_chained_call_when_no_metrics(self) -> None:
        original = availability_module._metrics
        try:
            availability_module._metrics = None
            proxy = MetricsProxy()

            # Should never raise even when chained — the no-op fully absorbs
            proxy.UNKNOWN_METRIC.labels(topic="t").inc(2)
            proxy.OTHER_METRIC.observe(0.5)
            proxy.GAUGE.set(99)
        finally:
            availability_module._metrics = original
