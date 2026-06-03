"""Unit tests for src.observability.metrics.

Tests cover:
- Counter/Histogram/Gauge presence and metadata
- Increment / label behaviour using prometheus_client's registry samples
- set_build_info()
- configure_metrics() behaviour: disabled, idempotent, OSError on start
- Backwards-compat aliases
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from prometheus_client import Counter, Gauge, Histogram, Info

import src.observability.metrics as metrics_module
from src.observability.config import ObservabilityConfig
from src.observability.metrics import (
    BUILD_INFO,
    DEFAULT_BUCKETS,
    FLINK_AGGREGATIONS_PRODUCED,
    JOB_DURATION,
    JOBS_COMPLETED,
    JOBS_CREATED,
    JOBS_FAILED,
    JOBS_IN_PROGRESS,
    KAFKA_CONSUMER_LAG,
    KAFKA_MESSAGES_CONSUMED,
    KAFKA_MESSAGES_PRODUCED,
    MINIO_OBJECTS_STORED,
    PIPELINE_UP,
    TASK_DURATION,
    TASKS_COMPLETED,
    TASKS_CREATED,
    TASKS_FAILED,
    TASKS_IN_PROGRESS,
    configure_metrics,
    set_build_info,
)


def _make_config(**overrides: object) -> ObservabilityConfig:
    defaults: dict[str, object] = {
        "tracing_enabled": False,
        "service_name": "tapline-test",
        "tracing_exporter": "none",
        "otlp_endpoint": "",
        "sample_rate": 1.0,
        "metrics_enabled": True,
        "metrics_port": 18000,
        "metrics_path": "/metrics",
        "include_runtime_metrics": True,
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


def _counter_value(counter: Counter, **labels: str) -> float:
    """Read the current value from a Counter via its samples."""
    if labels:
        child = counter.labels(**labels)
        # Counter children expose ._value.get()
        return child._value.get()  # type: ignore[no-any-return, attr-defined]
    return counter._value.get()  # type: ignore[no-any-return, attr-defined]


def _gauge_value(gauge: Gauge, **labels: str) -> float:
    if labels:
        child = gauge.labels(**labels)
        return child._value.get()  # type: ignore[no-any-return, attr-defined]
    return gauge._value.get()  # type: ignore[no-any-return, attr-defined]


class TestMetricTypesAndMetadata:
    def test_tasks_created_is_counter_with_directory_key_label(self) -> None:
        assert isinstance(TASKS_CREATED, Counter)
        # prometheus_client appends "_total" to counter name internally;
        # the metric `_name` attribute stores the base name
        assert TASKS_CREATED._name == "tapline_tasks_created"
        assert TASKS_CREATED._labelnames == ("directory_key",)

    def test_task_duration_is_histogram(self) -> None:
        assert isinstance(TASK_DURATION, Histogram)
        assert TASK_DURATION._name == "tapline_task_duration_seconds"
        assert TASK_DURATION._labelnames == ("processor_name",)

    def test_tasks_in_progress_is_gauge(self) -> None:
        assert isinstance(TASKS_IN_PROGRESS, Gauge)
        assert TASKS_IN_PROGRESS._labelnames == ("worker_id",)

    def test_pipeline_up_is_unlabeled_gauge(self) -> None:
        assert isinstance(PIPELINE_UP, Gauge)
        assert PIPELINE_UP._labelnames == ()

    def test_kafka_consumer_lag_labels(self) -> None:
        assert isinstance(KAFKA_CONSUMER_LAG, Gauge)
        assert KAFKA_CONSUMER_LAG._labelnames == ("topic", "partition", "consumer_group")

    def test_flink_aggregations_produced_is_unlabeled_counter(self) -> None:
        assert isinstance(FLINK_AGGREGATIONS_PRODUCED, Counter)
        assert FLINK_AGGREGATIONS_PRODUCED._labelnames == ()

    def test_build_info_is_info_metric(self) -> None:
        assert isinstance(BUILD_INFO, Info)


class TestDefaultBuckets:
    def test_default_buckets_are_a_tuple(self) -> None:
        assert isinstance(DEFAULT_BUCKETS, tuple)

    def test_default_buckets_are_sorted_ascending(self) -> None:
        assert list(DEFAULT_BUCKETS) == sorted(DEFAULT_BUCKETS)

    def test_default_buckets_cover_expected_range(self) -> None:
        assert DEFAULT_BUCKETS[0] == 0.005
        assert DEFAULT_BUCKETS[-1] == 10.0


class TestCounterIncrement:
    def test_counter_inc_increments_value(self) -> None:
        before = _counter_value(TASKS_CREATED, directory_key="unit-test-dir-1")
        TASKS_CREATED.labels(directory_key="unit-test-dir-1").inc()
        after = _counter_value(TASKS_CREATED, directory_key="unit-test-dir-1")
        assert after == before + 1

    def test_counter_inc_by_amount(self) -> None:
        before = _counter_value(TASKS_CREATED, directory_key="unit-test-dir-2")
        TASKS_CREATED.labels(directory_key="unit-test-dir-2").inc(5)
        after = _counter_value(TASKS_CREATED, directory_key="unit-test-dir-2")
        assert after == before + 5

    def test_different_labels_track_independently(self) -> None:
        TASKS_CREATED.labels(directory_key="label-a").inc()
        TASKS_CREATED.labels(directory_key="label-b").inc()
        TASKS_CREATED.labels(directory_key="label-a").inc()

        assert _counter_value(TASKS_CREATED, directory_key="label-a") >= 2
        assert _counter_value(TASKS_CREATED, directory_key="label-b") >= 1

    def test_multi_label_counter(self) -> None:
        before = _counter_value(
            TASKS_FAILED,
            processor_name="unit-proc",
            error_type="TimeoutError",
        )
        TASKS_FAILED.labels(
            processor_name="unit-proc",
            error_type="TimeoutError",
        ).inc()
        after = _counter_value(
            TASKS_FAILED,
            processor_name="unit-proc",
            error_type="TimeoutError",
        )
        assert after == before + 1

    def test_minio_objects_stored(self) -> None:
        before = _counter_value(MINIO_OBJECTS_STORED, bucket="unit-bucket-store")
        MINIO_OBJECTS_STORED.labels(bucket="unit-bucket-store").inc(3)
        after = _counter_value(MINIO_OBJECTS_STORED, bucket="unit-bucket-store")
        assert after == before + 3


def _histogram_count_sum(hist: Histogram, **labels: str) -> tuple[float, float]:
    """Read the (count, sum) for a histogram via collected samples."""
    count_val = 0.0
    sum_val = 0.0
    for metric in hist.collect():
        for sample in metric.samples:
            sample_labels = sample.labels
            if all(sample_labels.get(k) == v for k, v in labels.items()):
                if sample.name.endswith("_count"):
                    count_val = sample.value
                elif sample.name.endswith("_sum"):
                    sum_val = sample.value
    return count_val, sum_val


class TestHistogramObserve:
    def test_histogram_observe_increments_count(self) -> None:
        before_count, _ = _histogram_count_sum(TASK_DURATION, processor_name="unit-hist-1")
        TASK_DURATION.labels(processor_name="unit-hist-1").observe(0.123)
        after_count, _ = _histogram_count_sum(TASK_DURATION, processor_name="unit-hist-1")
        assert after_count == before_count + 1

    def test_histogram_observe_updates_sum(self) -> None:
        _, before_sum = _histogram_count_sum(TASK_DURATION, processor_name="unit-hist-2")
        TASK_DURATION.labels(processor_name="unit-hist-2").observe(0.5)
        _, after_sum = _histogram_count_sum(TASK_DURATION, processor_name="unit-hist-2")
        assert pytest.approx(after_sum - before_sum, rel=1e-6) == 0.5


class TestGaugeOperations:
    def test_gauge_set(self) -> None:
        PIPELINE_UP.set(1)
        assert _gauge_value(PIPELINE_UP) == 1

        PIPELINE_UP.set(0)
        assert _gauge_value(PIPELINE_UP) == 0

    def test_gauge_inc_dec(self) -> None:
        worker_id = "unit-worker-99"
        before = _gauge_value(TASKS_IN_PROGRESS, worker_id=worker_id)
        TASKS_IN_PROGRESS.labels(worker_id=worker_id).inc()
        assert _gauge_value(TASKS_IN_PROGRESS, worker_id=worker_id) == before + 1
        TASKS_IN_PROGRESS.labels(worker_id=worker_id).dec()
        assert _gauge_value(TASKS_IN_PROGRESS, worker_id=worker_id) == before


class TestKafkaMetrics:
    def test_kafka_messages_produced(self) -> None:
        before = _counter_value(KAFKA_MESSAGES_PRODUCED, topic="unit-topic")
        KAFKA_MESSAGES_PRODUCED.labels(topic="unit-topic").inc()
        after = _counter_value(KAFKA_MESSAGES_PRODUCED, topic="unit-topic")
        assert after == before + 1

    def test_kafka_messages_consumed_multi_label(self) -> None:
        before = _counter_value(
            KAFKA_MESSAGES_CONSUMED,
            topic="unit-topic",
            consumer_group="unit-group",
        )
        KAFKA_MESSAGES_CONSUMED.labels(
            topic="unit-topic",
            consumer_group="unit-group",
        ).inc(2)
        after = _counter_value(
            KAFKA_MESSAGES_CONSUMED,
            topic="unit-topic",
            consumer_group="unit-group",
        )
        assert after == before + 2


class TestSetBuildInfo:
    def test_set_build_info_sets_version_and_mode(self) -> None:
        set_build_info(version="9.9.9-test", mode="unit-test-mode")

        info = BUILD_INFO._value  # type: ignore[attr-defined]
        assert info["version"] == "9.9.9-test"
        assert info["mode"] == "unit-test-mode"

    def test_set_build_info_with_extra(self) -> None:
        set_build_info(version="1.0", mode="m", commit="deadbeef", branch="main")

        info = BUILD_INFO._value  # type: ignore[attr-defined]
        assert info["version"] == "1.0"
        assert info["mode"] == "m"
        assert info["commit"] == "deadbeef"
        assert info["branch"] == "main"


class TestConfigureMetrics:
    def setup_method(self) -> None:
        # Reset the module-level guard so each test starts from a clean state
        metrics_module._metrics_server_started = False

    def teardown_method(self) -> None:
        metrics_module._metrics_server_started = False

    def test_disabled_returns_false(self) -> None:
        cfg = _make_config(metrics_enabled=False)

        result = configure_metrics(cfg)

        assert result is False
        assert metrics_module._metrics_server_started is False

    def test_enabled_starts_server(self) -> None:
        cfg = _make_config(metrics_enabled=True, metrics_port=18001)

        with patch.object(metrics_module, "start_http_server") as mock_start:
            result = configure_metrics(cfg)

            assert result is True
            mock_start.assert_called_once_with(18001)
            assert metrics_module._metrics_server_started is True

    def test_idempotent_second_call_short_circuits(self) -> None:
        cfg = _make_config(metrics_enabled=True, metrics_port=18002)

        with patch.object(metrics_module, "start_http_server") as mock_start:
            first = configure_metrics(cfg)
            second = configure_metrics(cfg)

            assert first is True
            assert second is True
            # Server only started once
            assert mock_start.call_count == 1

    def test_oserror_returns_false(self) -> None:
        cfg = _make_config(metrics_enabled=True, metrics_port=18003)

        with patch.object(
            metrics_module,
            "start_http_server",
            side_effect=OSError("port in use"),
        ):
            result = configure_metrics(cfg)

            assert result is False
            # Should not be marked as started on failure
            assert metrics_module._metrics_server_started is False

    def test_runtime_metrics_disabled_unregisters_collectors(self) -> None:
        cfg = _make_config(
            metrics_enabled=True,
            metrics_port=18004,
            include_runtime_metrics=False,
        )

        with (
            patch.object(metrics_module, "start_http_server"),
            patch.object(metrics_module.REGISTRY, "unregister") as mock_unreg,
        ):
            configure_metrics(cfg)

            # 3 default collectors should be attempted to unregister
            assert mock_unreg.call_count == 3


class TestBackwardsCompatAliases:
    def test_jobs_aliases_point_to_tasks(self) -> None:
        assert JOBS_CREATED is TASKS_CREATED
        assert JOBS_COMPLETED is TASKS_COMPLETED
        assert JOBS_FAILED is TASKS_FAILED
        assert JOB_DURATION is TASK_DURATION
        assert JOBS_IN_PROGRESS is TASKS_IN_PROGRESS
