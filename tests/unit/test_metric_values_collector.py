"""Unit tests for metric values collector."""

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

from src.app.metric_values_collector import (
    MetricValuesWindow,
    _get_window_boundaries,
    _store_window_values,
)


class TestMetricValuesWindow:
    def test_create_window(self) -> None:
        window_start = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        window_end = datetime(2024, 1, 15, 12, 1, 0, tzinfo=UTC)

        window = MetricValuesWindow(window_start, window_end)

        assert window.window_start == window_start
        assert window.window_end == window_end
        assert len(window.metrics) == 0

    def test_add_single_metric(self) -> None:
        window = MetricValuesWindow(
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=UTC),
        )

        window.add(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            value=0.95,
            analysis_mask=1,
            meta={"unit": "percent"},
        )

        key = ("test_algo", "1.0.0", "accuracy")
        assert key in window.metrics
        assert len(window.metrics[key]) == 1
        assert window.metrics[key][0]["value"] == 0.95
        assert window.metrics[key][0]["analysis_mask"] == 1
        assert window.metrics[key][0]["meta"] == {"unit": "percent"}

    def test_add_multiple_values_same_metric(self) -> None:
        window = MetricValuesWindow(
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=UTC),
        )

        for i in range(5):
            window.add(
                algo_name="test_algo",
                algo_version="1.0.0",
                metric_name="accuracy",
                value=0.9 + i * 0.01,
                analysis_mask=1,
                meta=None,
            )

        key = ("test_algo", "1.0.0", "accuracy")
        assert len(window.metrics[key]) == 5

    def test_add_multiple_different_metrics(self) -> None:
        window = MetricValuesWindow(
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=UTC),
        )

        window.add(
            algo_name="algo1",
            algo_version="1.0.0",
            metric_name="accuracy",
            value=0.95,
            analysis_mask=1,
            meta=None,
        )
        window.add(
            algo_name="algo2",
            algo_version="2.0.0",
            metric_name="precision",
            value=0.90,
            analysis_mask=1,
            meta=None,
        )

        assert len(window.metrics) == 2
        assert ("algo1", "1.0.0", "accuracy") in window.metrics
        assert ("algo2", "2.0.0", "precision") in window.metrics

    def test_add_boolean_value(self) -> None:
        window = MetricValuesWindow(
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=UTC),
        )

        window.add(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="passed",
            value=True,
            analysis_mask=8,  # COUNTER
            meta=None,
        )

        key = ("test_algo", "1.0.0", "passed")
        assert window.metrics[key][0]["value"] is True

    def test_add_dict_value(self) -> None:
        window = MetricValuesWindow(
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=UTC),
        )

        window.add(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="position",
            value={"x": 1.0, "y": 2.0},
            analysis_mask=32,  # ELLIPSE_2D
            meta=None,
        )

        key = ("test_algo", "1.0.0", "position")
        assert window.metrics[key][0]["value"] == {"x": 1.0, "y": 2.0}

    def test_add_list_value(self) -> None:
        window = MetricValuesWindow(
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=UTC),
        )

        window.add(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="points",
            value=[1.0, 2.0, 3.0],
            analysis_mask=64,  # CONTOUR_2D
            meta=None,
        )

        key = ("test_algo", "1.0.0", "points")
        assert window.metrics[key][0]["value"] == [1.0, 2.0, 3.0]


class TestGetWindowBoundaries:
    def test_basic_window_boundaries(self) -> None:
        timestamp = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
        window_size_sec = 60

        start, end = _get_window_boundaries(timestamp, window_size_sec)

        assert start == datetime(2024, 1, 15, 12, 30, 0, tzinfo=UTC)
        assert end == datetime(2024, 1, 15, 12, 31, 0, tzinfo=UTC)

    def test_window_at_boundary(self) -> None:
        timestamp = datetime(2024, 1, 15, 12, 30, 0, tzinfo=UTC)
        window_size_sec = 60

        start, end = _get_window_boundaries(timestamp, window_size_sec)

        assert start == datetime(2024, 1, 15, 12, 30, 0, tzinfo=UTC)
        assert end == datetime(2024, 1, 15, 12, 31, 0, tzinfo=UTC)

    def test_window_5_minute_size(self) -> None:
        timestamp = datetime(2024, 1, 15, 12, 33, 45, tzinfo=UTC)
        window_size_sec = 300  # 5 minutes

        start, end = _get_window_boundaries(timestamp, window_size_sec)

        assert start == datetime(2024, 1, 15, 12, 30, 0, tzinfo=UTC)
        assert end == datetime(2024, 1, 15, 12, 35, 0, tzinfo=UTC)

    def test_window_10_second_size(self) -> None:
        timestamp = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
        window_size_sec = 10

        start, end = _get_window_boundaries(timestamp, window_size_sec)

        assert start == datetime(2024, 1, 15, 12, 30, 40, tzinfo=UTC)
        assert end == datetime(2024, 1, 15, 12, 30, 50, tzinfo=UTC)

    def test_window_hour_size(self) -> None:
        timestamp = datetime(2024, 1, 15, 12, 33, 45, tzinfo=UTC)
        window_size_sec = 3600  # 1 hour

        start, end = _get_window_boundaries(timestamp, window_size_sec)

        assert start == datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        assert end == datetime(2024, 1, 15, 13, 0, 0, tzinfo=UTC)


class TestStoreWindowValues:
    def test_store_window_values_success(self) -> None:
        window = MetricValuesWindow(
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=UTC),
        )
        window.add(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            value=0.95,
            analysis_mask=1,
            meta=None,
        )

        mock_storage = MagicMock()
        mock_storage.buckets = {"metric-values": "metric-values"}

        stored = _store_window_values(window, mock_storage)

        assert stored == 1
        mock_storage.store_with_key.assert_called_once()

    def test_store_window_values_empty_window(self) -> None:
        window = MetricValuesWindow(
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=UTC),
        )

        mock_storage = MagicMock()
        mock_storage.buckets = {"metric-values": "metric-values"}

        stored = _store_window_values(window, mock_storage)

        assert stored == 0
        mock_storage.store_with_key.assert_not_called()

    def test_store_window_values_multiple_metrics(self) -> None:
        window = MetricValuesWindow(
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=UTC),
        )
        window.add(
            algo_name="algo1",
            algo_version="1.0.0",
            metric_name="accuracy",
            value=0.95,
            analysis_mask=1,
            meta=None,
        )
        window.add(
            algo_name="algo2",
            algo_version="2.0.0",
            metric_name="precision",
            value=0.90,
            analysis_mask=1,
            meta=None,
        )

        mock_storage = MagicMock()
        mock_storage.buckets = {"metric-values": "metric-values"}

        stored = _store_window_values(window, mock_storage)

        assert stored == 2
        assert mock_storage.store_with_key.call_count == 2

    def test_store_window_values_with_storage_error(self) -> None:
        window = MetricValuesWindow(
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=UTC),
        )
        window.add(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            value=0.95,
            analysis_mask=1,
            meta=None,
        )

        mock_storage = MagicMock()
        mock_storage.buckets = {"metric-values": "metric-values"}
        mock_storage.store_with_key.side_effect = Exception("Storage error")

        # Should not raise
        stored = _store_window_values(window, mock_storage)

        assert stored == 0

    def test_store_window_values_uses_deterministic_key(self) -> None:
        window = MetricValuesWindow(
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=UTC),
        )
        window.add(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            value=0.95,
            analysis_mask=1,
            meta=None,
        )

        mock_storage = MagicMock()
        mock_storage.buckets = {"metric-values": "metric-values"}

        _store_window_values(window, mock_storage)

        # Verify key format: should be sharded by hash prefix
        call_args = mock_storage.store_with_key.call_args
        key = call_args.kwargs.get("key") or call_args[1].get("key")
        # Key should be in format: ab/cd/full_hash
        parts = key.split("/")
        assert len(parts) == 3
        assert len(parts[0]) == 2
        assert len(parts[1]) == 2
        assert len(parts[2]) == 64

    def test_store_window_values_skips_empty_entries(self) -> None:
        window = MetricValuesWindow(
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=UTC),
        )

        # Manually add an empty list
        window.metrics[("test_algo", "1.0.0", "empty_metric")] = []

        mock_storage = MagicMock()
        mock_storage.buckets = {"metric-values": "metric-values"}

        stored = _store_window_values(window, mock_storage)

        assert stored == 0
        mock_storage.store_with_key.assert_not_called()

    def test_store_window_values_preserves_analysis_mask(self) -> None:
        window = MetricValuesWindow(
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=UTC),
        )
        window.add(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            value=0.95,
            analysis_mask=42,  # Some specific mask
            meta=None,
        )

        mock_storage = MagicMock()
        mock_storage.buckets = {"metric-values": "metric-values"}

        _store_window_values(window, mock_storage)

        # Verify the stored document contains the analysis_mask
        call_args = mock_storage.store_with_key.call_args
        data = call_args.kwargs.get("data") or call_args[1].get("data")

        # Parse the stored JSON
        doc = json.loads(data.decode("utf-8") if isinstance(data, bytes) else data)
        assert doc["analysis_mask"] == 42

    def test_store_window_values_with_meta(self) -> None:
        window = MetricValuesWindow(
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=UTC),
        )
        window.add(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            value=0.95,
            analysis_mask=1,
            meta={"unit": "percent", "source": "validation"},
        )

        mock_storage = MagicMock()
        mock_storage.buckets = {"metric-values": "metric-values"}

        _store_window_values(window, mock_storage)

        call_args = mock_storage.store_with_key.call_args
        data = call_args.kwargs.get("data") or call_args[1].get("data")

        doc = json.loads(data.decode("utf-8") if isinstance(data, bytes) else data)
        assert doc["meta"] == {"unit": "percent", "source": "validation"}

    def test_store_window_values_document_structure(self) -> None:
        window = MetricValuesWindow(
            datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 15, 12, 1, 0, tzinfo=UTC),
        )
        window.add(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            value=0.95,
            analysis_mask=1,
            meta=None,
        )

        mock_storage = MagicMock()
        mock_storage.buckets = {"metric-values": "metric-values"}

        _store_window_values(window, mock_storage)

        call_args = mock_storage.store_with_key.call_args
        data = call_args.kwargs.get("data") or call_args[1].get("data")

        doc = json.loads(data.decode("utf-8") if isinstance(data, bytes) else data)

        # Verify document structure
        assert doc["algo_name"] == "test_algo"
        assert doc["algo_version"] == "1.0.0"
        assert doc["metric_name"] == "accuracy"
        assert doc["analysis_mask"] == 1
        assert "window_start" in doc
        assert "window_end" in doc
        assert "window_start_unix" in doc
        assert "window_end_unix" in doc
        assert doc["count"] == 1
        assert doc["values"] == [0.95]
