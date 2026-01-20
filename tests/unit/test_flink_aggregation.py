"""Unit tests for Python-based metric aggregation."""

import threading
from unittest.mock import MagicMock

from src.app.flink_aggregation import MetricAggregate, TumblingWindowAggregator


class TestMetricAggregate:
    def test_create_aggregate(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
        )

        assert agg.algo_name == "test_algo"
        assert agg.algo_version == "1.0.0"
        assert agg.metric_name == "accuracy"
        assert agg.analysis_mask == 1
        assert agg.values == []

    def test_count_empty(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
        )

        assert agg.count == 0

    def test_count_with_values(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
            values=[0.1, 0.2, 0.3],
        )

        assert agg.count == 3

    def test_numeric_values(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
            values=[0.1, "ignored", 0.2, {"x": 1, "y": 2}, 0.3],
        )

        assert agg.numeric_values == [0.1, 0.2, 0.3]

    def test_point_values(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="position",
            analysis_mask=32,  # ELLIPSE_2D
            values=[
                {"x": 1.0, "y": 2.0},
                {"x": 3.0, "y": 4.0},
                "ignored",
                0.5,
            ],
        )

        points = agg.point_values
        assert len(points) == 2
        assert points[0] == {"x": 1.0, "y": 2.0}
        assert points[1] == {"x": 3.0, "y": 4.0}

    def test_has_2d_data(self) -> None:
        agg_with_points = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="position",
            analysis_mask=32,
            values=[{"x": 1.0, "y": 2.0}],
        )
        assert agg_with_points.has_2d_data is True

        agg_without_points = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
            values=[0.1, 0.2],
        )
        assert agg_without_points.has_2d_data is False

    def test_sum_empty(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
        )

        assert agg.sum == 0.0

    def test_sum_with_values(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
            values=[1.0, 2.0, 3.0],
        )

        assert agg.sum == 6.0

    def test_mean_empty(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
        )

        assert agg.mean == 0.0

    def test_mean_with_values(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
            values=[1.0, 2.0, 3.0],
        )

        assert agg.mean == 2.0

    def test_min_empty(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
        )

        assert agg.min is None

    def test_min_with_values(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
            values=[3.0, 1.0, 2.0],
        )

        assert agg.min == 1.0

    def test_max_empty(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
        )

        assert agg.max is None

    def test_max_with_values(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
            values=[3.0, 1.0, 2.0],
        )

        assert agg.max == 3.0

    def test_std_insufficient_values(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
            values=[1.0],  # Only one value
        )

        assert agg.std == 0.0

    def test_std_with_values(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
            values=[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0],
        )

        # Standard deviation should be close to 2.0
        assert abs(agg.std - 2.0) < 0.01

    def test_to_summary_empty(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
        )

        summary = agg.to_summary()

        assert summary["count"] == 0
        assert summary["numeric_count"] == 0
        assert summary["sum"] == 0.0
        assert summary["mean"] == 0.0
        assert summary["min"] is None
        assert summary["max"] is None
        assert summary["median"] is None

    def test_to_summary_with_numeric_values(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
            values=[1.0, 2.0, 3.0, 4.0, 5.0],
        )

        summary = agg.to_summary()

        assert summary["count"] == 5
        assert summary["numeric_count"] == 5
        assert summary["sum"] == 15.0
        assert summary["mean"] == 3.0
        assert summary["min"] == 1.0
        assert summary["max"] == 5.0
        assert summary["median"] == 3.0
        assert summary["p95"] is not None
        assert summary["p99"] is not None

    def test_to_summary_with_2d_points(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="position",
            analysis_mask=32,
            values=[
                {"x": 1.0, "y": 10.0},
                {"x": 2.0, "y": 20.0},
                {"x": 3.0, "y": 30.0},
            ],
        )

        summary = agg.to_summary()

        assert summary["point_count"] == 3
        assert summary["x_mean"] == 2.0
        assert summary["y_mean"] == 20.0
        assert summary["x_min"] == 1.0
        assert summary["x_max"] == 3.0
        assert summary["y_min"] == 10.0
        assert summary["y_max"] == 30.0

    def test_median_even_count(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
            values=[1.0, 2.0, 3.0, 4.0],
        )

        summary = agg.to_summary()
        assert summary["median"] == 2.5  # (2 + 3) / 2

    def test_median_odd_count(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
            values=[1.0, 2.0, 3.0, 4.0, 5.0],
        )

        summary = agg.to_summary()
        assert summary["median"] == 3.0

    def test_percentile_calculation(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
            values=list(range(1, 101)),  # 1 to 100
        )

        summary = agg.to_summary()
        assert summary["p95"] == 95 or summary["p95"] == 96  # Allow for rounding
        assert summary["p99"] == 99 or summary["p99"] == 100

    def test_meta_field(self) -> None:
        agg = MetricAggregate(
            algo_name="test_algo",
            algo_version="1.0.0",
            metric_name="accuracy",
            analysis_mask=1,
            meta={"unit": "percent", "source": "validation"},
        )

        assert agg.meta == {"unit": "percent", "source": "validation"}


class TestTumblingWindowAggregator:
    def test_create_aggregator(self) -> None:
        mock_storage = MagicMock()
        mock_producer = MagicMock()

        aggregator = TumblingWindowAggregator(
            window_size_sec=60,
            storage=mock_storage,
            producer=mock_producer,
        )

        assert aggregator._window_size_sec == 60
        assert aggregator.aggregates_produced == 0

    def test_add_metric_initializes_window(self) -> None:
        mock_storage = MagicMock()
        mock_producer = MagicMock()

        aggregator = TumblingWindowAggregator(
            window_size_sec=60,
            storage=mock_storage,
            producer=mock_producer,
        )

        payload = {
            "algo_name": "test_algo",
            "algo_version": "1.0.0",
            "metric_name": "accuracy",
            "value": 0.95,
            "analysis_mask": 1,
        }

        aggregator.add_metric(payload)

        assert aggregator._window_start is not None
        assert aggregator._window_end is not None

    def test_add_metric_accumulates_values(self) -> None:
        mock_storage = MagicMock()
        mock_producer = MagicMock()

        aggregator = TumblingWindowAggregator(
            window_size_sec=60,
            storage=mock_storage,
            producer=mock_producer,
        )

        for i in range(5):
            payload = {
                "algo_name": "test_algo",
                "algo_version": "1.0.0",
                "metric_name": "accuracy",
                "value": 0.9 + i * 0.01,
                "analysis_mask": 1,
            }
            aggregator.add_metric(payload)

        key = ("test_algo", "1.0.0", "accuracy")
        assert key in aggregator._current_window
        assert aggregator._current_window[key].count == 5

    def test_add_metric_skips_none_values(self) -> None:
        mock_storage = MagicMock()
        mock_producer = MagicMock()

        aggregator = TumblingWindowAggregator(
            window_size_sec=60,
            storage=mock_storage,
            producer=mock_producer,
        )

        payload = {
            "algo_name": "test_algo",
            "algo_version": "1.0.0",
            "metric_name": "accuracy",
            "value": None,
            "analysis_mask": 1,
        }

        aggregator.add_metric(payload)

        assert len(aggregator._current_window) == 0

    def test_add_metric_handles_boolean_values(self) -> None:
        mock_storage = MagicMock()
        mock_producer = MagicMock()

        aggregator = TumblingWindowAggregator(
            window_size_sec=60,
            storage=mock_storage,
            producer=mock_producer,
        )

        aggregator.add_metric(
            {
                "algo_name": "test_algo",
                "algo_version": "1.0.0",
                "metric_name": "passed",
                "value": True,
                "analysis_mask": 8,  # COUNTER
            },
        )
        aggregator.add_metric(
            {
                "algo_name": "test_algo",
                "algo_version": "1.0.0",
                "metric_name": "passed",
                "value": False,
                "analysis_mask": 8,
            },
        )

        key = ("test_algo", "1.0.0", "passed")
        assert aggregator._current_window[key].values == [1, 0]

    def test_add_metric_handles_dict_values(self) -> None:
        mock_storage = MagicMock()
        mock_producer = MagicMock()

        aggregator = TumblingWindowAggregator(
            window_size_sec=60,
            storage=mock_storage,
            producer=mock_producer,
        )

        aggregator.add_metric(
            {
                "algo_name": "test_algo",
                "algo_version": "1.0.0",
                "metric_name": "position",
                "value": {"x": 1.0, "y": 2.0},
                "analysis_mask": 32,  # ELLIPSE_2D
            },
        )

        key = ("test_algo", "1.0.0", "position")
        assert aggregator._current_window[key].values == [{"x": 1.0, "y": 2.0}]

    def test_flush_stores_aggregates(self) -> None:
        mock_storage = MagicMock()
        mock_storage.store.return_value = MagicMock(full_path="aggregates/ab/cd/hash123")
        mock_producer = MagicMock()

        aggregator = TumblingWindowAggregator(
            window_size_sec=60,
            storage=mock_storage,
            producer=mock_producer,
        )

        # Add metrics
        for i in range(3):
            aggregator.add_metric(
                {
                    "algo_name": "test_algo",
                    "algo_version": "1.0.0",
                    "metric_name": "accuracy",
                    "value": 0.9 + i * 0.01,
                    "analysis_mask": 1,
                },
            )

        # Flush
        aggregator.flush()

        # Verify storage was called
        mock_storage.store.assert_called_once()
        mock_producer.publish_aggregate_produced.assert_called_once()
        assert aggregator.aggregates_produced == 1

    def test_flush_empty_window_does_nothing(self) -> None:
        mock_storage = MagicMock()
        mock_producer = MagicMock()

        aggregator = TumblingWindowAggregator(
            window_size_sec=60,
            storage=mock_storage,
            producer=mock_producer,
        )

        aggregator.flush()

        mock_storage.store.assert_not_called()
        mock_producer.publish_aggregate_produced.assert_not_called()

    def test_flush_skips_empty_aggregates(self) -> None:
        mock_storage = MagicMock()
        mock_storage.store.return_value = MagicMock(full_path="aggregates/ab/cd/hash123")
        mock_producer = MagicMock()

        aggregator = TumblingWindowAggregator(
            window_size_sec=60,
            storage=mock_storage,
            producer=mock_producer,
        )

        # Initialize window but don't add values
        aggregator.add_metric(
            {
                "algo_name": "test_algo",
                "algo_version": "1.0.0",
                "metric_name": "accuracy",
                "value": 0.95,
                "analysis_mask": 1,
            },
        )

        # Manually create an empty aggregate
        key = ("empty_algo", "1.0.0", "empty_metric")
        aggregator._current_window[key] = MetricAggregate(
            algo_name="empty_algo",
            algo_version="1.0.0",
            metric_name="empty_metric",
            analysis_mask=1,
            values=[],  # Empty
        )

        aggregator.flush()

        # Should only store one (non-empty) aggregate
        assert mock_storage.store.call_count == 1

    def test_window_alignment(self) -> None:
        mock_storage = MagicMock()
        mock_producer = MagicMock()

        aggregator = TumblingWindowAggregator(
            window_size_sec=60,
            storage=mock_storage,
            producer=mock_producer,
        )

        # Initialize window
        aggregator.add_metric(
            {
                "algo_name": "test_algo",
                "algo_version": "1.0.0",
                "metric_name": "accuracy",
                "value": 0.95,
                "analysis_mask": 1,
            },
        )

        # Window should be aligned to 60-second boundary
        if aggregator._window_start is not None:
            assert aggregator._window_start.second == 0

    def test_multiple_metrics_same_window(self) -> None:
        mock_storage = MagicMock()
        mock_storage.store.return_value = MagicMock(full_path="aggregates/ab/cd/hash123")
        mock_producer = MagicMock()

        aggregator = TumblingWindowAggregator(
            window_size_sec=60,
            storage=mock_storage,
            producer=mock_producer,
        )

        # Add different metrics
        aggregator.add_metric(
            {
                "algo_name": "algo1",
                "algo_version": "1.0.0",
                "metric_name": "accuracy",
                "value": 0.95,
                "analysis_mask": 1,
            },
        )
        aggregator.add_metric(
            {
                "algo_name": "algo2",
                "algo_version": "2.0.0",
                "metric_name": "precision",
                "value": 0.90,
                "analysis_mask": 1,
            },
        )

        assert len(aggregator._current_window) == 2
        assert ("algo1", "1.0.0", "accuracy") in aggregator._current_window
        assert ("algo2", "2.0.0", "precision") in aggregator._current_window

    def test_thread_safety(self) -> None:
        mock_storage = MagicMock()
        mock_storage.store.return_value = MagicMock(full_path="aggregates/ab/cd/hash123")
        mock_producer = MagicMock()

        aggregator = TumblingWindowAggregator(
            window_size_sec=60,
            storage=mock_storage,
            producer=mock_producer,
        )

        errors: list[Exception] = []

        def add_metrics() -> None:
            try:
                for i in range(10):
                    aggregator.add_metric(
                        {
                            "algo_name": "test_algo",
                            "algo_version": "1.0.0",
                            "metric_name": "accuracy",
                            "value": 0.9 + i * 0.001,
                            "analysis_mask": 1,
                        },
                    )
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=add_metrics) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        key = ("test_algo", "1.0.0", "accuracy")
        assert aggregator._current_window[key].count == 50

    def test_storage_error_handling(self) -> None:
        mock_storage = MagicMock()
        mock_storage.store.side_effect = Exception("Storage error")
        mock_producer = MagicMock()

        aggregator = TumblingWindowAggregator(
            window_size_sec=60,
            storage=mock_storage,
            producer=mock_producer,
        )

        aggregator.add_metric(
            {
                "algo_name": "test_algo",
                "algo_version": "1.0.0",
                "metric_name": "accuracy",
                "value": 0.95,
                "analysis_mask": 1,
            },
        )

        # Should not raise, but should log error
        aggregator.flush()

        # aggregate_produced should not increment on error
        assert aggregator.aggregates_produced == 0
