"""Unit tests for the metrics route blueprint."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from flask import Flask

from src.visualization.routes.metrics import (
    _parse_time_range,
    init_blueprint,
    metrics_bp,
)


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config["TESTING"] = True
    if "metrics" not in app.blueprints:
        app.register_blueprint(metrics_bp)
    yield app


@pytest.fixture
def client(app):
    with app.test_client() as c:
        yield c


@pytest.fixture(autouse=True)
def _reset_blueprint_state():
    init_blueprint(None, None, None)
    yield
    init_blueprint(None, None, None)


class TestHealth:
    def test_health_returns_status(self, client) -> None:
        resp = client.get("/api/health")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["status"] == "ok"
        assert body["minio"] is False
        assert body["discovery"] is False

    def test_health_reflects_initialization(self, client) -> None:
        init_blueprint(MagicMock(), MagicMock(), MagicMock())
        body = client.get("/api/health").get_json()
        assert body["minio"] is True
        assert body["discovery"] is True


class TestDirectories:
    def test_503_when_discovery_none(self, client) -> None:
        assert client.get("/api/directories").status_code == 503

    def test_success(self, client) -> None:
        d = MagicMock()
        d.key = "k"
        d.path = "/p"
        discovery = MagicMock()
        discovery.list_directories.return_value = [d]
        init_blueprint(None, discovery, None)

        body = client.get("/api/directories").get_json()
        assert body["directories"][0]["key"] == "k"
        assert body["directories"][0]["path"] == "/p"

    def test_handles_exception(self, client) -> None:
        discovery = MagicMock()
        discovery.list_directories.side_effect = RuntimeError("boom")
        init_blueprint(None, discovery, None)
        assert client.get("/api/directories").status_code == 500


class TestAlgorithms:
    def test_503_when_none(self, client) -> None:
        assert client.get("/api/algorithms").status_code == 503

    def test_success(self, client) -> None:
        a = MagicMock()
        a.name = "algoA"
        a.version = "1.0.0"
        a.settings_path = "s.toml"
        discovery = MagicMock()
        discovery.list_algorithms.return_value = [a]
        init_blueprint(None, discovery, None)

        body = client.get("/api/algorithms").get_json()
        assert body["algorithms"] == [
            {"name": "algoA", "version": "1.0.0", "settings_path": "s.toml"},
        ]

    def test_handles_exception(self, client) -> None:
        discovery = MagicMock()
        discovery.list_algorithms.side_effect = RuntimeError("boom")
        init_blueprint(None, discovery, None)
        assert client.get("/api/algorithms").status_code == 500


class TestRoutes:
    def test_503_when_none(self, client) -> None:
        assert client.get("/api/routes").status_code == 503

    def test_success(self, client) -> None:
        d = MagicMock()
        d.key = "path0"
        d.path = "/data"
        algo = MagicMock()
        algo.name = "algoA"
        algo.version = "1.0"
        discovery = MagicMock()
        discovery.list_directories.return_value = [d]
        discovery.get_algorithm_for_directory.return_value = algo
        init_blueprint(None, discovery, None)

        body = client.get("/api/routes").get_json()
        assert len(body["routes"]) == 1
        assert body["routes"][0]["directory"] == "path0"
        assert body["routes"][0]["algorithm"] == "algoA"

    def test_skips_directories_without_algorithm(self, client) -> None:
        d = MagicMock()
        d.key = "x"
        d.path = "/p"
        discovery = MagicMock()
        discovery.list_directories.return_value = [d]
        discovery.get_algorithm_for_directory.return_value = None
        init_blueprint(None, discovery, None)

        body = client.get("/api/routes").get_json()
        assert body["routes"] == []


class TestGetMetrics:
    def test_503_when_none(self, client) -> None:
        assert client.get("/api/metrics").status_code == 503

    def test_returns_metrics_list(self, client) -> None:
        metric = MagicMock()
        metric.metric_name = "accuracy"
        metric.processor_name = "proc"
        metric.processor_version = "1"
        metric.aggregation_mask = 1
        metric.aggregation_types = ["STATS"]
        metric.summary = {"count": 10, "mean": 0.5, "min": 0, "max": 1, "sum": 5, "std": 0.1}
        metric.artifact_key = "k"
        discovery = MagicMock()
        discovery.discover_metrics_from_minio.return_value = [metric]
        init_blueprint(None, discovery, None)

        body = client.get("/api/metrics").get_json()
        assert body["count"] == 1
        assert body["metrics"][0]["metric_name"] == "accuracy"
        # avg falls back to mean
        assert body["metrics"][0]["avg"] == 0.5

    def test_invalid_algorithm_name(self, client) -> None:
        init_blueprint(None, MagicMock(), None)
        resp = client.get("/api/metrics?algorithm=1bad")
        assert resp.status_code == 400

    def test_invalid_version(self, client) -> None:
        init_blueprint(None, MagicMock(), None)
        resp = client.get("/api/metrics?version=.bad.")
        assert resp.status_code == 400

    def test_time_range_passed_through(self, client) -> None:
        discovery = MagicMock()
        discovery.discover_metrics_from_minio.return_value = []
        init_blueprint(None, discovery, None)

        body = client.get("/api/metrics?time_range=30").get_json()
        assert body["time_range_minutes"] == 30
        kwargs = discovery.discover_metrics_from_minio.call_args.kwargs
        assert kwargs["time_range_minutes"] == 30

    def test_handles_exception(self, client) -> None:
        discovery = MagicMock()
        discovery.discover_metrics_from_minio.side_effect = RuntimeError("boom")
        init_blueprint(None, discovery, None)
        assert client.get("/api/metrics").status_code == 500


class TestGetMetricData:
    def test_invalid_name_returns_400(self, client) -> None:
        # No init needed - validation happens first
        resp = client.get("/api/metrics/1bad/data")
        assert resp.status_code == 400

    def test_503_when_discovery_none(self, client) -> None:
        resp = client.get("/api/metrics/accuracy/data")
        assert resp.status_code == 503

    def test_no_match_returns_no_data_response(self, client) -> None:
        discovery = MagicMock()
        discovery.discover_metrics_from_minio.return_value = []
        init_blueprint(None, discovery, None)

        body = client.get("/api/metrics/accuracy/data").get_json()
        assert body["no_data"] is True
        assert body["metric_name"] == "accuracy"

    def test_returns_full_metric_data(self, client) -> None:
        metric = MagicMock()
        metric.metric_name = "accuracy"
        metric.processor_name = "p"
        metric.processor_version = "1"
        metric.aggregation_mask = 0
        metric.aggregation_types = []
        metric.summary = {"count": 1, "sum": 1, "mean": 1.0, "min": 1, "max": 1}
        metric.artifact_key = "k"
        discovery = MagicMock()
        discovery.discover_metrics_from_minio.return_value = [metric]
        discovery.get_metric_artifact.return_value = {"histogram": {"counts": [1]}}
        discovery.get_metric_values_with_meta.return_value = {
            "values": [],
            "aggregation_mask": 0,
            "meta": None,
        }
        init_blueprint(None, discovery, None)

        body = client.get("/api/metrics/accuracy/data").get_json()
        assert body["metric_name"] == "accuracy"
        assert body["artifacts"]["histogram"] == {"counts": [1]}

    def test_invalid_algorithm_filter(self, client) -> None:
        init_blueprint(None, MagicMock(), None)
        resp = client.get("/api/metrics/accuracy/data?algorithm=1bad")
        assert resp.status_code == 400


class TestBuckets:
    def test_503_when_storage_none(self, client) -> None:
        assert client.get("/api/buckets").status_code == 503

    def test_returns_bucket_stats(self, client) -> None:
        storage = MagicMock()
        storage.list_objects.return_value = [{"size": 100}, {"size": 200}]
        init_blueprint(storage, None, None)

        body = client.get("/api/buckets").get_json()
        assert isinstance(body["buckets"], list)
        # 4 default buckets
        assert len(body["buckets"]) == 4
        first = body["buckets"][0]
        assert first["object_count"] == 2
        assert first["total_size_bytes"] == 300

    def test_failed_bucket_listing_falls_back(self, client) -> None:
        storage = MagicMock()
        storage.list_objects.side_effect = RuntimeError("err")
        init_blueprint(storage, None, None)

        body = client.get("/api/buckets").get_json()
        # All buckets should have error key and zero counts
        for b in body["buckets"]:
            assert b["object_count"] == 0
            assert b["total_size_bytes"] == 0
            assert "error" in b


class TestClearCache:
    def test_503_when_discovery_none(self, client) -> None:
        assert client.post("/api/cache/clear").status_code == 503

    def test_success_clears_cache(self, client) -> None:
        discovery = MagicMock()
        init_blueprint(None, discovery, None)
        resp = client.post("/api/cache/clear")
        assert resp.status_code == 200
        discovery.clear_cache.assert_called_once()

    def test_handles_exception(self, client) -> None:
        discovery = MagicMock()
        discovery.clear_cache.side_effect = RuntimeError("boom")
        init_blueprint(None, discovery, None)
        assert client.post("/api/cache/clear").status_code == 500


class TestListArtifacts:
    def test_503_when_storage_none(self, client) -> None:
        assert client.get("/api/artifacts").status_code == 503

    def test_invalid_bucket_400(self, client) -> None:
        init_blueprint(MagicMock(), None, None)
        assert client.get("/api/artifacts?bucket=secret").status_code == 400

    def test_path_traversal_400(self, client) -> None:
        init_blueprint(MagicMock(), None, None)
        assert client.get("/api/artifacts?prefix=../../etc").status_code == 400

    def test_success(self, client) -> None:
        storage = MagicMock()
        storage.list_objects.return_value = [{"key": "x"}]
        init_blueprint(storage, None, None)

        body = client.get("/api/artifacts?bucket=artifacts").get_json()
        assert body["count"] == 1
        assert body["bucket"] == "artifacts"

    def test_invalid_limit_falls_back_to_default(self, client) -> None:
        storage = MagicMock()
        storage.list_objects.return_value = []
        init_blueprint(storage, None, None)
        client.get("/api/artifacts?bucket=artifacts&limit=invalid")
        # Pull limit kwarg from the call
        _, kwargs = storage.list_objects.call_args
        assert kwargs["limit"] == 100  # DEFAULT_ARTIFACT_LIMIT

    def test_clamps_limit_to_max(self, client) -> None:
        storage = MagicMock()
        storage.list_objects.return_value = []
        init_blueprint(storage, None, None)
        client.get("/api/artifacts?bucket=artifacts&limit=99999")
        _, kwargs = storage.list_objects.call_args
        assert kwargs["limit"] == 1000  # MAX_ARTIFACT_LIMIT


class TestParseTimeRange:
    def test_none_returns_none(self) -> None:
        assert _parse_time_range(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert _parse_time_range("") is None

    def test_valid_value(self) -> None:
        assert _parse_time_range("30") == 30

    def test_zero_returns_none(self) -> None:
        assert _parse_time_range("0") is None

    def test_negative_returns_none(self) -> None:
        assert _parse_time_range("-5") is None

    def test_excessive_returns_none(self) -> None:
        # Just over max
        assert _parse_time_range("99999999") is None

    def test_non_numeric_returns_none(self) -> None:
        assert _parse_time_range("abc") is None
