"""Unit tests for Flask API server."""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.domain.evaluation import mask_to_kind_names
from src.visualization.api_server import (
    ALLOWED_BUCKETS,
    MAX_TIME_RANGE_MINUTES,
    app,
)


@pytest.fixture
def client():
    """Create a test client for the Flask app."""
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


class TestMaskToKindNames:
    def test_empty_mask(self) -> None:
        result = mask_to_kind_names(0)
        assert result == ()

    def test_single_kind(self) -> None:
        result = mask_to_kind_names(1)
        assert "SUMMARY" in result

    def test_multiple_kinds(self) -> None:
        result = mask_to_kind_names(3)
        assert "SUMMARY" in result
        assert "DISTRIBUTION_1D" in result

    def test_all_common_kinds(self) -> None:
        result = mask_to_kind_names(15)
        assert len(result) >= 4


class TestHealthEndpoint:
    def test_health_check(self, client) -> None:
        response = client.get("/api/health")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "status" in data
        assert data["status"] == "ok"


class TestSecurityHeaders:
    def test_security_headers_present(self, client) -> None:
        response = client.get("/api/health")

        assert "X-Content-Type-Options" in response.headers
        assert response.headers["X-Content-Type-Options"] == "nosniff"
        assert "X-Frame-Options" in response.headers
        assert response.headers["X-Frame-Options"] == "DENY"
        assert "X-XSS-Protection" in response.headers

    def test_cache_headers_on_api(self, client) -> None:
        response = client.get("/api/health")

        assert "Cache-Control" in response.headers
        assert "no-cache" in response.headers["Cache-Control"]

    def test_csp_header(self, client) -> None:
        response = client.get("/api/health")

        assert "Content-Security-Policy" in response.headers
        csp = response.headers["Content-Security-Policy"]
        assert "default-src 'self'" in csp


class TestDashboardRoutes:
    def test_serve_dashboard(self, client) -> None:
        response = client.get("/")

        # Should return HTML (or 404 if file doesn't exist in test env)
        assert response.status_code in [200, 404]


class TestPrometheusEndpoints:
    @patch("src.visualization.api_server.prometheus_service")
    def test_prometheus_query_missing_param(self, mock_prom, client) -> None:
        response = client.get("/api/prometheus/query")

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data

    @patch("src.visualization.api_server.prometheus_service")
    def test_prometheus_query_success(self, mock_prom, client) -> None:
        mock_result = MagicMock()
        mock_result.data = {"status": "success", "data": {"result": []}}
        mock_prom.query.return_value = mock_result

        response = client.get("/api/prometheus/query?query=up")

        assert response.status_code == 200

    @patch("src.visualization.api_server.prometheus_service", None)
    def test_prometheus_query_service_not_initialized(self, client) -> None:
        response = client.get("/api/prometheus/query?query=up")

        assert response.status_code == 503

    @patch("src.visualization.api_server.prometheus_service")
    def test_prometheus_query_range_missing_params(self, mock_prom, client) -> None:
        response = client.get("/api/prometheus/query_range?query=up")

        assert response.status_code == 400

    @patch("src.visualization.api_server.prometheus_service")
    def test_prometheus_query_range_success(self, mock_prom, client) -> None:
        mock_result = MagicMock()
        mock_result.data = {"status": "success", "data": {"result": []}}
        mock_prom.query_range.return_value = mock_result

        response = client.get(
            "/api/prometheus/query_range?query=up&start=1000&end=2000&step=60",
        )

        assert response.status_code == 200

    @patch("src.visualization.api_server.prometheus_service")
    def test_prometheus_healthy(self, mock_prom, client) -> None:
        mock_prom.is_healthy.return_value = True

        response = client.get("/api/prometheus/healthy")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["healthy"] is True


class TestJaegerEndpoints:
    @patch("src.visualization.api_server.jaeger_service", None)
    def test_jaeger_services_not_initialized(self, client) -> None:
        response = client.get("/api/jaeger/services")

        assert response.status_code == 503

    @patch("src.visualization.api_server.jaeger_service")
    def test_jaeger_services_success(self, mock_jaeger, client) -> None:
        mock_jaeger.get_services.return_value = {"healthy": True, "services": []}

        response = client.get("/api/jaeger/services")

        assert response.status_code == 200


class TestPipelineStatusEndpoint:
    @patch("src.visualization.api_server.prometheus_service", None)
    def test_pipeline_status_not_initialized(self, client) -> None:
        response = client.get("/api/pipeline/status")

        assert response.status_code == 503

    @patch("src.visualization.api_server.prometheus_service")
    def test_pipeline_status_success(self, mock_prom, client) -> None:
        mock_status = MagicMock()
        mock_status.pipeline_running = True
        mock_status.workers_active = 4
        mock_status.worker_pool_size = 8
        mock_status.status = "running"
        mock_status.detail = "All workers active"
        mock_prom.get_pipeline_status.return_value = mock_status

        response = client.get("/api/pipeline/status")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["pipeline_running"] is True
        assert data["workers_active"] == 4


class TestDirectoriesEndpoint:
    @patch("src.visualization.api_server.discovery", None)
    def test_directories_not_initialized(self, client) -> None:
        response = client.get("/api/directories")

        assert response.status_code == 503

    @patch("src.visualization.api_server.discovery")
    def test_directories_success(self, mock_discovery, client) -> None:
        mock_dir = MagicMock()
        mock_dir.key = "path0"
        mock_dir.path = "/data/images"
        mock_discovery.list_directories.return_value = [mock_dir]

        response = client.get("/api/directories")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "directories" in data


class TestAlgorithmsEndpoint:
    @patch("src.visualization.api_server.discovery", None)
    def test_algorithms_not_initialized(self, client) -> None:
        response = client.get("/api/algorithms")

        assert response.status_code == 503

    @patch("src.visualization.api_server.discovery")
    def test_algorithms_success(self, mock_discovery, client) -> None:
        mock_algo = MagicMock()
        mock_algo.name = "analysis_probe"
        mock_algo.version = "1.0.0"
        mock_algo.settings_path = "/config/algo.toml"
        mock_discovery.list_algorithms.return_value = [mock_algo]

        response = client.get("/api/algorithms")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "algorithms" in data


class TestRoutesEndpoint:
    @patch("src.visualization.api_server.discovery", None)
    def test_routes_not_initialized(self, client) -> None:
        response = client.get("/api/routes")

        assert response.status_code == 503


class TestMetricsEndpoint:
    @patch("src.visualization.api_server.discovery", None)
    def test_metrics_not_initialized(self, client) -> None:
        response = client.get("/api/metrics")

        assert response.status_code == 503

    @patch("src.visualization.api_server.discovery")
    def test_metrics_success(self, mock_discovery, client) -> None:
        mock_metric = MagicMock()
        mock_metric.metric_name = "accuracy"
        mock_metric.algo_name = "test_algo"
        mock_metric.algo_version = "1.0.0"
        mock_metric.analysis_mask = 1
        mock_metric.analysis_kinds = ["SUMMARY"]
        mock_metric.summary = {"count": 10, "mean": 0.95}
        mock_metric.artifact_key = "aggregates/ab/cd/hash123"
        mock_discovery.discover_metrics_from_minio.return_value = [mock_metric]

        response = client.get("/api/metrics")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "metrics" in data
        assert data["count"] == 1

    @patch("src.visualization.api_server.discovery")
    def test_metrics_with_time_range_filter(self, mock_discovery, client) -> None:
        mock_discovery.discover_metrics_from_minio.return_value = []

        response = client.get("/api/metrics?time_range=60")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["time_range_minutes"] == 60

    @patch("src.visualization.api_server.discovery")
    def test_metrics_with_invalid_time_range(self, mock_discovery, client) -> None:
        mock_discovery.discover_metrics_from_minio.return_value = []

        response = client.get("/api/metrics?time_range=-1")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["time_range_minutes"] is None

    @patch("src.visualization.api_server.discovery")
    def test_metrics_with_excessive_time_range(self, mock_discovery, client) -> None:
        mock_discovery.discover_metrics_from_minio.return_value = []

        response = client.get(f"/api/metrics?time_range={MAX_TIME_RANGE_MINUTES + 100}")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["time_range_minutes"] is None


class TestMetricDataEndpoint:
    @patch("src.visualization.api_server.discovery", None)
    def test_metric_data_not_initialized(self, client) -> None:
        response = client.get("/api/metrics/accuracy/data")

        assert response.status_code == 503

    @patch("src.visualization.api_server.artifact_computer")
    @patch("src.visualization.api_server.discovery")
    def test_metric_data_not_found(self, mock_discovery, mock_computer, client) -> None:
        mock_discovery.discover_metrics_from_minio.return_value = []

        response = client.get("/api/metrics/nonexistent/data")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data.get("no_data") is True


class TestBucketsEndpoint:
    @patch("src.visualization.api_server.storage", None)
    def test_buckets_not_initialized(self, client) -> None:
        response = client.get("/api/buckets")

        assert response.status_code == 503

    @patch("src.visualization.api_server.storage")
    def test_buckets_success(self, mock_storage, client) -> None:
        mock_storage.list_objects.return_value = [{"key": "test.json", "size": 100}]

        response = client.get("/api/buckets")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "buckets" in data


class TestArtifactsEndpoint:
    @patch("src.visualization.api_server.storage", None)
    def test_list_artifacts_not_initialized(self, client) -> None:
        response = client.get("/api/artifacts")

        assert response.status_code == 503

    @patch("src.visualization.api_server.storage")
    def test_list_artifacts_invalid_bucket(self, mock_storage, client) -> None:
        response = client.get("/api/artifacts?bucket=invalid_bucket")

        assert response.status_code == 400

    @patch("src.visualization.api_server.storage")
    def test_list_artifacts_path_traversal(self, mock_storage, client) -> None:
        response = client.get("/api/artifacts?prefix=../../../etc")

        assert response.status_code == 400

    @patch("src.visualization.api_server.storage")
    def test_list_artifacts_success(self, mock_storage, client) -> None:
        mock_storage.list_objects.return_value = [{"key": "test.json"}]

        response = client.get("/api/artifacts?bucket=artifacts")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "objects" in data


class TestGetArtifactEndpoint:
    @patch("src.visualization.api_server.storage", None)
    def test_get_artifact_not_initialized(self, client) -> None:
        response = client.get("/api/artifact/artifacts/ab/cd/hash123")

        assert response.status_code == 503

    @patch("src.visualization.api_server.storage")
    def test_get_artifact_invalid_bucket(self, mock_storage, client) -> None:
        response = client.get("/api/artifact/invalid_bucket/key123")

        assert response.status_code == 400

    @patch("src.visualization.api_server.storage")
    def test_get_artifact_path_traversal(self, mock_storage, client) -> None:
        response = client.get("/api/artifact/artifacts/../../../etc/passwd")

        assert response.status_code == 400

    @patch("src.visualization.api_server.storage")
    def test_get_artifact_leading_slash(self, mock_storage, client) -> None:
        # Flask normalizes double slashes with a redirect, so test path traversal in key
        response = client.get("/api/artifact/artifacts/%2F..%2F..%2Fetc%2Fpasswd")

        # Should be 400 or 404, but definitely not 200 with content
        assert response.status_code in [400, 404, 308]

    @patch("src.visualization.api_server.storage")
    def test_get_artifact_not_found(self, mock_storage, client) -> None:
        mock_storage.get_object_info.return_value = None
        mock_storage.retrieve_by_key.return_value = None

        response = client.get("/api/artifact/artifacts/ab/cd/hash123")

        assert response.status_code == 404

    @patch("src.visualization.api_server.storage")
    def test_get_artifact_json_content(self, mock_storage, client) -> None:
        mock_storage.get_object_info.return_value = {"content_type": "application/json", "metadata": {}}
        mock_storage.retrieve_by_key.return_value = b'{"key": "value"}'

        response = client.get("/api/artifact/artifacts/ab/cd/hash123")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["content"]["data"] == {"key": "value"}

    @patch("src.visualization.api_server.storage")
    def test_get_artifact_binary_content(self, mock_storage, client) -> None:
        mock_storage.get_object_info.return_value = {"content_type": "application/octet-stream", "metadata": {}}
        mock_storage.retrieve_by_key.return_value = b"\x00\x01\x02\x03"

        response = client.get("/api/artifact/artifacts/ab/cd/hash123")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["content"]["type"] == "binary"


class TestCacheClearEndpoint:
    @patch("src.visualization.api_server.discovery", None)
    def test_cache_clear_not_initialized(self, client) -> None:
        response = client.post("/api/cache/clear")

        assert response.status_code == 503

    @patch("src.visualization.api_server.discovery")
    def test_cache_clear_success(self, mock_discovery, client) -> None:
        response = client.post("/api/cache/clear")

        assert response.status_code == 200
        mock_discovery.clear_cache.assert_called_once()


class TestAuditEndpoints:
    @patch("src.visualization.api_server.storage", None)
    def test_audit_verify_not_initialized(self, client) -> None:
        response = client.get("/api/audit/verify")

        assert response.status_code == 503

    @patch("src.visualization.api_server.prometheus_service")
    @patch("src.visualization.api_server.storage")
    def test_audit_verify_no_events(self, mock_storage, mock_prom, client) -> None:
        mock_storage.list_objects.return_value = []
        mock_prom.query.return_value = MagicMock(data={"status": "success", "data": {"result": []}})

        response = client.get("/api/audit/verify")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "status" in data

    @patch("src.visualization.api_server.storage", None)
    def test_audit_stats_not_initialized(self, client) -> None:
        response = client.get("/api/audit/stats")

        assert response.status_code == 503

    @patch("src.visualization.api_server.storage")
    def test_audit_stats_success(self, mock_storage, client) -> None:
        mock_storage.list_objects.return_value = []

        response = client.get("/api/audit/stats")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "total_events" in data


class TestKafkaHealthEndpoint:
    @patch("src.visualization.api_server.AdminClient")
    def test_kafka_health_success(self, mock_admin_class, client) -> None:
        mock_admin = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.topics = {"visio.jobs": MagicMock(partitions={})}
        mock_metadata.brokers = {"1": MagicMock()}
        mock_admin.list_topics.return_value = mock_metadata
        mock_admin_class.return_value = mock_admin

        with patch("src.visualization.api_server.prometheus_service") as mock_prom:
            mock_prom.query.return_value = MagicMock(data={"status": "success", "data": {"result": []}})
            response = client.get("/api/kafka/health")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["healthy"] is True


class TestKafkaAuditLogCountEndpoint:
    @patch("src.visualization.api_server.Consumer")
    def test_audit_log_count_topic_not_found(self, mock_consumer_class, client) -> None:
        mock_consumer = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.topics = {}  # No audit-log topic
        mock_consumer.list_topics.return_value = mock_metadata
        mock_consumer_class.return_value = mock_consumer

        response = client.get("/api/kafka/audit-log/count")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["exists"] is False


class TestAllowedBuckets:
    def test_allowed_buckets_contains_expected(self) -> None:
        assert "artifacts" in ALLOWED_BUCKETS
        assert "inputs" in ALLOWED_BUCKETS
        assert "aggregates" in ALLOWED_BUCKETS
        assert "metric-values" in ALLOWED_BUCKETS

    def test_allowed_buckets_excludes_internal(self) -> None:
        # Should not include internal buckets
        assert "private" not in ALLOWED_BUCKETS
        assert "system" not in ALLOWED_BUCKETS
