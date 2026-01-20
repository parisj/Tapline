"""Unit tests for PrometheusService.

Tests for:
- Health check
- Query execution
- Range queries
- Pipeline status
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.visualization.services.prometheus_service import (
    JaegerService,
    PipelineStatus,
    PrometheusQueryResult,
    PrometheusService,
)


class TestPrometheusQueryResult:
    """Tests for PrometheusQueryResult dataclass."""

    def test_success_result(self) -> None:
        result = PrometheusQueryResult(
            success=True,
            data={"status": "success", "data": {"result": []}},
        )

        assert result.success is True
        assert result.error is None

    def test_error_result(self) -> None:
        result = PrometheusQueryResult(
            success=False,
            data={"status": "error"},
            error="Connection failed",
        )

        assert result.success is False
        assert result.error == "Connection failed"


class TestPipelineStatus:
    """Tests for PipelineStatus dataclass."""

    def test_healthy_status(self) -> None:
        status = PipelineStatus(
            pipeline_running=True,
            workers_active=3,
            worker_pool_size=8,
            status="healthy",
            detail="3/8 processing",
        )

        assert status.pipeline_running is True
        assert status.workers_active == 3
        assert status.status == "healthy"

    def test_stopped_status(self) -> None:
        status = PipelineStatus(
            pipeline_running=False,
            workers_active=0,
            worker_pool_size=0,
            status="stopped",
            detail="Pipeline not running",
        )

        assert status.pipeline_running is False
        assert status.status == "stopped"


class TestPrometheusService:
    """Tests for PrometheusService."""

    def test_init_sets_base_url(self) -> None:
        service = PrometheusService("http://prometheus:9090")

        assert service.base_url == "http://prometheus:9090"
        assert service.timeout == 10

    def test_init_strips_trailing_slash(self) -> None:
        service = PrometheusService("http://prometheus:9090/")

        assert service.base_url == "http://prometheus:9090"

    def test_init_with_custom_timeout(self) -> None:
        service = PrometheusService("http://localhost:9091", timeout=30)

        assert service.timeout == 30

    @patch("src.visualization.services.prometheus_service.requests")
    def test_is_healthy_returns_true_when_available(self, mock_requests: MagicMock) -> None:
        mock_requests.get.return_value.ok = True

        service = PrometheusService()
        result = service.is_healthy()

        assert result is True
        mock_requests.get.assert_called_once()

    @patch("src.visualization.services.prometheus_service.requests")
    def test_is_healthy_returns_false_on_error(self, mock_requests: MagicMock) -> None:
        mock_requests.get.side_effect = Exception("Connection refused")
        mock_requests.RequestException = Exception

        service = PrometheusService()
        result = service.is_healthy()

        assert result is False

    @patch("src.visualization.services.prometheus_service.requests")
    def test_query_returns_success_result(self, mock_requests: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "success",
            "data": {"result": [{"value": [1234567890, "42"]}]},
        }
        mock_requests.get.return_value = mock_response

        service = PrometheusService()
        result = service.query("up")

        assert result.success is True
        assert result.data["status"] == "success"

    @patch("src.visualization.services.prometheus_service.requests")
    def test_query_returns_error_result_on_exception(self, mock_requests: MagicMock) -> None:
        mock_requests.get.side_effect = Exception("Network error")
        mock_requests.RequestException = Exception

        service = PrometheusService()
        result = service.query("up")

        assert result.success is False
        assert result.error is not None

    @patch("src.visualization.services.prometheus_service.requests")
    def test_query_range_returns_success(self, mock_requests: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "success",
            "data": {"result": []},
        }
        mock_requests.get.return_value = mock_response

        service = PrometheusService()
        result = service.query_range("up", "1234567890", "1234567900", "60")

        assert result.success is True
        mock_requests.get.assert_called_once()

    @patch("src.visualization.services.prometheus_service.requests")
    def test_get_metric_value_returns_value(self, mock_requests: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "success",
            "data": {"result": [{"value": [1234567890, "42.5"]}]},
        }
        mock_requests.get.return_value = mock_response

        service = PrometheusService()
        result = service.get_metric_value("my_metric")

        assert result == 42.5

    @patch("src.visualization.services.prometheus_service.requests")
    def test_get_metric_value_returns_default_on_error(self, mock_requests: MagicMock) -> None:
        mock_requests.get.side_effect = Exception("Error")
        mock_requests.RequestException = Exception

        service = PrometheusService()
        result = service.get_metric_value("my_metric", default=0.0)

        assert result == 0.0

    @patch("src.visualization.services.prometheus_service.requests")
    def test_get_metric_value_returns_default_when_empty(self, mock_requests: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "success",
            "data": {"result": []},
        }
        mock_requests.get.return_value = mock_response

        service = PrometheusService()
        result = service.get_metric_value("my_metric", default=99.0)

        assert result == 99.0

    @patch.object(PrometheusService, "get_metric_value")
    def test_get_pipeline_status_healthy_with_active_workers(
        self,
        mock_get_metric: MagicMock,
    ) -> None:
        mock_get_metric.side_effect = [1.0, 3.0, 8.0]  # pipeline_up, workers_active, pool_size

        service = PrometheusService()
        status = service.get_pipeline_status()

        assert status.pipeline_running is True
        assert status.workers_active == 3
        assert status.worker_pool_size == 8
        assert status.status == "healthy"
        assert "3/8 processing" in status.detail

    @patch.object(PrometheusService, "get_metric_value")
    def test_get_pipeline_status_healthy_idle(self, mock_get_metric: MagicMock) -> None:
        mock_get_metric.side_effect = [1.0, 0.0, 8.0]  # pipeline_up, workers_active=0, pool_size

        service = PrometheusService()
        status = service.get_pipeline_status()

        assert status.pipeline_running is True
        assert status.workers_active == 0
        assert status.status == "healthy"
        assert "8 workers idle" in status.detail

    @patch.object(PrometheusService, "get_metric_value")
    def test_get_pipeline_status_stopped(self, mock_get_metric: MagicMock) -> None:
        mock_get_metric.side_effect = [0.0, 0.0, 0.0]  # all zeros

        service = PrometheusService()
        status = service.get_pipeline_status()

        assert status.pipeline_running is False
        assert status.status == "stopped"
        assert "not running" in status.detail


class TestJaegerService:
    """Tests for JaegerService."""

    def test_init_sets_base_url(self) -> None:
        service = JaegerService("http://jaeger:16686")

        assert service.base_url == "http://jaeger:16686"
        assert service.timeout == 5

    @patch("src.visualization.services.prometheus_service.requests")
    def test_is_healthy_returns_true(self, mock_requests: MagicMock) -> None:
        mock_requests.get.return_value.ok = True

        service = JaegerService()
        result = service.is_healthy()

        assert result is True

    @patch("src.visualization.services.prometheus_service.requests")
    def test_is_healthy_returns_false_on_error(self, mock_requests: MagicMock) -> None:
        mock_requests.get.side_effect = Exception("Connection refused")
        mock_requests.RequestException = Exception

        service = JaegerService()
        result = service.is_healthy()

        assert result is False

    @patch("src.visualization.services.prometheus_service.requests")
    def test_get_services_returns_data(self, mock_requests: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {"data": ["service1", "service2"]}
        mock_requests.get.return_value = mock_response

        service = JaegerService()
        result = service.get_services()

        assert result["healthy"] is True
        assert "data" in result

    @patch("src.visualization.services.prometheus_service.requests")
    def test_get_services_returns_error_on_failure(self, mock_requests: MagicMock) -> None:
        mock_requests.get.side_effect = Exception("Network error")
        mock_requests.RequestException = Exception

        service = JaegerService()
        result = service.get_services()

        assert result["healthy"] is False
        assert "error" in result
