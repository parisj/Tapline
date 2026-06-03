"""Unit tests for the Prometheus/Jaeger proxy route blueprint."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from flask import Flask

from src.visualization.routes.prometheus import init_blueprint, prometheus_bp


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config["TESTING"] = True
    if "prometheus" not in app.blueprints:
        app.register_blueprint(prometheus_bp)
    yield app


@pytest.fixture
def client(app):
    with app.test_client() as c:
        yield c


@pytest.fixture(autouse=True)
def _reset_blueprint_state():
    init_blueprint(None, None)
    yield
    init_blueprint(None, None)


class TestPrometheusQuery:
    def test_missing_query_param_returns_400(self, client) -> None:
        resp = client.get("/api/prometheus/query")
        assert resp.status_code == 400
        assert "error" in resp.get_json()

    def test_503_when_service_none(self, client) -> None:
        resp = client.get("/api/prometheus/query?query=up")
        assert resp.status_code == 503

    def test_returns_query_result(self, client) -> None:
        prom = MagicMock()
        result = MagicMock()
        result.data = {"status": "success", "data": {"result": [{"value": [1, "42"]}]}}
        prom.query.return_value = result
        init_blueprint(prom, None)

        resp = client.get("/api/prometheus/query?query=up")
        assert resp.status_code == 200
        assert resp.get_json() == result.data
        prom.query.assert_called_once_with("up")


class TestPrometheusQueryRange:
    def test_missing_params_returns_400(self, client) -> None:
        resp = client.get("/api/prometheus/query_range?query=up")
        assert resp.status_code == 400

    def test_503_when_service_none(self, client) -> None:
        resp = client.get(
            "/api/prometheus/query_range?query=up&start=1&end=2",
        )
        assert resp.status_code == 503

    def test_returns_range_result(self, client) -> None:
        prom = MagicMock()
        result = MagicMock()
        result.data = {"status": "success", "data": {"result": []}}
        prom.query_range.return_value = result
        init_blueprint(prom, None)

        resp = client.get(
            "/api/prometheus/query_range?query=up&start=1&end=2&step=15",
        )
        assert resp.status_code == 200
        prom.query_range.assert_called_once_with("up", "1", "2", "15")

    def test_default_step_used_when_not_provided(self, client) -> None:
        prom = MagicMock()
        result = MagicMock()
        result.data = {"status": "success"}
        prom.query_range.return_value = result
        init_blueprint(prom, None)

        resp = client.get("/api/prometheus/query_range?query=up&start=1&end=2")
        assert resp.status_code == 200
        prom.query_range.assert_called_once_with("up", "1", "2", "60")


class TestPrometheusHealthy:
    def test_no_service_returns_unhealthy(self, client) -> None:
        resp = client.get("/api/prometheus/healthy")
        assert resp.status_code == 200
        assert resp.get_json() == {"healthy": False}

    def test_service_reports_healthy(self, client) -> None:
        prom = MagicMock()
        prom.is_healthy.return_value = True
        init_blueprint(prom, None)

        body = client.get("/api/prometheus/healthy").get_json()
        assert body == {"healthy": True}

    def test_service_reports_unhealthy(self, client) -> None:
        prom = MagicMock()
        prom.is_healthy.return_value = False
        init_blueprint(prom, None)

        body = client.get("/api/prometheus/healthy").get_json()
        assert body == {"healthy": False}


class TestJaegerServices:
    def test_503_when_service_none(self, client) -> None:
        resp = client.get("/api/jaeger/services")
        assert resp.status_code == 503

    def test_returns_services_when_healthy(self, client) -> None:
        jaeger = MagicMock()
        jaeger.get_services.return_value = {"healthy": True, "data": ["s1", "s2"]}
        init_blueprint(None, jaeger)

        resp = client.get("/api/jaeger/services")
        assert resp.status_code == 200
        assert resp.get_json()["data"] == ["s1", "s2"]

    def test_returns_503_when_unhealthy(self, client) -> None:
        jaeger = MagicMock()
        jaeger.get_services.return_value = {"healthy": False, "error": "down"}
        init_blueprint(None, jaeger)

        resp = client.get("/api/jaeger/services")
        assert resp.status_code == 503


class TestPipelineStatus:
    def test_503_when_service_none(self, client) -> None:
        resp = client.get("/api/pipeline/status")
        assert resp.status_code == 503
        body = resp.get_json()
        assert body["pipeline_running"] is False

    def test_returns_status(self, client) -> None:
        prom = MagicMock()
        status = MagicMock()
        status.pipeline_running = True
        status.workers_active = 2
        status.worker_pool_size = 8
        status.status = "healthy"
        status.detail = "2/8 processing"
        prom.get_pipeline_status.return_value = status
        init_blueprint(prom, None)

        body = client.get("/api/pipeline/status").get_json()
        assert body["pipeline_running"] is True
        assert body["workers_active"] == 2
        assert body["worker_pool_size"] == 8
        assert body["status"] == "healthy"
        assert body["detail"] == "2/8 processing"

    def test_handles_exception(self, client) -> None:
        prom = MagicMock()
        prom.get_pipeline_status.side_effect = RuntimeError("boom")
        init_blueprint(prom, None)

        resp = client.get("/api/pipeline/status")
        assert resp.status_code == 500
        body = resp.get_json()
        assert body["pipeline_running"] is False
        assert body["status"] == "unknown"
