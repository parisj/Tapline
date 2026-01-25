"""Prometheus and Jaeger proxy routes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from flask import Blueprint, Response, jsonify, request

from src.utils.logging import get_logger

if TYPE_CHECKING:
    from src.visualization.services import PrometheusService
    from src.visualization.services.prometheus_service import JaegerService

logger = get_logger(__name__)

# Type alias for Flask route responses
FlaskResponse = Response | tuple[Response, int]

prometheus_bp = Blueprint("prometheus", __name__, url_prefix="/api")

# Services are injected via init_blueprint
_prometheus_service: PrometheusService | None = None
_jaeger_service: JaegerService | None = None


def init_blueprint(prometheus_service: PrometheusService | None, jaeger_service: JaegerService | None) -> None:
    """Initialize the blueprint with required services.

    Args:
        prometheus_service: Prometheus service instance
        jaeger_service: Jaeger service instance

    """
    global _prometheus_service, _jaeger_service
    _prometheus_service = prometheus_service
    _jaeger_service = jaeger_service


@prometheus_bp.route("/prometheus/query", methods=["GET"])
def prometheus_query() -> FlaskResponse:
    """Proxy Prometheus instant query to avoid CORS issues."""
    query = request.args.get("query", "")
    if not query:
        return jsonify({"error": "Missing query parameter"}), 400

    if _prometheus_service is None:
        return jsonify({"error": "Prometheus service not initialized"}), 503

    result = _prometheus_service.query(query)
    return jsonify(result.data)


@prometheus_bp.route("/prometheus/query_range", methods=["GET"])
def prometheus_query_range() -> FlaskResponse:
    """Proxy Prometheus range query to avoid CORS issues."""
    query = request.args.get("query", "")
    start = request.args.get("start", "")
    end = request.args.get("end", "")
    step = request.args.get("step", "60")

    if not query or not start or not end:
        return jsonify({"error": "Missing required parameters (query, start, end)"}), 400

    if _prometheus_service is None:
        return jsonify({"error": "Prometheus service not initialized"}), 503

    result = _prometheus_service.query_range(query, start, end, step)
    return jsonify(result.data)


@prometheus_bp.route("/prometheus/healthy", methods=["GET"])
def prometheus_healthy() -> FlaskResponse:
    """Check if Prometheus is reachable."""
    if _prometheus_service is None:
        return jsonify({"healthy": False})
    return jsonify({"healthy": _prometheus_service.is_healthy()})


@prometheus_bp.route("/jaeger/services", methods=["GET"])
def jaeger_services() -> FlaskResponse:
    """Proxy Jaeger services API to avoid CORS issues."""
    if _jaeger_service is None:
        return jsonify({"healthy": False, "error": "Jaeger service not initialized"}), 503

    result = _jaeger_service.get_services()
    if result["healthy"]:
        return jsonify(result)
    return jsonify(result), 503


@prometheus_bp.route("/pipeline/status", methods=["GET"])
def pipeline_status() -> FlaskResponse:
    """Get pipeline worker status from Prometheus metrics."""
    if _prometheus_service is None:
        return jsonify(
            {
                "error": "Prometheus service not initialized",
                "pipeline_running": False,
                "status": "unknown",
            },
        ), 503

    try:
        status = _prometheus_service.get_pipeline_status()
        return jsonify(
            {
                "pipeline_running": status.pipeline_running,
                "workers_active": status.workers_active,
                "worker_pool_size": status.worker_pool_size,
                "status": status.status,
                "detail": status.detail,
            },
        )
    except Exception as e:
        logger.exception("Failed to get pipeline status")
        return jsonify({"error": str(e), "pipeline_running": False, "status": "unknown"}), 500
