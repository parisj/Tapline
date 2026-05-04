"""Flask API server for the Tapline dashboard.

Serves the HTML dashboard and provides REST API endpoints for accessing
MinIO storage and Prometheus metrics.

Run with: python -m src.visualization.api_server
Or: pixi run dashboard
"""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, Response, send_from_directory

from src.config.loader import load_runtime_config
from src.dispatch.routes import load_routes_toml
from src.storage.config import load_minio_config
from src.storage.minio_service import MinioStorageService
from src.utils.logging import get_logger
from src.visualization.constants import PROMETHEUS_URL
from src.visualization.discovery import DiscoveryService
from src.visualization.middleware import register_middleware
from src.visualization.readers import MinioArtifactReader
from src.visualization.routes import (
    artifacts_bp,
    audit_bp,
    kafka_bp,
    metrics_bp,
    prometheus_bp,
)
from src.visualization.routes.artifacts import init_blueprint as init_artifacts_bp
from src.visualization.routes.audit import init_blueprint as init_audit_bp
from src.visualization.routes.kafka import init_blueprint as init_kafka_bp
from src.visualization.routes.metrics import init_blueprint as init_metrics_bp
from src.visualization.routes.prometheus import init_blueprint as init_prometheus_bp
from src.visualization.services import ArtifactComputer, PrometheusService
from src.visualization.services.prometheus_service import JaegerService
from src.visualization.viewers import get_default_viewer_registry

logger = get_logger(__name__)

# Flask app configuration
STATIC_DIR = Path(__file__).parent / "static"
app = Flask(__name__, static_folder=str(STATIC_DIR))

CONFIG_ROOT = Path("src/config")

# Global services (initialized on startup)
storage: MinioStorageService | None = None
discovery: DiscoveryService | None = None
reader: MinioArtifactReader | None = None
prometheus_service: PrometheusService | None = None
jaeger_service: JaegerService | None = None
artifact_computer: ArtifactComputer | None = None


def init_services() -> None:
    """Initialize MinIO and discovery services."""
    global storage, discovery, reader, prometheus_service, jaeger_service, artifact_computer

    try:
        runtime_cfg = load_runtime_config(CONFIG_ROOT / "pipeline.toml")
        routes = load_routes_toml(CONFIG_ROOT / "routes.toml", config_root=CONFIG_ROOT)
        minio_cfg = load_minio_config(CONFIG_ROOT / "minio.toml")

        storage = MinioStorageService(minio_cfg)
        discovery = DiscoveryService(runtime_cfg, routes, storage)
        reader = MinioArtifactReader(storage)
        prometheus_service = PrometheusService(PROMETHEUS_URL)
        jaeger_service = JaegerService()
        artifact_computer = ArtifactComputer()
        viewer_registry = get_default_viewer_registry()

        # Initialize blueprints with services
        init_prometheus_bp(prometheus_service, jaeger_service)
        init_kafka_bp(prometheus_service)
        init_metrics_bp(storage, discovery, artifact_computer)
        init_artifacts_bp(storage, discovery, viewer_registry)
        init_audit_bp(storage)

        logger.info("API server services initialized successfully")
    except Exception:
        logger.exception("Failed to initialize services")


# Register middleware (rate limiting, security headers)
register_middleware(app)

# Register blueprints
app.register_blueprint(prometheus_bp)
app.register_blueprint(kafka_bp)
app.register_blueprint(metrics_bp)
app.register_blueprint(artifacts_bp)
app.register_blueprint(audit_bp)


# =============================================================================
# Dashboard Routes
# =============================================================================


@app.route("/")
def serve_dashboard() -> Response:
    """Serve the main HTML dashboard."""
    return send_from_directory(STATIC_DIR, "dashboard.html")


@app.route("/static/<path:filename>")
def serve_static(filename: str) -> Response:
    """Serve static files (CSS, JS, images)."""
    return send_from_directory(STATIC_DIR, filename)


def main() -> None:
    """Run the dashboard server."""
    load_dotenv()
    init_services()
    logger.info("=" * 60)
    logger.info("Tapline Dashboard Server")
    logger.info("=" * 60)
    logger.info("Dashboard:  http://localhost:5007")
    logger.info("API:        http://localhost:5007/api/health")
    logger.info("=" * 60)
    # Bind to all interfaces for Docker/container access (development server)
    # In production, use a proper WSGI server (gunicorn, uwsgi) behind a reverse proxy
    app.run(host="0.0.0.0", port=5007, debug=False)  # noqa: S104  # nosec B104


if __name__ == "__main__":
    main()
