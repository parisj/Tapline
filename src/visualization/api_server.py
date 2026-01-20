"""Flask API server for the VisioEval dashboard.

Serves the HTML dashboard and provides REST API endpoints for accessing
MinIO storage and Prometheus metrics.

Run with: python -m src.visualization.api_server
Or: pixi run dashboard
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, request, send_from_directory

from src.audit.chain import HashChainVerifier
from src.config.loader import load_runtime_config
from src.dispatch.routes import load_routes_toml
from src.domain.evaluation import AnalysisKind
from src.domain.events import EventEnvelope, EventType
from src.storage.config import load_minio_config
from src.storage.minio_service import MinioStorageService
from src.utils.logging import get_logger
from src.visualization.discovery import DiscoveryService
from src.visualization.readers import MinioArtifactReader
from src.visualization.services import ArtifactComputer, PrometheusService
from src.visualization.services.prometheus_service import JaegerService

# Type alias for Flask route responses (single Response or Response with status code)
FlaskResponse = Response | tuple[Response, int]


def _mask_to_kind_names(mask: int) -> list[str]:
    """Convert AnalysisKind bitmask to list of kind names."""
    return [kind.name for kind in AnalysisKind if mask & kind.value and kind.name is not None]


logger = get_logger(__name__)

# Flask app configuration
STATIC_DIR = Path(__file__).parent / "static"
app = Flask(__name__, static_folder=str(STATIC_DIR))

CONFIG_ROOT = Path("src/config")
PROMETHEUS_URL = "http://localhost:9091"

# Allowed buckets for artifact access (security: prevent bucket enumeration)
ALLOWED_BUCKETS = frozenset({"artifacts", "inputs", "aggregates", "metric-values"})

# Maximum time range for metric queries (30 days in minutes)
MAX_TIME_RANGE_MINUTES = 43200


# Security and cache headers for all responses
@app.after_request
def add_security_headers(response: Response) -> Response:
    """Add security and cache headers to all responses."""
    # Security headers (OWASP recommendations)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"

    # Content Security Policy - restrict script sources
    csp_parts = [
        "default-src 'self'",
        "script-src 'self' 'unsafe-inline' https://cdn.plot.ly "
        "https://ajax.googleapis.com https://maxcdn.bootstrapcdn.com",
        "style-src 'self' 'unsafe-inline' https://maxcdn.bootstrapcdn.com",
        "img-src 'self' data:",
        "font-src 'self' https://maxcdn.bootstrapcdn.com",
        "connect-src 'self' http://localhost:* ws://localhost:*",
    ]
    response.headers["Content-Security-Policy"] = "; ".join(csp_parts)

    # No-cache headers for API responses to prevent stale data
    if request.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"

    return response


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

        logger.info("API server services initialized successfully")
    except Exception:
        logger.exception("Failed to initialize services")


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


# =============================================================================
# Prometheus Proxy Endpoints
# =============================================================================


@app.route("/api/prometheus/query", methods=["GET"])
def prometheus_query() -> FlaskResponse:
    """Proxy Prometheus instant query to avoid CORS issues."""
    query = request.args.get("query", "")
    if not query:
        return jsonify({"error": "Missing query parameter"}), 400

    if prometheus_service is None:
        return jsonify({"error": "Prometheus service not initialized"}), 503

    result = prometheus_service.query(query)
    return jsonify(result.data)


@app.route("/api/prometheus/query_range", methods=["GET"])
def prometheus_query_range() -> FlaskResponse:
    """Proxy Prometheus range query to avoid CORS issues."""
    query = request.args.get("query", "")
    start = request.args.get("start", "")
    end = request.args.get("end", "")
    step = request.args.get("step", "60")

    if not query or not start or not end:
        return jsonify({"error": "Missing required parameters (query, start, end)"}), 400

    if prometheus_service is None:
        return jsonify({"error": "Prometheus service not initialized"}), 503

    result = prometheus_service.query_range(query, start, end, step)
    return jsonify(result.data)


@app.route("/api/prometheus/healthy", methods=["GET"])
def prometheus_healthy() -> FlaskResponse:
    """Check if Prometheus is reachable."""
    if prometheus_service is None:
        return jsonify({"healthy": False})
    return jsonify({"healthy": prometheus_service.is_healthy()})


@app.route("/api/jaeger/services", methods=["GET"])
def jaeger_services() -> FlaskResponse:
    """Proxy Jaeger services API to avoid CORS issues."""
    if jaeger_service is None:
        return jsonify({"healthy": False, "error": "Jaeger service not initialized"}), 503

    result = jaeger_service.get_services()
    if result["healthy"]:
        return jsonify(result)
    return jsonify(result), 503


@app.route("/api/pipeline/status", methods=["GET"])
def pipeline_status() -> FlaskResponse:
    """Get pipeline worker status from Prometheus metrics."""
    if prometheus_service is None:
        return jsonify(
            {
                "error": "Prometheus service not initialized",
                "pipeline_running": False,
                "status": "unknown",
            },
        ), 503

    try:
        status = prometheus_service.get_pipeline_status()
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


# =============================================================================
# MinIO & Discovery API Endpoints
# =============================================================================


@app.route("/api/health", methods=["GET"])
def health() -> FlaskResponse:
    """Health check endpoint."""
    return jsonify(
        {
            "status": "ok",
            "minio": storage is not None,
            "discovery": discovery is not None,
        },
    )


@app.route("/api/directories", methods=["GET"])
def get_directories() -> FlaskResponse:
    """Get list of configured directories."""
    if discovery is None:
        return jsonify({"error": "Discovery service not initialized"}), 503

    try:
        directories = discovery.list_directories()
        # Convert DirectoryInfo objects to dicts
        result = [{"key": d.key, "path": str(d.path)} for d in directories]
        return jsonify({"directories": result})
    except Exception as e:
        logger.exception("Failed to list directories")
        return jsonify({"error": str(e)}), 500


@app.route("/api/algorithms", methods=["GET"])
def get_algorithms() -> FlaskResponse:
    """Get list of available algorithms."""
    if discovery is None:
        return jsonify({"error": "Discovery service not initialized"}), 503

    try:
        algorithms = discovery.list_algorithms()
        # Convert AlgorithmInfo objects to dicts
        result = [{"name": a.name, "version": a.version, "settings_path": a.settings_path} for a in algorithms]
        return jsonify({"algorithms": result})
    except Exception as e:
        logger.exception("Failed to list algorithms")
        return jsonify({"error": str(e)}), 500


@app.route("/api/routes", methods=["GET"])
def get_routes() -> FlaskResponse:
    """Get configured routes (directory -> algorithm mapping)."""
    if discovery is None:
        return jsonify({"error": "Discovery service not initialized"}), 503

    try:
        routes = []
        for dir_info in discovery.list_directories():
            algo_info = discovery.get_algorithm_for_directory(dir_info.key)
            if algo_info:
                routes.append(
                    {
                        "directory": dir_info.key,
                        "path": str(dir_info.path),
                        "algorithm": algo_info.name,
                        "version": algo_info.version,
                    },
                )
        return jsonify({"routes": routes})
    except Exception as e:
        logger.exception("Failed to get routes")
        return jsonify({"error": str(e)}), 500


@app.route("/api/metrics", methods=["GET"])
def get_metrics() -> FlaskResponse:
    """Get available metrics from MinIO aggregates bucket.

    Query parameters:
        algorithm: Filter by algorithm name
        version: Filter by algorithm version
        refresh: Force cache refresh (true/false)
        time_range: Filter to last N minutes (e.g., 10, 60, 120)
    """
    if discovery is None:
        return jsonify({"error": "Discovery service not initialized"}), 503

    try:
        # Optional filters
        algorithm = request.args.get("algorithm")
        version = request.args.get("version")
        refresh = request.args.get("refresh", "").lower() == "true"

        # Time range filter (in minutes) with validation
        time_range_str = request.args.get("time_range")
        time_range_minutes: int | None = None
        if time_range_str:
            try:
                time_range_minutes = int(time_range_str)
                # Validate bounds: must be positive and reasonable (max 30 days)
                if time_range_minutes <= 0 or time_range_minutes > MAX_TIME_RANGE_MINUTES:
                    time_range_minutes = None
            except ValueError:
                pass

        # Use discovery service filters
        metrics = discovery.discover_metrics_from_minio(
            algo_name=algorithm if algorithm else None,
            algo_version=version if version else None,
            refresh=refresh,
            time_range_minutes=time_range_minutes,
        )

        # Convert MetricInfo objects to JSON-serializable format
        result = []
        for m in metrics:
            # Extract summary stats if available
            summary = m.summary or {}
            result.append(
                {
                    "metric_name": m.metric_name,
                    "algo_name": m.algo_name,
                    "algo_version": m.algo_version,
                    "analysis_mask": m.analysis_mask,
                    "analysis_kinds": m.analysis_kinds,
                    "count": summary.get("count"),
                    "sum": summary.get("sum"),
                    "avg": summary.get("avg") or summary.get("mean"),
                    "min": summary.get("min"),
                    "max": summary.get("max"),
                    "std": summary.get("std"),
                    "artifact_key": m.artifact_key,
                },
            )

        return jsonify(
            {
                "metrics": result,
                "count": len(result),
                "time_range_minutes": time_range_minutes,
            },
        )
    except Exception as e:
        logger.exception("Failed to discover metrics")
        return jsonify({"error": str(e)}), 500


@app.route("/api/metrics/<metric_name>/data", methods=["GET"])
def get_metric_data(metric_name: str) -> FlaskResponse:
    """Get detailed metric data including histogram/distribution from MinIO.

    Query parameters:
        algorithm: Filter by algorithm name
        version: Filter by algorithm version
        time_range: Filter to last N minutes (e.g., 10, 60, 120)
    """
    if discovery is None:
        return jsonify({"error": "Services not initialized"}), 503

    try:
        algorithm = request.args.get("algorithm")
        version = request.args.get("version")

        # Time range filter (in minutes) with validation
        time_range_str = request.args.get("time_range")
        time_range_minutes: int | None = None
        if time_range_str:
            try:
                time_range_minutes = int(time_range_str)
                # Validate bounds: must be positive and reasonable (max 30 days)
                if time_range_minutes <= 0 or time_range_minutes > MAX_TIME_RANGE_MINUTES:
                    time_range_minutes = None
            except ValueError:
                pass

        # Find the metric with time filtering
        metrics = discovery.discover_metrics_from_minio(
            time_range_minutes=time_range_minutes,
            refresh=True,  # Always refresh for detail view
        )
        matching = [m for m in metrics if m.metric_name == metric_name]

        if algorithm:
            matching = [m for m in matching if m.algo_name == algorithm]
        if version:
            matching = [m for m in matching if m.algo_version == version]

        if not matching:
            # Return empty data structure instead of 404 when time range has no data
            return jsonify(
                {
                    "metric_name": metric_name,
                    "algo_name": algorithm or "",
                    "algo_version": version or "",
                    "analysis_mask": 0,
                    "analysis_kinds": [],
                    "count": 0,
                    "sum": 0,
                    "avg": None,
                    "min": None,
                    "max": None,
                    "std": None,
                    "median": None,
                    "p95": None,
                    "p99": None,
                    "artifacts": {},
                    "no_data": True,
                    "time_range_minutes": time_range_minutes,
                },
            )

        metric = matching[0]

        # Get full artifact document
        artifact_doc = discovery.get_metric_artifact(metric)
        summary = metric.summary or {}

        # Build response with summary stats and any additional artifact data
        artifacts: dict[str, Any] = {}
        response: dict[str, Any] = {
            "metric_name": metric.metric_name,
            "algo_name": metric.algo_name,
            "algo_version": metric.algo_version,
            "analysis_mask": metric.analysis_mask,
            "analysis_kinds": metric.analysis_kinds,
            "count": summary.get("count"),
            "sum": summary.get("sum"),
            "avg": summary.get("avg") or summary.get("mean"),
            "min": summary.get("min"),
            "max": summary.get("max"),
            "std": summary.get("std"),
            "median": summary.get("median"),
            "p95": summary.get("p95"),
            "p99": summary.get("p99"),
            "artifacts": artifacts,
        }

        # Try to get raw values from the metric-values bucket first
        raw_values = None
        values_data = discovery.get_metric_values_with_meta(
            algo_name=metric.algo_name,
            algo_version=metric.algo_version,
            metric_name=metric.metric_name,
            time_range_minutes=time_range_minutes,
        )
        if values_data.get("values"):
            raw_values = values_data["values"]
            # Update analysis_mask if not set
            if metric.analysis_mask == 0 and values_data.get("analysis_mask"):
                response["analysis_mask"] = values_data["analysis_mask"]
                response["analysis_kinds"] = _mask_to_kind_names(values_data["analysis_mask"])

        # Fall back to artifact document values
        if artifact_doc:
            # Include histogram data if present
            if "histogram" in artifact_doc:
                artifacts["histogram"] = artifact_doc["histogram"]
            # Include quantiles if present
            if "quantiles" in artifact_doc:
                artifacts["quantiles"] = artifact_doc["quantiles"]
            # Include raw values from artifact if not already set
            if raw_values is None and "values" in artifact_doc:
                raw_values = artifact_doc["values"]

            # Include any other artifact data
            for key in ["ellipse", "contour", "categories", "outliers", "rate"]:
                if key in artifact_doc and key not in artifacts:
                    artifacts[key] = artifact_doc[key]

        # Compute on-the-fly artifacts based on analysis_mask and raw values
        if raw_values and artifact_computer:
            artifacts["values"] = raw_values
            analysis_mask: int = response.get("analysis_mask") or metric.analysis_mask

            # Generate histogram for DISTRIBUTION_1D (mask value 2)
            if analysis_mask & 2 and "histogram" not in artifacts:
                histogram = artifact_computer.compute_histogram(raw_values)
                if histogram:
                    artifacts["histogram"] = histogram

            # Generate categories for COUNTER (mask value 8)
            if analysis_mask & 8 and "categories" not in artifacts:
                categories = artifact_computer.compute_categories(raw_values)
                if categories:
                    artifacts["categories"] = categories

            # Generate rate CI for RATE (mask value 16)
            if analysis_mask & 16 and "rate" not in artifacts:
                rate_data = artifact_computer.compute_rate(raw_values)
                if rate_data:
                    artifacts["rate"] = rate_data

            # Generate ellipse data for ELLIPSE_2D (mask value 32)
            if analysis_mask & 32 and "ellipse" not in artifacts:
                ellipse_data = artifact_computer.compute_ellipse(raw_values)
                if ellipse_data:
                    artifacts["ellipse"] = ellipse_data

            # Generate contour data for CONTOUR_2D (mask value 64)
            if analysis_mask & 64 and "contour" not in artifacts:
                contour_data = artifact_computer.compute_contour(raw_values)
                if contour_data:
                    artifacts["contour"] = contour_data

        return jsonify(response)
    except Exception as e:
        logger.exception("Failed to get metric data")
        return jsonify({"error": str(e)}), 500


@app.route("/api/buckets", methods=["GET"])
def get_buckets() -> FlaskResponse:
    """Get MinIO bucket statistics."""
    if storage is None:
        return jsonify({"error": "Storage service not initialized"}), 503

    try:
        buckets = ["artifacts", "inputs", "aggregates", "metric-values"]
        result = []

        for bucket_name in buckets:
            try:
                objects = list(storage.list_objects(bucket_name, limit=1000))
                total_size = sum(obj.get("size", 0) for obj in objects if isinstance(obj, dict))
                result.append(
                    {
                        "name": bucket_name,
                        "object_count": len(objects),
                        "total_size_bytes": total_size,
                        "total_size_mb": round(total_size / (1024 * 1024), 2),
                    },
                )
            except Exception as e:
                logger.warning("Failed to get stats for bucket %s: %s", bucket_name, e)
                result.append(
                    {
                        "name": bucket_name,
                        "object_count": 0,
                        "total_size_bytes": 0,
                        "total_size_mb": 0,
                        "error": str(e),
                    },
                )

        return jsonify({"buckets": result})
    except Exception as e:
        logger.exception("Failed to get bucket stats")
        return jsonify({"error": str(e)}), 500


@app.route("/api/cache/clear", methods=["POST"])
def clear_cache() -> FlaskResponse:
    """Clear the discovery service cache to force fresh data on next request."""
    if discovery is None:
        return jsonify({"error": "Discovery service not initialized"}), 503

    try:
        discovery.clear_cache()
        return jsonify({"status": "ok", "message": "Cache cleared"})
    except Exception as e:
        logger.exception("Failed to clear cache")
        return jsonify({"error": str(e)}), 500


@app.route("/api/artifacts", methods=["GET"])
def list_artifacts() -> FlaskResponse:
    """List artifacts from MinIO."""
    if storage is None:
        return jsonify({"error": "Storage service not initialized"}), 503

    try:
        bucket = request.args.get("bucket", "artifacts")
        prefix = request.args.get("prefix", "")

        # Security: validate bucket against allowlist
        if bucket not in ALLOWED_BUCKETS:
            return jsonify({"error": "Invalid bucket"}), 400

        # Security: validate prefix doesn't contain path traversal
        if ".." in prefix:
            return jsonify({"error": "Invalid prefix"}), 400

        # Validate and bound limit parameter
        try:
            limit = min(int(request.args.get("limit", 100)), 1000)
            if limit <= 0:
                limit = 100
        except ValueError:
            limit = 100

        objects = list(storage.list_objects(bucket, prefix=prefix, limit=limit))

        return jsonify(
            {
                "bucket": bucket,
                "objects": objects,
                "count": len(objects),
            },
        )
    except Exception as e:
        logger.exception("Failed to list artifacts")
        return jsonify({"error": str(e)}), 500


@app.route("/api/artifact/<bucket>/<path:key>", methods=["GET"])
def get_artifact(bucket: str, key: str) -> FlaskResponse:
    """Get a specific artifact's metadata and content."""
    if storage is None:
        return jsonify({"error": "Storage service not initialized"}), 503

    # Security: validate bucket name against allowlist
    if bucket not in ALLOWED_BUCKETS:
        return jsonify({"error": "Invalid bucket"}), 400

    # Security: validate key doesn't contain path traversal
    if ".." in key or key.startswith("/"):
        return jsonify({"error": "Invalid key"}), 400

    try:
        # Get object info
        data = storage.retrieve_by_key(bucket, key)
        if data is None:
            return jsonify({"error": "Artifact not found"}), 404

        # Try to parse as JSON if it looks like JSON
        content = None
        try:
            content = json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            # Binary data - return base64 or just metadata
            content = {"binary": True, "size": len(data)}

        return jsonify(
            {
                "bucket": bucket,
                "key": key,
                "content": content,
            },
        )
    except Exception as e:
        logger.exception("Failed to get artifact")
        return jsonify({"error": str(e)}), 500


# =============================================================================
# Audit Chain Verification Endpoints
# =============================================================================


@app.route("/api/audit/verify", methods=["GET"])
def verify_audit_chain() -> FlaskResponse:
    """Verify the audit chain integrity.

    Checks that the hash chain in the audit log is intact and untampered.
    Returns verification status, event count, and any integrity errors.

    Note: Audit events are currently stored in Kafka (visio.audit-log topic)
    but not persisted to MinIO. Full chain verification requires reading
    from Kafka or implementing an audit log consumer.
    """
    if storage is None:
        return jsonify({"error": "Storage service not initialized"}), 503

    try:
        events: list[EventEnvelope] = []
        errors: list[str] = []
        kafka_audit_count = 0

        # Try to get Kafka audit log message count via Prometheus
        try:
            prom_url = "http://localhost:9091"
            # Query for messages produced to audit-log topic (use our own metrics)
            query = 'sum(visioeval_kafka_messages_produced_total{topic="visio.audit-log"}) or vector(0)'
            resp = requests.get(f"{prom_url}/api/v1/query", params={"query": query}, timeout=5)
            if resp.ok:
                data = resp.json()
                if data.get("status") == "success" and data.get("data", {}).get("result"):
                    result = data["data"]["result"]
                    if result and len(result) > 0:
                        kafka_audit_count = int(float(result[0].get("value", [0, 0])[1]))
        except Exception as e:
            logger.debug("Could not query Kafka audit metrics: %s", e)

        # Check for audit log objects in MinIO aggregates bucket
        try:
            audit_objects = list(storage.list_objects("aggregates", prefix="audit/", limit=100))
            for obj in audit_objects:
                if isinstance(obj, dict) and obj.get("key"):
                    try:
                        data = storage.retrieve_by_key("aggregates", obj["key"])
                        if data:
                            doc = json.loads(data.decode("utf-8"))
                            if "events" in doc:
                                for evt_data in doc["events"]:
                                    try:
                                        evt = EventEnvelope(
                                            event_id=evt_data.get("event_id", ""),
                                            event_type=EventType(evt_data.get("event_type", "JOB_CREATED")),
                                            source_id=evt_data.get("source_id", ""),
                                            timestamp=evt_data.get("timestamp", ""),
                                            payload=evt_data.get("payload", {}),
                                            content_hash=evt_data.get("content_hash", ""),
                                            prev_hash=evt_data.get("prev_hash"),
                                            signature=evt_data.get("signature"),
                                        )
                                        events.append(evt)
                                    except Exception as e:
                                        errors.append(f"Failed to parse event: {e}")
                    except Exception as e:
                        errors.append(f"Failed to read audit object {obj.get('key')}: {e}")
        except Exception as e:
            logger.debug("No audit log objects in MinIO: %s", e)

        # Determine chain status
        chain_status = "unknown"
        event_count = len(events)

        if events:
            # Verify the chain from stored events
            verifier = HashChainVerifier()
            result = verifier.verify_chain(events)
            chain_status = "valid" if result.valid else "invalid"
            errors.extend(result.errors)
        elif kafka_audit_count > 0:
            # Events exist in Kafka but not stored in MinIO for verification
            chain_status = "kafka_only"
            event_count = kafka_audit_count
        else:
            chain_status = "no_events"

        return jsonify(
            {
                "status": chain_status,
                "event_count": event_count,
                "kafka_events": kafka_audit_count,
                "stored_events": len(events),
                "errors": errors,
                "verified": chain_status == "valid",
                "message": {
                    "valid": "Audit chain integrity verified - no tampering detected",
                    "invalid": f"Audit chain integrity check FAILED - {len(errors)} error(s) found",
                    "kafka_only": f"Audit events in Kafka ({kafka_audit_count}) - requires audit consumer",
                    "no_events": "No audit events found - run the pipeline to generate audit trail",
                    "unknown": "Unable to verify audit chain",
                }.get(chain_status, "Unknown status"),
            },
        )
    except Exception as e:
        logger.exception("Failed to verify audit chain")
        return jsonify({"error": str(e), "status": "error"}), 500


@app.route("/api/audit/stats", methods=["GET"])
def audit_stats() -> FlaskResponse:
    """Get audit chain statistics without full verification."""
    if storage is None:
        return jsonify({"error": "Storage service not initialized"}), 503

    try:
        stats = {
            "partitions": 0,
            "total_events": 0,
            "last_event_time": None,
            "audit_objects": 0,
        }

        # Count audit objects in storage
        try:
            audit_objects = list(storage.list_objects("aggregates", prefix="audit/", limit=1000))
            stats["audit_objects"] = len(audit_objects)

            # Get latest event info if available
            if audit_objects:
                # Objects are typically sorted by time
                latest = audit_objects[-1] if audit_objects else None
                if latest and isinstance(latest, dict):
                    stats["last_event_time"] = latest.get("last_modified")
        except Exception as e:
            logger.warning("Failed to get audit stats: %s", e)

        return jsonify(stats)
    except Exception as e:
        logger.exception("Failed to get audit stats")
        return jsonify({"error": str(e)}), 500


def main() -> None:
    """Run the dashboard server."""
    load_dotenv()
    init_services()
    logger.info("=" * 60)
    logger.info("VisioEval Dashboard Server")
    logger.info("=" * 60)
    logger.info("Dashboard:  http://localhost:5007")
    logger.info("API:        http://localhost:5007/api/health")
    logger.info("=" * 60)
    # Bind to all interfaces for Docker/container access (development server)
    # In production, use a proper WSGI server (gunicorn, uwsgi) behind a reverse proxy
    app.run(host="0.0.0.0", port=5007, debug=False)  # noqa: S104


if __name__ == "__main__":
    main()
