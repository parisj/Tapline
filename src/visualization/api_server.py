"""Simple API server for the HTML dashboard to access MinIO and other services.

Run with: python -m src.visualization.api_server
Or: pixi run dashboard-api
"""

from __future__ import annotations

import json
from pathlib import Path

from flask import Flask, jsonify, request
from flask_cors import CORS

from src.config.loader import load_runtime_config
from src.dispatch.routes import load_routes_toml
from src.storage.config import load_minio_config
from src.storage.minio_service import MinioStorageService
from src.utils.logging import get_logger
from src.visualization.discovery import DiscoveryService
from src.visualization.readers import MinioArtifactReader

logger = get_logger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for browser access

CONFIG_ROOT = Path("src/config")

# Global services (initialized on startup)
storage: MinioStorageService | None = None
discovery: DiscoveryService | None = None
reader: MinioArtifactReader | None = None


def init_services() -> None:
    """Initialize MinIO and discovery services."""
    global storage, discovery, reader

    try:
        runtime_cfg = load_runtime_config(CONFIG_ROOT / "pipeline.toml")
        routes = load_routes_toml(CONFIG_ROOT / "routes.toml", config_root=CONFIG_ROOT)
        minio_cfg = load_minio_config(CONFIG_ROOT / "minio.toml")

        storage = MinioStorageService(minio_cfg)
        discovery = DiscoveryService(runtime_cfg, routes, storage)
        reader = MinioArtifactReader(storage)

        logger.info("API server services initialized successfully")
    except Exception as e:
        logger.error("Failed to initialize services: %s", e)


@app.route("/api/health", methods=["GET"])
def health() -> tuple:
    """Health check endpoint."""
    return jsonify({
        "status": "ok",
        "minio": storage is not None,
        "discovery": discovery is not None,
    })


@app.route("/api/directories", methods=["GET"])
def get_directories() -> tuple:
    """Get list of configured directories."""
    if discovery is None:
        return jsonify({"error": "Discovery service not initialized"}), 503

    try:
        directories = discovery.list_directories()
        return jsonify({"directories": directories})
    except Exception as e:
        logger.exception("Failed to list directories")
        return jsonify({"error": str(e)}), 500


@app.route("/api/algorithms", methods=["GET"])
def get_algorithms() -> tuple:
    """Get list of available algorithms."""
    if discovery is None:
        return jsonify({"error": "Discovery service not initialized"}), 503

    try:
        algorithms = discovery.list_algorithms()
        return jsonify({"algorithms": algorithms})
    except Exception as e:
        logger.exception("Failed to list algorithms")
        return jsonify({"error": str(e)}), 500


@app.route("/api/routes", methods=["GET"])
def get_routes() -> tuple:
    """Get configured routes (directory -> algorithm mapping)."""
    if discovery is None:
        return jsonify({"error": "Discovery service not initialized"}), 503

    try:
        routes = []
        for dir_key in discovery.list_directories():
            algo_info = discovery.get_algorithm_for_directory(dir_key)
            if algo_info:
                routes.append({
                    "directory": dir_key,
                    "algorithm": algo_info.get("name", "unknown"),
                    "version": algo_info.get("version", "1.0"),
                })
        return jsonify({"routes": routes})
    except Exception as e:
        logger.exception("Failed to get routes")
        return jsonify({"error": str(e)}), 500


@app.route("/api/metrics", methods=["GET"])
def get_metrics() -> tuple:
    """Get available metrics from MinIO aggregates bucket."""
    if discovery is None:
        return jsonify({"error": "Discovery service not initialized"}), 503

    try:
        # Optional filters
        directory = request.args.get("directory")
        algorithm = request.args.get("algorithm")
        version = request.args.get("version")

        metrics = discovery.discover_metrics_from_minio()

        # Apply filters
        if directory:
            metrics = [m for m in metrics if m.directory_key == directory]
        if algorithm:
            metrics = [m for m in metrics if m.algo_name == algorithm]
        if version:
            metrics = [m for m in metrics if m.algo_version == version]

        # Convert to JSON-serializable format
        result = []
        for m in metrics:
            result.append({
                "metric_name": m.metric_name,
                "algo_name": m.algo_name,
                "algo_version": m.algo_version,
                "directory_key": m.directory_key,
                "analysis_mask": m.analysis_mask,
                "window_start": m.window_start.isoformat() if m.window_start else None,
                "window_end": m.window_end.isoformat() if m.window_end else None,
                "count": m.count,
                "sum": m.sum,
                "avg": m.avg,
                "min": m.min,
                "max": m.max,
                "artifact_refs": m.artifact_refs,
            })

        return jsonify({"metrics": result, "count": len(result)})
    except Exception as e:
        logger.exception("Failed to discover metrics")
        return jsonify({"error": str(e)}), 500


@app.route("/api/metrics/<metric_name>/data", methods=["GET"])
def get_metric_data(metric_name: str) -> tuple:
    """Get detailed metric data including histogram/distribution from MinIO."""
    if discovery is None or reader is None:
        return jsonify({"error": "Services not initialized"}), 503

    try:
        algorithm = request.args.get("algorithm")
        version = request.args.get("version")

        # Find the metric
        metrics = discovery.discover_metrics_from_minio()
        matching = [m for m in metrics if m.metric_name == metric_name]

        if algorithm:
            matching = [m for m in matching if m.algo_name == algorithm]
        if version:
            matching = [m for m in matching if m.algo_version == version]

        if not matching:
            return jsonify({"error": "Metric not found"}), 404

        metric = matching[0]

        # Try to fetch artifact data
        artifact_data = {}
        if metric.artifact_refs:
            for ref in metric.artifact_refs:
                if isinstance(ref, dict):
                    name = ref.get("name", "")
                    content_hash = ref.get("content_hash")
                    bucket = ref.get("bucket", "artifacts")

                    if content_hash:
                        try:
                            npz_data = reader.fetch_npz(bucket, content_hash)
                            if npz_data:
                                # Convert numpy arrays to lists for JSON
                                artifact_data[name] = {
                                    k: v.tolist() if hasattr(v, "tolist") else v
                                    for k, v in npz_data.items()
                                }
                        except Exception as e:
                            logger.warning("Failed to fetch artifact %s: %s", name, e)

        return jsonify({
            "metric_name": metric.metric_name,
            "algo_name": metric.algo_name,
            "algo_version": metric.algo_version,
            "count": metric.count,
            "sum": metric.sum,
            "avg": metric.avg,
            "min": metric.min,
            "max": metric.max,
            "artifacts": artifact_data,
        })
    except Exception as e:
        logger.exception("Failed to get metric data")
        return jsonify({"error": str(e)}), 500


@app.route("/api/buckets", methods=["GET"])
def get_buckets() -> tuple:
    """Get MinIO bucket statistics."""
    if storage is None:
        return jsonify({"error": "Storage service not initialized"}), 503

    try:
        buckets = ["artifacts", "inputs", "aggregates"]
        result = []

        for bucket_name in buckets:
            try:
                objects = list(storage.list_objects(bucket_name, limit=1000))
                total_size = sum(obj.get("size", 0) for obj in objects if isinstance(obj, dict))
                result.append({
                    "name": bucket_name,
                    "object_count": len(objects),
                    "total_size_bytes": total_size,
                    "total_size_mb": round(total_size / (1024 * 1024), 2),
                })
            except Exception as e:
                logger.warning("Failed to get stats for bucket %s: %s", bucket_name, e)
                result.append({
                    "name": bucket_name,
                    "object_count": 0,
                    "total_size_bytes": 0,
                    "total_size_mb": 0,
                    "error": str(e),
                })

        return jsonify({"buckets": result})
    except Exception as e:
        logger.exception("Failed to get bucket stats")
        return jsonify({"error": str(e)}), 500


@app.route("/api/artifacts", methods=["GET"])
def list_artifacts() -> tuple:
    """List artifacts from MinIO."""
    if storage is None:
        return jsonify({"error": "Storage service not initialized"}), 503

    try:
        bucket = request.args.get("bucket", "artifacts")
        prefix = request.args.get("prefix", "")
        limit = min(int(request.args.get("limit", 100)), 1000)

        objects = list(storage.list_objects(bucket, prefix=prefix, limit=limit))

        return jsonify({
            "bucket": bucket,
            "objects": objects,
            "count": len(objects),
        })
    except Exception as e:
        logger.exception("Failed to list artifacts")
        return jsonify({"error": str(e)}), 500


@app.route("/api/artifact/<bucket>/<path:key>", methods=["GET"])
def get_artifact(bucket: str, key: str) -> tuple:
    """Get a specific artifact's metadata and content."""
    if storage is None:
        return jsonify({"error": "Storage service not initialized"}), 503

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

        return jsonify({
            "bucket": bucket,
            "key": key,
            "content": content,
        })
    except Exception as e:
        logger.exception("Failed to get artifact")
        return jsonify({"error": str(e)}), 500


def main() -> None:
    """Run the API server."""
    init_services()
    logger.info("Starting API server on http://localhost:5007")
    app.run(host="0.0.0.0", port=5007, debug=False)


if __name__ == "__main__":
    main()
