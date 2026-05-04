"""Metrics and discovery routes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from flask import Blueprint, Response, jsonify, request

from src.domain.evaluation import mask_to_kind_names
from src.utils.logging import get_logger
from src.visualization.constants import (
    ALLOWED_BUCKETS,
    DEFAULT_ARTIFACT_LIMIT,
    MAX_ARTIFACT_LIMIT,
    MAX_TIME_RANGE_MINUTES,
    validate_algorithm_name,
    validate_metric_name,
    validate_version,
)

if TYPE_CHECKING:
    from src.storage.minio_service import MinioStorageService
    from src.visualization.discovery import DiscoveryService, MetricInfo
    from src.visualization.services import ArtifactComputer

logger = get_logger(__name__)

# Type alias for Flask route responses
FlaskResponse = Response | tuple[Response, int]

metrics_bp = Blueprint("metrics", __name__, url_prefix="/api")

# Services injected via init_blueprint
_storage: MinioStorageService | None = None
_discovery: DiscoveryService | None = None
_artifact_computer: ArtifactComputer | None = None


def init_blueprint(
    storage: MinioStorageService | None,
    discovery: DiscoveryService | None,
    artifact_computer: ArtifactComputer | None,
) -> None:
    """Initialize the blueprint with required services.

    Args:
        storage: MinIO storage service instance
        discovery: Discovery service instance
        artifact_computer: Artifact computer instance

    """
    global _storage, _discovery, _artifact_computer
    _storage = storage
    _discovery = discovery
    _artifact_computer = artifact_computer


@metrics_bp.route("/health", methods=["GET"])
def health() -> FlaskResponse:
    """Health check endpoint."""
    return jsonify(
        {
            "status": "ok",
            "minio": _storage is not None,
            "discovery": _discovery is not None,
        },
    )


@metrics_bp.route("/directories", methods=["GET"])
def get_directories() -> FlaskResponse:
    """Get list of configured directories."""
    if _discovery is None:
        return jsonify({"error": "Discovery service not initialized"}), 503

    try:
        directories = _discovery.list_directories()
        result = [{"key": d.key, "path": str(d.path)} for d in directories]
        return jsonify({"directories": result})
    except Exception as e:
        logger.exception("Failed to list directories")
        return jsonify({"error": str(e)}), 500


@metrics_bp.route("/algorithms", methods=["GET"])
def get_algorithms() -> FlaskResponse:
    """Get list of available algorithms."""
    if _discovery is None:
        return jsonify({"error": "Discovery service not initialized"}), 503

    try:
        algorithms = _discovery.list_algorithms()
        result = [{"name": a.name, "version": a.version, "settings_path": a.settings_path} for a in algorithms]
        return jsonify({"algorithms": result})
    except Exception as e:
        logger.exception("Failed to list algorithms")
        return jsonify({"error": str(e)}), 500


@metrics_bp.route("/routes", methods=["GET"])
def get_routes() -> FlaskResponse:
    """Get configured routes (directory -> algorithm mapping)."""
    if _discovery is None:
        return jsonify({"error": "Discovery service not initialized"}), 503

    try:
        routes = []
        for dir_info in _discovery.list_directories():
            algo_info = _discovery.get_algorithm_for_directory(dir_info.key)
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


@metrics_bp.route("/metrics", methods=["GET"])
def get_metrics() -> FlaskResponse:
    """Get available metrics from MinIO aggregates bucket.

    Query parameters:
        algorithm: Filter by algorithm name
        version: Filter by algorithm version
        refresh: Force cache refresh (true/false)
        time_range: Filter to last N minutes (e.g., 10, 60, 120)
    """
    if _discovery is None:
        return jsonify({"error": "Discovery service not initialized"}), 503

    try:
        # Optional filters with validation
        algorithm = request.args.get("algorithm")
        version = request.args.get("version")
        refresh = request.args.get("refresh", "").lower() == "true"

        # Validate algorithm and version if provided
        if algorithm and not validate_algorithm_name(algorithm):
            return jsonify({"error": "Invalid algorithm name format"}), 400
        if version and not validate_version(version):
            return jsonify({"error": "Invalid version format"}), 400

        # Time range filter (in minutes) with validation
        time_range_minutes = _parse_time_range(request.args.get("time_range"))

        # Use discovery service filters
        metrics = _discovery.discover_metrics_from_minio(
            processor_name=algorithm if algorithm else None,
            processor_version=version if version else None,
            refresh=refresh,
            time_range_minutes=time_range_minutes,
        )

        # Convert MetricInfo objects to JSON-serializable format
        result = []
        for m in metrics:
            summary = m.summary or {}
            result.append(
                {
                    "metric_name": m.metric_name,
                    "processor_name": m.processor_name,
                    "processor_version": m.processor_version,
                    "aggregation_mask": m.aggregation_mask,
                    "aggregation_types": m.aggregation_types,
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


@metrics_bp.route("/metrics/<metric_name>/data", methods=["GET"])
def get_metric_data(metric_name: str) -> FlaskResponse:
    """Get detailed metric data including histogram/distribution from MinIO.

    Query parameters:
        algorithm: Filter by algorithm name
        version: Filter by algorithm version
        time_range: Filter to last N minutes (e.g., 10, 60, 120)
    """
    # Input validation for metric_name
    if not validate_metric_name(metric_name):
        return jsonify({"error": "Invalid metric name format"}), 400

    if _discovery is None:
        return jsonify({"error": "Services not initialized"}), 503

    try:
        algorithm = request.args.get("algorithm")
        version = request.args.get("version")

        # Validate algorithm and version if provided
        if algorithm and not validate_algorithm_name(algorithm):
            return jsonify({"error": "Invalid algorithm name format"}), 400
        if version and not validate_version(version):
            return jsonify({"error": "Invalid version format"}), 400

        time_range_minutes = _parse_time_range(request.args.get("time_range"))

        # Find the metric with time filtering
        metrics = _discovery.discover_metrics_from_minio(
            time_range_minutes=time_range_minutes,
            refresh=True,  # Always refresh for detail view
        )
        matching = [m for m in metrics if m.metric_name == metric_name]

        if algorithm:
            matching = [m for m in matching if m.processor_name == algorithm]
        if version:
            matching = [m for m in matching if m.processor_version == version]

        if not matching:
            return _empty_metric_response(metric_name, algorithm, version, time_range_minutes)

        metric = matching[0]
        return _build_metric_data_response(metric, time_range_minutes)
    except Exception as e:
        logger.exception("Failed to get metric data")
        return jsonify({"error": str(e)}), 500


@metrics_bp.route("/buckets", methods=["GET"])
def get_buckets() -> FlaskResponse:
    """Get MinIO bucket statistics."""
    if _storage is None:
        return jsonify({"error": "Storage service not initialized"}), 503

    try:
        buckets = ["artifacts", "inputs", "aggregates", "metric-values"]
        result = []

        for bucket_name in buckets:
            try:
                objects = list(_storage.list_objects(bucket_name, limit=1000))
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


@metrics_bp.route("/cache/clear", methods=["POST"])
def clear_cache() -> FlaskResponse:
    """Clear the discovery service cache to force fresh data on next request."""
    if _discovery is None:
        return jsonify({"error": "Discovery service not initialized"}), 503

    try:
        _discovery.clear_cache()
        return jsonify({"status": "ok", "message": "Cache cleared"})
    except Exception as e:
        logger.exception("Failed to clear cache")
        return jsonify({"error": str(e)}), 500


@metrics_bp.route("/artifacts", methods=["GET"])
def list_artifacts() -> FlaskResponse:
    """List artifacts from MinIO."""
    if _storage is None:
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
            limit = min(int(request.args.get("limit", DEFAULT_ARTIFACT_LIMIT)), MAX_ARTIFACT_LIMIT)
            if limit <= 0:
                limit = DEFAULT_ARTIFACT_LIMIT
        except ValueError:
            limit = DEFAULT_ARTIFACT_LIMIT

        objects = list(_storage.list_objects(bucket, prefix=prefix, limit=limit))

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


def _parse_time_range(time_range_str: str | None) -> int | None:
    """Parse and validate time range parameter.

    Args:
        time_range_str: Time range in minutes as string

    Returns:
        Validated time range in minutes, or None if invalid

    """
    if not time_range_str:
        return None

    try:
        time_range_minutes = int(time_range_str)
        if time_range_minutes <= 0 or time_range_minutes > MAX_TIME_RANGE_MINUTES:
            return None
        return time_range_minutes
    except ValueError:
        return None


def _empty_metric_response(
    metric_name: str,
    algorithm: str | None,
    version: str | None,
    time_range_minutes: int | None,
) -> FlaskResponse:
    """Build an empty metric response for when no data is found."""
    return jsonify(
        {
            "metric_name": metric_name,
            "processor_name": algorithm or "",
            "processor_version": version or "",
            "aggregation_mask": 0,
            "aggregation_types": [],
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


def _build_metric_data_response(metric: MetricInfo, time_range_minutes: int | None) -> FlaskResponse:
    """Build the full metric data response with artifacts.

    Args:
        metric: MetricInfo object
        time_range_minutes: Time range filter in minutes

    Returns:
        JSON response with metric data and computed artifacts

    """
    # Get full artifact document
    artifact_doc = _discovery.get_metric_artifact(metric) if _discovery else None
    summary = metric.summary or {}

    # Build response with summary stats and any additional artifact data
    artifacts: dict[str, Any] = {}
    response: dict[str, Any] = {
        "metric_name": metric.metric_name,
        "processor_name": metric.processor_name,
        "processor_version": metric.processor_version,
        "aggregation_mask": metric.aggregation_mask,
        "aggregation_types": metric.aggregation_types,
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
    raw_values = _fetch_metric_values(metric, time_range_minutes, response)

    # Fall back to artifact document values
    if artifact_doc:
        _extract_artifact_data(artifact_doc, artifacts, raw_values)

    # Compute on-the-fly artifacts based on aggregation_mask and raw values
    if raw_values and _artifact_computer:
        artifacts["values"] = raw_values
        _compute_artifacts_for_mask(raw_values, response, artifacts)

    return jsonify(response)


def _fetch_metric_values(
    metric: MetricInfo,
    time_range_minutes: int | None,
    response: dict[str, Any],
) -> list[Any] | None:
    """Fetch raw metric values from the metric-values bucket.

    Args:
        metric: MetricInfo object
        time_range_minutes: Time range filter
        response: Response dict to update with aggregation info

    Returns:
        List of raw values, or None if not found

    """
    if _discovery is None:
        return None

    values_data = _discovery.get_metric_values_with_meta(
        processor_name=metric.processor_name,
        processor_version=metric.processor_version,
        metric_name=metric.metric_name,
        time_range_minutes=time_range_minutes,
    )

    if values_data.get("values"):
        raw_values: list[Any] = values_data["values"]
        if metric.aggregation_mask == 0 and values_data.get("aggregation_mask"):
            response["aggregation_mask"] = values_data["aggregation_mask"]
            response["aggregation_types"] = list(mask_to_kind_names(values_data["aggregation_mask"]))
        return raw_values

    return None


def _extract_artifact_data(
    artifact_doc: dict[str, Any],
    artifacts: dict[str, Any],
    raw_values: list[Any] | None,
) -> list[Any] | None:
    """Extract artifact data from the artifact document.

    Args:
        artifact_doc: Artifact document from MinIO
        artifacts: Artifacts dict to populate
        raw_values: Existing raw values (may be None)

    Returns:
        Updated raw values

    """
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

    return raw_values


def _compute_artifacts_for_mask(
    raw_values: list[Any],
    response: dict[str, Any],
    artifacts: dict[str, Any],
) -> None:
    """Compute artifacts on-the-fly based on aggregation mask.

    Args:
        raw_values: List of raw metric values
        response: Response dict with aggregation_mask
        artifacts: Artifacts dict to populate

    """
    if _artifact_computer is None:
        return

    aggregation_mask: int = response.get("aggregation_mask", 0)

    # Generate histogram for DISTRIBUTION_1D (mask value 2)
    if aggregation_mask & 2 and "histogram" not in artifacts:
        histogram = _artifact_computer.compute_histogram(raw_values)
        if histogram:
            artifacts["histogram"] = histogram

    # Generate categories for COUNTER (mask value 8)
    if aggregation_mask & 8 and "categories" not in artifacts:
        categories = _artifact_computer.compute_categories(raw_values)
        if categories:
            artifacts["categories"] = categories

    # Generate rate CI for RATE (mask value 16)
    if aggregation_mask & 16 and "rate" not in artifacts:
        rate_data = _artifact_computer.compute_rate(raw_values)
        if rate_data:
            artifacts["rate"] = rate_data

    # Generate ellipse data for ELLIPSE_2D (mask value 32)
    if aggregation_mask & 32 and "ellipse" not in artifacts:
        ellipse_data = _artifact_computer.compute_ellipse(raw_values)
        if ellipse_data:
            artifacts["ellipse"] = ellipse_data

    # Generate contour data for CONTOUR_2D (mask value 64)
    if aggregation_mask & 64 and "contour" not in artifacts:
        contour_data = _artifact_computer.compute_contour(raw_values)
        if contour_data:
            artifacts["contour"] = contour_data
