"""Task and artifact viewer routes."""

from __future__ import annotations

import base64
import json
from typing import TYPE_CHECKING, Any

from flask import Blueprint, Response, jsonify, request
from minio.error import S3Error

from src.storage.models import ObjectRef
from src.utils.logging import get_logger
from src.visualization.constants import (
    ALLOWED_BUCKETS,
    DEFAULT_TASK_LIMIT,
    MAX_TASK_LIMIT,
    get_mime_from_extension,
    validate_task_id,
)

if TYPE_CHECKING:
    from src.storage.minio_service import MinioStorageService
    from src.visualization.discovery import DiscoveryService
    from src.visualization.viewers import ViewerRegistry

logger = get_logger(__name__)

# Type alias for Flask route responses
FlaskResponse = Response | tuple[Response, int]

artifacts_bp = Blueprint("artifacts", __name__, url_prefix="/api")

# Services injected via init_blueprint
_storage: MinioStorageService | None = None
_discovery: DiscoveryService | None = None
_viewer_registry: ViewerRegistry | None = None


def init_blueprint(
    storage: MinioStorageService | None,
    discovery: DiscoveryService | None,
    viewer_registry: ViewerRegistry | None,
) -> None:
    """Initialize the blueprint with required services.

    Args:
        storage: MinIO storage service instance
        discovery: Discovery service instance
        viewer_registry: Viewer registry for rendering artifacts

    """
    global _storage, _discovery, _viewer_registry
    _storage = storage
    _discovery = discovery
    _viewer_registry = viewer_registry


@artifacts_bp.route("/artifact/<bucket>/<path:key>", methods=["GET"])
def get_artifact(bucket: str, key: str) -> FlaskResponse:
    """Get a specific artifact's metadata and content.

    For images (image/png, image/jpeg), returns base64-encoded data.
    For JSON, returns parsed content.
    For other binary, returns metadata only.

    Query params:
        raw: If "true", return raw binary data with appropriate Content-Type
    """
    if _storage is None:
        return jsonify({"error": "Storage service not initialized"}), 503

    # Security: validate bucket name against allowlist
    if bucket not in ALLOWED_BUCKETS:
        return jsonify({"error": "Invalid bucket"}), 400

    # Security: validate key doesn't contain path traversal
    if ".." in key or key.startswith("/"):
        return jsonify({"error": "Invalid key"}), 400

    try:
        # Get object info for mime type
        obj_ref = ObjectRef.from_hash(
            content_hash=key.split("/")[-1],
            bucket=bucket,
            size=0,
        )
        obj_info = _storage.get_object_info(obj_ref)
        content_type = obj_info.get("content_type", "application/octet-stream") if obj_info else None
        metadata = obj_info.get("metadata", {}) if obj_info else {}

        # Get object data
        data = _storage.retrieve_by_key(bucket, key)
        if data is None:
            return jsonify({"error": "Artifact not found"}), 404

        # Check if raw binary response requested
        if request.args.get("raw") == "true":
            return Response(
                data,
                mimetype=content_type or "application/octet-stream",
                headers={"Content-Disposition": f"inline; filename={key.split('/')[-1]}"},
            )

        # Handle based on content type
        content = _process_artifact_content(data, content_type)

        return jsonify(
            {
                "bucket": bucket,
                "key": key,
                "content": content,
                "metadata": metadata,
            },
        )
    except Exception as e:
        logger.exception("Failed to get artifact")
        return jsonify({"error": str(e)}), 500


@artifacts_bp.route("/tasks", methods=["GET"])
def list_tasks() -> FlaskResponse:
    """List recent tasks with their artifacts.

    Query parameters:
        processor: Filter by processor name
        limit: Maximum number of tasks to return (default 50, max 200)
        offset: Number of tasks to skip for pagination (default 0)
    """
    if _discovery is None:
        return jsonify({"error": "Discovery service not initialized"}), 503

    try:
        processor_name = request.args.get("processor")

        # Validate and bound limit parameter
        try:
            limit = min(int(request.args.get("limit", DEFAULT_TASK_LIMIT)), MAX_TASK_LIMIT)
            if limit <= 0:
                limit = DEFAULT_TASK_LIMIT
        except ValueError:
            limit = DEFAULT_TASK_LIMIT

        # Validate offset parameter
        try:
            offset = max(int(request.args.get("offset", 0)), 0)
        except ValueError:
            offset = 0

        tasks = _discovery.list_recent_tasks(
            processor_name=processor_name,
            limit=limit,
            offset=offset,
        )

        return jsonify(
            {
                "tasks": tasks,
                "count": len(tasks),
            },
        )
    except Exception as e:
        logger.exception("Failed to list tasks")
        return jsonify({"error": str(e)}), 500


@artifacts_bp.route("/tasks/<task_id>/artifacts", methods=["GET"])
def list_task_artifacts(task_id: str) -> FlaskResponse:
    """List all artifacts for a specific task."""
    if _discovery is None:
        return jsonify({"error": "Discovery service not initialized"}), 503

    # Security: validate task_id format
    if not validate_task_id(task_id):
        return jsonify({"error": "Invalid task ID format"}), 400

    try:
        artifacts = _discovery.list_task_artifacts(task_id)

        return jsonify(
            {
                "task_id": task_id,
                "artifacts": artifacts,
                "count": len(artifacts),
            },
        )
    except Exception as e:
        logger.exception("Failed to list task artifacts")
        return jsonify({"error": str(e)}), 500


@artifacts_bp.route("/artifacts/view/<bucket>/<path:key>", methods=["GET"])
def view_artifact(bucket: str, key: str) -> FlaskResponse:
    """View an artifact using the appropriate viewer.

    Returns viewer-ready data (Plotly config, base64 image, JSON, etc.)
    based on the artifact type and aggregation mask.

    Query parameters:
        aggregation_mask: Optional aggregation mask for specialized viewing
    """
    if _storage is None or _viewer_registry is None:
        return jsonify({"error": "Services not initialized"}), 503

    # Security: validate bucket name against allowlist
    if bucket not in ALLOWED_BUCKETS:
        return jsonify({"error": "Invalid bucket"}), 400

    # Security: validate key doesn't contain path traversal
    if ".." in key or key.startswith("/"):
        return jsonify({"error": "Invalid key"}), 400

    try:
        # Get artifact data
        data = _storage.retrieve_by_key(bucket, key)
        if data is None:
            return jsonify({"error": "Artifact not found"}), 404

        # Get object metadata and determine MIME type
        metadata, stored_mime = _get_artifact_metadata_for_view(bucket, key)

        # Determine MIME type: prefer stored content_type, fall back to extension
        if stored_mime and stored_mime != "application/octet-stream":
            mime = stored_mime
        else:
            name_for_mime = metadata.get("artifact_name", key)
            mime = get_mime_from_extension(name_for_mime)

        # Get aggregation mask from query param
        try:
            aggregation_mask = int(request.args.get("aggregation_mask", 0))
        except ValueError:
            aggregation_mask = 0

        # Use viewer registry to render the artifact
        result = _viewer_registry.view(
            data=data,
            mime=mime,
            aggregation_mask=aggregation_mask,
            metadata=metadata,
        )

        return jsonify(
            {
                "viewer_type": result.viewer_type,
                "render_type": result.render_type,
                "data": result.data,
                "metadata": result.metadata,
                "error": result.error,
            },
        )
    except Exception as e:
        logger.exception("Failed to view artifact")
        return jsonify({"error": str(e)}), 500


@artifacts_bp.route("/processors", methods=["GET"])
def list_processors() -> FlaskResponse:
    """List all processors that have produced artifacts.

    Returns unique processor names from the artifacts bucket.
    """
    if _discovery is None:
        return jsonify({"error": "Discovery service not initialized"}), 503

    try:
        # Get all tasks and extract unique processor names
        tasks = _discovery.list_recent_tasks(limit=1000)
        processors: dict[str, dict[str, Any]] = {}

        for task in tasks:
            proc_name = task.get("processor_name", "")
            proc_version = task.get("processor_version", "")
            key = f"{proc_name}|{proc_version}"

            if key not in processors:
                processors[key] = {
                    "name": proc_name,
                    "version": proc_version,
                    "task_count": 0,
                }
            processors[key]["task_count"] += 1

        return jsonify(
            {
                "processors": list(processors.values()),
                "count": len(processors),
            },
        )
    except Exception as e:
        logger.exception("Failed to list processors")
        return jsonify({"error": str(e)}), 500


def _process_artifact_content(data: bytes, content_type: str | None) -> dict[str, Any]:
    """Process artifact content based on content type.

    Args:
        data: Raw artifact data
        content_type: MIME content type

    Returns:
        Dict with processed content

    """
    # Image types - return base64 encoded
    if content_type and content_type.startswith("image/"):
        return {
            "type": "image",
            "mime": content_type,
            "size": len(data),
            "data_base64": base64.b64encode(data).decode("ascii"),
        }

    # JSON content
    if content_type == "application/json" or (content_type is None and data[:1] in (b"{", b"[")):
        try:
            return {
                "type": "json",
                "mime": "application/json",
                "size": len(data),
                "data": json.loads(data.decode("utf-8")),
            }
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {"type": "binary", "mime": content_type, "size": len(data)}

    # Other binary
    return {"type": "binary", "mime": content_type, "size": len(data)}


def _get_artifact_metadata_for_view(bucket: str, key: str) -> tuple[dict[str, Any], str | None]:
    """Get artifact metadata for viewing.

    Args:
        bucket: Bucket name
        key: Object key

    Returns:
        Tuple of (metadata dict, stored MIME type)

    """
    metadata: dict[str, Any] = {"key": key, "bucket": bucket}
    stored_mime: str | None = None

    if _storage is None:
        return metadata, stored_mime

    try:
        stat = _storage._client.stat_object(bucket, key)
        obj_metadata = stat.metadata or {}
        metadata["task_id"] = obj_metadata.get("x-amz-meta-task_id", "")
        metadata["artifact_name"] = obj_metadata.get("x-amz-meta-name", key.split("/")[-1])
        metadata["processor_name"] = obj_metadata.get("x-amz-meta-processor_name", "")
        metadata["processor_version"] = obj_metadata.get("x-amz-meta-processor_version", "")
        stored_mime = stat.content_type
    except S3Error as e:
        logger.debug("Failed to get artifact metadata for %s/%s: %s", bucket, key, e)

    return metadata, stored_mime
