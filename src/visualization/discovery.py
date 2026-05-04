"""Discovery service for enumerating directories, algorithms, and metrics."""

from __future__ import annotations

import json
import time as time_module
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from src.domain.evaluation import mask_to_kind_names
from src.utils.logging import get_logger
from src.visualization.constants import MINIO_MAX_OBJECTS, get_mime_from_extension

if TYPE_CHECKING:
    from pathlib import Path

    from src.config.loader import RuntimeConfig
    from src.dispatch.routes import LoadedRoute
    from src.storage.minio_service import MinioStorageService

logger = get_logger(__name__)


@dataclass(frozen=True)
class DirectoryInfo:
    """Information about a configured directory."""

    key: str
    path: Path


@dataclass(frozen=True)
class AlgorithmInfo:
    """Information about an algorithm assignment."""

    name: str
    version: str
    settings_path: str


@dataclass
class MetricInfo:
    """Information about a discovered metric."""

    metric_name: str
    processor_name: str
    processor_version: str
    aggregation_mask: int
    aggregation_types: list[str] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)
    artifact_key: str | None = None

    def __post_init__(self) -> None:
        if not self.aggregation_types:
            self.aggregation_types = list(mask_to_kind_names(self.aggregation_mask))


class DiscoveryService:
    """Service for discovering available directories, algorithms, and metrics.

    Reads configuration files and queries MinIO to enumerate what data
    is available for visualization.
    """

    def __init__(
        self,
        runtime_config: RuntimeConfig,
        routes: dict[str, LoadedRoute],
        storage: MinioStorageService,
    ) -> None:
        self._runtime_config = runtime_config
        self._routes = routes
        self._storage = storage
        self._metrics_cache: dict[str, list[MetricInfo]] = {}

    def list_directories(self) -> list[DirectoryInfo]:
        """Return all configured directories from pipeline.toml."""
        return [DirectoryInfo(key=key, path=path) for key, path in self._runtime_config.directories.items()]

    def get_algorithm_for_directory(self, directory_key: str) -> AlgorithmInfo | None:
        """Look up algorithm + version from routes.toml for a directory."""
        route = self._routes.get(directory_key)
        if route is None:
            return None
        return AlgorithmInfo(
            name=route.algorithm,
            version=route.version,
            settings_path=route.settings_relpath,
        )

    def list_algorithms(self) -> list[AlgorithmInfo]:
        """Return all unique algorithms from routes."""
        seen: set[tuple[str, str]] = set()
        algorithms: list[AlgorithmInfo] = []
        for route in self._routes.values():
            key = (route.algorithm, route.version)
            if key not in seen:
                seen.add(key)
                algorithms.append(
                    AlgorithmInfo(
                        name=route.algorithm,
                        version=route.version,
                        settings_path=route.settings_relpath,
                    ),
                )
        return algorithms

    def discover_metrics_from_minio(
        self,
        processor_name: str | None = None,
        processor_version: str | None = None,
        *,
        refresh: bool = False,
        time_range_minutes: int | None = None,
    ) -> list[MetricInfo]:
        """Discover available metrics by listing objects in the aggregates bucket.

        Parses JSON aggregate documents to extract metric information.

        Args:
            processor_name: Filter by algorithm name (optional)
            processor_version: Filter by algorithm version (optional)
            refresh: Force refresh of cached metrics
            time_range_minutes: Filter to only include windows within last N minutes

        Returns:
            List of MetricInfo objects for discovered metrics

        """
        cache_key = f"{processor_name or '*'}|{processor_version or '*'}|{time_range_minutes or '*'}"
        if not refresh and cache_key in self._metrics_cache:
            return self._metrics_cache[cache_key]

        metrics: list[MetricInfo] = []
        # Aggregate stats across all time windows for each unique metric
        aggregated: dict[str, dict[str, Any]] = {}

        # Calculate time cutoff if time_range_minutes specified
        time_cutoff: float | None = None
        if time_range_minutes is not None:
            time_cutoff = time_module.time() - (time_range_minutes * 60)

        try:
            bucket = self._storage.buckets["aggregates"]
            objects = self._storage.list_objects(bucket, prefix="", limit=MINIO_MAX_OBJECTS)

            for obj in objects:
                try:
                    data = self._storage.retrieve_by_key(bucket, obj["key"])
                    doc = json.loads(data.decode("utf-8"))

                    # Apply time filter if specified
                    if time_cutoff is not None:
                        window_end = doc.get("window_end_unix")
                        if window_end is not None and window_end < time_cutoff:
                            continue

                    doc_algo = doc.get("processor_name", "")
                    doc_version = doc.get("processor_version", "")

                    if processor_name and doc_algo != processor_name:
                        continue
                    if processor_version and doc_version != processor_version:
                        continue

                    metric_name = doc.get("metric_name", "")
                    if not metric_name:
                        continue

                    metric_key = f"{doc_algo}|{doc_version}|{metric_name}"
                    summary = doc.get("summary", {})

                    if metric_key not in aggregated:
                        # Initialize with first occurrence
                        aggregated[metric_key] = {
                            "metric_name": metric_name,
                            "processor_name": doc_algo,
                            "processor_version": doc_version,
                            "aggregation_mask": doc.get("aggregation_mask", 0),
                            "artifact_key": obj["key"],
                            "count": summary.get("count", 0),
                            "sum": summary.get("sum", 0),
                            "min": summary.get("min"),
                            "max": summary.get("max"),
                        }
                    else:
                        # Aggregate with existing data
                        existing = aggregated[metric_key]
                        existing["count"] = (existing.get("count") or 0) + (summary.get("count") or 0)
                        existing["sum"] = (existing.get("sum") or 0) + (summary.get("sum") or 0)
                        if summary.get("min") is not None:
                            if existing["min"] is None:
                                existing["min"] = summary["min"]
                            else:
                                existing["min"] = min(existing["min"], summary["min"])
                        if summary.get("max") is not None:
                            if existing["max"] is None:
                                existing["max"] = summary["max"]
                            else:
                                existing["max"] = max(existing["max"], summary["max"])
                        # Keep latest artifact key
                        existing["artifact_key"] = obj["key"]

                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.debug("Failed to parse aggregate object %s: %s", obj["key"], e)
                    continue

            # Convert aggregated data to MetricInfo objects
            for agg in aggregated.values():
                count = agg.get("count", 0)
                total = agg.get("sum", 0)
                avg = total / count if count > 0 else None
                metrics.append(
                    MetricInfo(
                        metric_name=agg["metric_name"],
                        processor_name=agg["processor_name"],
                        processor_version=agg["processor_version"],
                        aggregation_mask=agg["aggregation_mask"],
                        summary={
                            "count": count,
                            "sum": total,
                            "avg": avg,
                            "min": agg["min"],
                            "max": agg["max"],
                        },
                        artifact_key=agg["artifact_key"],
                    ),
                )

        except Exception as e:
            logger.warning("Failed to discover metrics from MinIO: %s", e)

        self._metrics_cache[cache_key] = metrics
        return metrics

    def get_metric_artifact(self, metric: MetricInfo) -> dict[str, Any] | None:
        """Retrieve the full aggregate document for a metric.

        Args:
            metric: MetricInfo with artifact_key

        Returns:
            Parsed JSON document or None if not found

        """
        if not metric.artifact_key:
            return None

        try:
            bucket = self._storage.buckets["aggregates"]
            data = self._storage.retrieve_by_key(bucket, metric.artifact_key)
            result: dict[str, Any] = json.loads(data.decode("utf-8"))
            return result
        except Exception as e:
            logger.warning("Failed to retrieve metric artifact: %s", e)
            return None

    def get_metric_values(
        self,
        processor_name: str,
        processor_version: str,
        metric_name: str,
        time_range_minutes: int | None = None,
    ) -> list[Any]:
        """Retrieve raw metric values from the metric-values bucket.

        Args:
            processor_name: Algorithm name
            processor_version: Algorithm version
            metric_name: Metric name
            time_range_minutes: Filter to only include windows within last N minutes

        Returns:
            List of raw metric values aggregated across time windows

        """
        values: list[Any] = []
        aggregation_mask = 0
        meta: dict[str, Any] | None = None

        # Calculate time cutoff
        time_cutoff: float | None = None
        if time_range_minutes is not None:
            time_cutoff = time_module.time() - (time_range_minutes * 60)

        try:
            bucket = self._storage.buckets.get("metric-values", "metric-values")
            objects = self._storage.list_objects(bucket, prefix="", limit=MINIO_MAX_OBJECTS)

            for obj in objects:
                try:
                    data = self._storage.retrieve_by_key(bucket, obj["key"])
                    doc = json.loads(data.decode("utf-8"))

                    # Filter by metric identity
                    if doc.get("processor_name") != processor_name:
                        continue
                    if doc.get("processor_version") != processor_version:
                        continue
                    if doc.get("metric_name") != metric_name:
                        continue

                    # Apply time filter
                    if time_cutoff is not None:
                        window_end = doc.get("window_end_unix")
                        if window_end is not None and window_end < time_cutoff:
                            continue

                    # Collect values
                    doc_values = doc.get("values", [])
                    values.extend(doc_values)

                    # Capture aggregation_mask and meta from first match
                    if aggregation_mask == 0:
                        aggregation_mask = doc.get("aggregation_mask", 0)
                    if meta is None:
                        meta = doc.get("meta")

                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.debug("Failed to parse metric values object %s: %s", obj["key"], e)
                    continue

        except Exception as e:
            logger.warning("Failed to retrieve metric values from MinIO: %s", e)

        return values

    def get_metric_values_with_meta(
        self,
        processor_name: str,
        processor_version: str,
        metric_name: str,
        time_range_minutes: int | None = None,
    ) -> dict[str, Any]:
        """Retrieve raw metric values with metadata.

        Checks both the metric-values bucket (from metric_values_collector)
        and the aggregates bucket (from Python aggregation which includes values).

        Args:
            processor_name: Algorithm name
            processor_version: Algorithm version
            metric_name: Metric name
            time_range_minutes: Filter to only include windows within last N minutes

        Returns:
            Dict with 'values', 'aggregation_mask', and 'meta' keys

        """
        values: list[Any] = []
        aggregation_mask = 0
        meta: dict[str, Any] | None = None

        # Calculate time cutoff
        time_cutoff: float | None = None
        if time_range_minutes is not None:
            time_cutoff = time_module.time() - (time_range_minutes * 60)

        # First, try the metric-values bucket
        try:
            bucket = self._storage.buckets.get("metric-values", "metric-values")
            objects = self._storage.list_objects(bucket, prefix="", limit=MINIO_MAX_OBJECTS)

            for obj in objects:
                try:
                    data = self._storage.retrieve_by_key(bucket, obj["key"])
                    doc = json.loads(data.decode("utf-8"))

                    # Filter by metric identity
                    if doc.get("processor_name") != processor_name:
                        continue
                    if doc.get("processor_version") != processor_version:
                        continue
                    if doc.get("metric_name") != metric_name:
                        continue

                    # Apply time filter
                    if time_cutoff is not None:
                        window_end = doc.get("window_end_unix")
                        if window_end is not None and window_end < time_cutoff:
                            continue

                    # Collect values
                    doc_values = doc.get("values", [])
                    values.extend(doc_values)

                    # Capture aggregation_mask and meta from first match
                    if aggregation_mask == 0:
                        aggregation_mask = doc.get("aggregation_mask", 0)
                    if meta is None:
                        meta = doc.get("meta")

                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.debug("Failed to parse metric values object %s: %s", obj["key"], e)
                    continue

        except Exception as e:
            logger.debug("No values in metric-values bucket: %s", e)

        # Also check the aggregates bucket (Python aggregation stores values there)
        try:
            bucket = self._storage.buckets.get("aggregates", "aggregates")
            objects = self._storage.list_objects(bucket, prefix="", limit=MINIO_MAX_OBJECTS)

            for obj in objects:
                try:
                    data = self._storage.retrieve_by_key(bucket, obj["key"])
                    doc = json.loads(data.decode("utf-8"))

                    # Filter by metric identity
                    if doc.get("processor_name") != processor_name:
                        continue
                    if doc.get("processor_version") != processor_version:
                        continue
                    if doc.get("metric_name") != metric_name:
                        continue

                    # Apply time filter
                    if time_cutoff is not None:
                        window_end = doc.get("window_end_unix")
                        if window_end is not None and window_end < time_cutoff:
                            continue

                    # Collect values (from Python aggregation, values are stored in aggregate)
                    doc_values = doc.get("values", [])
                    if doc_values:
                        values.extend(doc_values)

                        # Capture aggregation_mask and meta if not already set
                        if aggregation_mask == 0:
                            aggregation_mask = doc.get("aggregation_mask", 0)
                        if meta is None:
                            meta = doc.get("meta")

                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.debug("Failed to parse aggregate object %s: %s", obj["key"], e)
                    continue

        except Exception as e:
            logger.debug("No values in aggregates bucket: %s", e)

        return {
            "values": values,
            "aggregation_mask": aggregation_mask,
            "meta": meta,
        }

    def clear_cache(self) -> None:
        """Clear the metrics discovery cache."""
        self._metrics_cache.clear()

    def _get_artifact_metadata(self, bucket: str, key: str) -> dict[str, Any]:
        """Get metadata for an artifact from MinIO.

        Args:
            bucket: Bucket name
            key: Object key

        Returns:
            Dict with task_id, name, processor info, and other metadata

        """
        try:
            stat = self._storage._client.stat_object(bucket, key)
            # MinIO stores user metadata with lowercase keys and x-amz-meta- prefix
            metadata = stat.metadata or {}
            # Convert datetime to ISO format string for JSON serialization
            last_modified = stat.last_modified
            last_modified_str = last_modified.isoformat() if last_modified else None
            return {
                "task_id": metadata.get("x-amz-meta-task_id", ""),
                "name": metadata.get("x-amz-meta-name", key.split("/")[-1]),
                "processor_name": metadata.get("x-amz-meta-processor_name", ""),
                "processor_version": metadata.get("x-amz-meta-processor_version", ""),
                "content_type": stat.content_type,
                "size": stat.size,
                "last_modified": last_modified_str,
            }
        except Exception as e:
            logger.debug("Failed to get metadata for %s/%s: %s", bucket, key, e)
            return {}

    def list_recent_tasks(
        self,
        processor_name: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """List recent tasks from the artifacts bucket.

        Tasks are discovered by reading artifact metadata stored by workers.

        Args:
            processor_name: Filter by processor name (optional)
            limit: Maximum number of tasks to return
            offset: Number of tasks to skip for pagination

        Returns:
            List of task info dicts with task_id, processor, artifacts, etc.

        """
        tasks: dict[str, dict[str, Any]] = {}

        try:
            bucket = self._storage.buckets.get("artifacts", "artifacts")
            objects = self._storage.list_objects(bucket, prefix="", limit=MINIO_MAX_OBJECTS)

            for obj in objects:
                key = obj.get("key", "")
                # Get metadata from object (contains task_id, name, processor info)
                metadata = self._get_artifact_metadata(bucket, key)
                task_id = metadata.get("task_id", "")
                artifact_name = metadata.get("name", "")
                proc_name = metadata.get("processor_name", "")
                proc_version = metadata.get("processor_version", "")

                # Skip objects without task_id metadata
                if not task_id:
                    continue

                # Apply processor filter
                if processor_name and proc_name and proc_name != processor_name:
                    continue

                # Get mime type from content_type or guess from name
                content_type = metadata.get("content_type", "")
                mime = content_type if content_type else self._guess_mime_type(artifact_name)

                if task_id not in tasks:
                    tasks[task_id] = {
                        "task_id": task_id,
                        "processor_name": proc_name,
                        "processor_version": proc_version,
                        "artifacts": [],
                        "status": "completed",
                        "timestamp": metadata.get("last_modified"),
                    }
                else:
                    # Update processor info if not already set
                    if proc_name and not tasks[task_id].get("processor_name"):
                        tasks[task_id]["processor_name"] = proc_name
                    if proc_version and not tasks[task_id].get("processor_version"):
                        tasks[task_id]["processor_version"] = proc_version

                tasks[task_id]["artifacts"].append(
                    {
                        "name": artifact_name,
                        "key": key,
                        "bucket": bucket,
                        "size": metadata.get("size", 0),
                        "mime": mime,
                    },
                )
                # Update timestamp to latest
                if metadata.get("last_modified"):
                    existing_ts = tasks[task_id].get("timestamp")
                    if not existing_ts or metadata["last_modified"] > existing_ts:
                        tasks[task_id]["timestamp"] = metadata["last_modified"]

        except Exception as e:
            logger.warning("Failed to list tasks: %s", e)

        # Sort by timestamp (most recent first), apply offset and limit
        sorted_tasks = sorted(
            tasks.values(),
            key=lambda t: t.get("timestamp") or "",
            reverse=True,
        )
        return sorted_tasks[offset : offset + limit]

    def list_task_artifacts(self, task_id: str) -> list[dict[str, Any]]:
        """List all artifacts for a specific task.

        Args:
            task_id: The task ID to look up

        Returns:
            List of artifact info dicts with name, key, size, mime type

        """
        artifacts: list[dict[str, Any]] = []

        try:
            bucket = self._storage.buckets.get("artifacts", "artifacts")
            objects = self._storage.list_objects(bucket, prefix="", limit=MINIO_MAX_OBJECTS)

            for obj in objects:
                key = obj.get("key", "")
                # Get metadata from object
                metadata = self._get_artifact_metadata(bucket, key)
                obj_task_id = metadata.get("task_id", "")

                # Only include artifacts belonging to this task
                if obj_task_id == task_id:
                    artifact_name = metadata.get("name", key.split("/")[-1])
                    content_type = metadata.get("content_type", "")
                    mime = content_type if content_type else self._guess_mime_type(artifact_name)
                    artifacts.append(
                        {
                            "name": artifact_name,
                            "key": key,
                            "bucket": bucket,
                            "size": metadata.get("size", 0),
                            "mime": mime,
                            "last_modified": metadata.get("last_modified"),
                        },
                    )

        except Exception as e:
            logger.warning("Failed to list task artifacts: %s", e)

        return artifacts

    def _guess_mime_type(self, filename: str) -> str:
        """Guess MIME type from filename extension.

        Args:
            filename: Artifact filename

        Returns:
            MIME type string

        """
        return get_mime_from_extension(filename)
