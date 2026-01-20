"""Discovery service for enumerating directories, algorithms, and metrics."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from src.domain.evaluation import AnalysisKind
from src.utils.logging import get_logger

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
    algo_name: str
    algo_version: str
    analysis_mask: int
    analysis_kinds: list[str] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)
    artifact_key: str | None = None

    def __post_init__(self) -> None:
        if not self.analysis_kinds:
            self.analysis_kinds = _mask_to_kind_names(self.analysis_mask)


def _mask_to_kind_names(mask: int) -> list[str]:
    """Convert AnalysisKind bitmask to list of kind names."""
    return [kind.name for kind in AnalysisKind if mask & kind.value and kind.name is not None]


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
        algo_name: str | None = None,
        algo_version: str | None = None,
        *,
        refresh: bool = False,
        time_range_minutes: int | None = None,
    ) -> list[MetricInfo]:
        """Discover available metrics by listing objects in the aggregates bucket.

        Parses JSON aggregate documents to extract metric information.

        Args:
            algo_name: Filter by algorithm name (optional)
            algo_version: Filter by algorithm version (optional)
            refresh: Force refresh of cached metrics
            time_range_minutes: Filter to only include windows within last N minutes

        Returns:
            List of MetricInfo objects for discovered metrics

        """
        import time as time_module

        cache_key = f"{algo_name or '*'}|{algo_version or '*'}|{time_range_minutes or '*'}"
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
            objects = self._storage.list_objects(bucket, prefix="", limit=10000)

            for obj in objects:
                try:
                    data = self._storage.retrieve_by_key(bucket, obj["key"])
                    doc = json.loads(data.decode("utf-8"))

                    # Apply time filter if specified
                    if time_cutoff is not None:
                        window_end = doc.get("window_end_unix")
                        if window_end is not None and window_end < time_cutoff:
                            continue

                    doc_algo = doc.get("algo_name", "")
                    doc_version = doc.get("algo_version", "")

                    if algo_name and doc_algo != algo_name:
                        continue
                    if algo_version and doc_version != algo_version:
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
                            "algo_name": doc_algo,
                            "algo_version": doc_version,
                            "analysis_mask": doc.get("analysis_mask", 0),
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
                        algo_name=agg["algo_name"],
                        algo_version=agg["algo_version"],
                        analysis_mask=agg["analysis_mask"],
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
        algo_name: str,
        algo_version: str,
        metric_name: str,
        time_range_minutes: int | None = None,
    ) -> list[Any]:
        """Retrieve raw metric values from the metric-values bucket.

        Args:
            algo_name: Algorithm name
            algo_version: Algorithm version
            metric_name: Metric name
            time_range_minutes: Filter to only include windows within last N minutes

        Returns:
            List of raw metric values aggregated across time windows

        """
        import time as time_module

        values: list[Any] = []
        analysis_mask = 0
        meta: dict[str, Any] | None = None

        # Calculate time cutoff
        time_cutoff: float | None = None
        if time_range_minutes is not None:
            time_cutoff = time_module.time() - (time_range_minutes * 60)

        try:
            bucket = self._storage.buckets.get("metric-values", "metric-values")
            objects = self._storage.list_objects(bucket, prefix="", limit=10000)

            for obj in objects:
                try:
                    data = self._storage.retrieve_by_key(bucket, obj["key"])
                    doc = json.loads(data.decode("utf-8"))

                    # Filter by metric identity
                    if doc.get("algo_name") != algo_name:
                        continue
                    if doc.get("algo_version") != algo_version:
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

                    # Capture analysis_mask and meta from first match
                    if analysis_mask == 0:
                        analysis_mask = doc.get("analysis_mask", 0)
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
        algo_name: str,
        algo_version: str,
        metric_name: str,
        time_range_minutes: int | None = None,
    ) -> dict[str, Any]:
        """Retrieve raw metric values with metadata.

        Checks both the metric-values bucket (from metric_values_collector)
        and the aggregates bucket (from Python aggregation which includes values).

        Args:
            algo_name: Algorithm name
            algo_version: Algorithm version
            metric_name: Metric name
            time_range_minutes: Filter to only include windows within last N minutes

        Returns:
            Dict with 'values', 'analysis_mask', and 'meta' keys

        """
        import time as time_module

        values: list[Any] = []
        analysis_mask = 0
        meta: dict[str, Any] | None = None

        # Calculate time cutoff
        time_cutoff: float | None = None
        if time_range_minutes is not None:
            time_cutoff = time_module.time() - (time_range_minutes * 60)

        # First, try the metric-values bucket
        try:
            bucket = self._storage.buckets.get("metric-values", "metric-values")
            objects = self._storage.list_objects(bucket, prefix="", limit=10000)

            for obj in objects:
                try:
                    data = self._storage.retrieve_by_key(bucket, obj["key"])
                    doc = json.loads(data.decode("utf-8"))

                    # Filter by metric identity
                    if doc.get("algo_name") != algo_name:
                        continue
                    if doc.get("algo_version") != algo_version:
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

                    # Capture analysis_mask and meta from first match
                    if analysis_mask == 0:
                        analysis_mask = doc.get("analysis_mask", 0)
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
            objects = self._storage.list_objects(bucket, prefix="", limit=10000)

            for obj in objects:
                try:
                    data = self._storage.retrieve_by_key(bucket, obj["key"])
                    doc = json.loads(data.decode("utf-8"))

                    # Filter by metric identity
                    if doc.get("algo_name") != algo_name:
                        continue
                    if doc.get("algo_version") != algo_version:
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

                        # Capture analysis_mask and meta if not already set
                        if analysis_mask == 0:
                            analysis_mask = doc.get("analysis_mask", 0)
                        if meta is None:
                            meta = doc.get("meta")

                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.debug("Failed to parse aggregate object %s: %s", obj["key"], e)
                    continue

        except Exception as e:
            logger.debug("No values in aggregates bucket: %s", e)

        return {
            "values": values,
            "analysis_mask": analysis_mask,
            "meta": meta,
        }

    def clear_cache(self) -> None:
        """Clear the metrics discovery cache."""
        self._metrics_cache.clear()
