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
    return [kind.name for kind in AnalysisKind if mask & kind.value]


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
    ) -> list[MetricInfo]:
        """Discover available metrics by listing objects in the aggregates bucket.

        Parses JSON aggregate documents to extract metric information.

        Args:
            algo_name: Filter by algorithm name (optional)
            algo_version: Filter by algorithm version (optional)
            refresh: Force refresh of cached metrics

        Returns:
            List of MetricInfo objects for discovered metrics

        """
        cache_key = f"{algo_name or '*'}|{algo_version or '*'}"
        if not refresh and cache_key in self._metrics_cache:
            return self._metrics_cache[cache_key]

        metrics: list[MetricInfo] = []
        seen: set[str] = set()

        try:
            bucket = self._storage.buckets["aggregates"]
            objects = self._storage.list_objects(bucket, prefix="", limit=10000)

            for obj in objects:
                try:
                    data = self._storage.retrieve_by_key(bucket, obj["key"])
                    doc = json.loads(data.decode("utf-8"))

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
                    if metric_key in seen:
                        continue
                    seen.add(metric_key)

                    metrics.append(
                        MetricInfo(
                            metric_name=metric_name,
                            algo_name=doc_algo,
                            algo_version=doc_version,
                            analysis_mask=doc.get("analysis_mask", 0),
                            summary=doc.get("summary", {}),
                            artifact_key=obj["key"],
                        ),
                    )
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.debug("Failed to parse aggregate object %s: %s", obj["key"], e)
                    continue

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
            return json.loads(data.decode("utf-8"))
        except Exception as e:
            logger.warning("Failed to retrieve metric artifact: %s", e)
            return None

    def clear_cache(self) -> None:
        """Clear the metrics discovery cache."""
        self._metrics_cache.clear()
