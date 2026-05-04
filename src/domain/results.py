from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.domain.evaluation import AggregationType
    from src.storage.minio_service import MinioStorageService
    from src.storage.models import ArtifactRef


@dataclass(frozen=True)
class Artifact:
    """Binary artifact produced by a processor (mask, thumbnail, overlays, etc.).

    Can hold data directly (for immediate processing) or reference an
    object stored in MinIO (for persistence). Use artifact_ref for
    content-addressed storage with deduplication.
    """

    name: str
    mime: str
    data: bytes | None = None
    artifact_ref: ArtifactRef | None = None

    def get_data(self, storage_service: MinioStorageService | None = None) -> bytes:
        """Get artifact data, fetching from storage if needed.

        Args:
            storage_service: MinioStorageService for fetching remote data

        Returns:
            Binary artifact data

        Raises:
            ValueError: If no data and no storage service provided

        """
        if self.data is not None:
            return self.data
        if self.artifact_ref is not None and storage_service is not None:
            return storage_service.retrieve(self.artifact_ref)
        msg = "Artifact has no data and no storage service provided"
        raise ValueError(msg)

    @property
    def has_data(self) -> bool:
        """Check if artifact has inline data."""
        return self.data is not None

    @property
    def is_stored(self) -> bool:
        """Check if artifact is stored in MinIO."""
        return self.artifact_ref is not None

    @property
    def content_hash(self) -> str | None:
        """Get content hash if stored in MinIO."""
        return self.artifact_ref.content_hash if self.artifact_ref else None


@dataclass(frozen=True)
class ProcessorResult:
    """Processor output, independent from persistence and threading concerns."""

    metrics: dict[str, Measurement] | None = None
    artifacts: tuple[Artifact, ...] = ()


@dataclass(frozen=True)
class PersistedResultRef:
    """Reference returned after persistence. Keep small and stable."""

    task_id: str
    processor_name: str
    processor_version: str
    result_id: int


@dataclass(frozen=True)
class Measurement:
    """A single metric measurement produced by a processor."""

    value: Any
    aggregation: AggregationType
    meta: Mapping[str, Any] | None = None


def measurements_to_jsonable(measurements: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, m in measurements.items():
        # Measurement dataclass case
        if hasattr(m, "value") and hasattr(m, "aggregation"):
            aggregation = getattr(m, "aggregation", None)
            meta = getattr(m, "meta", None) or {}
            aggregation_value = getattr(aggregation, "value", 0)
            out[name] = {
                "value": getattr(m, "value", None),
                "aggregation_mask": (int(aggregation_value) if aggregation_value is not None else 0),
                "meta": dict(meta) if isinstance(meta, Mapping) else {},
            }
            continue

        # Backward-compatible: plain scalar value
        out[name] = {"value": m, "aggregation_mask": 0, "meta": {}}
    return out


# Backwards compatibility aliases (deprecated)
AlgoResult = ProcessorResult
MetricValue = Measurement
metrics_to_jsonable = measurements_to_jsonable

# Backwards compatibility for old field name
Artifact.__annotations__["object_ref"] = "ArtifactRef | None"
