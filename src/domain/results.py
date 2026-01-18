from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.domain.evaluation import AnalysisKind
    from src.storage.minio_service import MinioStorageService
    from src.storage.models import ObjectRef


@dataclass(frozen=True)
class Artifact:
    """Binary artifact produced by an algorithm (mask, thumbnail, overlays, etc.).

    Can hold data directly (for immediate processing) or reference an
    object stored in MinIO (for persistence). Use object_ref for
    content-addressed storage with deduplication.
    """

    name: str
    mime: str
    data: bytes | None = None
    object_ref: ObjectRef | None = None

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
        if self.object_ref is not None and storage_service is not None:
            return storage_service.retrieve(self.object_ref)
        msg = "Artifact has no data and no storage service provided"
        raise ValueError(msg)

    @property
    def has_data(self) -> bool:
        """Check if artifact has inline data."""
        return self.data is not None

    @property
    def is_stored(self) -> bool:
        """Check if artifact is stored in MinIO."""
        return self.object_ref is not None

    @property
    def content_hash(self) -> str | None:
        """Get content hash if stored in MinIO."""
        return self.object_ref.content_hash if self.object_ref else None


@dataclass(frozen=True)
class AlgoResult:
    """Algorithm output, independent from persistence and threading concerns."""

    metrics:dict[str, MetricValue] = None
    artifacts: tuple[Artifact, ...] = ()


@dataclass(frozen=True)
class PersistedResultRef:
    """Reference returned after persistence. Keep small and stable."""

    job_id: str
    algo_name: str
    algo_version: str
    result_id: int


@dataclass(frozen=True)
class MetricValue:
    """A single metric value produced by an algorithm."""

    value: Any
    analysis: AnalysisKind
    meta: Mapping[str, Any] | None = None


def metrics_to_jsonable(metrics: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, mv in metrics.items():
        # MetricValue dataclass case
        if hasattr(mv, "value") and hasattr(mv, "analysis"):
            analysis = getattr(mv, "analysis", None)
            meta = getattr(mv, "meta", None) or {}
            out[name] = {
                "value": getattr(mv, "value", None),
                "analysis_mask": int(getattr(analysis, "value", None)),
                "meta": dict(meta) if isinstance(meta, Mapping) else {},
            }
            continue

        # Backward-compatible: plain scalar value
        out[name] = {"value": mv, "analysis_mask": 0, "meta": {}}
    return out
