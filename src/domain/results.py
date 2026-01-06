from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    from src.domain.evaluation import AnalysisKind


@dataclass(frozen=True)
class Artifact:
    """Binary artifact produced by an algorithm (mask, thumbnail, overlays, etc.)."""

    name: str
    mime: str
    data: bytes


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
