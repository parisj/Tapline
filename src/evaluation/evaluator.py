# src/evaluation/evaluator.py
from __future__ import annotations

import json
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from src.domain.evaluation import AnalysisKind
from src.evaluation.pipeline import ANALYSIS_PIPELINE
from src.utils.hashing import make_aggregate_artifact_hash

if TYPE_CHECKING:
    from src.persistence.service import PersistenceService


class EvaluationModule:
    """Computes aggregates for a time window and persists summaries + optional artifacts."""

    def __init__(self, persistence: PersistenceService) -> None:
        self._persistence = persistence

    def run_window(self, window_start_unix: float, window_end_unix: float) -> None:
        rows = self._persistence.fetch_completed_results(window_start_unix, window_end_unix)

        values_by_metric: defaultdict[tuple[str, str, str], list[Any]] = defaultdict(list)
        meta_by_metric: dict[tuple[str, str, str], dict[str, Any]] = {}
        mask_by_metric: dict[tuple[str, str, str], int] = {}

        for r in rows:
            algo_name = r["algo_name"]
            algo_version = r["algo_version"]

            metrics_doc = r["metrics_json"]
            if isinstance(metrics_doc, str):
                metrics_doc = json.loads(metrics_doc)

            if not isinstance(metrics_doc, dict):
                continue

            for metric_name, payload in metrics_doc.items():
                if not isinstance(payload, dict) or "value" not in payload:
                    continue

                key = (algo_name, algo_version, metric_name)
                values_by_metric[key].append(payload["value"])

                meta = payload.get("meta") or {}
                if isinstance(meta, dict):
                    meta_by_metric[key] = meta

                mask = int(payload.get("analysis_mask", 0))
                mask_by_metric[key] = mask_by_metric.get(key, 0) | mask

        # Run analyzers per metric
        for (algo_name, algo_version, metric_name), values in values_by_metric.items():
            meta = meta_by_metric.get((algo_name, algo_version, metric_name), {})
            mask = AnalysisKind(mask_by_metric.get((algo_name, algo_version, metric_name), 0))

            for kind, factories in ANALYSIS_PIPELINE.items():
                if not (mask & kind):
                    continue

                for factory in factories:
                    analyzer = factory()
                    result = analyzer.run(values=values, meta=meta)

                    summary = result.get("summary", {})
                    artifact = result.get("artifact")

                    artifact_hash: str | None = None
                    if artifact is not None:
                        artifact_hash = make_aggregate_artifact_hash(
                            algo_name=algo_name,
                            algo_version=algo_version,
                            metric_name=metric_name,
                            analysis_kind=kind.name,
                            window_start_unix=window_start_unix,
                            window_end_unix=window_end_unix,
                            artifact_name=artifact.name,
                        )

                        self._persistence.persist_aggregate_artifact(
                            artifact_hash=artifact_hash,
                            algo_name=algo_name,
                            algo_version=algo_version,
                            metric_name=metric_name,
                            analysis_kind=kind.name,
                            window_start_unix=window_start_unix,
                            window_end_unix=window_end_unix,
                            artifact=artifact,
                        )

                    self._persistence.persist_aggregate(
                        algo_name=algo_name,
                        algo_version=algo_version,
                        metric_name=metric_name,
                        analysis_kind=kind.name,
                        window_start_unix=window_start_unix,
                        window_end_unix=window_end_unix,
                        summary=summary,
                        artifact_hash=artifact_hash,
                    )
