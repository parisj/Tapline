from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any

from src.algorithms.base import Algorithm
from src.domain.evaluation import AnalysisKind
from src.domain.results import AlgoResult, MetricValue

if TYPE_CHECKING:
    from collections.abc import Mapping


class ModelInferenceAlgo(Algorithm):
    @property
    def name(self) -> str:
        return "model_inference"

    @property
    def version(self) -> str:
        return "0.1.0"

    def initialize(self, settings: Mapping[str, Any]) -> None:
        self._model_path = str(settings.get("model_path", "model.onnx"))

    def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> AlgoResult:
        score = (
            (hashlib.sha1(image_bytes[:64]).digest()[0] / 255.0) if image_bytes else 0.0
        )
        return AlgoResult(
            metrics={
                "score": MetricValue(
                    score, AnalysisKind.SUMMARY | AnalysisKind.DISTRIBUTION_1D,
                ),
                "model_path": MetricValue(
                    getattr(self, "_model_path", None), AnalysisKind.INFO,
                ),
            },
        )
