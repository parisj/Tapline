from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any

from src.algorithms.base import Processor
from src.domain.evaluation import AggregationType
from src.domain.results import Measurement, ProcessorResult

if TYPE_CHECKING:
    from collections.abc import Mapping


class ModelInferenceProcessor(Processor):
    @property
    def name(self) -> str:
        return "model_inference"

    @property
    def version(self) -> str:
        return "0.1.0"

    def initialize(self, settings: Mapping[str, Any]) -> None:
        self._model_path = str(settings.get("model_path", "model.onnx"))

    def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> ProcessorResult:  # noqa: ARG002
        score = (hashlib.sha1(image_bytes[:64], usedforsecurity=False).digest()[0] / 255.0) if image_bytes else 0.0  # nosec B324
        return ProcessorResult(
            metrics={
                "score": Measurement(
                    score,
                    AggregationType.STATS | AggregationType.HISTOGRAM,
                ),
                "model_path": Measurement(
                    getattr(self, "_model_path", None),
                    AggregationType.RAW,
                ),
            },
        )


# Backwards compatibility alias
ModelInferenceAlgo = ModelInferenceProcessor
