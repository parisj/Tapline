"""Processor registry with lazy loading for faster startup."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from src.utils.logging import get_logger

if TYPE_CHECKING:
    from src.algorithms.base import Processor

logger = get_logger(__name__)

ProcessorFactory = Callable[[], "Processor"]


class ProcessorRegistry:
    """Registry mapping (name, version) to processor factory.

    Enables declarative routing via routes.toml without hardcoding
    processor selection in application code.
    """

    def __init__(self) -> None:
        self._factories: dict[tuple[str, str], ProcessorFactory] = {}

    def register(self, name: str, version: str, factory: ProcessorFactory) -> None:
        """Register a processor factory."""
        key = (name, version)
        if key in self._factories:
            logger.error("Attempt to re-register processor: %s %s", name, version)
            msg = f"Processor already registered: {name} {version}"
            raise ValueError(msg)
        self._factories[key] = factory

    def create(self, name: str, version: str) -> "Processor":
        """Create processor instance by name and version."""
        key = (name, version)
        if key not in self._factories:
            logger.error("Processor not found in registry: %s %s", name, version)
            raise KeyError(key)
        return self._factories[key]()


def build_default_registry() -> ProcessorRegistry:
    """Create registry with lazy-loaded processors.

    Processors are imported only when their factory is called,
    avoiding heavy dependencies at module load time.
    """
    reg = ProcessorRegistry()

    def make_analysis_probe() -> "Processor":
        from src.algorithms.cv.analysis_probe import AnalysisProbeProcessor

        return AnalysisProbeProcessor()

    def make_blob_detection() -> "Processor":
        from src.algorithms.cv.blob_detection import BlobDetectionProcessor

        return BlobDetectionProcessor()

    def make_image_quality() -> "Processor":
        from src.algorithms.cv.image_quality import ImageQualityProcessor

        return ImageQualityProcessor()

    def make_edge_detection() -> "Processor":
        from src.algorithms.cv.edge_detection import EdgeDetectionProcessor

        return EdgeDetectionProcessor()

    def make_histogram_analysis() -> "Processor":
        from src.algorithms.cv.histogram_analysis import HistogramAnalysisProcessor

        return HistogramAnalysisProcessor()

    def make_contour_analysis() -> "Processor":
        from src.algorithms.cv.contour_analysis import ContourAnalysisProcessor

        return ContourAnalysisProcessor()

    def make_model_inference() -> "Processor":
        from src.algorithms.models.inference import ModelInferenceProcessor

        return ModelInferenceProcessor()

    def make_yolo_segmentation() -> "Processor":
        from src.algorithms.models.yolo_segmentation import YoloSegmentationProcessor

        return YoloSegmentationProcessor()

    reg.register("analysis_probe", "1.0.0", make_analysis_probe)
    reg.register("blob_detection", "1.0.0", make_blob_detection)
    reg.register("image_quality", "1.0.0", make_image_quality)
    reg.register("edge_detection", "1.0.0", make_edge_detection)
    reg.register("histogram_analysis", "1.0.0", make_histogram_analysis)
    reg.register("contour_analysis", "1.0.0", make_contour_analysis)
    reg.register("model_inference", "0.1.0", make_model_inference)
    reg.register("model_yolo_segmentation", "1.0.0", make_yolo_segmentation)

    return reg


# Backwards compatibility aliases (deprecated)
AlgorithmFactory = ProcessorFactory
AlgorithmRegistry = ProcessorRegistry
