"""Algorithm registry with lazy loading for faster startup."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from src.utils.logging import get_logger

if TYPE_CHECKING:
    from src.algorithms.base import Algorithm

logger = get_logger(__name__)

AlgorithmFactory = Callable[[], "Algorithm"]


class AlgorithmRegistry:
    """Registry mapping (name, version) to algorithm factory.

    Enables declarative routing via routes.toml without hardcoding
    algorithm selection in application code.
    """

    def __init__(self) -> None:
        self._factories: dict[tuple[str, str], AlgorithmFactory] = {}

    def register(self, name: str, version: str, factory: AlgorithmFactory) -> None:
        """Register an algorithm factory."""
        key = (name, version)
        if key in self._factories:
            logger.error("Attempt to re-register algorithm: %s %s", name, version)
            msg = f"Algorithm already registered: {name} {version}"
            raise ValueError(msg)
        self._factories[key] = factory

    def create(self, name: str, version: str) -> "Algorithm":
        """Create algorithm instance by name and version."""
        key = (name, version)
        if key not in self._factories:
            logger.error("Algorithm not found in registry: %s %s", name, version)
            raise KeyError(key)
        return self._factories[key]()


def build_default_registry() -> AlgorithmRegistry:
    """Create registry with lazy-loaded algorithms.

    Algorithms are imported only when their factory is called,
    avoiding heavy dependencies at module load time.
    """
    reg = AlgorithmRegistry()

    def make_analysis_probe() -> "Algorithm":
        from src.algorithms.cv.analysis_probe import AnalysisProbeAlgo

        return AnalysisProbeAlgo()

    def make_blob_detection() -> "Algorithm":
        from src.algorithms.cv.blob_detection import BlobDetectionAlgo

        return BlobDetectionAlgo()

    def make_image_quality() -> "Algorithm":
        from src.algorithms.cv.image_quality import ImageQualityAlgo

        return ImageQualityAlgo()

    def make_edge_detection() -> "Algorithm":
        from src.algorithms.cv.edge_detection import EdgeDetectionAlgo

        return EdgeDetectionAlgo()

    def make_histogram_analysis() -> "Algorithm":
        from src.algorithms.cv.histogram_analysis import HistogramAnalysisAlgo

        return HistogramAnalysisAlgo()

    def make_contour_analysis() -> "Algorithm":
        from src.algorithms.cv.contour_analysis import ContourAnalysisAlgo

        return ContourAnalysisAlgo()

    def make_model_inference() -> "Algorithm":
        from src.algorithms.models.inference import ModelInferenceAlgo

        return ModelInferenceAlgo()

    def make_yolo_segmentation() -> "Algorithm":
        from src.algorithms.models.yolo_segmentation import YoloSegmentationAlgo

        return YoloSegmentationAlgo()

    reg.register("analysis_probe", "1.0.0", make_analysis_probe)
    reg.register("blob_detection", "1.0.0", make_blob_detection)
    reg.register("image_quality", "1.0.0", make_image_quality)
    reg.register("edge_detection", "1.0.0", make_edge_detection)
    reg.register("histogram_analysis", "1.0.0", make_histogram_analysis)
    reg.register("contour_analysis", "1.0.0", make_contour_analysis)
    reg.register("model_inference", "0.1.0", make_model_inference)
    reg.register("model_yolo_segmentation", "1.0.0", make_yolo_segmentation)

    return reg
