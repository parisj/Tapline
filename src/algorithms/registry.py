from __future__ import annotations

from collections.abc import Callable

from src.algorithms.base import Algorithm
from src.algorithms.cv.analysis_probe import AnalysisProbeAlgo
from src.algorithms.cv.blob_detection import BlobDetectionAlgo
from src.algorithms.cv.contour_analysis import ContourAnalysisAlgo
from src.algorithms.cv.edge_detection import EdgeDetectionAlgo
from src.algorithms.cv.histogram_analysis import HistogramAnalysisAlgo
from src.algorithms.cv.image_quality import ImageQualityAlgo
from src.algorithms.models.inference import ModelInferenceAlgo
from src.algorithms.models.yolo_segmentation import YoloSegmentationAlgo
from src.utils.logging import get_logger

logger = get_logger(__name__)

AlgorithmFactory = Callable[[], Algorithm]


class AlgorithmRegistry:
    """Simple registry: (name, version) -> factory.

    This allows routes.toml to stay declarative and prevents hardcoding algorithm
    class selection in app/main.py.
    """

    def __init__(self) -> None:
        self._factories: dict[tuple[str, str], AlgorithmFactory] = {}

    def register(self, name: str, version: str, factory: AlgorithmFactory) -> None:
        key = (name, version)
        if key in self._factories:
            logger.error(f"Attempt to re-register algorithm: {name} {version}")
            msg = f"Algorithm already registered: {name} {version}"
            raise ValueError(msg)
        self._factories[key] = factory

    def create(self, name: str, version: str) -> Algorithm:
        key = (name, version)
        if key not in self._factories:
            logger.error(f"Algorithm not found in registry: {name} {version}")
            raise KeyError(key)
        return self._factories[key]()


def build_default_registry() -> AlgorithmRegistry:
    """Create default registry."""
    reg = AlgorithmRegistry()
    # Test/probe algorithm
    reg.register("analysis_probe", "1.0.0", AnalysisProbeAlgo)
    # Computer vision algorithms
    reg.register("blob_detection", "1.0.0", BlobDetectionAlgo)
    reg.register("image_quality", "1.0.0", ImageQualityAlgo)
    reg.register("edge_detection", "1.0.0", EdgeDetectionAlgo)
    reg.register("histogram_analysis", "1.0.0", HistogramAnalysisAlgo)
    reg.register("contour_analysis", "1.0.0", ContourAnalysisAlgo)
    # ML model algorithms
    reg.register("model_inference", "0.1.0", ModelInferenceAlgo)
    reg.register("yolo_segmentation", "1.0.0", YoloSegmentationAlgo)
    return reg
