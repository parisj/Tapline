from __future__ import annotations

from collections.abc import Callable

from src.algorithms.base import Algorithm
from src.algorithms.cv.analysis_probe import AnalysisProbeAlgo
from src.algorithms.models.inference import ModelInferenceAlgo
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
    reg.register("analysis_probe", "1.0.0", AnalysisProbeAlgo)
    reg.register("model_inference", "0.1.0", ModelInferenceAlgo)
    return reg
