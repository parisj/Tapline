"""Registry for mapping AggregationType to analyzer factories.

Provides a centralized, extensible way to register and retrieve analyzers
based on aggregation type flags.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from src.domain.evaluation import AggregationType

if TYPE_CHECKING:
    from src.evaluation.analyzers.base import Analyzer

# Use Any for the factory return type to avoid forward reference issues
AnalyzerFactory = Callable[[], Any]


class AnalyzerRegistry:
    """Registry mapping AggregationType to analyzer factories.

    Supports lazy loading of analyzer classes to reduce import overhead
    at startup. Analyzers are only imported when first requested.
    """

    def __init__(self) -> None:
        self._factories: dict[AggregationType, list[AnalyzerFactory]] = {}

    def register(
        self,
        aggregation_type: AggregationType,
        factory: AnalyzerFactory,
    ) -> None:
        """Register an analyzer factory for an aggregation type.

        Args:
            aggregation_type: The AggregationType flag this analyzer handles
            factory: A callable that returns an Analyzer instance

        """
        if aggregation_type not in self._factories:
            self._factories[aggregation_type] = []
        self._factories[aggregation_type].append(factory)

    def get_factories(
        self,
        aggregation_type: AggregationType,
    ) -> list[AnalyzerFactory]:
        """Get all factories registered for a specific aggregation type.

        Args:
            aggregation_type: The AggregationType to look up

        Returns:
            List of factory callables (may be empty)

        """
        return self._factories.get(aggregation_type, [])

    def create_analyzers(
        self,
        aggregation_type: AggregationType,
    ) -> list[Any]:
        """Create analyzer instances for a specific aggregation type.

        Args:
            aggregation_type: The AggregationType to create analyzers for

        Returns:
            List of Analyzer instances

        """
        return [f() for f in self.get_factories(aggregation_type)]

    def create_analyzers_for_mask(self, mask: int) -> list[Any]:
        """Create analyzers for all types set in a bitmask.

        Args:
            mask: Integer bitmask of AggregationType values

        Returns:
            List of Analyzer instances for all matching types

        """
        analyzers: list[Analyzer] = []
        for agg_type in AggregationType:
            if mask & agg_type.value:
                analyzers.extend(self.create_analyzers(agg_type))
        return analyzers

    def to_pipeline_dict(self) -> dict[AggregationType, list[AnalyzerFactory]]:
        """Convert registry to pipeline dict format for backwards compatibility.

        Returns:
            Dict mapping AggregationType to list of factories

        """
        return dict(self._factories)

    @property
    def registered_types(self) -> list[AggregationType]:
        """Get list of all registered aggregation types."""
        return list(self._factories.keys())


def _lazy_summary() -> Any:
    from src.evaluation.analyzers.summary import SummaryAnalyzer

    return SummaryAnalyzer()


def _lazy_histogram() -> Any:
    from src.evaluation.analyzers.histogram import HistogramAnalyzer

    return HistogramAnalyzer()


def _lazy_outliers() -> Any:
    from src.evaluation.analyzers.outliers1d import OutliersAnalyzer

    return OutliersAnalyzer()


def _lazy_counter() -> Any:
    from src.evaluation.analyzers.counter import CounterAnalyzer

    return CounterAnalyzer()


def _lazy_rate() -> Any:
    from src.evaluation.analyzers.rate import RateAnalyzer

    return RateAnalyzer()


def _lazy_ellipse() -> Any:
    from src.evaluation.analyzers.ellipse2d import CovEllipseAnalyzer

    return CovEllipseAnalyzer()


def _lazy_contour() -> Any:
    from src.evaluation.analyzers.contour2d import ContourAnalyzer

    return ContourAnalyzer()


def build_default_registry() -> AnalyzerRegistry:
    """Build the default analyzer registry with all built-in analyzers.

    Uses lazy loading to defer imports until analyzers are actually needed.

    Returns:
        AnalyzerRegistry with all default analyzers registered

    """
    registry = AnalyzerRegistry()

    registry.register(AggregationType.STATS, _lazy_summary)
    registry.register(AggregationType.HISTOGRAM, _lazy_histogram)
    registry.register(AggregationType.OUTLIERS, _lazy_outliers)
    registry.register(AggregationType.TALLY, _lazy_counter)
    registry.register(AggregationType.RATE, _lazy_rate)
    registry.register(AggregationType.SCATTER_ELLIPSE, _lazy_ellipse)
    registry.register(AggregationType.DENSITY_MAP, _lazy_contour)

    return registry


_default_registry: AnalyzerRegistry | None = None


def get_default_registry() -> AnalyzerRegistry:
    """Get or create the default analyzer registry singleton.

    Returns:
        The default AnalyzerRegistry instance

    """
    global _default_registry
    if _default_registry is None:
        _default_registry = build_default_registry()
    return _default_registry
