"""Unit tests for the analyzer registry.

Tests the AnalyzerRegistry class for:
- Registration and retrieval of analyzers
- Lazy loading behavior
- Pipeline dict compatibility
"""

from __future__ import annotations

import pytest

from src.domain.evaluation import AggregationType
from src.evaluation.analyzer_registry import (
    AnalyzerRegistry,
    build_default_registry,
    get_default_registry,
)


class TestAnalyzerRegistry:
    """Tests for AnalyzerRegistry class."""

    def test_register_and_get_factories(self) -> None:
        registry = AnalyzerRegistry()

        def factory() -> object:
            return object()

        registry.register(AggregationType.STATS, factory)
        factories = registry.get_factories(AggregationType.STATS)

        assert len(factories) == 1
        assert factories[0] is factory

    def test_get_factories_returns_empty_for_unregistered(self) -> None:
        registry = AnalyzerRegistry()
        factories = registry.get_factories(AggregationType.STATS)

        assert factories == []

    def test_register_multiple_factories_for_same_type(self) -> None:
        registry = AnalyzerRegistry()

        def factory1() -> object:
            return "analyzer1"

        def factory2() -> object:
            return "analyzer2"

        registry.register(AggregationType.HISTOGRAM, factory1)
        registry.register(AggregationType.HISTOGRAM, factory2)

        factories = registry.get_factories(AggregationType.HISTOGRAM)
        assert len(factories) == 2

    def test_create_analyzers_calls_factories(self) -> None:
        registry = AnalyzerRegistry()
        call_count = [0]

        def factory() -> str:
            call_count[0] += 1
            return f"analyzer_{call_count[0]}"

        registry.register(AggregationType.STATS, factory)

        analyzers = registry.create_analyzers(AggregationType.STATS)
        assert len(analyzers) == 1
        assert analyzers[0] == "analyzer_1"
        assert call_count[0] == 1

    def test_create_analyzers_for_mask_with_multiple_types(self) -> None:
        registry = AnalyzerRegistry()

        registry.register(AggregationType.STATS, lambda: "stats")
        registry.register(AggregationType.HISTOGRAM, lambda: "histogram")
        registry.register(AggregationType.TALLY, lambda: "tally")

        # Mask with STATS | HISTOGRAM
        mask = AggregationType.STATS.value | AggregationType.HISTOGRAM.value
        analyzers = registry.create_analyzers_for_mask(mask)

        assert len(analyzers) == 2
        assert "stats" in analyzers
        assert "histogram" in analyzers
        assert "tally" not in analyzers

    def test_create_analyzers_for_mask_returns_empty_for_zero(self) -> None:
        registry = AnalyzerRegistry()
        registry.register(AggregationType.STATS, lambda: "stats")

        analyzers = registry.create_analyzers_for_mask(0)
        assert analyzers == []

    def test_to_pipeline_dict_returns_copy(self) -> None:
        registry = AnalyzerRegistry()
        factory = lambda: "analyzer"  # noqa: E731
        registry.register(AggregationType.STATS, factory)

        pipeline_dict = registry.to_pipeline_dict()

        assert AggregationType.STATS in pipeline_dict
        assert pipeline_dict[AggregationType.STATS] == [factory]

    def test_registered_types_property(self) -> None:
        registry = AnalyzerRegistry()
        registry.register(AggregationType.STATS, lambda: "stats")
        registry.register(AggregationType.HISTOGRAM, lambda: "histogram")

        types = registry.registered_types
        assert AggregationType.STATS in types
        assert AggregationType.HISTOGRAM in types
        assert len(types) == 2


class TestBuildDefaultRegistry:
    """Tests for build_default_registry function."""

    def test_returns_registry_with_all_types(self) -> None:
        registry = build_default_registry()

        expected_types = [
            AggregationType.STATS,
            AggregationType.HISTOGRAM,
            AggregationType.OUTLIERS,
            AggregationType.TALLY,
            AggregationType.RATE,
            AggregationType.SCATTER_ELLIPSE,
            AggregationType.DENSITY_MAP,
        ]

        for agg_type in expected_types:
            factories = registry.get_factories(agg_type)
            assert len(factories) == 1, f"Missing factory for {agg_type}"

    def test_factories_create_valid_analyzers(self) -> None:
        registry = build_default_registry()

        # Test STATS analyzer creation
        analyzers = registry.create_analyzers(AggregationType.STATS)
        assert len(analyzers) == 1
        assert hasattr(analyzers[0], "run")

    def test_lazy_loading_defers_imports(self) -> None:
        # Build a fresh registry
        registry = build_default_registry()

        # Getting factories should not import analyzer modules
        factories = registry.get_factories(AggregationType.STATS)
        assert len(factories) == 1

        # Only calling the factory should import and create
        analyzer = factories[0]()
        assert hasattr(analyzer, "run")


class TestGetDefaultRegistry:
    """Tests for get_default_registry singleton function."""

    def test_returns_same_instance(self) -> None:
        registry1 = get_default_registry()
        registry2 = get_default_registry()

        assert registry1 is registry2

    def test_registry_is_fully_configured(self) -> None:
        registry = get_default_registry()

        # Should have all 7 default analyzer types
        assert len(registry.registered_types) == 7


class TestPipelineCompatibility:
    """Tests for backwards compatibility with ANALYSIS_PIPELINE dict."""

    def test_pipeline_dict_format_matches_original(self) -> None:
        from src.evaluation.pipeline import ANALYSIS_PIPELINE

        # Should have entries for all standard types
        assert AggregationType.STATS in ANALYSIS_PIPELINE
        assert AggregationType.HISTOGRAM in ANALYSIS_PIPELINE
        assert AggregationType.OUTLIERS in ANALYSIS_PIPELINE
        assert AggregationType.TALLY in ANALYSIS_PIPELINE
        assert AggregationType.RATE in ANALYSIS_PIPELINE
        assert AggregationType.SCATTER_ELLIPSE in ANALYSIS_PIPELINE
        assert AggregationType.DENSITY_MAP in ANALYSIS_PIPELINE

    def test_pipeline_factories_return_analyzers(self) -> None:
        from src.evaluation.pipeline import ANALYSIS_PIPELINE

        for agg_type, factories in ANALYSIS_PIPELINE.items():
            for factory in factories:
                analyzer = factory()
                assert hasattr(analyzer, "run"), f"Invalid analyzer for {agg_type}"
