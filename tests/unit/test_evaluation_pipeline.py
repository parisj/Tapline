"""Unit tests for evaluation pipeline.

Tests for:
- ANALYSIS_PIPELINE mapping
- Analyzer factory functions
- AggregationType to analyzer mapping
"""

from __future__ import annotations

from src.domain.evaluation import AggregationType
from src.evaluation.analyzers import (
    ContourAnalyzer,
    CounterAnalyzer,
    CovEllipseAnalyzer,
    HistogramAnalyzer,
    OutliersAnalyzer,
    RateAnalyzer,
    SummaryAnalyzer,
)
from src.evaluation.pipeline import ANALYSIS_PIPELINE


class TestAnalysisPipeline:
    """Tests for ANALYSIS_PIPELINE mapping."""

    def test_pipeline_contains_expected_kinds(self) -> None:
        """Verify all expected AggregationTypes are in the pipeline."""
        expected_kinds = [
            AggregationType.STATS,
            AggregationType.HISTOGRAM,
            AggregationType.OUTLIERS,
            AggregationType.TALLY,
            AggregationType.RATE,
            AggregationType.SCATTER_ELLIPSE,
            AggregationType.DENSITY_MAP,
        ]

        for kind in expected_kinds:
            assert kind in ANALYSIS_PIPELINE, f"Missing {kind} in ANALYSIS_PIPELINE"

    def test_pipeline_does_not_contain_info(self) -> None:
        """RAW kind has no analyzers in the pipeline."""
        assert AggregationType.RAW not in ANALYSIS_PIPELINE

    def test_stats_factory_creates_summary_analyzer(self) -> None:
        factories = ANALYSIS_PIPELINE[AggregationType.STATS]

        assert len(factories) == 1
        analyzer = factories[0]()
        assert isinstance(analyzer, SummaryAnalyzer)

    def test_histogram_factory_creates_histogram_analyzer(self) -> None:
        factories = ANALYSIS_PIPELINE[AggregationType.HISTOGRAM]

        assert len(factories) == 1
        analyzer = factories[0]()
        assert isinstance(analyzer, HistogramAnalyzer)

    def test_outliers_factory_creates_outliers_analyzer(self) -> None:
        factories = ANALYSIS_PIPELINE[AggregationType.OUTLIERS]

        assert len(factories) == 1
        analyzer = factories[0]()
        assert isinstance(analyzer, OutliersAnalyzer)

    def test_tally_factory_creates_counter_analyzer(self) -> None:
        factories = ANALYSIS_PIPELINE[AggregationType.TALLY]

        assert len(factories) == 1
        analyzer = factories[0]()
        assert isinstance(analyzer, CounterAnalyzer)

    def test_rate_factory_creates_rate_analyzer(self) -> None:
        factories = ANALYSIS_PIPELINE[AggregationType.RATE]

        assert len(factories) == 1
        analyzer = factories[0]()
        assert isinstance(analyzer, RateAnalyzer)

    def test_scatter_ellipse_factory_creates_cov_ellipse_analyzer(self) -> None:
        factories = ANALYSIS_PIPELINE[AggregationType.SCATTER_ELLIPSE]

        assert len(factories) == 1
        analyzer = factories[0]()
        assert isinstance(analyzer, CovEllipseAnalyzer)

    def test_density_map_factory_creates_contour_analyzer(self) -> None:
        factories = ANALYSIS_PIPELINE[AggregationType.DENSITY_MAP]

        assert len(factories) == 1
        analyzer = factories[0]()
        assert isinstance(analyzer, ContourAnalyzer)

    def test_all_factories_return_analyzer_instances(self) -> None:
        """Verify all factories return valid analyzer instances."""
        for kind, factories in ANALYSIS_PIPELINE.items():
            for factory in factories:
                analyzer = factory()
                assert hasattr(analyzer, "run"), f"Factory for {kind} didn't return an analyzer"

    def test_factories_are_callable(self) -> None:
        """Verify all factories are callable."""
        for kind, factories in ANALYSIS_PIPELINE.items():
            for factory in factories:
                assert callable(factory), f"Factory for {kind} is not callable"

    def test_factories_return_new_instances(self) -> None:
        """Verify factories return new instances each time."""
        for kind, factories in ANALYSIS_PIPELINE.items():
            for factory in factories:
                instance1 = factory()
                instance2 = factory()
                # They should be different instances (not singletons)
                assert instance1 is not instance2, f"Factory for {kind} returns same instance"


class TestAnalyzerInstantiation:
    """Tests for analyzer instantiation from pipeline."""

    def test_summary_analyzer_can_run(self) -> None:
        factory = ANALYSIS_PIPELINE[AggregationType.STATS][0]
        analyzer = factory()

        result = analyzer.run(values=[1.0, 2.0, 3.0], meta={})

        assert "summary" in result
        assert result["summary"]["count"] == 3

    def test_histogram_analyzer_can_run(self) -> None:
        factory = ANALYSIS_PIPELINE[AggregationType.HISTOGRAM][0]
        analyzer = factory()

        result = analyzer.run(values=[1.0, 2.0, 3.0, 4.0, 5.0], meta={})

        assert "summary" in result
        assert result["summary"]["count"] == 5

    def test_counter_analyzer_can_run(self) -> None:
        factory = ANALYSIS_PIPELINE[AggregationType.TALLY][0]
        analyzer = factory()

        result = analyzer.run(values=["a", "b", "a"], meta={})

        assert "summary" in result
        assert result["summary"]["count"] == 3

    def test_rate_analyzer_can_run(self) -> None:
        factory = ANALYSIS_PIPELINE[AggregationType.RATE][0]
        analyzer = factory()

        result = analyzer.run(values=[True, False, True], meta={})

        assert "summary" in result
        assert result["summary"]["yes"] == 2

    def test_ellipse_analyzer_can_run(self) -> None:
        factory = ANALYSIS_PIPELINE[AggregationType.SCATTER_ELLIPSE][0]
        analyzer = factory()

        result = analyzer.run(values=[(1.0, 2.0), (3.0, 4.0)], meta={})

        assert "summary" in result
        assert result["summary"]["count"] == 2

    def test_contour_analyzer_can_run(self) -> None:
        factory = ANALYSIS_PIPELINE[AggregationType.DENSITY_MAP][0]
        analyzer = factory()

        # Generate enough points to avoid degenerate case
        points = [(float(x), float(y)) for x in range(5) for y in range(5)]
        result = analyzer.run(values=points, meta={})

        assert "summary" in result
        assert result["summary"]["count"] == 25
