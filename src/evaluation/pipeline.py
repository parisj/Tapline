# src/evaluation/pipeline.py
from collections.abc import Callable

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
from src.evaluation.analyzers.base import Analyzer

AnalyzerFactory = Callable[[], Analyzer]

ANALYSIS_PIPELINE: dict[AggregationType, list[AnalyzerFactory]] = {
    AggregationType.STATS: [
        SummaryAnalyzer,
    ],
    AggregationType.HISTOGRAM: [
        HistogramAnalyzer,
    ],
    AggregationType.OUTLIERS: [
        OutliersAnalyzer,
    ],
    AggregationType.TALLY: [
        CounterAnalyzer,
    ],
    AggregationType.RATE: [
        RateAnalyzer,
    ],
    AggregationType.SCATTER_ELLIPSE: [
        CovEllipseAnalyzer,
    ],
    AggregationType.DENSITY_MAP: [
        ContourAnalyzer,
    ],
}


# Backwards compatibility alias (deprecated)
AnalysisKind = AggregationType
