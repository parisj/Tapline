# src/evaluation/pipeline.py
from collections.abc import Callable

from src.domain.evaluation import AnalysisKind
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

ANALYSIS_PIPELINE: dict[AnalysisKind, list[AnalyzerFactory]] = {
    AnalysisKind.SUMMARY: [
        SummaryAnalyzer,
    ],
    AnalysisKind.DISTRIBUTION_1D: [
        HistogramAnalyzer,
    ],
    AnalysisKind.OUTLIERS_1D: [
        OutliersAnalyzer,
    ],
    AnalysisKind.COUNTER: [
        CounterAnalyzer,
    ],
    AnalysisKind.RATE: [
        RateAnalyzer,
    ],
    AnalysisKind.ELLIPSE_2D: [
        CovEllipseAnalyzer,
    ],
    AnalysisKind.CONTOUR_2D: [
        ContourAnalyzer,
    ],
}
