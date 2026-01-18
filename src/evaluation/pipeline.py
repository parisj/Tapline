# src/evaluation/pipeline.py
from collections.abc import Callable
from typing import Any

from src.domain.evaluation import AnalysisKind
from src.evaluation.analyzers import (
    ContourAnalyzer,
    CounterAnalyzer,
    CovEllipseAnalyzer,
    HistogramAnalyzer,
    RateAnalyzer,
    SummaryAnalyzer,
)
from src.evaluation.analyzers.base import Analyzer

AnalyzerFactory = Callable[[dict[str, Any]], Analyzer]

ANALYSIS_PIPELINE: dict[AnalysisKind, list[AnalyzerFactory]] = {
    AnalysisKind.SUMMARY: [
        lambda: SummaryAnalyzer(),
    ],
    AnalysisKind.DISTRIBUTION_1D: [
        lambda: HistogramAnalyzer(),
    ],
    AnalysisKind.COUNTER: [
        lambda: CounterAnalyzer(),
    ],
    AnalysisKind.RATE: [
        lambda: RateAnalyzer(),
    ],
    AnalysisKind.ELLIPSE_2D: [
        lambda: CovEllipseAnalyzer(),
    ],
    AnalysisKind.CONTOUR_2D: [
        lambda: ContourAnalyzer(),
    ],
}
