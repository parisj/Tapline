# src/evaluation/pipeline.py
"""Pipeline configuration for metric aggregation.

This module provides the mapping between AggregationType and analyzer factories.
Uses the AnalyzerRegistry for extensible analyzer management.
"""

from src.domain.evaluation import AggregationType
from src.evaluation.analyzer_registry import (
    AnalyzerFactory,
    AnalyzerRegistry,
    get_default_registry,
)

# Export AnalyzerFactory type for external use
__all__ = [
    "ANALYSIS_PIPELINE",
    "AnalysisKind",
    "AnalyzerFactory",
    "AnalyzerRegistry",
    "get_default_registry",
]

# Backwards-compatible dict format using the registry
ANALYSIS_PIPELINE = get_default_registry().to_pipeline_dict()

# Backwards compatibility alias (deprecated)
AnalysisKind = AggregationType
