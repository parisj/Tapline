"""Visualization components - Flask-based dashboard.

The dashboard is served by Flask at http://localhost:5007.
Run with: pixi run dashboard
"""

from src.visualization.discovery import (
    AlgorithmInfo,
    DirectoryInfo,
    DiscoveryService,
    MetricInfo,
)
from src.visualization.readers import MinioArtifactReader

__all__ = [
    "AlgorithmInfo",
    "DirectoryInfo",
    "DiscoveryService",
    "MetricInfo",
    "MinioArtifactReader",
]
