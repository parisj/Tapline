"""PyFlink stream processing module for VisioEval.

Provides:
- Metric aggregation with time windows
- Job state tracking
- Integration with Kafka and MinIO
"""

from src.flink.config import FlinkConfig, load_flink_config

__all__ = [
    "FlinkConfig",
    "load_flink_config",
]
