"""PyFlink stream processing module for VisioEval.

Provides:
- Metric aggregation with time windows
- Job state tracking
- Integration with Kafka and MinIO

Note: PyFlink runtime features require apache-flink to be installed separately.
PyFlink is an optional dependency due to its pyarrow version constraints.
Install with: pip install "apache-flink>=2.0"

The FlinkConfig and load_flink_config are always available (no PyFlink required).
Use check_pyflink_available() to check if PyFlink is installed before using
FlinkRunner or other PyFlink-dependent features.
"""

from src.flink.config import FlinkConfig, load_flink_config
from src.flink.runner import PYFLINK_AVAILABLE, check_pyflink_available

__all__ = [
    "PYFLINK_AVAILABLE",
    "FlinkConfig",
    "check_pyflink_available",
    "load_flink_config",
]
