"""Flink job definitions for VisioEval stream processing.

Jobs:
- metric_aggregation.sql: SQL-based metric aggregation
- metric_aggregation_job.py: PyFlink alternative for complex processing
- submit_job.py: Script to submit SQL jobs to Flink cluster

Note: PyFlink jobs require apache-flink to be installed separately.
PyFlink is an optional dependency due to its pyarrow version constraints.
Install with: pip install "apache-flink>=2.0"
"""

from __future__ import annotations

from typing import TYPE_CHECKING

# Lazy import to avoid ImportError when PyFlink is not installed
if TYPE_CHECKING:
    from src.flink.jobs.job_state_tracker import JobStateTrackerJob

__all__ = [
    "JobStateTrackerJob",
]


def __getattr__(name: str) -> type:
    """Lazy load PyFlink-dependent classes."""
    if name == "JobStateTrackerJob":
        try:
            from src.flink.jobs.job_state_tracker import (  # noqa: PLC0415
                JobStateTrackerJob,
            )

            return JobStateTrackerJob
        except ImportError as e:
            msg = (
                f"Cannot import {name}: PyFlink is not installed.\n"
                "Install with: pip install 'apache-flink>=2.0'\n"
                "Note: PyFlink is optional. Python aggregation (default) works without it."
            )
            raise ImportError(msg) from e
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
