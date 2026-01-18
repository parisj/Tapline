"""Flink job definitions for VisioEval stream processing.

Jobs:
- metric_aggregation.sql: SQL-based metric aggregation (preferred)
- metric_aggregation_job.py: PyFlink alternative for complex processing
- submit_job.py: Script to submit SQL jobs to Flink cluster
"""

from src.flink.jobs.job_state_tracker import JobStateTrackerJob

__all__ = [
    "JobStateTrackerJob",
]
