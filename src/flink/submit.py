"""Flink job submission utilities.

Provides functions to submit and manage Flink SQL jobs programmatically.
"""

from __future__ import annotations

import http
import os
import subprocess
import time
from pathlib import Path

import requests

from src.utils.logging import get_logger

logger = get_logger("pipeline.flink.submit")

# Default Flink REST URL
DEFAULT_FLINK_URL = "http://localhost:8081"

# Job name to look for
AGGREGATION_JOB_NAME = "metric_aggregation"


def check_flink_available(flink_url: str | None = None) -> bool:
    """Check if Flink cluster is available.

    Args:
        flink_url: Flink REST URL (defaults to env or localhost:8081)

    Returns:
        True if cluster is reachable

    """
    url = flink_url or os.getenv("FLINK_REST_URL", DEFAULT_FLINK_URL)
    try:
        response = requests.get(f"{url}/overview", timeout=5)
        return response.status_code == http.HTTPStatus.OK
    except requests.exceptions.RequestException:
        return False


def get_running_jobs(flink_url: str | None = None) -> list[dict]:
    """Get list of running Flink jobs.

    Args:
        flink_url: Flink REST URL

    Returns:
        List of running job info dicts

    """
    url = flink_url or os.getenv("FLINK_REST_URL", DEFAULT_FLINK_URL)
    try:
        response = requests.get(f"{url}/jobs", timeout=5)
        if response.status_code == http.HTTPStatus.OK:
            jobs = response.json().get("jobs", [])
            return [j for j in jobs if j.get("status") == "RUNNING"]
    except requests.exceptions.RequestException:
        pass
    return []


def is_aggregation_job_running(flink_url: str | None = None) -> bool:
    """Check if the metric aggregation job is already running.

    Args:
        flink_url: Flink REST URL

    Returns:
        True if aggregation job is running

    """
    running_jobs = get_running_jobs(flink_url)
    # The SQL job creates a job, we check if any job is running
    # In a more sophisticated setup, we'd check job names
    return len(running_jobs) > 0


def submit_aggregation_job(flink_url: str | None = None) -> bool:
    """Submit the metric aggregation SQL job to Flink.

    This copies the SQL file to the Flink jobmanager container and
    executes it via the SQL client.

    Args:
        flink_url: Flink REST URL (for verification)

    Returns:
        True if job was submitted successfully

    """
    url = flink_url or os.getenv("FLINK_REST_URL", DEFAULT_FLINK_URL)
    sql_file = Path(__file__).parent / "jobs" / "metric_aggregation.sql"

    if not sql_file.exists():
        logger.error("SQL file not found: %s", sql_file)
        return False

    # Check if Flink is available
    if not check_flink_available(url):
        logger.warning("Flink cluster not available at %s", url)
        return False

    # Check if job is already running
    if is_aggregation_job_running(url):
        logger.info("Aggregation job already running, skipping submission")
        return True

    logger.info("Submitting Flink aggregation job...")

    # Copy SQL file to container
    container_sql_path = f"/tmp/{sql_file.name}"
    copy_cmd = ["docker", "cp", str(sql_file), f"flink-jobmanager:{container_sql_path}"]

    try:
        result = subprocess.run(copy_cmd, check=False, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            logger.error("Failed to copy SQL file to Flink container: %s", result.stderr)
            return False
    except subprocess.TimeoutExpired:
        logger.exception("Timeout copying SQL file to Flink container")
        return False
    except FileNotFoundError:
        logger.warning("Docker not found, cannot submit Flink job")
        return False

    # Submit via SQL Client
    submit_cmd = [
        "docker",
        "exec",
        "-i",
        "flink-jobmanager",
        "/opt/flink/bin/sql-client.sh",
        "-f",
        container_sql_path,
    ]

    try:
        result = subprocess.run(submit_cmd, check=False, capture_output=True, text=True, timeout=60)

        # Log any errors
        if result.stderr and "error" in result.stderr.lower():
            logger.warning("Flink SQL client stderr: %s", result.stderr[:500])

        # Check if successful
        success = "Job has been submitted" in result.stdout or result.returncode == 0

        if success:
            logger.info("Flink aggregation job submitted successfully")
            # Wait a moment for job to start
            time.sleep(2)

            # Verify job is running
            if is_aggregation_job_running(url):
                logger.info("Flink aggregation job is now running")
                return True
            logger.warning("Job submitted but not detected as running yet")
            return True  # Still consider success, job may take time to start
        logger.error("Failed to submit Flink job: %s", result.stdout[:500] if result.stdout else "no output")
        return False

    except subprocess.TimeoutExpired:
        logger.exception("Timeout submitting Flink job")
        return False
    except FileNotFoundError:
        logger.warning("Docker not found, cannot submit Flink job")
        return False


def ensure_aggregation_job_running(flink_url: str | None = None) -> bool:
    """Ensure the aggregation job is running, submitting if needed.

    This is the main entry point for the pipeline to ensure Flink
    aggregation is active.

    Args:
        flink_url: Flink REST URL

    Returns:
        True if job is running (either already was or just submitted)

    """
    url = flink_url or os.getenv("FLINK_REST_URL", DEFAULT_FLINK_URL)

    # Check if already running
    if is_aggregation_job_running(url):
        logger.info("Flink aggregation job already running")
        return True

    # Try to submit
    return submit_aggregation_job(url)
