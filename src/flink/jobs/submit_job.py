"""Submit Flink SQL job to cluster.

This script submits the metric aggregation SQL job to the Flink cluster.
The job runs entirely inside the Flink cluster using SQL, which doesn't
require Python to be installed in the Flink containers.

Usage:
    pixi run run-flink-job
"""

from __future__ import annotations

import http
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests


def check_flink_cluster(flink_url: str) -> dict[str, Any] | None:
    """Check if Flink cluster is available and return info."""
    try:
        response = requests.get(f"{flink_url}/overview", timeout=5)
        if response.status_code == http.HTTPStatus.OK:
            result: dict[str, Any] = response.json()
            return result
    except requests.exceptions.RequestException:
        pass
    return None


def get_jobs(flink_url: str) -> list[dict[str, Any]]:
    """Get list of all jobs."""
    try:
        response = requests.get(f"{flink_url}/jobs", timeout=5)
        if response.status_code == http.HTTPStatus.OK:
            jobs: list[dict[str, Any]] = response.json().get("jobs", [])
            return jobs
    except requests.exceptions.RequestException:
        pass
    return []


def submit_sql_job(sql_file: Path, _flink_url: str) -> bool:
    """Submit SQL job via Flink SQL Client in Docker container."""
    # Copy SQL file to container
    container_sql_path = f"/tmp/{sql_file.name}"
    copy_cmd = ["docker", "cp", str(sql_file), f"flink-jobmanager:{container_sql_path}"]
    result = subprocess.run(copy_cmd, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        return False

    # Submit via SQL Client
    # The SQL client runs the SQL statements and exits
    submit_cmd = [
        "docker",
        "exec",
        "-i",
        "flink-jobmanager",
        "/opt/flink/bin/sql-client.sh",
        "-f",
        container_sql_path,
    ]

    result = subprocess.run(submit_cmd, check=False, capture_output=True, text=True, timeout=60)

    if result.stderr:
        pass

    # Check if job was submitted (SQL client doesn't give clear status)
    return bool("Job has been submitted" in result.stdout or result.returncode == 0)


def main() -> None:
    """Main entry point."""
    flink_url = os.getenv("FLINK_REST_URL", "http://localhost:8081")
    sql_file = Path(__file__).parent / "metric_aggregation.sql"

    # Check cluster health
    cluster_info = check_flink_cluster(flink_url)
    if not cluster_info:
        sys.exit(1)

    # Check for existing jobs
    initial_jobs = get_jobs(flink_url)
    len([j for j in initial_jobs if j.get("status") == "RUNNING"])

    # Submit SQL job
    if not sql_file.exists():
        sys.exit(1)

    success = submit_sql_job(sql_file, flink_url)

    if not success:
        pass

    # Wait and verify
    time.sleep(5)

    # Check jobs again
    current_jobs = get_jobs(flink_url)
    current_running = [j for j in current_jobs if j.get("status") == "RUNNING"]

    if current_running:
        for job in current_running:
            # Get job details
            try:
                detail_resp = requests.get(f"{flink_url}/jobs/{job['id']}", timeout=5)
                if detail_resp.status_code == http.HTTPStatus.OK:
                    detail = detail_resp.json()
                    start_time = detail.get("start-time", 0)
                    if start_time:
                        datetime.fromtimestamp(start_time / 1000)
            except Exception:
                pass
    else:
        all_jobs = get_jobs(flink_url)
        if all_jobs:
            for _job in all_jobs:
                pass
        else:
            pass


if __name__ == "__main__":
    main()
