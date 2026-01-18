"""Submit Flink SQL job to cluster.

This script submits the metric aggregation SQL job to the Flink cluster.
The job runs entirely inside the Flink cluster using SQL, which doesn't
require Python to be installed in the Flink containers.

Usage:
    pixi run run-flink-job
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import requests


def check_flink_cluster(flink_url: str) -> dict | None:
    """Check if Flink cluster is available and return info."""
    try:
        response = requests.get(f"{flink_url}/overview", timeout=5)
        if response.status_code == 200:
            return response.json()
    except requests.exceptions.RequestException:
        pass
    return None


def get_jobs(flink_url: str) -> list[dict]:
    """Get list of all jobs."""
    try:
        response = requests.get(f"{flink_url}/jobs", timeout=5)
        if response.status_code == 200:
            return response.json().get("jobs", [])
    except requests.exceptions.RequestException:
        pass
    return []


def submit_sql_job(sql_file: Path, flink_url: str) -> bool:
    """Submit SQL job via Flink SQL Client in Docker container."""
    print(f"\nSubmitting SQL job: {sql_file.name}")

    # Copy SQL file to container
    container_sql_path = f"/tmp/{sql_file.name}"
    copy_cmd = ["docker", "cp", str(sql_file), f"flink-jobmanager:{container_sql_path}"]
    print(f"Copying SQL file to container...")
    result = subprocess.run(copy_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Failed to copy SQL file: {result.stderr}")
        return False

    # Submit via SQL Client
    # The SQL client runs the SQL statements and exits
    submit_cmd = [
        "docker", "exec", "-i", "flink-jobmanager",
        "/opt/flink/bin/sql-client.sh",
        "-f", container_sql_path,
    ]

    print(f"Submitting to Flink SQL Client...")
    result = subprocess.run(submit_cmd, capture_output=True, text=True, timeout=60)

    print(f"SQL Client output:\n{result.stdout}")
    if result.stderr:
        print(f"SQL Client stderr:\n{result.stderr}")

    # Check if job was submitted (SQL client doesn't give clear status)
    if "Job has been submitted" in result.stdout or result.returncode == 0:
        return True

    return False


def main():
    """Main entry point."""
    flink_url = os.getenv("FLINK_REST_URL", "http://localhost:8081")
    sql_file = Path(__file__).parent / "metric_aggregation.sql"

    print("=" * 60)
    print("VisioEval Flink SQL Job Submission")
    print("=" * 60)
    print(f"Flink REST URL: {flink_url}")
    print(f"SQL file: {sql_file}")

    # Check cluster health
    print("\nChecking Flink cluster...")
    cluster_info = check_flink_cluster(flink_url)
    if not cluster_info:
        print("Error: Flink cluster is not available")
        sys.exit(1)

    print(f"Cluster: {cluster_info.get('taskmanagers', 0)} task managers, "
          f"{cluster_info.get('slots-total', 0)} slots, "
          f"{cluster_info.get('jobs-running', 0)} running jobs")

    # Check for existing jobs
    initial_jobs = get_jobs(flink_url)
    initial_running = len([j for j in initial_jobs if j.get("status") == "RUNNING"])

    # Submit SQL job
    if not sql_file.exists():
        print(f"Error: SQL file not found: {sql_file}")
        sys.exit(1)

    success = submit_sql_job(sql_file, flink_url)

    if not success:
        print("\nNote: SQL submission output may not indicate success clearly.")
        print("Checking job status directly...")

    # Wait and verify
    print("\nWaiting for job to start...")
    time.sleep(5)

    # Check jobs again
    current_jobs = get_jobs(flink_url)
    current_running = [j for j in current_jobs if j.get("status") == "RUNNING"]

    print(f"\n{'=' * 60}")
    print("Job Status:")
    print(f"{'=' * 60}")

    if current_running:
        print(f"Running jobs: {len(current_running)}")
        for job in current_running:
            print(f"  Job ID: {job['id']}")
            # Get job details
            try:
                detail_resp = requests.get(f"{flink_url}/jobs/{job['id']}", timeout=5)
                if detail_resp.status_code == 200:
                    detail = detail_resp.json()
                    print(f"  Name: {detail.get('name', 'unknown')}")
                    print(f"  State: {detail.get('state', 'unknown')}")
                    start_time = detail.get('start-time', 0)
                    if start_time:
                        from datetime import datetime
                        start_dt = datetime.fromtimestamp(start_time / 1000)
                        print(f"  Started: {start_dt.isoformat()}")
            except Exception as e:
                pass
            print()
    else:
        all_jobs = get_jobs(flink_url)
        if all_jobs:
            print("All jobs:")
            for job in all_jobs:
                print(f"  {job['id']}: {job['status']}")
        else:
            print("No jobs found in cluster")

    print(f"\nFlink UI: {flink_url}")


if __name__ == "__main__":
    main()
