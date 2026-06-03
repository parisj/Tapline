"""Unit tests for src.flink.jobs.submit_job.

Tests cover:
- HTTP probes to the Flink REST API (mocked requests)
- Subprocess call shape for SQL job submission
- The main() entry point and its branching logic
"""

from __future__ import annotations

import http
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.flink.jobs import submit_job as submit_job_mod
from src.flink.jobs.submit_job import (
    check_flink_cluster,
    get_jobs,
    submit_sql_job,
)


def _ok(payload: object) -> MagicMock:
    resp = MagicMock()
    resp.status_code = http.HTTPStatus.OK
    resp.json.return_value = payload
    return resp


def _bad(status: int = 500) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = {}
    return resp


class TestCheckFlinkCluster:
    def test_returns_payload_on_ok(self) -> None:
        payload = {"flink-version": "1.18.0", "taskmanagers": 2}
        with patch.object(submit_job_mod.requests, "get", return_value=_ok(payload)):
            assert check_flink_cluster("http://x:8081") == payload

    def test_returns_none_on_bad_status(self) -> None:
        with patch.object(submit_job_mod.requests, "get", return_value=_bad(503)):
            assert check_flink_cluster("http://x:8081") is None

    def test_returns_none_on_exception(self) -> None:
        with patch.object(
            submit_job_mod.requests,
            "get",
            side_effect=requests.exceptions.ConnectionError("nope"),
        ):
            assert check_flink_cluster("http://x:8081") is None


class TestGetJobs:
    def test_returns_jobs_list(self) -> None:
        payload = {"jobs": [{"id": "1", "status": "RUNNING"}, {"id": "2", "status": "FINISHED"}]}
        with patch.object(submit_job_mod.requests, "get", return_value=_ok(payload)):
            jobs = get_jobs("http://x:8081")
        assert len(jobs) == 2

    def test_returns_empty_on_bad_status(self) -> None:
        with patch.object(submit_job_mod.requests, "get", return_value=_bad(500)):
            assert get_jobs("http://x:8081") == []

    def test_returns_empty_on_exception(self) -> None:
        with patch.object(
            submit_job_mod.requests,
            "get",
            side_effect=requests.exceptions.Timeout("slow"),
        ):
            assert get_jobs("http://x:8081") == []


class TestSubmitSqlJob:
    def test_docker_cp_failure_returns_false(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "job.sql"
        sql_file.write_text("SELECT 1;")
        cp_result = MagicMock(returncode=1, stderr="no", stdout="")
        with patch.object(submit_job_mod.subprocess, "run", return_value=cp_result) as run_mock:
            assert submit_sql_job(sql_file, "http://x:8081") is False
        # only one subprocess call when docker cp fails
        assert run_mock.call_count == 1
        args = run_mock.call_args.args[0]
        assert args[:2] == ["docker", "cp"]
        assert args[3] == f"flink-jobmanager:/opt/flink/temp/{sql_file.name}"

    def test_submit_success_via_submit_marker(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "job.sql"
        sql_file.write_text("SELECT 1;")
        cp_result = MagicMock(returncode=0, stderr="", stdout="")
        submit_result = MagicMock(returncode=1, stderr="", stdout="Job has been submitted")
        with patch.object(
            submit_job_mod.subprocess,
            "run",
            side_effect=[cp_result, submit_result],
        ) as run_mock:
            assert submit_sql_job(sql_file, "http://x:8081") is True
        # Verify the SQL client argv
        submit_args = run_mock.call_args_list[1].args[0]
        assert submit_args[:5] == [
            "docker",
            "exec",
            "-i",
            "flink-jobmanager",
            "/opt/flink/bin/sql-client.sh",
        ]
        assert submit_args[5] == "-f"
        assert submit_args[6].endswith(sql_file.name)

    def test_submit_success_via_returncode_zero(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "job.sql"
        sql_file.write_text("SELECT 1;")
        cp_result = MagicMock(returncode=0, stderr="", stdout="")
        submit_result = MagicMock(returncode=0, stderr="", stdout="no marker")
        with patch.object(
            submit_job_mod.subprocess,
            "run",
            side_effect=[cp_result, submit_result],
        ):
            assert submit_sql_job(sql_file, "http://x:8081") is True

    def test_submit_failure_returncode_and_no_marker(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "job.sql"
        sql_file.write_text("SELECT 1;")
        cp_result = MagicMock(returncode=0, stderr="", stdout="")
        submit_result = MagicMock(returncode=2, stderr="err", stdout="explosions")
        with patch.object(
            submit_job_mod.subprocess,
            "run",
            side_effect=[cp_result, submit_result],
        ):
            assert submit_sql_job(sql_file, "http://x:8081") is False

    def test_submit_subprocess_timeout_propagates(self, tmp_path: Path) -> None:
        # The submit branch does not catch TimeoutExpired; verify behaviour.
        sql_file = tmp_path / "job.sql"
        sql_file.write_text("SELECT 1;")
        cp_result = MagicMock(returncode=0, stderr="", stdout="")
        with (
            patch.object(
                submit_job_mod.subprocess,
                "run",
                side_effect=[
                    cp_result,
                    subprocess.TimeoutExpired(cmd="docker", timeout=60),
                ],
            ),
            pytest.raises(subprocess.TimeoutExpired),
        ):
            submit_sql_job(sql_file, "http://x:8081")


class TestMain:
    def test_main_exits_when_cluster_unreachable(self) -> None:
        with (
            patch.object(submit_job_mod, "check_flink_cluster", return_value=None),
            pytest.raises(SystemExit) as exc_info,
        ):
            submit_job_mod.main()
        assert exc_info.value.code == 1

    def test_main_exits_when_sql_file_missing(self) -> None:
        fake_path = MagicMock()
        fake_path.exists.return_value = False

        with (
            patch.object(submit_job_mod, "check_flink_cluster", return_value={"ok": True}),
            patch.object(submit_job_mod, "get_jobs", return_value=[]),
            patch("src.flink.jobs.submit_job.Path") as path_cls,
        ):
            # Path(__file__).parent / "metric_aggregation.sql"
            path_cls.return_value.parent.__truediv__.return_value = fake_path
            with pytest.raises(SystemExit) as exc_info:
                submit_job_mod.main()
        assert exc_info.value.code == 1

    def test_main_completes_when_submit_succeeds_with_running_jobs(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "metric_aggregation.sql"
        sql_file.write_text("SELECT 1;")

        running_jobs = [{"id": "abc", "status": "RUNNING"}]
        with (
            patch.object(submit_job_mod, "check_flink_cluster", return_value={"ok": True}),
            patch.object(submit_job_mod, "get_jobs", side_effect=[[], running_jobs]),
            patch.object(submit_job_mod, "submit_sql_job", return_value=True),
            patch.object(submit_job_mod.time, "sleep"),
            patch.object(
                submit_job_mod.requests,
                "get",
                return_value=_ok({"start-time": 1700000000000}),
            ),
            patch("src.flink.jobs.submit_job.Path") as path_cls,
        ):
            path_cls.return_value.parent.__truediv__.return_value = sql_file
            # Must not raise / not call sys.exit
            submit_job_mod.main()

    def test_main_handles_no_running_jobs_after_submit(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "metric_aggregation.sql"
        sql_file.write_text("SELECT 1;")

        # main() calls get_jobs three times when nothing is running:
        # initial scan, post-submit scan, and a final reporting scan.
        with (
            patch.object(submit_job_mod, "check_flink_cluster", return_value={"ok": True}),
            patch.object(submit_job_mod, "get_jobs", side_effect=[[], [], []]),
            patch.object(submit_job_mod, "submit_sql_job", return_value=False),
            patch.object(submit_job_mod.time, "sleep"),
            patch("src.flink.jobs.submit_job.Path") as path_cls,
        ):
            path_cls.return_value.parent.__truediv__.return_value = sql_file
            submit_job_mod.main()

    def test_main_handles_no_running_jobs_with_other_jobs_present(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "metric_aggregation.sql"
        sql_file.write_text("SELECT 1;")

        # First scan empty, second empty (no RUNNING), third returns a
        # FINISHED job (exercises the "else" branch listing all jobs).
        all_jobs = [{"id": "x", "status": "FINISHED"}]
        with (
            patch.object(submit_job_mod, "check_flink_cluster", return_value={"ok": True}),
            patch.object(submit_job_mod, "get_jobs", side_effect=[[], [], all_jobs]),
            patch.object(submit_job_mod, "submit_sql_job", return_value=True),
            patch.object(submit_job_mod.time, "sleep"),
            patch("src.flink.jobs.submit_job.Path") as path_cls,
        ):
            path_cls.return_value.parent.__truediv__.return_value = sql_file
            submit_job_mod.main()
