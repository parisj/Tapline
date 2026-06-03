"""Unit tests for src.flink.submit.

Covers:
- Flink REST API availability checks (mocked requests)
- Running-job inspection
- Subprocess argument construction for SQL job submission
- Error handling (timeouts, missing docker, missing SQL file, non-zero exit)
"""

from __future__ import annotations

import http
import subprocess
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import requests

from src.flink import submit as submit_mod
from src.flink.submit import (
    DEFAULT_FLINK_URL,
    check_flink_available,
    ensure_aggregation_job_running,
    get_running_jobs,
    is_aggregation_job_running,
    submit_aggregation_job,
)

if TYPE_CHECKING:
    import pytest


def _ok_response(payload: object) -> MagicMock:
    resp = MagicMock()
    resp.status_code = http.HTTPStatus.OK
    resp.json.return_value = payload
    return resp


def _bad_response(status: int = 500) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = {}
    return resp


class TestCheckFlinkAvailable:
    def test_returns_true_on_ok(self) -> None:
        with patch.object(submit_mod.requests, "get", return_value=_ok_response({})) as mock_get:
            assert check_flink_available("http://x:8081") is True
        mock_get.assert_called_once_with("http://x:8081/overview", timeout=5)

    def test_returns_false_on_non_ok(self) -> None:
        with patch.object(submit_mod.requests, "get", return_value=_bad_response(503)):
            assert check_flink_available("http://x:8081") is False

    def test_returns_false_on_connection_error(self) -> None:
        with patch.object(
            submit_mod.requests,
            "get",
            side_effect=requests.exceptions.ConnectionError("nope"),
        ):
            assert check_flink_available("http://x:8081") is False

    def test_uses_env_when_no_url_given(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("FLINK_REST_URL", "http://env-host:7070")
        with patch.object(submit_mod.requests, "get", return_value=_ok_response({})) as mock_get:
            check_flink_available()
        mock_get.assert_called_once_with("http://env-host:7070/overview", timeout=5)

    def test_uses_default_when_no_url_and_no_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("FLINK_REST_URL", raising=False)
        with patch.object(submit_mod.requests, "get", return_value=_ok_response({})) as mock_get:
            check_flink_available()
        mock_get.assert_called_once_with(f"{DEFAULT_FLINK_URL}/overview", timeout=5)


class TestGetRunningJobs:
    def test_returns_only_running_jobs(self) -> None:
        payload = {
            "jobs": [
                {"id": "1", "status": "RUNNING"},
                {"id": "2", "status": "FINISHED"},
                {"id": "3", "status": "RUNNING"},
            ],
        }
        with patch.object(submit_mod.requests, "get", return_value=_ok_response(payload)):
            running = get_running_jobs("http://x:8081")
        assert len(running) == 2
        assert {j["id"] for j in running} == {"1", "3"}

    def test_returns_empty_on_non_ok(self) -> None:
        with patch.object(submit_mod.requests, "get", return_value=_bad_response(500)):
            assert get_running_jobs("http://x:8081") == []

    def test_returns_empty_on_request_exception(self) -> None:
        with patch.object(
            submit_mod.requests,
            "get",
            side_effect=requests.exceptions.Timeout("slow"),
        ):
            assert get_running_jobs("http://x:8081") == []


class TestIsAggregationJobRunning:
    def test_true_when_jobs_running(self) -> None:
        payload = {"jobs": [{"id": "1", "status": "RUNNING"}]}
        with patch.object(submit_mod.requests, "get", return_value=_ok_response(payload)):
            assert is_aggregation_job_running("http://x:8081") is True

    def test_false_when_no_jobs(self) -> None:
        with patch.object(
            submit_mod.requests,
            "get",
            return_value=_ok_response({"jobs": []}),
        ):
            assert is_aggregation_job_running("http://x:8081") is False


class TestSubmitAggregationJob:
    def test_returns_false_when_sql_file_missing(self, tmp_path) -> None:
        # Patch the SQL file path to point at a non-existent file
        fake_sql = tmp_path / "nope.sql"
        with patch("src.flink.submit.Path") as path_cls:
            # Path(__file__).parent / "jobs" / "metric_aggregation.sql"
            path_cls.return_value.parent.__truediv__.return_value.__truediv__.return_value = fake_sql
            # Don't intercept Path(jar_path).exists() etc — only one Path call here
            assert submit_aggregation_job("http://x:8081") is False

    def test_returns_false_when_flink_not_available(self) -> None:
        with patch.object(submit_mod, "check_flink_available", return_value=False):
            assert submit_aggregation_job("http://x:8081") is False

    def test_returns_true_when_already_running(self) -> None:
        with (
            patch.object(submit_mod, "check_flink_available", return_value=True),
            patch.object(submit_mod, "is_aggregation_job_running", return_value=True),
        ):
            assert submit_aggregation_job("http://x:8081") is True

    def test_returns_false_when_docker_cp_fails(self) -> None:
        cp_result = MagicMock(returncode=1, stderr="permission denied", stdout="")
        with (
            patch.object(submit_mod, "check_flink_available", return_value=True),
            patch.object(submit_mod, "is_aggregation_job_running", return_value=False),
            patch.object(submit_mod.subprocess, "run", return_value=cp_result) as run_mock,
        ):
            assert submit_aggregation_job("http://x:8081") is False
        # Only one subprocess.run call (docker cp) before bailing
        assert run_mock.call_count == 1
        args = run_mock.call_args.args[0]
        assert args[0] == "docker"
        assert args[1] == "cp"
        # Target inside container
        assert "flink-jobmanager:/opt/flink/temp/metric_aggregation.sql" in args[3]

    def test_returns_false_on_docker_cp_timeout(self) -> None:
        with (
            patch.object(submit_mod, "check_flink_available", return_value=True),
            patch.object(submit_mod, "is_aggregation_job_running", return_value=False),
            patch.object(
                submit_mod.subprocess,
                "run",
                side_effect=subprocess.TimeoutExpired(cmd="docker", timeout=30),
            ),
        ):
            assert submit_aggregation_job("http://x:8081") is False

    def test_returns_false_when_docker_not_installed(self) -> None:
        with (
            patch.object(submit_mod, "check_flink_available", return_value=True),
            patch.object(submit_mod, "is_aggregation_job_running", return_value=False),
            patch.object(submit_mod.subprocess, "run", side_effect=FileNotFoundError),
        ):
            assert submit_aggregation_job("http://x:8081") is False

    def test_submit_constructs_sql_client_args(self) -> None:
        cp_result = MagicMock(returncode=0, stderr="", stdout="")
        submit_result = MagicMock(
            returncode=0,
            stderr="",
            stdout="Job has been submitted",
        )
        # First call: docker cp, second call: docker exec ... sql-client.sh
        run_side_effects = [cp_result, submit_result]

        with (
            patch.object(submit_mod, "check_flink_available", return_value=True),
            patch.object(submit_mod, "is_aggregation_job_running", side_effect=[False, True]),
            patch.object(submit_mod.time, "sleep"),
            patch.object(submit_mod.subprocess, "run", side_effect=run_side_effects) as run_mock,
        ):
            result = submit_aggregation_job("http://x:8081")

        assert result is True
        assert run_mock.call_count == 2
        submit_args = run_mock.call_args_list[1].args[0]
        assert submit_args[:5] == ["docker", "exec", "-i", "flink-jobmanager", "/opt/flink/bin/sql-client.sh"]
        assert submit_args[5] == "-f"
        assert submit_args[6].endswith("metric_aggregation.sql")

    def test_submit_success_even_when_post_check_says_not_running(self) -> None:
        cp_result = MagicMock(returncode=0, stderr="", stdout="")
        submit_result = MagicMock(
            returncode=0,
            stderr="",
            stdout="Job has been submitted",
        )
        with (
            patch.object(submit_mod, "check_flink_available", return_value=True),
            patch.object(submit_mod, "is_aggregation_job_running", side_effect=[False, False]),
            patch.object(submit_mod.time, "sleep"),
            patch.object(submit_mod.subprocess, "run", side_effect=[cp_result, submit_result]),
        ):
            # Per source, this still returns True (job may take time to start)
            assert submit_aggregation_job("http://x:8081") is True

    def test_submit_returncode_nonzero_and_no_submit_marker_returns_false(self) -> None:
        cp_result = MagicMock(returncode=0, stderr="", stdout="")
        submit_result = MagicMock(returncode=1, stderr="boom", stdout="failure")
        with (
            patch.object(submit_mod, "check_flink_available", return_value=True),
            patch.object(submit_mod, "is_aggregation_job_running", return_value=False),
            patch.object(submit_mod.subprocess, "run", side_effect=[cp_result, submit_result]),
        ):
            assert submit_aggregation_job("http://x:8081") is False

    def test_submit_timeout_during_submission(self) -> None:
        cp_result = MagicMock(returncode=0, stderr="", stdout="")
        with (
            patch.object(submit_mod, "check_flink_available", return_value=True),
            patch.object(submit_mod, "is_aggregation_job_running", return_value=False),
            patch.object(
                submit_mod.subprocess,
                "run",
                side_effect=[cp_result, subprocess.TimeoutExpired(cmd="docker", timeout=60)],
            ),
        ):
            assert submit_aggregation_job("http://x:8081") is False

    def test_submit_filenotfound_during_submission(self) -> None:
        cp_result = MagicMock(returncode=0, stderr="", stdout="")
        with (
            patch.object(submit_mod, "check_flink_available", return_value=True),
            patch.object(submit_mod, "is_aggregation_job_running", return_value=False),
            patch.object(submit_mod.subprocess, "run", side_effect=[cp_result, FileNotFoundError]),
        ):
            assert submit_aggregation_job("http://x:8081") is False


class TestEnsureAggregationJobRunning:
    def test_short_circuits_when_already_running(self) -> None:
        with (
            patch.object(submit_mod, "is_aggregation_job_running", return_value=True),
            patch.object(submit_mod, "submit_aggregation_job") as submit_mock,
        ):
            assert ensure_aggregation_job_running("http://x:8081") is True
        submit_mock.assert_not_called()

    def test_falls_through_to_submit(self) -> None:
        with (
            patch.object(submit_mod, "is_aggregation_job_running", return_value=False),
            patch.object(submit_mod, "submit_aggregation_job", return_value=True) as submit_mock,
        ):
            assert ensure_aggregation_job_running("http://x:8081") is True
        submit_mock.assert_called_once_with("http://x:8081")

    def test_uses_env_when_no_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("FLINK_REST_URL", "http://env-host:1")
        with (
            patch.object(submit_mod, "is_aggregation_job_running", return_value=False),
            patch.object(submit_mod, "submit_aggregation_job", return_value=False) as submit_mock,
        ):
            ensure_aggregation_job_running()
        submit_mock.assert_called_once_with("http://env-host:1")
