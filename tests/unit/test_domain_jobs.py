"""Unit tests for backwards-compatibility shim src.domain.jobs.

The module is a thin re-export of Task (aliased as Job) from src.domain.tasks.
Tests verify the public surface and that Job and Task remain the same type.
"""

from __future__ import annotations

import src.domain.jobs as jobs_module
from src.domain.jobs import Job, Task


class TestJobsModuleExports:
    """Tests for the public surface of src.domain.jobs."""

    def test_module_exposes_job_and_task(self) -> None:
        assert hasattr(jobs_module, "Job")
        assert hasattr(jobs_module, "Task")

    def test_all_lists_job_and_task(self) -> None:
        assert "Job" in jobs_module.__all__
        assert "Task" in jobs_module.__all__

    def test_job_is_alias_of_task(self) -> None:
        # The module is a backwards-compat shim; Job is just Task.
        assert Job is Task

    def test_re_export_matches_canonical_module(self) -> None:
        from src.domain.tasks import Task as CanonicalTask

        assert Task is CanonicalTask
        assert jobs_module.Task is CanonicalTask
        assert jobs_module.Job is CanonicalTask


class TestJobAliasBehaviour:
    """Tests verifying Job behaves like Task when used via the shim."""

    def test_construct_via_job_alias(self) -> None:
        job = Job(
            task_id="t1",
            directory_key="inbox",
            path="/tmp/x.png",
            created_at_unix=1.0,
            fingerprint="fp",
        )

        assert isinstance(job, Task)
        assert job.task_id == "t1"

    def test_job_and_task_instances_compare_equal(self) -> None:
        kwargs = {
            "task_id": "t1",
            "directory_key": "inbox",
            "path": "/tmp/x.png",
            "created_at_unix": 1.0,
            "fingerprint": "fp",
        }
        a = Job(**kwargs)
        b = Task(**kwargs)

        assert a == b
        assert type(a) is type(b)
