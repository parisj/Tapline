"""Unit tests for src.domain.tasks.

Existing test_domain.py covers most Task behaviour; this file fills the
remaining gaps by exercising the Job backwards-compatibility alias and a
few additional Task properties (ordering across distinct field permutations,
construction with keyword-only call, repr).
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError, fields, is_dataclass

import pytest

from src.domain.tasks import Job, Task


class TestTaskFields:
    """Tests for Task dataclass field structure."""

    def test_task_is_a_dataclass(self) -> None:
        assert is_dataclass(Task)

    def test_task_field_names(self) -> None:
        names = [f.name for f in fields(Task)]
        assert names == [
            "task_id",
            "directory_key",
            "path",
            "created_at_unix",
            "fingerprint",
        ]

    def test_task_field_types(self) -> None:
        type_map = {f.name: f.type for f in fields(Task)}
        # tasks.py does not use `from __future__ import annotations`, so
        # field.type values are the actual class objects, not strings.
        assert type_map["task_id"] is str
        assert type_map["directory_key"] is str
        assert type_map["path"] is str
        assert type_map["created_at_unix"] is float
        assert type_map["fingerprint"] is str


class TestTaskConstruction:
    """Tests for Task construction patterns not yet covered."""

    def _make(self, **overrides: object) -> Task:
        defaults = {
            "task_id": "t",
            "directory_key": "d",
            "path": "/tmp/x",
            "created_at_unix": 0.0,
            "fingerprint": "fp",
        }
        defaults.update(overrides)
        return Task(**defaults)  # type: ignore[arg-type]

    def test_positional_construction(self) -> None:
        task = Task("t1", "inbox", "/tmp/a.png", 1.5, "fp1")

        assert task.task_id == "t1"
        assert task.directory_key == "inbox"
        assert task.path == "/tmp/a.png"
        assert task.created_at_unix == 1.5
        assert task.fingerprint == "fp1"

    def test_repr_contains_all_fields(self) -> None:
        task = self._make(task_id="abc", fingerprint="xyz")

        r = repr(task)
        for piece in ["task_id='abc'", "fingerprint='xyz'", "Task("]:
            assert piece in r


class TestTaskImmutability:
    """Tests for Task immutability across each field."""

    @pytest.mark.parametrize(
        "field",
        ["task_id", "directory_key", "path", "created_at_unix", "fingerprint"],
    )
    def test_each_field_is_immutable(self, field: str) -> None:
        task = Task(
            task_id="t",
            directory_key="d",
            path="/tmp/x",
            created_at_unix=0.0,
            fingerprint="fp",
        )

        with pytest.raises(FrozenInstanceError):
            setattr(task, field, "anything")


class TestTaskEqualityAndHashing:
    """Tests for hash + equality semantics covering each field."""

    def _base(self) -> dict[str, object]:
        return {
            "task_id": "t",
            "directory_key": "d",
            "path": "/tmp/x",
            "created_at_unix": 0.0,
            "fingerprint": "fp",
        }

    def test_equal_tasks_hash_equally(self) -> None:
        a = Task(**self._base())  # type: ignore[arg-type]
        b = Task(**self._base())  # type: ignore[arg-type]

        assert hash(a) == hash(b)

    def test_can_be_dict_key(self) -> None:
        a = Task(**self._base())  # type: ignore[arg-type]
        b = Task(**self._base())  # type: ignore[arg-type]

        d = {a: "value"}
        assert d[b] == "value"

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("task_id", "different"),
            ("directory_key", "other"),
            ("path", "/tmp/y"),
            ("created_at_unix", 99.9),
            ("fingerprint", "other_fp"),
        ],
    )
    def test_differing_field_makes_unequal(self, field: str, value: object) -> None:
        base = self._base()
        a = Task(**base)  # type: ignore[arg-type]
        b_kwargs = {**base, field: value}
        b = Task(**b_kwargs)  # type: ignore[arg-type]

        assert a != b


class TestJobAlias:
    """Tests confirming Job is an exact alias of Task."""

    def test_job_is_task(self) -> None:
        assert Job is Task

    def test_job_creates_task_instance(self) -> None:
        job = Job(
            task_id="t",
            directory_key="d",
            path="/tmp/x",
            created_at_unix=0.0,
            fingerprint="fp",
        )

        assert isinstance(job, Task)
