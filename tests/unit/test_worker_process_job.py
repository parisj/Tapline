# tests/unit/persistence/test_persistence_service_unit.py

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, cast
from unittest.mock import MagicMock

import pytest
from psycopg.types.json import Jsonb

from src.domain.jobs import Job
from src.persistence.service import PersistenceService

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

# -----------------------------
# Minimal protocols (typed seams)
# -----------------------------


class Cursor(Protocol):
    """Protocol for the subset of psycopg cursor API used by PersistenceService."""

    def execute(self, query: str, params: Sequence[Any] | None = None) -> None: ...
    def fetchone(self) -> Mapping[str, Any] | None: ...
    def fetchall(self) -> Sequence[Mapping[str, Any]]: ...


class CursorContextManager(Protocol):
    """Protocol for context-managed cursor() results."""

    def __enter__(self) -> Cursor: ...
    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool: ...


class Connection(Protocol):
    """Protocol for the subset of psycopg connection API used by PersistenceService."""

    def cursor(self) -> CursorContextManager: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...

@dataclass(frozen=True, slots=True)
class FakeArtifact:
    """Small, explicit artifact shape used by persist_result."""

    name: str
    mime: str
    data: bytes


@dataclass(frozen=True, slots=True)
class FakeAlgo:
    """Small, explicit algo shape used by persist_result."""

    name: str
    version: str


@dataclass(frozen=True, slots=True)
class FakeAlgoResult:
    """Small, explicit result shape used by persist_result."""

    metrics: Mapping[str, Any]
    artifacts: Sequence[FakeArtifact]


@dataclass(frozen=True, slots=True)
class DbFakes:
    """Grouped fakes for convenience and clarity."""

    con: Connection
    cur: Cursor
    con_mock: MagicMock
    cur_mock: MagicMock


# -----------------------------
# Fixtures
# -----------------------------


@pytest.fixture()
def db_fakes() -> DbFakes:
    """Build a fully in-memory fake psycopg connection/cursor with context-manager behavior.

    Returns:
        DbFakes: Typed wrapper around MagicMock objects configured to behave like psycopg.

    """
    con_mock: MagicMock = MagicMock(name="con")
    cur_mock: MagicMock = MagicMock(name="cur")

    # cursor() returns a context manager whose __enter__ yields the cursor mock.
    cursor_cm: MagicMock = MagicMock(name="cursor_cm")
    cursor_cm.__enter__.return_value = cur_mock
    cursor_cm.__exit__.return_value = False
    con_mock.cursor.return_value = cursor_cm

    con = cast("Connection", con_mock)
    cur = cast("Cursor", cur_mock)

    return DbFakes(con=con, cur=cur, con_mock=con_mock, cur_mock=cur_mock)


@pytest.fixture()
def service(monkeypatch: pytest.MonkeyPatch, db_fakes: DbFakes) -> PersistenceService:
    """Create a PersistenceService instance that never touches the environment or a real database.

    Implementation details:
    - Bypasses DSN read and schema initialization
    - Replaces _connect() with a deterministic fake connection

    Returns:
        PersistenceService: Service under test with DB interactions fully mocked.

    """
    monkeypatch.setattr(PersistenceService, "_read_dsn_from_env", lambda _self: "dsn")
    monkeypatch.setattr(PersistenceService, "_init_db_postgres", lambda _self: None)

    svc: PersistenceService = PersistenceService()
    monkeypatch.setattr(svc, "_connect", lambda: db_fakes.con)

    return svc


# -----------------------------
# Helpers
# -----------------------------


def _get_execute_call(
    cursor_mock: MagicMock,
    index: int = 0,
) -> tuple[str, Sequence[Any] | None]:
    """Get a specific cursor.execute call (query, params) from call_args_list.

    Args:
        cursor_mock: The cursor MagicMock.
        index: Which call to inspect.

    Returns:
        Tuple[str, Sequence[Any] | None]: (sql, params)

    """
    sql = cast("str", cursor_mock.execute.call_args_list[index].args[0])
    params = cast("Sequence[Any] | None", cursor_mock.execute.call_args_list[index].args[1])
    return sql, params


def _patch_time(monkeypatch: pytest.MonkeyPatch, value: float) -> None:
    """Patch time.time inside src.persistence.service to a deterministic value.

    Args:
        monkeypatch: pytest monkeypatch fixture.
        value: Fixed timestamp.

    """
    monkeypatch.setattr("src.persistence.service.time.time", lambda: value)


# -----------------------------
# Tests: Jobs lifecycle
# -----------------------------


def test_upsert_job_created_executes_insert_and_commits(
    service: PersistenceService,
    db_fakes: DbFakes,
) -> None:
    """upsert_job_created should:.
    
    - execute INSERT with ON CONFLICT DO NOTHING
    - commit
    - close the connection
    """
    job = Job(
        job_id="job-1",
        directory_key="path0",
        path="/tmp/x.jpg",
        fingerprint="fp",
        created_at_unix=123.0,
    )

    service.upsert_job_created(job)

    assert db_fakes.cur_mock.execute.call_count == 1

    sql, params = _get_execute_call(db_fakes.cur_mock, 0)
    assert "INSERT INTO jobs" in sql
    assert "ON CONFLICT (job_id) DO NOTHING" in sql
    assert params == (job.job_id, job.directory_key, job.path, job.fingerprint, job.created_at_unix)

    db_fakes.con_mock.commit.assert_called_once()
    db_fakes.con_mock.rollback.assert_not_called()
    db_fakes.con_mock.close.assert_called_once()


def test_mark_job_started_uses_time_and_commits(
    service: PersistenceService,
    db_fakes: DbFakes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """mark_job_started should.

    - update status + started_at_unix with time.time()
    - commit
    - close connection
    """
    _patch_time(monkeypatch, 1000.0)

    service.mark_job_started("job-1")

    sql, params = _get_execute_call(db_fakes.cur_mock, 0)
    assert "UPDATE jobs SET status='started'" in sql
    assert params == (1000.0, "job-1")

    db_fakes.con_mock.commit.assert_called_once()
    db_fakes.con_mock.close.assert_called_once()


def test_mark_job_failed_uses_time_and_commits(
    service: PersistenceService,
    db_fakes: DbFakes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """mark_job_failed should:.

    - update status + finished_at_unix + error with time.time()
    - commit
    - close connection
    """
    _patch_time(monkeypatch, 2000.0)

    service.mark_job_failed("job-1", error="boom")

    sql, params = _get_execute_call(db_fakes.cur_mock, 0)
    assert "UPDATE jobs SET status='failed'" in sql
    assert params == (2000.0, "boom", "job-1")

    db_fakes.con_mock.commit.assert_called_once()
    db_fakes.con_mock.close.assert_called_once()


def test_upsert_job_created_rolls_back_on_exception(
    service: PersistenceService,
    db_fakes: DbFakes,
) -> None:
    """On execute exception, upsert_job_created should:.

    - rollback
    - re-raise
    - close connection
    """
    db_fakes.cur_mock.execute.side_effect = RuntimeError("db down")

    job = Job(
        job_id="job-1",
        directory_key="path0",
        path="/tmp/x.jpg",
        fingerprint="fp",
        created_at_unix=123.0,
    )

    with pytest.raises(RuntimeError, match="db down"):
        service.upsert_job_created(job)

    db_fakes.con_mock.rollback.assert_called_once()
    db_fakes.con_mock.commit.assert_not_called()
    db_fakes.con_mock.close.assert_called_once()


# -----------------------------
# Tests: Results + artifacts
# -----------------------------


def test_persist_result_upserts_result_inserts_artifacts_and_returns_ref(
    service: PersistenceService,
    db_fakes: DbFakes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """persist_result should:.

    - upsert the results row with JSONB metrics
    - fetch RETURNING id
    - upsert each artifact for that result_id
    - commit
    - return PersistedResultRef with the returned result_id
    """
    _patch_time(monkeypatch, 1111.0)

    algo = FakeAlgo(name="dummy", version="1.0.0")
    result = FakeAlgoResult(
        metrics={"size": 6},
        artifacts=(
            FakeArtifact(name="a.png", mime="image/png", data=b"png"),
            FakeArtifact(name="b.txt", mime="text/plain", data=b"txt"),
        ),
    )

    db_fakes.cur_mock.fetchone.return_value = {"id": 42}

    ref = service.persist_result(job_id="job-1", algo=cast(Any, algo), result=cast(Any, result))

    # 1st call: results upsert
    assert db_fakes.cur_mock.execute.call_count == 1 + len(result.artifacts)

    first_sql, first_params = _get_execute_call(db_fakes.cur_mock, 0)
    assert "INSERT INTO results" in first_sql
    assert "ON CONFLICT (job_id, algo_name, algo_version)" in first_sql
    assert "RETURNING id" in first_sql

    assert first_params is not None
    assert first_params[0] == "job-1"
    assert first_params[1] == "dummy"
    assert first_params[2] == "1.0.0"
    assert isinstance(first_params[3], Jsonb)
    assert first_params[4] == 1111.0

    # Next calls: artifacts upserts
    for i, art in enumerate(result.artifacts, start=1):
        sql, params = _get_execute_call(db_fakes.cur_mock, i)
        assert "INSERT INTO artifacts" in sql
        assert params == (42, art.name, art.mime, art.data)

    db_fakes.con_mock.commit.assert_called_once()
    db_fakes.con_mock.rollback.assert_not_called()
    db_fakes.con_mock.close.assert_called_once()

    assert ref.job_id == "job-1"
    assert ref.algo_name == "dummy"
    assert ref.algo_version == "1.0.0"
    assert ref.result_id == 42


def test_persist_result_rolls_back_on_exception(
    service: PersistenceService,
    db_fakes: DbFakes,
) -> None:
    """On exception inside persist_result, the service should rollback, re-raise, and close."""
    db_fakes.cur_mock.execute.side_effect = RuntimeError("write failed")

    algo = FakeAlgo(name="dummy", version="1.0.0")
    result = FakeAlgoResult(metrics={"size": 6}, artifacts=())

    with pytest.raises(RuntimeError, match="write failed"):
        service.persist_result(job_id="job-1", algo=cast(Any, algo), result=cast(Any, result))

    db_fakes.con_mock.rollback.assert_called_once()
    db_fakes.con_mock.commit.assert_not_called()
    db_fakes.con_mock.close.assert_called_once()
