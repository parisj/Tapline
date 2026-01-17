from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from src.domain.results import PersistedResultRef, metrics_to_jsonable
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping

    from src.algorithms.base import Algorithm
    from src.domain.jobs import Job
    from src.domain.results import AlgoResult, Artifact

logger = get_logger(__name__)


class PersistenceService:
    """PostgreSQL persistence service.

    Responsibilities:
    - Owns schema initialization (DDL)
    - Performs idempotent writes (ON CONFLICT)
    - Stores JSON in JSONB, binary artifacts in BYTEA
    """

    def __init__(self) -> None:
        self._dsn = self._read_dsn_from_env()
        self._init_db_postgres()

    @staticmethod
    def _read_dsn_from_env() -> str:
        """Build a psycopg DSN string from environment variables.

        Required:
          DB_HOST, DB_NAME, DB_USER, DB_PASSWORD
        Optional:
          DB_PORT (default 5432)
          DB_SSLMODE (default prefer)
        """
        host = os.environ["DB_HOST"]
        port = int(os.environ.get("DB_PORT", "5432"))
        name = os.environ["DB_NAME"]
        user = os.environ["DB_USER"]
        password = os.environ["DB_PASSWORD"]
        sslmode = os.environ.get("DB_SSLMODE", "prefer")

        # DSN format psycopg accepts
        return f"host={host} port={port} dbname={name} user={user} password={password} sslmode={sslmode}"

    def _connect(self) -> psycopg.Connection:
        """Open a new connection per call.

        This is simple and safe. If you later need more throughput, move to a pool.
        """
        return psycopg.connect(self._dsn, connect_timeout=30, row_factory=dict_row)

    def _init_db_postgres(self) -> None:
        """Initialize schema in Postgres.

        Uses schema_postgres.sql from the package directory.
        """
        schema_path = Path(__file__).with_name("schema_postgres.sql")
        if not schema_path.exists():
            msg = f"Schema file missing: {schema_path}"
            raise FileNotFoundError(msg)

        ddl = schema_path.read_text(encoding="utf-8")

        # psycopg does NOT have executescript; we split on semicolons safely enough
        # for our simple DDL file (no stored procedures).
        statements = [s.strip() for s in ddl.split(";") if s.strip()]

        con = self._connect()
        try:
            with con.cursor() as cur:
                for stmt in statements:
                    cur.execute(stmt)
            con.commit()
        except Exception:
            con.rollback()
            logger.exception("Failed to initialize Postgres schema.")
            raise
        finally:
            con.close()

        logger.info("Postgres schema initialized using %s", schema_path)

    # -------------------------
    # Jobs lifecycle
    # -------------------------

    def upsert_job_created(self, job: Job) -> None:
        con = self._connect()
        try:
            with con.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO jobs(job_id, directory_key, path, fingerprint, status, created_at_unix)
                    VALUES (%s, %s, %s, %s, 'created', %s)
                    ON CONFLICT (job_id) DO NOTHING;
                    """,
                    (job.job_id, job.directory_key, job.path, job.fingerprint, job.created_at_unix),
                )
            con.commit()
        except Exception:
            con.rollback()
            logger.exception("upsert_job_created failed for job_id=%s", job.job_id)
            raise
        finally:
            con.close()

    def mark_job_started(self, job_id: str) -> None:
        con = self._connect()
        try:
            with con.cursor() as cur:
                cur.execute(
                    "UPDATE jobs SET status='started', started_at_unix=%s WHERE job_id=%s;",
                    (time.time(), job_id),
                )
            con.commit()
        except Exception:
            con.rollback()
            logger.exception("mark_job_started failed for job_id=%s", job_id)
            raise
        finally:
            con.close()

    def mark_job_finished(self, job_id: str) -> None:
        con = self._connect()
        try:
            with con.cursor() as cur:
                cur.execute(
                    "UPDATE jobs SET status='completed', finished_at_unix=%s WHERE job_id=%s;",
                    (time.time(), job_id),
                )
            con.commit()
        except Exception:
            con.rollback()
            logger.exception("mark_job_finished failed for job_id=%s", job_id)
            raise
        finally:
            con.close()

    def mark_job_failed(self, job_id: str, error: str) -> None:
        con = self._connect()
        try:
            with con.cursor() as cur:
                cur.execute(
                    "UPDATE jobs SET status='failed', finished_at_unix=%s, error=%s WHERE job_id=%s;",
                    (time.time(), error, job_id),
                )
            con.commit()
        except Exception:
            con.rollback()
            logger.exception("mark_job_failed failed for job_id=%s", job_id)
            raise
        finally:
            con.close()

    # -------------------------
    # Results + artifacts
    # -------------------------

    def persist_result(self, job_id: str, algo: Algorithm, result: AlgoResult) -> PersistedResultRef:
        """Insert result row (idempotent) and insert artifacts (idempotent per result_id+name)."""
        con = self._connect()
        try:
            metrics_jsonable = metrics_to_jsonable(result.metrics)

            with con.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO results(job_id, algo_name, algo_version, metrics_json, created_at_unix)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (job_id, algo_name, algo_version)
                    DO UPDATE SET
                        metrics_json = EXCLUDED.metrics_json,
                        created_at_unix = EXCLUDED.created_at_unix
                    RETURNING id;
                    """,
                    (job_id, algo.name, algo.version, Jsonb(metrics_jsonable), time.time()),
                )
                row = cur.fetchone()
                if not row or "id" not in row:
                    msg = "Failed to persist/return results.id"
                    raise RuntimeError(msg)
                result_id = int(row["id"])
                artifacts = result.artifacts or []
                for art in artifacts:
                    cur.execute(
                        """
                        INSERT INTO artifacts(result_id, name, mime, data)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (result_id, name) DO UPDATE SET
                            mime = EXCLUDED.mime,
                            data = EXCLUDED.data;
                        """,
                        (result_id, art.name, art.mime, art.data),
                    )

            con.commit()
            return PersistedResultRef(
                job_id=job_id,
                algo_name=algo.name,
                algo_version=algo.version,
                result_id=result_id,
            )

        except Exception:
            con.rollback()
            logger.exception("persist_result failed for job_id=%s algo=%s:%s", job_id, algo.name, algo.version)
            raise
        finally:
            con.close()

    def fetch_completed_results(self, window_start_unix: float, window_end_unix: float) -> list[dict[str, Any]]:
        con = self._connect()
        try:
            with con.cursor() as cur:
                cur.execute(
                    """
                    SELECT r.id, r.job_id, r.algo_name, r.algo_version, r.metrics_json, r.created_at_unix
                    FROM results r
                    JOIN jobs j ON j.job_id = r.job_id
                    WHERE j.status='completed'
                      AND r.created_at_unix BETWEEN %s AND %s;
                    """,
                    (window_start_unix, window_end_unix),
                )
                return list(cur.fetchall())
        finally:
            con.close()

    # -------------------------
    # Aggregates + aggregate artifacts
    # -------------------------

    def persist_aggregate(
        self,
        *,
        algo_name: str,
        algo_version: str,
        metric_name: str,
        analysis_kind: str,
        window_start_unix: float,
        window_end_unix: float,
        summary: Mapping[str, Any],
        artifact_hash: str,
    ) -> int:
        """Upsert aggregate row and return aggregates.id."""
        con = self._connect()
        try:
            with con.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO aggregates(
                        algo_name, algo_version, metric_name, analysis_kind,
                        window_start_unix, window_end_unix,
                        summary_json, artifact_hash, created_at_unix
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (
                        algo_name, algo_version, metric_name, analysis_kind,
                        window_start_unix, window_end_unix
                    )
                    DO UPDATE SET
                        summary_json = EXCLUDED.summary_json,
                        created_at_unix = EXCLUDED.created_at_unix
                    RETURNING id;
                    """,
                    (
                        algo_name,
                        algo_version,
                        metric_name,
                        analysis_kind,
                        window_start_unix,
                        window_end_unix,
                        Jsonb(dict(summary)),
                        artifact_hash,
                        time.time(),
                    ),
                )
                row = cur.fetchone()
                if not row or "id" not in row:
                    raise RuntimeError("Failed to persist/return aggregates.id")

            con.commit()
            return int(row["id"])
        except Exception:
            con.rollback()
            logger.exception("persist_aggregate failed for %s %s %s %s", algo_name, algo_version, metric_name, analysis_kind)
            raise
        finally:
            con.close()

    def persist_aggregate_artifact(
        self,
        *,
        artifact_hash: str,
        algo_name: str,
        algo_version: str,
        metric_name: str,
        analysis_kind: str,
        window_start_unix: float,
        window_end_unix: float,
        artifact: Artifact,
    ) -> None:
        """Store binary artifact for a metric+analysis+window (idempotent by artifact_hash)."""
        con = self._connect()
        try:
            with con.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO aggregate_artifacts(
                        artifact_hash,
                        algo_name, algo_version,
                        metric_name, analysis_kind,
                        window_start_unix, window_end_unix,
                        name, mime, data, created_at_unix
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (artifact_hash)
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        mime = EXCLUDED.mime,
                        data = EXCLUDED.data,
                        created_at_unix = EXCLUDED.created_at_unix;
                    """,
                    (
                        artifact_hash,
                        algo_name,
                        algo_version,
                        metric_name,
                        analysis_kind,
                        window_start_unix,
                        window_end_unix,
                        artifact.name,
                        artifact.mime,
                        artifact.data,
                        time.time(),
                    ),
                )
            con.commit()
        except Exception:
            con.rollback()
            logger.exception("persist_aggregate_artifact failed hash=%s", artifact_hash)
            raise
        finally:
            con.close()
