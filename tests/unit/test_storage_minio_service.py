"""Unit tests for MinioStorageService.

The MinIO client is mocked at the src.storage.minio_service.Minio boundary
so no live MinIO connection is required. Tests cover:

- Bucket existence checks and creation on initialisation
- Content-addressed store path including dedup short-circuit
- Hash-checked store via store_with_hash
- Retrieve / retrieve_by_hash / retrieve_by_key (including S3Error mapping)
- Existence and metadata helpers
- iter_objects / list_objects pagination semantics
- Retry behaviour on transient S3Errors
- delete with NoSuchKey -> False mapping
- buckets property exposes config-derived names
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from minio.error import S3Error

from src.storage.minio_service import MinioStorageService
from src.storage.models import ArtifactRef, hash_to_key


@dataclass(frozen=True)
class MockMinioConfig:
    """Minimal stand-in for MinioConfig used by MinioStorageService."""

    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"  # noqa: S105 - test fixture not a real secret
    secure: bool = False
    region: str = "us-east-1"
    bucket_artifacts: str = "artifacts"
    bucket_inputs: str = "inputs"
    bucket_aggregates: str = "aggregates"
    bucket_metric_values: str = "metric-values"
    path_prefix_length: int = 4
    max_object_size: int = 104857600
    max_retries: int = 3
    retry_delay_sec: float = 0.0  # 0 keeps retry tests fast
    artifacts_retention_days: int = 0
    inputs_retention_days: int = 30
    aggregates_retention_days: int = 90
    connect_timeout: float = 10.0
    read_timeout: float = 30.0


def _make_s3_error(code: str, message: str = "fail") -> S3Error:
    """Build an S3Error with the given code (positional args required).

    The constructor signature is (response, code, message, resource, request_id, host_id),
    so 'code' is the second positional argument.
    """
    return S3Error("response", code, message, "resource", "rid", "hid")


def _build_service(
    config: MockMinioConfig | None = None,
    *,
    bucket_exists: bool = True,
) -> tuple[MinioStorageService, MagicMock]:
    """Create a service backed by a MagicMock Minio client."""
    config = config or MockMinioConfig()
    with patch("src.storage.minio_service.Minio") as minio_cls:
        client = MagicMock()
        client.bucket_exists.return_value = bucket_exists
        minio_cls.return_value = client
        service = MinioStorageService(config)
    return service, client


class TestInitAndBucketEnsure:
    def test_init_constructs_minio_client_with_config(self) -> None:
        config = MockMinioConfig()
        with patch("src.storage.minio_service.Minio") as minio_cls:
            client = MagicMock()
            client.bucket_exists.return_value = True
            minio_cls.return_value = client

            MinioStorageService(config)

            minio_cls.assert_called_once()
            kwargs = minio_cls.call_args.kwargs
            assert kwargs["endpoint"] == config.endpoint
            assert kwargs["access_key"] == config.access_key
            assert kwargs["secret_key"] == config.secret_key
            assert kwargs["secure"] is False
            assert kwargs["region"] == "us-east-1"

    def test_init_does_not_make_existing_buckets(self) -> None:
        _, client = _build_service(bucket_exists=True)

        # All four buckets exist, so make_bucket must not be called.
        assert client.make_bucket.call_count == 0
        # Existence was probed once per configured bucket.
        assert client.bucket_exists.call_count == 4

    def test_init_creates_missing_buckets(self) -> None:
        _, client = _build_service(bucket_exists=False)

        created = {call.args[0] for call in client.make_bucket.call_args_list}
        assert created == {"artifacts", "inputs", "aggregates", "metric-values"}

    def test_buckets_property_returns_named_mapping(self) -> None:
        service, _ = _build_service()

        assert service.buckets == {
            "artifacts": "artifacts",
            "inputs": "inputs",
            "aggregates": "aggregates",
            "metric-values": "metric-values",
        }


class TestStore:
    def test_store_puts_object_when_missing(self) -> None:
        service, client = _build_service()
        # Object lookup raises NoSuchKey -> treated as missing.
        client.stat_object.side_effect = _make_s3_error("NoSuchKey")

        ref = service.store(b"hello world", "artifacts", mime="text/plain")

        assert isinstance(ref, ArtifactRef)
        assert ref.bucket == "artifacts"
        assert ref.size == len(b"hello world")
        assert ref.key == hash_to_key(ref.content_hash, 4)
        assert client.put_object.call_count == 1

        put_kwargs = client.put_object.call_args.kwargs
        assert put_kwargs["bucket_name"] == "artifacts"
        assert put_kwargs["object_name"] == ref.key
        assert put_kwargs["length"] == len(b"hello world")
        assert put_kwargs["content_type"] == "text/plain"

    def test_store_passes_metadata_through(self) -> None:
        service, client = _build_service()
        client.stat_object.side_effect = _make_s3_error("NoSuchKey")

        service.store(b"x", "artifacts", metadata={"k": "v"})

        assert client.put_object.call_args.kwargs["metadata"] == {"k": "v"}

    def test_store_deduplicates_existing_object(self) -> None:
        service, client = _build_service()
        # stat_object returns a stat (no exception) -> object exists.
        client.stat_object.return_value = MagicMock()

        ref = service.store(b"hello world", "artifacts")

        assert client.put_object.call_count == 0
        assert ref.bucket == "artifacts"
        assert ref.size == len(b"hello world")


class TestStoreWithHash:
    def test_rejects_hash_mismatch(self) -> None:
        service, _ = _build_service()

        with pytest.raises(ValueError, match="Hash mismatch"):
            service.store_with_hash(b"data", content_hash="bad", bucket="artifacts")

    def test_uses_provided_hash_when_matching(self) -> None:
        import hashlib

        service, client = _build_service()
        client.stat_object.side_effect = _make_s3_error("NoSuchKey")
        data = b"matched"
        ch = hashlib.sha256(data).hexdigest()

        ref = service.store_with_hash(data, content_hash=ch, bucket="inputs")

        assert ref.content_hash == ch
        assert ref.bucket == "inputs"
        assert client.put_object.call_count == 1

    def test_dedup_path_skips_put(self) -> None:
        import hashlib

        service, client = _build_service()
        client.stat_object.return_value = MagicMock()
        data = b"already-stored"
        ch = hashlib.sha256(data).hexdigest()

        ref = service.store_with_hash(data, content_hash=ch, bucket="artifacts")

        assert ref.content_hash == ch
        assert client.put_object.call_count == 0


class TestRetrieve:
    def _make_response(self, payload: bytes) -> MagicMock:
        response = MagicMock()
        response.read.return_value = payload
        return response

    def test_retrieve_by_ref(self) -> None:
        service, client = _build_service()
        client.get_object.return_value = self._make_response(b"payload")

        ref = ArtifactRef(
            content_hash="hash",
            bucket="artifacts",
            key="ab/cd/hash",
            size=7,
            stored_at=datetime.now(),
        )

        assert service.retrieve(ref) == b"payload"
        client.get_object.assert_called_once_with("artifacts", "ab/cd/hash")

    def test_retrieve_releases_connection(self) -> None:
        service, client = _build_service()
        response = self._make_response(b"payload")
        client.get_object.return_value = response

        ref = ArtifactRef(
            content_hash="hash",
            bucket="artifacts",
            key="ab/cd/hash",
            size=7,
            stored_at=datetime.now(),
        )
        service.retrieve(ref)

        response.close.assert_called_once()
        response.release_conn.assert_called_once()

    def test_retrieve_by_hash_constructs_key(self) -> None:
        service, client = _build_service()
        client.get_object.return_value = self._make_response(b"abc")
        ch = "deadbeef" + "0" * 56

        service.retrieve_by_hash("artifacts", ch)

        expected_key = hash_to_key(ch, 4)
        client.get_object.assert_called_once_with("artifacts", expected_key)

    def test_retrieve_by_key_maps_nosuchkey_to_filenotfound(self) -> None:
        service, client = _build_service()
        client.get_object.side_effect = _make_s3_error("NoSuchKey")

        with pytest.raises(FileNotFoundError, match="Object not found"):
            service.retrieve_by_key("artifacts", "missing")

    def test_retrieve_by_key_reraises_other_s3error(self) -> None:
        service, client = _build_service()
        client.get_object.side_effect = _make_s3_error("AccessDenied")

        with pytest.raises(S3Error):
            service.retrieve_by_key("artifacts", "blocked")


class TestExistsAndInfo:
    def test_exists_true_when_stat_succeeds(self) -> None:
        service, client = _build_service()
        client.stat_object.return_value = MagicMock()

        assert service.exists("artifacts", "abc123") is True

    def test_exists_false_when_nosuchkey(self) -> None:
        service, client = _build_service()
        client.stat_object.side_effect = _make_s3_error("NoSuchKey")

        assert service.exists("artifacts", "abc123") is False

    def test_exists_raises_on_other_error(self) -> None:
        service, client = _build_service()
        client.stat_object.side_effect = _make_s3_error("InternalError")

        with pytest.raises(S3Error):
            service.exists("artifacts", "abc123")

    def test_exists_ref(self) -> None:
        service, client = _build_service()
        client.stat_object.return_value = MagicMock()
        ref = ArtifactRef(
            content_hash="h",
            bucket="artifacts",
            key="k",
            size=1,
            stored_at=datetime.now(),
        )

        assert service.exists_ref(ref) is True
        client.stat_object.assert_called_with("artifacts", "k")

    def test_get_object_info_returns_metadata(self) -> None:
        service, client = _build_service()
        stat = MagicMock(
            size=123,
            last_modified="ts",
            etag="etag",
            content_type="text/plain",
            metadata={"k": "v"},
        )
        client.stat_object.return_value = stat

        ref = ArtifactRef(
            content_hash="h",
            bucket="artifacts",
            key="k",
            size=1,
            stored_at=datetime.now(),
        )
        info = service.get_object_info(ref)

        assert info is not None
        assert info["size"] == 123
        assert info["etag"] == "etag"
        assert info["content_type"] == "text/plain"
        assert info["metadata"] == {"k": "v"}

    def test_get_object_info_returns_none_when_missing(self) -> None:
        service, client = _build_service()
        client.stat_object.side_effect = _make_s3_error("NoSuchKey")

        ref = ArtifactRef(
            content_hash="h",
            bucket="artifacts",
            key="k",
            size=1,
            stored_at=datetime.now(),
        )
        assert service.get_object_info(ref) is None


class TestListing:
    def test_iter_objects_yields_dicts(self) -> None:
        service, client = _build_service()
        client.list_objects.return_value = iter(
            [
                MagicMock(object_name="ab/cd/h1", size=1, last_modified="t1", etag="e1"),
                MagicMock(object_name="ab/cd/h2", size=2, last_modified="t2", etag="e2"),
            ],
        )

        results = list(service.iter_objects("artifacts"))

        assert len(results) == 2
        assert results[0] == {"key": "ab/cd/h1", "size": 1, "last_modified": "t1", "etag": "e1"}

    def test_iter_objects_respects_limit(self) -> None:
        service, client = _build_service()
        client.list_objects.return_value = iter(
            [MagicMock(object_name=f"k{i}", size=i, last_modified="t", etag="e") for i in range(5)],
        )

        results = list(service.iter_objects("artifacts", limit=2))

        assert len(results) == 2

    def test_list_objects_materialises_iterator(self) -> None:
        service, client = _build_service()
        client.list_objects.return_value = iter(
            [
                MagicMock(object_name="k", size=1, last_modified="t", etag="e"),
            ],
        )

        results = service.list_objects("artifacts", prefix="ab/", limit=10)

        assert isinstance(results, list)
        assert len(results) == 1
        # Prefix and recursive passed through to client.list_objects.
        client.list_objects.assert_called_once()
        assert client.list_objects.call_args.kwargs["prefix"] == "ab/"
        assert client.list_objects.call_args.kwargs["recursive"] is True


class TestDelete:
    def test_delete_returns_true_on_success(self) -> None:
        service, client = _build_service()
        ref = ArtifactRef(
            content_hash="h",
            bucket="artifacts",
            key="k",
            size=1,
            stored_at=datetime.now(),
        )

        assert service.delete(ref) is True
        client.remove_object.assert_called_once_with("artifacts", "k")

    def test_delete_returns_false_on_missing(self) -> None:
        service, client = _build_service()
        client.remove_object.side_effect = _make_s3_error("NoSuchKey")
        ref = ArtifactRef(
            content_hash="h",
            bucket="artifacts",
            key="k",
            size=1,
            stored_at=datetime.now(),
        )

        assert service.delete(ref) is False

    def test_delete_raises_on_other_error(self) -> None:
        service, client = _build_service()
        client.remove_object.side_effect = _make_s3_error("AccessDenied")
        ref = ArtifactRef(
            content_hash="h",
            bucket="artifacts",
            key="k",
            size=1,
            stored_at=datetime.now(),
        )

        with pytest.raises(S3Error):
            service.delete(ref)


class TestRetryBehaviour:
    def test_put_retries_on_transient_error(self) -> None:
        service, client = _build_service()
        client.stat_object.side_effect = _make_s3_error("NoSuchKey")
        # First two attempts fail, third succeeds.
        client.put_object.side_effect = [
            _make_s3_error("InternalError"),
            _make_s3_error("InternalError"),
            None,
        ]

        ref = service.store(b"data", "artifacts")

        assert client.put_object.call_count == 3
        assert ref.bucket == "artifacts"

    def test_put_raises_runtimeerror_after_max_retries(self) -> None:
        service, client = _build_service()
        client.stat_object.side_effect = _make_s3_error("NoSuchKey")
        client.put_object.side_effect = _make_s3_error("InternalError")

        with pytest.raises(RuntimeError, match="Failed to store object after"):
            service.store(b"data", "artifacts")

        assert client.put_object.call_count == 3  # default max_retries


class TestStoreWithKey:
    def test_uses_explicit_key(self) -> None:
        service, client = _build_service()

        service.store_with_key(
            b"data",
            bucket="aggregates",
            key="window/2024/01/01.bin",
            mime="application/octet-stream",
            metadata={"x": "y"},
        )

        client.put_object.assert_called_once()
        kwargs = client.put_object.call_args.kwargs
        assert kwargs["bucket_name"] == "aggregates"
        assert kwargs["object_name"] == "window/2024/01/01.bin"
        assert kwargs["metadata"] == {"x": "y"}
