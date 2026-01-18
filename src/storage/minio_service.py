"""MinIO storage service for content-addressed artifact storage.

Provides:
- Content-addressed storage with hash-based paths
- Automatic deduplication via content hash
- Retry logic for resilience
- Bucket management
"""

from __future__ import annotations

import hashlib
import io
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any

from minio import Minio
from minio.error import S3Error

from src.storage.models import ObjectRef, hash_to_key
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from src.storage.config import MinioConfig

logger = get_logger(__name__)


class MinioStorageService:
    """MinIO storage service with content-addressed storage.

    Usage:
        service = MinioStorageService(config)

        # Store artifact (content-addressed)
        ref = service.store(data, bucket="artifacts", mime="application/octet-stream")

        # Retrieve by reference
        data = service.retrieve(ref)

        # Check existence by hash
        exists = service.exists("artifacts", content_hash)
    """

    def __init__(self, config: MinioConfig) -> None:
        self._config = config
        self._client = Minio(
            endpoint=config.endpoint,
            access_key=config.access_key,
            secret_key=config.secret_key,
            secure=config.secure,
            region=config.region,
        )
        self._ensure_buckets()

        logger.info(
            "MinioStorageService initialized: endpoint=%s",
            config.endpoint,
        )

    def _ensure_buckets(self) -> None:
        """Ensure required buckets exist."""
        buckets = [
            self._config.bucket_artifacts,
            self._config.bucket_inputs,
            self._config.bucket_aggregates,
        ]
        for bucket in buckets:
            if not self._client.bucket_exists(bucket):
                self._client.make_bucket(bucket)
                logger.info("Created bucket: %s", bucket)

    def store(
        self,
        data: bytes,
        bucket: str,
        *,
        mime: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
    ) -> ObjectRef:
        """Store data with content-addressed key.

        If data with same hash already exists, returns reference
        to existing object (deduplication).

        Args:
            data: Binary data to store
            bucket: Target bucket name
            mime: MIME type
            metadata: Optional object metadata

        Returns:
            ObjectRef pointing to stored object

        """
        content_hash = hashlib.sha256(data).hexdigest()
        key = hash_to_key(content_hash, self._config.path_prefix_length)

        # Check for existing object (dedup)
        if self._object_exists(bucket, key):
            logger.debug("Object already exists: %s/%s", bucket, key)
            return ObjectRef(
                content_hash=content_hash,
                bucket=bucket,
                key=key,
                size=len(data),
                stored_at=datetime.now(),
            )

        # Store with retries
        self._put_object_with_retry(
            bucket=bucket,
            key=key,
            data=data,
            mime=mime,
            metadata=metadata,
        )

        ref = ObjectRef(
            content_hash=content_hash,
            bucket=bucket,
            key=key,
            size=len(data),
            stored_at=datetime.now(),
        )

        logger.debug("Stored object: %s", ref.full_path)
        return ref

    def store_with_hash(
        self,
        data: bytes,
        content_hash: str,
        bucket: str,
        *,
        mime: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
    ) -> ObjectRef:
        """Store data with pre-computed hash.

        Use when hash is already known (e.g., from event payload).

        Args:
            data: Binary data to store
            content_hash: Pre-computed SHA-256 hash
            bucket: Target bucket name
            mime: MIME type
            metadata: Optional object metadata

        Returns:
            ObjectRef pointing to stored object

        """
        # Verify hash matches
        actual_hash = hashlib.sha256(data).hexdigest()
        if actual_hash != content_hash:
            msg = f"Hash mismatch: expected {content_hash}, got {actual_hash}"
            raise ValueError(
                msg,
            )

        key = hash_to_key(content_hash, self._config.path_prefix_length)

        if self._object_exists(bucket, key):
            return ObjectRef(
                content_hash=content_hash,
                bucket=bucket,
                key=key,
                size=len(data),
                stored_at=datetime.now(),
            )

        self._put_object_with_retry(
            bucket=bucket,
            key=key,
            data=data,
            mime=mime,
            metadata=metadata,
        )

        return ObjectRef(
            content_hash=content_hash,
            bucket=bucket,
            key=key,
            size=len(data),
            stored_at=datetime.now(),
        )

    def retrieve(self, ref: ObjectRef) -> bytes:
        """Retrieve data by ObjectRef.

        Args:
            ref: Object reference

        Returns:
            Binary data

        Raises:
            FileNotFoundError: If object doesn't exist

        """
        return self.retrieve_by_key(ref.bucket, ref.key)

    def retrieve_by_hash(self, bucket: str, content_hash: str) -> bytes:
        """Retrieve data by content hash.

        Args:
            bucket: Bucket name
            content_hash: SHA-256 content hash

        Returns:
            Binary data

        Raises:
            FileNotFoundError: If object doesn't exist

        """
        key = hash_to_key(content_hash, self._config.path_prefix_length)
        return self.retrieve_by_key(bucket, key)

    def retrieve_by_key(self, bucket: str, key: str) -> bytes:
        """Retrieve data by bucket and key.

        Args:
            bucket: Bucket name
            key: Object key

        Returns:
            Binary data

        Raises:
            FileNotFoundError: If object doesn't exist

        """
        try:
            response = self._client.get_object(bucket, key)
            try:
                return response.read()
            finally:
                response.close()
                response.release_conn()
        except S3Error as e:
            if e.code == "NoSuchKey":
                msg = f"Object not found: {bucket}/{key}"
                raise FileNotFoundError(msg) from e
            raise

    def exists(self, bucket: str, content_hash: str) -> bool:
        """Check if object exists by content hash.

        Args:
            bucket: Bucket name
            content_hash: SHA-256 content hash

        Returns:
            True if object exists

        """
        key = hash_to_key(content_hash, self._config.path_prefix_length)
        return self._object_exists(bucket, key)

    def exists_ref(self, ref: ObjectRef) -> bool:
        """Check if object exists by reference.

        Args:
            ref: Object reference

        Returns:
            True if object exists

        """
        return self._object_exists(ref.bucket, ref.key)

    def delete(self, ref: ObjectRef) -> bool:
        """Delete object by reference.

        Args:
            ref: Object reference

        Returns:
            True if deleted, False if not found

        """
        try:
            self._client.remove_object(ref.bucket, ref.key)
            logger.debug("Deleted object: %s", ref.full_path)
            return True
        except S3Error as e:
            if e.code == "NoSuchKey":
                return False
            raise

    def get_object_info(self, ref: ObjectRef) -> dict[str, Any] | None:
        """Get object metadata.

        Args:
            ref: Object reference

        Returns:
            Object info dict or None if not found

        """
        try:
            stat = self._client.stat_object(ref.bucket, ref.key)
            return {
                "size": stat.size,
                "last_modified": stat.last_modified,
                "etag": stat.etag,
                "content_type": stat.content_type,
                "metadata": stat.metadata,
            }
        except S3Error as e:
            if e.code == "NoSuchKey":
                return None
            raise

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """List objects in bucket.

        Args:
            bucket: Bucket name
            prefix: Key prefix filter
            limit: Maximum objects to return

        Returns:
            List of object info dicts

        """
        objects = []
        for obj in self._client.list_objects(bucket, prefix=prefix, recursive=True):
            if len(objects) >= limit:
                break
            objects.append(
                {
                    "key": obj.object_name,
                    "size": obj.size,
                    "last_modified": obj.last_modified,
                    "etag": obj.etag,
                },
            )
        return objects

    def _object_exists(self, bucket: str, key: str) -> bool:
        """Check if object exists by key."""
        try:
            self._client.stat_object(bucket, key)
            return True
        except S3Error as e:
            if e.code == "NoSuchKey":
                return False
            raise

    def _put_object_with_retry(
        self,
        bucket: str,
        key: str,
        data: bytes,
        mime: str,
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Put object with retry logic."""
        last_error = None

        for attempt in range(self._config.max_retries):
            try:
                self._client.put_object(
                    bucket_name=bucket,
                    object_name=key,
                    data=io.BytesIO(data),
                    length=len(data),
                    content_type=mime,
                    metadata=metadata,
                )
                return
            except S3Error as e:
                last_error = e
                logger.warning(
                    "Put object failed (attempt %d/%d): %s/%s - %s",
                    attempt + 1,
                    self._config.max_retries,
                    bucket,
                    key,
                    e,
                )
                if attempt < self._config.max_retries - 1:
                    time.sleep(self._config.retry_delay_sec * (attempt + 1))

        msg = f"Failed to store object after {self._config.max_retries} attempts"
        raise RuntimeError(
            msg,
        ) from last_error

    @property
    def buckets(self) -> dict[str, str]:
        """Get configured bucket names."""
        return {
            "artifacts": self._config.bucket_artifacts,
            "inputs": self._config.bucket_inputs,
            "aggregates": self._config.bucket_aggregates,
        }
