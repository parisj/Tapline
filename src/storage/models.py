"""Storage models for MinIO artifact references."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ObjectRef:
    """Reference to an object stored in MinIO.

    Content-addressed storage means the key is derived from
    the content hash, enabling deduplication and verification.

    Path format: bucket/ab/cd/{full_hash}
    where ab/cd are first 4 chars of hash for directory sharding.
    """

    content_hash: str
    bucket: str
    key: str
    size: int
    stored_at: datetime

    @classmethod
    def from_hash(
        cls,
        content_hash: str,
        bucket: str,
        size: int,
        stored_at: datetime | None = None,
        prefix_length: int = 4,
    ) -> ObjectRef:
        """Create ObjectRef from content hash.

        Args:
            content_hash: SHA-256 hash of content
            bucket: Target bucket name
            size: Size in bytes
            stored_at: Storage timestamp (defaults to now)
            prefix_length: Number of chars for directory sharding

        Returns:
            ObjectRef with computed key path
        """
        key = hash_to_key(content_hash, prefix_length)
        return cls(
            content_hash=content_hash,
            bucket=bucket,
            key=key,
            size=size,
            stored_at=stored_at or datetime.now(),
        )

    @property
    def full_path(self) -> str:
        """Full S3-style path: bucket/key."""
        return f"{self.bucket}/{self.key}"

    def to_dict(self) -> dict:
        """Serialize to dictionary."""
        return {
            "content_hash": self.content_hash,
            "bucket": self.bucket,
            "key": self.key,
            "size": self.size,
            "stored_at": self.stored_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> ObjectRef:
        """Deserialize from dictionary."""
        stored_at = data["stored_at"]
        if isinstance(stored_at, str):
            stored_at = datetime.fromisoformat(stored_at)
        return cls(
            content_hash=data["content_hash"],
            bucket=data["bucket"],
            key=data["key"],
            size=data["size"],
            stored_at=stored_at,
        )


def hash_to_key(content_hash: str, prefix_length: int = 4) -> str:
    """Convert content hash to storage key with directory sharding.

    Example:
        hash_to_key("abcdef123456...") -> "ab/cd/abcdef123456..."

    Args:
        content_hash: Full content hash
        prefix_length: Number of chars for directory prefix (must be even)

    Returns:
        Key path with directory sharding
    """
    if prefix_length < 2 or prefix_length > len(content_hash):
        prefix_length = 4

    # Ensure even prefix for balanced sharding
    prefix_length = prefix_length - (prefix_length % 2)

    parts = []
    for i in range(0, prefix_length, 2):
        parts.append(content_hash[i : i + 2])
    parts.append(content_hash)

    return "/".join(parts)


def key_to_hash(key: str) -> str:
    """Extract content hash from storage key.

    Example:
        key_to_hash("ab/cd/abcdef123456...") -> "abcdef123456..."

    Args:
        key: Storage key with directory prefix

    Returns:
        Original content hash
    """
    parts = key.split("/")
    return parts[-1] if parts else key
