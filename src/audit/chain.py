"""Hash chain implementation for GxP-compliant audit trails.

This module provides:
- Per-partition hash chain tracking for event ordering
- Chain verification for tamper detection
- Signature provider interface for future PKI integration

The hash chain creates a cryptographic link between events:
    event_n.prev_hash = event_{n-1}.content_hash

This allows verification that:
1. No events have been removed from the chain
2. No events have been modified after creation
3. Event ordering is preserved
"""

from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from src.domain.events import compute_content_hash
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from src.domain.events import EventEnvelope

logger = get_logger(__name__)


class SignatureProvider(Protocol):
    """Protocol for PKI signature providers.

    Implementations can use various signing mechanisms:
    - Local private key (for development)
    - HSM-backed keys (for production)
    - Cloud KMS (AWS KMS, Azure Key Vault, etc.)

    This is an interface-only definition for future implementation.
    """

    def sign(self, data: bytes) -> str:
        """Sign data and return base64-encoded signature."""
        ...

    def verify(self, data: bytes, signature: str) -> bool:
        """Verify signature against data."""
        ...

    @property
    def key_id(self) -> str:
        """Return identifier for the signing key."""
        ...


class NullSignatureProvider:
    """No-op signature provider for development/testing."""

    def sign(self, _data: bytes) -> str:
        return ""

    def verify(self, _data: bytes, _signature: str) -> bool:
        return True

    @property
    def key_id(self) -> str:
        return "null"


@dataclass
class ChainState:
    """State of a hash chain for a single partition."""

    partition_id: str
    last_hash: str | None
    event_count: int


class HashChainTracker:
    """Thread-safe tracker for per-partition hash chains.

    Maintains the last hash for each partition to enable proper
    chain linking when creating new events.

    Usage:
        tracker = HashChainTracker()

        # Get prev_hash for new event
        prev = tracker.get_prev_hash("partition-1")

        # After event is created and persisted
        tracker.update("partition-1", event.content_hash)
    """

    def __init__(self) -> None:
        self._chains: dict[str, ChainState] = {}
        self._lock = threading.Lock()

    def get_prev_hash(self, partition_id: str) -> str | None:
        """Get the last hash for a partition (for linking new events).

        Args:
            partition_id: Partition identifier (typically source_id)

        Returns:
            Last content_hash in the partition, or None for genesis event

        """
        with self._lock:
            state = self._chains.get(partition_id)
            return state.last_hash if state else None

    def update(self, partition_id: str, content_hash: str) -> None:
        """Update chain state after event is persisted.

        Args:
            partition_id: Partition identifier
            content_hash: Hash of the newly persisted event

        """
        with self._lock:
            state = self._chains.get(partition_id)
            if state:
                state = ChainState(
                    partition_id=partition_id,
                    last_hash=content_hash,
                    event_count=state.event_count + 1,
                )
            else:
                state = ChainState(
                    partition_id=partition_id,
                    last_hash=content_hash,
                    event_count=1,
                )
            self._chains[partition_id] = state

    def get_state(self, partition_id: str) -> ChainState | None:
        """Get current chain state for a partition."""
        with self._lock:
            return self._chains.get(partition_id)

    def get_all_partitions(self) -> list[str]:
        """Get list of all tracked partition IDs."""
        with self._lock:
            return list(self._chains.keys())

    def reset(self, partition_id: str | None = None) -> None:
        """Reset chain state for a partition or all partitions.

        Args:
            partition_id: Specific partition to reset, or None for all

        """
        with self._lock:
            if partition_id is None:
                self._chains.clear()
            elif partition_id in self._chains:
                del self._chains[partition_id]


class HashChainVerifier:
    """Verifier for hash chain integrity.

    Checks that a sequence of events forms a valid chain:
    1. Each event's content_hash matches its payload
    2. Each event's prev_hash matches the previous event's content_hash
    3. No gaps in the chain
    """

    def __init__(self, signature_provider: SignatureProvider | None = None) -> None:
        self._signature_provider = signature_provider or NullSignatureProvider()

    def verify_chain(self, events: list[EventEnvelope]) -> VerificationResult:
        """Verify a sequence of events forms a valid chain.

        Args:
            events: Ordered list of events to verify

        Returns:
            VerificationResult with status and any errors

        """
        if not events:
            return VerificationResult(valid=True, errors=[], event_count=0)

        errors: list[str] = []

        for i, event in enumerate(events):
            # Verify content hash
            expected_hash = compute_content_hash(event.payload)
            if event.content_hash != expected_hash:
                errors.append(
                    f"Event {i} ({event.event_id}): content_hash mismatch. "
                    f"Expected {expected_hash}, got {event.content_hash}",
                )

            # Verify chain linkage
            if i == 0:
                # Genesis event should have no prev_hash
                if event.prev_hash is not None:
                    # Allow non-None for mid-chain verification
                    logger.debug(
                        "First event in verification set has prev_hash=%s",
                        event.prev_hash,
                    )
            else:
                expected_prev = events[i - 1].content_hash
                if event.prev_hash != expected_prev:
                    errors.append(
                        f"Event {i} ({event.event_id}): prev_hash mismatch. "
                        f"Expected {expected_prev}, got {event.prev_hash}",
                    )

            # Verify signature if present
            if event.signature:
                data = (event.content_hash + (event.prev_hash or "")).encode()
                if not self._signature_provider.verify(data, event.signature):
                    errors.append(
                        f"Event {i} ({event.event_id}): invalid signature",
                    )

        return VerificationResult(
            valid=len(errors) == 0,
            errors=errors,
            event_count=len(events),
        )

    def verify_single(
        self,
        event: EventEnvelope,
        expected_prev_hash: str | None = None,
    ) -> bool:
        """Verify a single event's integrity.

        Args:
            event: Event to verify
            expected_prev_hash: Expected prev_hash (for chain verification)

        Returns:
            True if event is valid

        """
        # Verify content hash
        expected_content = compute_content_hash(event.payload)
        if event.content_hash != expected_content:
            logger.warning(
                "Event %s: content_hash mismatch",
                event.event_id,
            )
            return False

        # Verify chain linkage if expected_prev_hash provided
        if expected_prev_hash is not None and event.prev_hash != expected_prev_hash:
            logger.warning(
                "Event %s: prev_hash mismatch. Expected %s, got %s",
                event.event_id,
                expected_prev_hash,
                event.prev_hash,
            )
            return False

        return True


@dataclass
class VerificationResult:
    """Result of chain verification."""

    valid: bool
    errors: list[str]
    event_count: int

    def __bool__(self) -> bool:
        return self.valid


def verify_event_chain(events: list[EventEnvelope]) -> VerificationResult:
    """Convenience function to verify an event chain.

    Args:
        events: Ordered list of events to verify

    Returns:
        VerificationResult with status and any errors

    """
    verifier = HashChainVerifier()
    return verifier.verify_chain(events)


def compute_event_hash(payload: dict, prev_hash: str | None) -> str:
    """Compute hash for an event including chain linkage.

    This is the hash that would be used for signatures.

    Args:
        payload: Event payload
        prev_hash: Previous event's content hash

    Returns:
        Combined hash of payload and chain

    """
    content_hash = compute_content_hash(payload)
    chain_data = content_hash + (prev_hash or "")
    return hashlib.sha256(chain_data.encode()).hexdigest()
