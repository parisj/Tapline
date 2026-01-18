"""Unit tests for hash chain tracking and verification."""


from src.audit.chain import (
    HashChainTracker,
    HashChainVerifier,
    NullSignatureProvider,
    verify_event_chain,
)
from src.domain.events import EventEnvelope, EventType


class TestHashChainTracker:
    def test_initial_state_empty(self) -> None:
        tracker = HashChainTracker()
        assert tracker.get_prev_hash("partition-1") is None
        assert tracker.get_state("partition-1") is None

    def test_update_creates_state(self) -> None:
        tracker = HashChainTracker()
        tracker.update("partition-1", "hash-abc")

        state = tracker.get_state("partition-1")
        assert state is not None
        assert state.partition_id == "partition-1"
        assert state.last_hash == "hash-abc"
        assert state.event_count == 1

    def test_update_increments_count(self) -> None:
        tracker = HashChainTracker()
        tracker.update("partition-1", "hash-1")
        tracker.update("partition-1", "hash-2")
        tracker.update("partition-1", "hash-3")

        state = tracker.get_state("partition-1")
        assert state.event_count == 3
        assert state.last_hash == "hash-3"

    def test_get_prev_hash_returns_last(self) -> None:
        tracker = HashChainTracker()
        tracker.update("partition-1", "hash-1")
        tracker.update("partition-1", "hash-2")

        assert tracker.get_prev_hash("partition-1") == "hash-2"

    def test_multiple_partitions(self) -> None:
        tracker = HashChainTracker()
        tracker.update("partition-1", "hash-a")
        tracker.update("partition-2", "hash-b")

        assert tracker.get_prev_hash("partition-1") == "hash-a"
        assert tracker.get_prev_hash("partition-2") == "hash-b"

    def test_get_all_partitions(self) -> None:
        tracker = HashChainTracker()
        tracker.update("partition-1", "hash-a")
        tracker.update("partition-2", "hash-b")
        tracker.update("partition-3", "hash-c")

        partitions = tracker.get_all_partitions()
        assert set(partitions) == {"partition-1", "partition-2", "partition-3"}

    def test_reset_single_partition(self) -> None:
        tracker = HashChainTracker()
        tracker.update("partition-1", "hash-a")
        tracker.update("partition-2", "hash-b")

        tracker.reset("partition-1")

        assert tracker.get_state("partition-1") is None
        assert tracker.get_state("partition-2") is not None

    def test_reset_all_partitions(self) -> None:
        tracker = HashChainTracker()
        tracker.update("partition-1", "hash-a")
        tracker.update("partition-2", "hash-b")

        tracker.reset()

        assert tracker.get_all_partitions() == []


class TestNullSignatureProvider:
    def test_sign_returns_empty(self) -> None:
        provider = NullSignatureProvider()
        sig = provider.sign(b"test data")
        assert sig == ""

    def test_verify_always_true(self) -> None:
        provider = NullSignatureProvider()
        assert provider.verify(b"data", "") is True
        assert provider.verify(b"data", "any-signature") is True

    def test_key_id(self) -> None:
        provider = NullSignatureProvider()
        assert provider.key_id == "null"


class TestHashChainVerifier:
    def test_verify_empty_chain(self) -> None:
        verifier = HashChainVerifier()
        result = verifier.verify_chain([])
        assert result.valid is True
        assert result.event_count == 0
        assert len(result.errors) == 0

    def test_verify_single_event(self) -> None:
        verifier = HashChainVerifier()
        event = EventEnvelope.create(
            event_type=EventType.JOB_CREATED,
            source_id="source-1",
            payload={"job_id": "job-123"},
        )

        result = verifier.verify_chain([event])
        assert result.valid is True
        assert result.event_count == 1

    def test_verify_valid_chain(self) -> None:
        verifier = HashChainVerifier()

        event1 = EventEnvelope.create(
            event_type=EventType.JOB_CREATED,
            source_id="source-1",
            payload={"job_id": "job-123"},
            prev_hash=None,
        )

        event2 = EventEnvelope.create(
            event_type=EventType.JOB_STARTED,
            source_id="source-1",
            payload={"job_id": "job-123", "algo": "test"},
            prev_hash=event1.content_hash,
        )

        event3 = EventEnvelope.create(
            event_type=EventType.JOB_COMPLETED,
            source_id="source-1",
            payload={"job_id": "job-123", "duration": 100},
            prev_hash=event2.content_hash,
        )

        result = verifier.verify_chain([event1, event2, event3])
        assert result.valid is True
        assert result.event_count == 3
        assert len(result.errors) == 0

    def test_verify_broken_chain(self) -> None:
        verifier = HashChainVerifier()

        event1 = EventEnvelope.create(
            event_type=EventType.JOB_CREATED,
            source_id="source-1",
            payload={"job_id": "job-123"},
        )

        # Create event2 with wrong prev_hash
        event2 = EventEnvelope.create(
            event_type=EventType.JOB_STARTED,
            source_id="source-1",
            payload={"job_id": "job-123", "algo": "test"},
            prev_hash="wrong-hash",
        )

        result = verifier.verify_chain([event1, event2])
        assert result.valid is False
        assert len(result.errors) == 1
        assert "prev_hash mismatch" in result.errors[0]

    def test_verify_single_valid(self) -> None:
        verifier = HashChainVerifier()
        event = EventEnvelope.create(
            event_type=EventType.JOB_CREATED,
            source_id="source-1",
            payload={"job_id": "job-123"},
        )

        assert verifier.verify_single(event) is True

    def test_verify_single_with_expected_prev(self) -> None:
        verifier = HashChainVerifier()

        event1 = EventEnvelope.create(
            event_type=EventType.JOB_CREATED,
            source_id="source-1",
            payload={"job_id": "job-123"},
        )

        event2 = EventEnvelope.create(
            event_type=EventType.JOB_STARTED,
            source_id="source-1",
            payload={"job_id": "job-123"},
            prev_hash=event1.content_hash,
        )

        assert verifier.verify_single(event2, expected_prev_hash=event1.content_hash) is True
        assert verifier.verify_single(event2, expected_prev_hash="wrong") is False


class TestVerifyEventChain:
    def test_convenience_function(self) -> None:
        events = [
            EventEnvelope.create(
                event_type=EventType.JOB_CREATED,
                source_id="source-1",
                payload={"job_id": "job-123"},
            ),
        ]

        result = verify_event_chain(events)
        assert result.valid is True
        assert bool(result) is True

    def test_result_bool_false_on_invalid(self) -> None:
        event1 = EventEnvelope.create(
            event_type=EventType.JOB_CREATED,
            source_id="source-1",
            payload={"job_id": "job-123"},
        )

        event2 = EventEnvelope.create(
            event_type=EventType.JOB_STARTED,
            source_id="source-1",
            payload={"job_id": "job-123"},
            prev_hash="invalid",
        )

        result = verify_event_chain([event1, event2])
        assert result.valid is False
        assert bool(result) is False
