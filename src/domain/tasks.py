from dataclasses import dataclass


@dataclass(frozen=True)
class Task:
    """A unit of work: a single file to be processed by one processor pipeline."""

    task_id: str
    directory_key: str
    path: str
    created_at_unix: float
    fingerprint: str  # Used for dedup (e.g., path+mtime+size hash)


# Backwards compatibility alias (deprecated)
Job = Task
