from dataclasses import dataclass

@dataclass(frozen=True)
class Job:
    """A unit of work: a single image to be processed by one algorithm pipeline."""
    job_id: str
    directory_key: str
    path: str
    created_at_unix: float
    fingerprint: str  # Used for dedup (e.g., path+mtime+size hash)
