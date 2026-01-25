# Backwards compatibility - import from new location
# This file is deprecated, use src/domain/tasks.py instead
from src.domain.tasks import Job, Task

__all__ = ["Job", "Task"]
