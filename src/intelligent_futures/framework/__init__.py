"""Dependency-free allowlisted JSON tasks on a bounded, thread-only executor.

Registered callables are trusted application code, not sandboxed code. This layer
never imports callables by name or evaluates annotation strings. Completed task
records remain available until explicitly forgotten.
"""
from .tasks import TaskDefinition, TaskRegistry, TaskService, TaskServiceError

__all__ = ["TaskDefinition", "TaskRegistry", "TaskService", "TaskServiceError"]
