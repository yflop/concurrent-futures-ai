"""Local stdio MCP server: python examples/mcp_server.py (install .[mcp])."""
from intelligent_futures.framework import TaskRegistry, TaskService
from intelligent_futures.integrations.mcp import run_stdio


def add(a: int, b: int) -> int:
    """Add two integers without external side effects."""
    return a + b


if __name__ == "__main__":
    registry = TaskRegistry()
    registry.register("add", add, description="Add two integers")
    run_stdio(TaskService(registry, max_workers=2, max_pending=8, max_tasks=32))
