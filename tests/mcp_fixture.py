"""Deterministic allowlisted server used by the real stdio client tests."""
import json
from pathlib import Path
import sys
import time

from intelligent_futures.framework import TaskRegistry, TaskService
from intelligent_futures.integrations.mcp import run_stdio


def add(left: int, right: int = 1) -> int:
    return left + right


def hold(seconds: float = 0.6) -> str:
    if len(sys.argv) > 2:
        # The test process controls release; seconds are not a timing assertion.
        release = Path(sys.argv[2])
        deadline = time.monotonic() + 10
        while not release.exists():
            if time.monotonic() >= deadline:
                raise TimeoutError("Test did not release the task")
            time.sleep(0.005)
    else:
        time.sleep(seconds)
    return "finished"


def fail() -> None:
    raise RuntimeError("private-exception-value-must-not-escape")


def nonjson():
    return object()


def nonfinite():
    return float("nan")


def surrogate():
    return "\ud800"


def surrogate_key():
    return {"\ud800": "invalid Unicode key"}


def oversized():
    return "x" * 16384


def noisy() -> str:
    print("TASK OUTPUT MUST NOT CORRUPT THE MCP WIRE", flush=True)
    return "printed"


class ObservedService(TaskService):
    """Persist cleanup evidence after EOF, outside the client-facing API."""
    def close(self, *, wait=True, cancel_futures=True):
        super().close(wait=wait, cancel_futures=cancel_futures)
        if len(sys.argv) > 1 and sys.argv[1] != "-":
            Path(sys.argv[1]).write_text(json.dumps({
                "tasks": self.list_tasks(), "metrics": self.snapshot(),
                "wait": wait, "cancel_futures": cancel_futures,
            }), encoding="utf-8")
        if len(sys.argv) > 2:
            Path(sys.argv[2]).touch()


if __name__ == "__main__":
    registry = TaskRegistry()
    for function in (add, hold, fail, nonjson, nonfinite, surrogate, surrogate_key, oversized, noisy):
        registry.register(function.__name__, function, description=f"Test {function.__name__}.")
    service = ObservedService(registry, max_workers=1, max_pending=2,
                              max_tasks=8, max_input_bytes=1024, max_result_bytes=2048)
    run_stdio(service)
