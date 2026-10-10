"""Dependency-free task IDs, JSON results, metrics, and explicit cleanup."""
import time

from intelligent_futures.framework import TaskRegistry, TaskService


def summarize(values: list[float]) -> dict:
    return {"count": len(values), "total": sum(values)}


def main():
    registry = TaskRegistry()
    registry.register("summarize", summarize, description="Summarize a JSON number list")
    with TaskService(registry, max_workers=1, max_pending=4, max_tasks=8) as service:
        task = service.submit("summarize", {"values": [1, 2.5, 3]})
        deadline = time.monotonic() + 5
        while task["status"] in {"queued", "running"}:
            if time.monotonic() >= deadline:
                raise TimeoutError("Example result did not become available")
            time.sleep(0.01)
            task = service.get(task["task_id"])
        assert task["status"] == "completed", task
        assert task["result"] == {"count": 3, "total": 6.5}
        print(task["result"])
        service.forget(task["task_id"])
    print(service.snapshot())


if __name__ == "__main__":
    main()
