"""Cancel queued work and cooperatively stop running thread work.

Run: python -m examples.cancellation
"""
import threading
from intelligent_futures import IntelligentExecutor


def cooperative_job(started: threading.Event, stop: threading.Event) -> str:
    started.set()
    # Real work should periodically check an application-level cancellation flag.
    # The timeout is a safeguard for the example, not a forced task deadline.
    if not stop.wait(timeout=5):
        raise TimeoutError("The example's stop signal was never delivered")
    return "Stopped cooperatively"


def main() -> None:
    started, stop = threading.Event(), threading.Event()
    executor = IntelligentExecutor(max_workers=1, max_pending=3, policy="threads")
    try:
        running = executor.submit(cooperative_job, started, stop)
        if not started.wait(timeout=5):
            raise TimeoutError("The worker did not start")
        queued = executor.submit(lambda: "This queued callable should not run")
        print(f"Cancel queued work: {queued.cancel()}")
        print(f"Cancel running work: {running.cancel()}")
        stop.set()
        print(running.result(timeout=5))
    finally:
        stop.set()
        executor.shutdown(wait=True, cancel_futures=True)
    print(executor.snapshot()["counts"])
    print("Future.cancel() cannot interrupt a running callable.")


if __name__ == "__main__":
    main()
