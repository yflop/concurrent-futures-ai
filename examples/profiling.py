"""Inspect learned completion latency without promising automatic classification.

Run: python -m examples.profiling
"""
import json
import time
from intelligent_futures import IntelligentExecutor, TaskHints


def checksum(seed: int) -> int:
    value = seed
    for _ in range(150_000):
        value = (value * 1664525 + 1013904223) & 0xFFFFFFFF
    return value


def fetch_simulated(identifier: int) -> str:
    time.sleep(.01)
    return f"record-{identifier}"


def main() -> None:
    executor = IntelligentExecutor(max_workers=2, process_workers=2,
                                   max_pending=8, policy="adaptive")
    with executor:
        for batch in range(3):
            futures = []
            for index in range(12):
                # Comparable payload sizes share a small, stable profile key.
                # Explicit process consent allows exploration, but does not force it.
                futures.append(executor.submit_task(
                    checksum, batch * 12 + index,
                    hints=TaskHints(key="checksum-150k", process_safe=True)))
                futures.append(executor.submit_task(
                    fetch_simulated, index,
                    hints=TaskHints(kind="io", key="simulated-fetch")))
            for future in futures:
                future.result()
    # Shutdown joins callbacks, so the final counters have settled.
    print(json.dumps(executor.snapshot(), indent=2))
    print("Profiles include pool queueing and execution, but exclude admission waiting.")
    print("Changing payload sizes, queue load, and process startup can change the chosen route.")


if __name__ == "__main__":
    main()
