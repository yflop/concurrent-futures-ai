"""Supply an auditable policy without changing the executor implementation.

Run: python -m examples.custom_policy
"""
import json
from intelligent_futures import IntelligentExecutor, TaskHints


class NamedQueuePolicy:
    """Send an explicitly opted-in CPU workload to processes; everything else to threads.

    choose executes under a scheduling lock: do not block, perform I/O, or
    recursively call this executor here. A policy must respect process consent.
    """
    def choose(self, hints, profiles, process_available):
        # `profiles` is an immutable key -> backend -> Profile mapping and can
        # support additional rules. This simple policy intentionally needs none.
        if (hints.key.startswith("batch-cpu/") and hints.kind == "cpu"
                and hints.process_safe and process_available):
            return "process"
        return "thread"


def sum_squares(limit: int) -> int:
    return sum(number * number for number in range(limit))


def main() -> None:
    executor = IntelligentExecutor(max_workers=2, process_workers=2,
                                   max_pending=8, policy=NamedQueuePolicy())
    with executor:
        heavy = executor.submit_task(
            sum_squares, 250_000,
            hints=TaskHints(kind="cpu", key="batch-cpu/sum-squares", process_safe=True))
        light = executor.submit(sum_squares, 10)
        print(f"Heavy result: {heavy.result()}; light result: {light.result()}")
    print(json.dumps(executor.snapshot(), indent=2))


if __name__ == "__main__":
    main()
