"""Use explicit process routing with importable callables and small payloads.

Run: python -m examples.process_hints
"""
from intelligent_futures import IntelligentExecutor, TaskHints


def count_primes(limit: int) -> int:
    count = 0
    for value in range(2, limit):
        if all(value % divisor for divisor in range(2, int(value ** .5) + 1)):
            count += 1
    return count


def main() -> None:
    with IntelligentExecutor(max_workers=2, process_workers=2,
                             max_pending=4, policy="hints") as executor:
        # process_safe=True is the caller's assertion: picklable inputs/results,
        # importable callable, and no reliance on shared thread-local state.
        jobs = [executor.submit_task(
            count_primes, limit,
            hints=TaskHints(kind="cpu", key="count-primes", process_safe=True))
            for limit in (10_000, 12_000, 14_000, 16_000)]
        print([job.result() for job in jobs])
    print("Process startup and serialization can outweigh gains for small tasks.")


if __name__ == "__main__":
    main()
