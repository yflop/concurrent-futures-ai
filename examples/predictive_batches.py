"""Opt-in profiles and actual bounded chunk execution, without duplicate work."""
from intelligent_futures import (
    ExecutionObjective, IntelligentExecutor, TaskHints, WorkloadProfile,
)


def square_batch(items):
    return [item * item for item in items]


def main():
    with IntelligentExecutor(max_workers=4, predictive=True,
            objective=ExecutionObjective(max_concurrency=4)) as executor:
        assert executor.submit_profiled(sum, [1, 2, 3],
            hints=TaskHints(key="sum-v1", kind="cpu"), features={"count": 3}).result() == 6
        chunks = list(executor.map_batches(square_batch, range(50),
            min_batch_size=2, max_batch_size=16, buffersize=2,
            hints=TaskHints(key="square-batch-v1", kind="cpu")))
        assert [value for chunk in chunks for value in chunk] == [n*n for n in range(50)]
    exported = [profile.to_json() for profile in executor.export_profiles()]
    restored = [WorkloadProfile.from_json(text) for text in exported]
    assert restored
    print("Items:", sum(map(len, chunks)), "Actual batch dispatches:", len(chunks))
    print("Bounded reusable profiles:", len(restored))
    print(executor.snapshot()["predictive"])


if __name__ == "__main__":
    main()
