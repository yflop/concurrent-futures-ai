"""One factory switch for new work; no replay, retries, or performance claims.

Run from an installed checkout:
    python -m examples.incremental_adoption --engine stdlib
    python -m examples.incremental_adoption --engine intelligent

Choose --engine stdlib before the next batch to roll back. Never use rollback
to retry an arbitrary failed task: it may already have produced side effects.
"""
from __future__ import annotations

import argparse
from concurrent.futures import Future, wait
from itertools import islice

from intelligent_futures.evaluation import EvaluationConfiguration
from intelligent_futures.evaluation_workloads import make_executor


CONFIGURATION = EvaluationConfiguration(backend="thread", workers=2)
MAX_PENDING = 4


def application_executor(engine):
    """Keep the executor choice in one place, before any new submissions."""
    return make_executor(engine, CONFIGURATION, max_pending=MAX_PENDING)


def square(value):
    return value * value


def run(engine):
    inputs = iter(range(12))
    results = []
    # Context exit is a waiting shutdown. Do not rely on interpreter teardown.
    with application_executor(engine) as executor:
        # Apply the SAME caller-side window to both executor families. The
        # stdlib factory does not add an admission bound to ThreadPoolExecutor.
        while batch := tuple(islice(inputs, MAX_PENDING)):
            futures = [executor.submit(square, value) for value in batch]
            assert all(isinstance(future, Future) for future in futures)
            wait(futures)
            results.extend(future.result() for future in futures)
        # map is also compatible; this finite input is at most our window.
        # Larger streams need an explicit bound: host-version map semantics vary.
        mapped = list(executor.map(square, (12, 13, 14, 15)))
    assert results + mapped == [value * value for value in range(16)]
    print(f"{engine}: {results + mapped}")
    # Never wait for another same-executor Future in a completion callback.
    # Normal IntelligentExecutor callbacks share one serial completion pump.


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("stdlib", "intelligent"), default="stdlib")
    args = parser.parse_args()
    run(args.engine)


if __name__ == "__main__":
    main()
