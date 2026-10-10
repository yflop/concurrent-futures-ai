"""Deterministic workloads; all worker callables are safe to import with spawn."""
from __future__ import annotations

from dataclasses import dataclass
import random
import time


@dataclass(frozen=True)
class TaskSpec:
    index: int
    kind: str
    iterations: int
    delay: float


def cpu_work(iterations: int, seed: int) -> int:
    """Run a pure-Python integer loop, deliberately without a native extension.

    >>> cpu_work(0, 0)
    1
    >>> cpu_work(1, 0)
    1015568748
    """
    value = (seed + 1) & 0xFFFFFFFF
    for position in range(iterations):
        value = (value * 1664525 + 1013904223 + position) & 0xFFFFFFFF
    return value


def io_work(delay: float, seed: int) -> int:
    """Model waiting with sleep. This is not a network or filesystem benchmark."""
    time.sleep(delay)
    return seed


def execute_task(spec: TaskSpec) -> tuple[int, float]:
    """Return a deterministic result and worker-local wall-clock service time."""
    started = time.perf_counter()
    if spec.kind == "cpu":
        value = cpu_work(spec.iterations, spec.index)
    elif spec.kind == "io":
        value = io_work(spec.delay, spec.index)
    else:
        raise ValueError(f"Unknown task kind: {spec.kind!r}")
    return value, time.perf_counter() - started


def build_workload(name: str, count: int, iterations: int, delay: float,
                   seed: int) -> list[TaskSpec]:
    if name not in {"cpu", "io", "mixed"}:
        raise ValueError(f"Unknown workload: {name!r}")
    specs = [TaskSpec(index=index,
                      kind=("cpu" if index % 2 == 0 else "io")
                      if name == "mixed" else name,
                      iterations=iterations, delay=delay)
             for index in range(count)]
    # Mixed batches use a fixed shuffled order rather than an all-CPU prefix.
    if name == "mixed":
        random.Random(seed).shuffle(specs)
    return specs
