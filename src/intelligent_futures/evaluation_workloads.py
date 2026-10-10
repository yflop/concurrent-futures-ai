"""Explicit fixture adapters for evidence-gated evaluation.

Nothing wraps or intercepts ordinary application submissions. Callers supply a
fresh context-managed fixture, explicitly attest repeatability and declare independence,
and separately attest process safety before the adapter can execute anything.
"""
from __future__ import annotations

from collections import deque
from concurrent.futures import CancelledError, ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from itertools import islice
import multiprocessing
import time
from typing import Callable, ContextManager, Iterable

from .evaluation import EvaluationConfiguration, RepeatableWorkload
from .executor import IntelligentExecutor
from .policies import TaskHints
from .prediction import ExecutionObjective
from .tuning import Measurement


@dataclass(frozen=True)
class EvaluationFixture:
    """One bounded collection of single-argument calls and an ordered validator.

    ``validate`` receives a tuple in input order and must return an actual bool.
    Inputs and results stay local to this measurement and are never report data.
    Constructing a fixture does not consume its iterable or call its validator.
    """

    inputs: Iterable
    validate: Callable[[tuple], bool]

    def __post_init__(self):
        if not callable(self.validate):
            raise TypeError("validate must be callable")


def _positive_integer(value, name):
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _validate_configuration(engine, configuration):
    if engine not in ("stdlib", "intelligent"):
        raise ValueError("engine must be stdlib or intelligent")
    if type(configuration) is not EvaluationConfiguration:
        raise TypeError("configuration must be EvaluationConfiguration")
    configuration.__post_init__()
    if engine == "stdlib" and (configuration.backend == "adaptive" or configuration.predictive):
        raise ValueError("stdlib requires a fixed, non-predictive configuration")


def make_executor(engine, configuration, *, max_pending, allow_process=False):
    """Construct one explicitly selected executor, renewing process consent.

    ``workers`` is the active slot budget. Adaptive configurations split that
    budget between thread and process workers. A fixed process configuration has
    an unused, lazy one-slot thread pool because IntelligentExecutor requires it;
    its fixed process policy never submits work to that pool. Both process
    engines use spawn. The caller owns shutdown and any batching/window policy.

    Merely loading an evaluation recommendation does not authorize processes.
    Both stdlib and Intelligent process pools require ``allow_process=True``.
    """
    _validate_configuration(engine, configuration)
    _positive_integer(max_pending, "max_pending")
    if type(allow_process) is not bool:
        raise ValueError("allow_process must be bool")
    if configuration.uses_process and not allow_process:
        raise ValueError("process execution requires allow_process=True")

    if engine == "stdlib":
        if configuration.backend == "thread":
            return ThreadPoolExecutor(max_workers=configuration.workers)
        return ProcessPoolExecutor(max_workers=configuration.workers,
                                   mp_context=multiprocessing.get_context("spawn"))

    if configuration.backend == "process":
        threads, processes = 1, configuration.workers
    elif configuration.backend == "adaptive":
        processes = configuration.process_workers
        threads = configuration.workers - processes
    else:
        threads, processes = configuration.workers, 0
    return IntelligentExecutor(
        max_workers=threads, process_workers=processes, max_pending=max_pending,
        policy="threads" if configuration.backend == "thread" else configuration.backend,
        predictive=configuration.predictive,
        profile_sample_interval=configuration.profile_sample_interval,
        objective=(ExecutionObjective(mode="throughput", max_concurrency=configuration.workers)
                   if configuration.predictive else None),
        mp_context=multiprocessing.get_context("spawn") if processes else None,
    )


def _tuple_batch(fn, inputs):
    """The identical, top-level, spawn-picklable kernel used by both engines."""
    return tuple(fn(value) for value in inputs)


def _collect_inputs(inputs, max_tasks):
    # One overflow probe is necessary to distinguish an exact-bound fixture
    # from an oversized or infinite iterator. No task is submitted before this.
    collected = tuple(islice(inputs, max_tasks + 1))
    if not collected:
        raise ValueError("fixture must contain at least one task")
    if len(collected) > max_tasks:
        raise ValueError("fixture exceeds max_tasks")
    return collected


def _execute_batches(executor, engine, fn, inputs, configuration, max_pending, process_safe):
    """Fixed batches, a bounded ordered window, and no replay on any failure."""
    pending = deque()
    results = []
    next_input = 0
    hints = TaskHints(key="evaluation-fixture", process_safe=process_safe)
    try:
        while next_input < len(inputs) or pending:
            while next_input < len(inputs) and len(pending) < max_pending:
                batch = inputs[next_input:next_input + configuration.batch_size]
                if engine == "intelligent":
                    future = executor.submit_task(_tuple_batch, fn, batch, hints=hints)
                else:
                    future = executor.submit(_tuple_batch, fn, batch)
                pending.append(future)
                next_input += len(batch)
            results.extend(pending.popleft().result())
        return tuple(results)
    finally:
        # Already-running work is drained by pool shutdown, never killed or
        # resubmitted. Cancel only outstanding work that has not started yet.
        for future in pending:
            future.cancel()


def callable_workload(
    fn, fixture_factory: Callable[[int], ContextManager[EvaluationFixture]], *,
    name: str, version: str, repeatable=False, independent=False,
    process_safe=False, max_tasks=10_000, max_pending=8,
) -> RepeatableWorkload:
    """Adapt an explicitly safe single-input callable into fresh measurements.

    Construction performs no fixture work. ``repeatable`` must be True before
    *any* measurement work. ``independent=False`` permits exploratory timing,
    but the evaluator must not recommend adoption from dependent observations.
    ``process_safe``
    additionally attests that the callable, inputs, and results are picklable
    and semantically safe under spawn; it does not replace the evaluation
    plan's separate ``allow_process`` consent. Use a guarded Python entry point
    (``if __name__ == '__main__':``) when starting process evaluations.

    For each measure call, the factory receives that measurement's seed and
    yields a fresh fixture. Collection consumes at most ``max_tasks + 1`` inputs
    (one overflow probe), before creating a pool. Empty/oversized fixtures fail
    closed. At most ``max_pending`` batches are submitted but not yet retrieved;
    input/result retention is bounded by the accepted fixture's finite size.

    ``metric_scope='pool_lifecycle'`` means pool construction, one execution of
    the fixture, and waiting shutdown. Fixture setup, input collection,
    validation, and fixture cleanup are outside that timer. Both engines use
    the same tuple kernel, batching, ordered window, and no warmup/replay.
    ``warm_seconds`` aliases the cold measurement solely for Measurement
    compatibility; this workload supports only the cold evaluation objective.
    CPU, process-tree RSS, and batch p95 are deliberately unavailable. Use a
    caller-measured RepeatableWorkload for genuinely measured resource metrics.

    Ordinary failures produce a sanitized incorrect Measurement; no exception
    text, input, validator, or result is stored there. Cancellation and process
    interrupts propagate after normal pool/fixture cleanup. A bounded fixture
    bounds count, not task duration: running user calls cannot be forcibly stopped.
    """
    if not callable(fn) or not callable(fixture_factory):
        raise TypeError("fn and fixture_factory must be callable")
    for label, value in (("repeatable", repeatable), ("independent", independent),
                         ("process_safe", process_safe)):
        if type(value) is not bool:
            raise ValueError(f"{label} must be bool")
    _positive_integer(max_tasks, "max_tasks")
    _positive_integer(max_pending, "max_pending")

    def measure(engine, configuration, seed):
        # These consent/shape checks intentionally precede fixture construction,
        # iteration, and the ordinary-error sanitization boundary.
        if not repeatable:
            raise ValueError("fixture execution requires repeatable=True")
        _validate_configuration(engine, configuration)
        if configuration.uses_process and not process_safe:
            raise ValueError("process fixture execution requires process_safe=True")

        elapsed = 1e-12
        outcome = None
        interruption = None
        try:
            with fixture_factory(seed) as fixture:
                try:
                    if type(fixture) is not EvaluationFixture:
                        raise TypeError("fixture factory must yield EvaluationFixture")
                    inputs = _collect_inputs(fixture.inputs, max_tasks)
                    started = time.perf_counter()
                    try:
                        with make_executor(engine, configuration, max_pending=max_pending,
                                           allow_process=process_safe) as executor:
                            results = _execute_batches(executor, engine, fn, inputs,
                                                       configuration, max_pending, process_safe)
                    finally:
                        elapsed = max(1e-12, time.perf_counter() - started)
                    correct = fixture.validate(results)
                    if type(correct) is not bool:
                        raise TypeError("fixture validate must return bool")
                    outcome = correct
                except BaseException as error:
                    if isinstance(error, CancelledError) or not isinstance(error, Exception):
                        interruption = error
                    raise
        except BaseException as error:
            # A fixture's cleanup must not suppress an interrupt or replace it
            # with an ordinary exception that would become a ranking record.
            if interruption is not None:
                raise interruption
            if isinstance(error, CancelledError) or not isinstance(error, Exception):
                raise
            # Exception class/text and raw workload values are deliberately not
            # retained. BaseException interrupts continue out of the adapter.
            return Measurement(elapsed, elapsed, False, error_count=1)
        if interruption is not None:
            raise interruption
        # A context manager that suppresses an execution error must not turn an
        # incomplete run into a successful observation.
        if outcome is None:
            return Measurement(elapsed, elapsed, False, error_count=1)
        return Measurement(elapsed, elapsed, outcome)

    # Primitive declarations let the evaluator verify the plan's common window
    # and process-safety gate before opening any baseline or candidate fixture.
    measure._evaluation_max_pending = max_pending
    measure._evaluation_process_safe = process_safe
    return RepeatableWorkload(name=name, version=version, measure=measure,
                              repeatable=repeatable, independent=independent,
                              metric_scope="pool_lifecycle")
