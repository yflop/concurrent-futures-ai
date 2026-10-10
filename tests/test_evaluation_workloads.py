"""Small correctness checks for the explicit, bounded fixture adapter."""
from concurrent.futures import CancelledError
from contextlib import contextmanager
from dataclasses import asdict
import json
import threading
import unittest
from unittest.mock import patch

from intelligent_futures.evaluation import EvaluationConfiguration, EvaluationPlan, evaluate_workload
from intelligent_futures.evaluation_workloads import (
    EvaluationFixture, callable_workload, make_executor,
)


def square(value):
    return value * value


def fail_with_private_message(value):
    raise RuntimeError("private payload: " + str(value))


@contextmanager
def square_fixture(seed):
    values = tuple(range(seed, seed + 5))
    yield EvaluationFixture(values, lambda results: results == tuple(map(square, values)))


class _DeferredFuture:
    def __init__(self, owner, fn, args, kwargs):
        self.owner, self.fn, self.args, self.kwargs = owner, fn, args, kwargs
        self.done = False

    def result(self):
        self.done = True
        self.owner.pending -= 1
        return self.fn(*self.args, **self.kwargs)

    def cancel(self):
        if not self.done:
            self.done = True
            self.owner.pending -= 1
            self.owner.cancelled += 1


class _DeferredExecutor:
    def __init__(self, tick=None):
        self.pending = self.peak = self.cancelled = 0
        self.submissions, self.hints = [], []
        self.closed = False
        self.tick = tick or (lambda value: None)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True
        self.tick(7)

    def submit(self, fn, *args, **kwargs):
        self.submissions.append((fn, args, kwargs))
        self.pending += 1
        self.peak = max(self.peak, self.pending)
        self.tick(3)
        return _DeferredFuture(self, fn, args, kwargs)

    def submit_task(self, fn, *args, hints=None, **kwargs):
        self.hints.append(hints)
        return self.submit(fn, *args, **kwargs)


class EvaluationWorkloadTests(unittest.TestCase):
    def workload(self, fn=square, fixture_factory=square_fixture, **kwargs):
        options = dict(name="square-fixture", version="1", repeatable=True, independent=True)
        options.update(kwargs)
        return callable_workload(fn, fixture_factory, **options)

    def test_construction_and_repeatability_opt_out_do_no_work(self):
        events = []
        def factory(seed):
            events.append("fixture")
            raise AssertionError("must not construct a fixture")
        workload = self.workload(fixture_factory=factory, repeatable=False)
        self.assertEqual(events, [])
        with self.assertRaises(ValueError):
            workload.measure("stdlib", EvaluationConfiguration(), 0)
        self.assertEqual(events, [])
        self.assertFalse(workload.repeatable)

    def test_dependent_fixture_can_be_explored_but_is_declared(self):
        workload = self.workload(independent=False)
        self.assertFalse(workload.independent)
        self.assertTrue(workload.measure("stdlib", EvaluationConfiguration(), 0).correct)

    def test_fresh_seeded_fixture_once_per_measure_and_ordered_results(self):
        events = []
        @contextmanager
        def factory(seed):
            events.append(("enter", seed))
            try:
                yield EvaluationFixture((3, 1, 2), lambda results: results == (9, 1, 4))
            finally:
                events.append(("exit", seed))
        workload = self.workload(fixture_factory=factory)
        for engine in ("stdlib", "intelligent"):
            measurement = workload.measure(engine, EvaluationConfiguration("thread", 2, 2), 17)
            self.assertTrue(measurement.correct)
            self.assertEqual(measurement.error_count, 0)
            self.assertEqual(measurement.cold_seconds, measurement.warm_seconds)
            self.assertIsNone(measurement.cpu_seconds)
            self.assertIsNone(measurement.peak_rss_bytes)
            self.assertIsNone(measurement.batch_p95_seconds)
        self.assertEqual(events, [("enter", 17), ("exit", 17)] * 2)
        self.assertEqual(workload.metric_scope, "pool_lifecycle")

    def test_overflow_bounded_before_pool_creation_and_cleanup(self):
        pulls, lifecycle = [], []
        def values():
            while True:
                pulls.append(len(pulls))
                yield len(pulls)
        @contextmanager
        def fixture(seed):
            try:
                yield EvaluationFixture(values(), lambda result: True)
            finally:
                lifecycle.append("closed")
        workload = self.workload(fixture_factory=fixture, max_tasks=4)
        with patch("intelligent_futures.evaluation_workloads.make_executor") as create:
            measurement = workload.measure("stdlib", EvaluationConfiguration(), 0)
        self.assertEqual(len(pulls), 5)
        self.assertEqual(lifecycle, ["closed"])
        create.assert_not_called()
        self.assertFalse(measurement.correct)
        self.assertEqual(measurement.error_count, 1)

    def test_exact_task_bound_is_accepted_and_empty_fixture_rejected(self):
        @contextmanager
        def fixture(seed):
            yield EvaluationFixture(range(seed), lambda results: len(results) == seed)
        workload = self.workload(fixture_factory=fixture, max_tasks=4)
        self.assertTrue(workload.measure("stdlib", EvaluationConfiguration(), 4).correct)
        self.assertFalse(workload.measure("stdlib", EvaluationConfiguration(), 0).correct)

    def test_same_batch_kernel_window_and_no_extra_warmup(self):
        calls = []
        def fn(value):
            calls.append(value)
            return square(value)
        workload = self.workload(fn=fn, max_pending=2)
        captures = []
        for engine in ("stdlib", "intelligent"):
            executor = _DeferredExecutor()
            with patch("intelligent_futures.evaluation_workloads.make_executor", return_value=executor):
                result = workload.measure(engine, EvaluationConfiguration("thread", 2, 2), 0)
            self.assertTrue(result.correct)
            self.assertEqual(executor.peak, 2)
            self.assertEqual(executor.pending, 0)
            self.assertTrue(executor.closed)
            self.assertEqual([args[1] for _, args, _ in executor.submissions], [(0, 1), (2, 3), (4,)])
            captures.append(executor.submissions[0][0])
        self.assertIs(captures[0], captures[1])
        self.assertEqual(calls, list(range(5)) * 2)
        self.assertEqual(workload.measure._evaluation_max_pending, 2)
        self.assertFalse(workload.measure._evaluation_process_safe)

    def test_cold_timing_includes_pool_lifecycle_only(self):
        clock = [0.0]
        def tick(amount):
            clock[0] += amount
        def validate(results):
            tick(11)
            return results == (4,)
        @contextmanager
        def fixture(seed):
            tick(10)
            try:
                yield EvaluationFixture((2,), validate)
            finally:
                tick(13)
        def create(*args, **kwargs):
            tick(2)
            return _DeferredExecutor(tick)
        with patch("intelligent_futures.evaluation_workloads.time.perf_counter", side_effect=lambda: clock[0]), \
             patch("intelligent_futures.evaluation_workloads.make_executor", side_effect=create):
            result = self.workload(fixture_factory=fixture).measure("stdlib", EvaluationConfiguration(), 0)
        self.assertEqual(result.cold_seconds, 12)  # 2 create + 3 submit/execute + 7 shutdown
        self.assertEqual(clock[0], 46)

    def test_process_opt_in_before_fixture_and_factory_opt_in_before_pool(self):
        config = EvaluationConfiguration("process", 1, 1)
        events = []
        def fixture(seed):
            events.append(seed)
            raise AssertionError("unsafe fixture")
        workload = self.workload(fixture_factory=fixture)
        with self.assertRaises(ValueError):
            workload.measure("intelligent", config, 0)
        self.assertEqual(events, [])
        for engine in ("stdlib", "intelligent"):
            with patch("intelligent_futures.evaluation_workloads.ProcessPoolExecutor") as create:
                with self.assertRaises(ValueError):
                    make_executor(engine, config, max_pending=1)
                create.assert_not_called()

    def test_spawn_execution_both_engines_and_cleanup(self):
        workload = self.workload(process_safe=True)
        config = EvaluationConfiguration("process", 2, 2)
        for engine in ("stdlib", "intelligent"):
            created = []
            def create(*args, **kwargs):
                executor = make_executor(*args, **kwargs)
                created.append(executor)
                process_pool = executor if engine == "stdlib" else executor._processes
                self.assertEqual(process_pool._mp_context.get_start_method(), "spawn")
                return executor
            with patch("intelligent_futures.evaluation_workloads.make_executor", side_effect=create):
                result = workload.measure(engine, config, 3)
            self.assertTrue(result.correct)
            process_pool = created[0] if engine == "stdlib" else created[0]._processes
            self.assertIsNone(process_pool._processes)
            if engine == "intelligent":
                self.assertEqual(created[0].snapshot()["counts"]["thread_submitted"], 0)
                self.assertEqual(created[0].snapshot()["counts"]["process_submitted"], 3)

    def test_process_failure_is_sanitized_without_thread_fallback(self):
        workload = self.workload(fn=fail_with_private_message, process_safe=True, max_pending=1)
        for engine in ("stdlib", "intelligent"):
            result = workload.measure(engine, EvaluationConfiguration("process", 1, 1), 4)
            self.assertFalse(result.correct)
            self.assertEqual(result.error_count, 1)
            self.assertNotIn("private", json.dumps(asdict(result)))

    def test_unpicklable_callable_is_not_replayed_in_parent_threads(self):
        calls = []
        def unpicklable(value):
            calls.append(value)
            return value
        workload = self.workload(fn=unpicklable, process_safe=True, max_pending=1)
        result = workload.measure("intelligent", EvaluationConfiguration("process", 1, 1), 0)
        self.assertFalse(result.correct)
        self.assertEqual(result.error_count, 1)
        self.assertEqual(calls, [])

    def test_evaluator_rejects_incompatible_scope_window_and_process_before_work(self):
        calls = []
        @contextmanager
        def fixture(seed):
            calls.append(seed)
            yield EvaluationFixture((1,), lambda results: True)
        workload = self.workload(fixture_factory=fixture, max_pending=2)
        config = EvaluationConfiguration("thread", 1, 1)
        for plan in (EvaluationPlan(max_pending=1), EvaluationPlan(max_pending=2, metric="warm_seconds")):
            with self.assertRaises(ValueError):
                evaluate_workload(workload, stdlib_configurations=[config],
                                  intelligent_configurations=[config], fixed_baseline=config, plan=plan)
        process = EvaluationConfiguration("process", 1, 1)
        with self.assertRaises(ValueError):
            evaluate_workload(workload, stdlib_configurations=[config, process],
                              intelligent_configurations=[process], fixed_baseline=config,
                              plan=EvaluationPlan(max_pending=2), allow_process=True)
        self.assertEqual(calls, [])

    def test_tiny_integration_runs_only_predeclared_measurements(self):
        seeds, calls = [], []
        def fn(value):
            calls.append(value)
            return square(value)
        @contextmanager
        def fixture(seed):
            seeds.append(seed)
            yield EvaluationFixture((1, 2), lambda results: results == (1, 4))
        workload = self.workload(fn, fixture, max_pending=2)
        config = EvaluationConfiguration("thread", 1, 2)
        report = evaluate_workload(workload, stdlib_configurations=[config],
                                   intelligent_configurations=[config], fixed_baseline=config,
                                   plan=EvaluationPlan(training_blocks=1, heldout_blocks=1,
                                                       max_workers=1, max_pending=2))
        self.assertEqual(report.attempted_evaluations, 4)
        self.assertEqual(report.successful_evaluations, 4)
        self.assertEqual(calls, [1, 2] * 4)
        self.assertEqual(len(set(seeds)), 2)
        self.assertIsNone(report.recommendation)  # One holdout block cannot establish adoption.
        json.dumps(report.to_json(), allow_nan=False)

    def test_failure_cancels_queued_batches_without_replay_and_cleans_fixture(self):
        calls, cleanup = [], []
        def fail(value):
            calls.append(value)
            raise RuntimeError("private-data")
        @contextmanager
        def fixture(seed):
            try:
                yield EvaluationFixture(range(5), lambda results: True)
            finally:
                cleanup.append(True)
        executor = _DeferredExecutor()
        with patch("intelligent_futures.evaluation_workloads.make_executor", return_value=executor):
            result = self.workload(fail, fixture, max_pending=2).measure("stdlib", EvaluationConfiguration(), 0)
        self.assertEqual(calls, [0])
        self.assertEqual(executor.cancelled, 1)
        self.assertTrue(executor.closed)
        self.assertEqual(cleanup, [True])
        self.assertFalse(result.correct)
        self.assertEqual(result.error_count, 1)
        self.assertNotIn("private", json.dumps(asdict(result)))

    def test_validator_requires_strict_bool_and_sanitizes_errors(self):
        for verdict in (1, None, "private-data", (True,), False):
            @contextmanager
            def fixture(seed):
                yield EvaluationFixture((2,), lambda results: verdict)
            result = self.workload(fixture_factory=fixture).measure("stdlib", EvaluationConfiguration(), 0)
            self.assertFalse(result.correct)
            self.assertEqual(result.error_count, 0 if verdict is False else 1)
            self.assertNotIn("private", json.dumps(asdict(result)))

    def test_fixture_enter_exit_and_validation_errors_fail_closed(self):
        for where in ("enter", "validate", "exit"):
            def validate(results):
                if where == "validate":
                    raise RuntimeError("private-validator")
                return True
            @contextmanager
            def fixture(seed):
                if where == "enter":
                    raise RuntimeError("private-fixture")
                yield EvaluationFixture((2,), validate)
                if where == "exit":
                    raise RuntimeError("private-cleanup")
            result = self.workload(fixture_factory=fixture).measure("stdlib", EvaluationConfiguration(), 0)
            self.assertFalse(result.correct)
            self.assertEqual(result.error_count, 1)
            self.assertNotIn("private", json.dumps(asdict(result)))

    def test_cancellation_and_interrupts_propagate_after_cleanup(self):
        for exception in (CancelledError, KeyboardInterrupt, SystemExit):
            cleanup = []
            def fail(value):
                raise exception()
            @contextmanager
            def fixture(seed):
                try:
                    yield EvaluationFixture((1,), lambda results: True)
                finally:
                    cleanup.append(True)
            executor = _DeferredExecutor()
            with patch("intelligent_futures.evaluation_workloads.make_executor", return_value=executor):
                with self.assertRaises(exception):
                    self.workload(fail, fixture).measure("stdlib", EvaluationConfiguration(), 0)
            self.assertTrue(executor.closed)
            self.assertEqual(cleanup, [True])

    def test_fixture_suppression_cannot_hide_failure_or_interrupt(self):
        for exception in (RuntimeError, CancelledError, KeyboardInterrupt, SystemExit):
            def fail(value):
                raise exception()
            @contextmanager
            def fixture(seed):
                try:
                    yield EvaluationFixture((1,), lambda results: True)
                except BaseException:
                    pass
            workload = self.workload(fail, fixture)
            executor = _DeferredExecutor()
            with patch("intelligent_futures.evaluation_workloads.make_executor", return_value=executor):
                if exception is RuntimeError:
                    result = workload.measure("stdlib", EvaluationConfiguration(), 0)
                    self.assertFalse(result.correct)
                    self.assertEqual(result.error_count, 1)
                else:
                    with self.assertRaises(exception):
                        workload.measure("stdlib", EvaluationConfiguration(), 0)
            self.assertTrue(executor.closed)

    def test_fixed_adaptive_and_predictive_budgets(self):
        configurations = (
            (EvaluationConfiguration("thread", 3, 2), 3, 0),
            (EvaluationConfiguration("process", 3, 2), 1, 3),
            (EvaluationConfiguration("adaptive", 3, 2, process_workers=1), 2, 1),
            (EvaluationConfiguration("thread", 3, 2, predictive=True, profile_sample_interval=2), 3, 0),
        )
        for config, threads, processes in configurations:
            with make_executor("intelligent", config, max_pending=2, allow_process=True) as executor:
                self.assertEqual(executor._worker_limits, {"thread": threads, "process": processes})
                self.assertEqual(executor._max_pending, 2)
                self.assertEqual(executor._sample_interval, config.profile_sample_interval)
            if config.backend == "adaptive" or config.predictive:
                with self.assertRaises(ValueError):
                    make_executor("stdlib", config, max_pending=2, allow_process=True)

    def test_invalid_declarations_and_configuration_do_not_open_fixture(self):
        for option in (dict(repeatable=1), dict(independent=1), dict(process_safe=1),
                       dict(max_tasks=True), dict(max_pending=0)):
            with self.assertRaises((TypeError, ValueError)):
                self.workload(**option)
        with self.assertRaises(TypeError):
            EvaluationFixture((1,), False)
        for engine, config in (("other", EvaluationConfiguration()), ("stdlib", object()),
                               ("stdlib", EvaluationConfiguration("thread", predictive=True))):
            with self.assertRaises((TypeError, ValueError)):
                self.workload().measure(engine, config, 0)

    def test_wrapper_does_not_steal_callable_arguments_and_threads_join(self):
        def argument(value):
            return value["hints"]
        @contextmanager
        def fixture(seed):
            yield EvaluationFixture(({"hints": "application-value"},),
                                    lambda results: results == ("application-value",))
        self.assertTrue(self.workload(argument, fixture).measure("intelligent", EvaluationConfiguration(), 0).correct)
        self.assertFalse(any(thread.name.startswith("intelligent-") and thread.is_alive()
                             for thread in threading.enumerate()))
        def keyword(*, hints):
            return hints
        with make_executor("intelligent", EvaluationConfiguration(), max_pending=1) as executor:
            self.assertEqual(executor.submit(keyword, hints="application-value").result(), "application-value")


if __name__ == "__main__":
    unittest.main()
