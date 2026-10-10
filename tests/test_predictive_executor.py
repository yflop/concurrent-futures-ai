"""Predictive opt-in integration contracts; no timing-dependent speed claims."""
from concurrent.futures import Future
from concurrent.futures.process import BrokenProcessPool
import json
import os
import threading
import unittest
from unittest.mock import patch

from intelligent_futures import BackpressureError, IntelligentExecutor, TaskHints
from intelligent_futures.prediction import ExecutionObjective, HardwareCapabilities

WAIT = 10


def identity(value):
    return value


def keywords(**kwargs):
    return kwargs


def batch_identity(values):
    return os.getpid(), tuple(values)


def crash_worker():
    os._exit(7)


class PredictiveRuntimeTests(unittest.TestCase):
    def executor(self, **kwargs):
        executor = IntelligentExecutor(predictive=True, **kwargs)
        self.addCleanup(executor.shutdown, wait=True, cancel_futures=True)
        return executor

    def gate(self):
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)

        def blocked(value=None):
            entered.set()
            if not release.wait(WAIT):
                raise AssertionError("gate not released")
            return value
        return blocked, entered, release

    def settled(self, executor):
        with executor._condition:
            self.assertTrue(executor._condition.wait_for(lambda: executor._pending == 0, WAIT))

    def invariants(self, executor):
        snapshot = executor.snapshot()
        counts = snapshot["counts"]
        self.assertEqual(counts["submitted"], counts["completed"] + counts["failed"] + counts["cancelled"] + snapshot["pending"])
        self.assertEqual(counts["submitted"], counts["thread_submitted"] + counts["process_submitted"])
        self.assertLessEqual(counts["peak_pending"], snapshot["max_pending"])
        self.assertGreaterEqual(snapshot["pending"], 0)
        json.dumps(snapshot, allow_nan=False)
        return snapshot

    def test_standard_submit_keeps_reserved_keywords_for_callable(self):
        executor = self.executor()
        expected = dict(hints="owned by callable", features="private payload", fn=7)
        self.assertEqual(executor.submit(keywords, **expected).result(WAIT), expected)
        self.assertEqual(executor.submit_task(keywords, features=9).result(WAIT), {"features": 9})

    def test_opt_in_is_explicit(self):
        with IntelligentExecutor() as executor:
            self.assertEqual(executor.submit(identity, 3).result(WAIT), 3)
            self.assertEqual(executor.export_profiles(), ())

    def test_successful_thread_samples_export_without_payload(self):
        executor = self.executor()
        secret = "private-argument-not-for-profiles"
        self.assertEqual(executor.submit_profiled(identity, secret, hints=TaskHints(key="summary")).result(WAIT), secret)
        executor.shutdown()
        profiles = executor.export_profiles()
        self.assertIsInstance(profiles, tuple)
        self.assertTrue(profiles)
        self.assertNotIn(secret, repr(profiles))
        self.assertNotIn(secret, json.dumps(self.invariants(executor)))

    def test_failed_execution_is_never_replayed(self):
        executor = self.executor()
        calls = []
        def fail():
            calls.append(1)
            raise ValueError("intentional")
        with self.assertRaisesRegex(ValueError, "intentional"):
            executor.submit_profiled(fail).result(WAIT)
        executor.shutdown()
        self.assertEqual(calls, [1])
        self.assertEqual(executor.export_profiles(), ())
        self.assertEqual(self.invariants(executor)["counts"]["failed"], 1)

    def test_objective_caps_admission_and_preserves_pool(self):
        executor = self.executor(max_workers=4, max_pending=8, submit_timeout=0,
                                 objective=ExecutionObjective(max_concurrency=1))
        blocked, entered, release = self.gate()
        pool = executor._threads
        future = executor.submit(blocked, 1)
        self.assertTrue(entered.wait(WAIT))
        with self.assertRaises(BackpressureError):
            executor.submit(identity, 2)
        self.assertIs(executor._threads, pool)
        self.assertEqual(pool._max_workers, 4)
        release.set()
        self.assertEqual(future.result(WAIT), 1)
        self.settled(executor)
        self.assertEqual(executor.submit(identity, 3).result(WAIT), 3)
        executor.shutdown()
        self.assertEqual(self.invariants(executor)["counts"]["peak_pending"], 1)

    def test_shutdown_wakes_producer_waiting_on_predictive_admission(self):
        executor = self.executor(max_workers=3, objective=ExecutionObjective(max_concurrency=1))
        blocked, entered, release = self.gate()
        executor.submit(blocked)
        self.assertTrue(entered.wait(WAIT))
        waiting = threading.Event()
        original = executor._condition.wait
        def observed(timeout=None):
            waiting.set()
            return original(timeout)
        outcomes = []
        def produce():
            try:
                outcomes.append(executor.submit(identity, 2))
            except BaseException as error:
                outcomes.append(error)
        with patch.object(executor._condition, "wait", observed):
            producer = threading.Thread(target=produce, daemon=True)
            producer.start()
            self.assertTrue(waiting.wait(WAIT))
            executor.shutdown(wait=False, cancel_futures=True)
            producer.join(WAIT)
        self.assertFalse(producer.is_alive())
        self.assertEqual(len(outcomes), 1)
        self.assertIsInstance(outcomes[0], RuntimeError)
        release.set()
        executor.shutdown()
        self.assertEqual(self.invariants(executor)["counts"]["submitted"], 1)

    def test_concurrent_producers_preserve_accounting(self):
        executor = self.executor(max_workers=4, max_pending=3,
                                 objective=ExecutionObjective(max_concurrency=2))
        barrier = threading.Barrier(5)
        futures, errors = [], []
        lock = threading.Lock()
        def produce(offset):
            try:
                barrier.wait(WAIT)
                local = [executor.submit_profiled(identity, offset + n) for n in range(30)]
                with lock:
                    futures.extend(local)
            except BaseException as error:
                errors.append(error)
        producers = [threading.Thread(target=produce, args=(i * 30,), daemon=True) for i in range(4)]
        for producer in producers:
            producer.start()
        barrier.wait(WAIT)
        for producer in producers:
            producer.join(WAIT)
            self.assertFalse(producer.is_alive())
        self.assertFalse(errors)
        self.assertEqual(sorted(f.result(WAIT) for f in futures), list(range(120)))
        executor.shutdown()
        snapshot = self.invariants(executor)
        self.assertLessEqual(snapshot["counts"]["peak_pending"], 2)

    def test_adaptive_map_orders_results_and_calls_each_input_once(self):
        executor = self.executor(max_workers=4, max_pending=3)
        seen = []
        lock = threading.Lock()
        def task(value):
            with lock:
                seen.append(value)
            return value * 2
        self.assertEqual(list(executor.map_adaptive(task, range(101), min_batch_size=2, max_batch_size=7)), [i * 2 for i in range(101)])
        self.assertCountEqual(seen, range(101))
        executor.shutdown()
        self.assertEqual(self.invariants(executor)["counts"]["completed"], 101)

    def test_adaptive_map_prefetch_is_bounded_and_lazy(self):
        executor = self.executor()
        consumed = []
        def source():
            for value in range(100):
                consumed.append(value)
                yield value
        results = executor.map_adaptive(identity, source(), min_batch_size=2, max_batch_size=5)
        self.assertEqual(consumed, [])
        self.assertEqual(next(results), 0)
        self.assertLessEqual(len(consumed), 5)
        results.close()
        executor.shutdown()
        self.assertLessEqual(len(consumed), 5)
        self.invariants(executor)

    def test_adaptive_map_failure_does_not_retry_or_consume_next_window(self):
        executor = self.executor()
        calls = []
        def fail(value):
            calls.append(value)
            if value == 0:
                raise ValueError("map failure")
            return value
        with self.assertRaisesRegex(ValueError, "map failure"):
            list(executor.map_adaptive(fail, range(100), min_batch_size=3, max_batch_size=3))
        executor.shutdown()
        self.assertEqual(calls.count(0), 1)
        self.assertEqual(len(calls), len(set(calls)))
        self.assertTrue(set(calls).issubset({0, 1, 2}))
        self.invariants(executor)

    def test_adaptive_map_empty(self):
        executor = self.executor()
        self.assertEqual(list(executor.map_adaptive(identity, [])), [])
        self.assertEqual(executor.snapshot()["counts"]["submitted"], 0)

    def test_adaptive_map_validates_parameters_before_consuming(self):
        executor = self.executor()
        for name, values in {
            "min_batch_size": [0, -1, True, 1.5, "2"],
            "max_batch_size": [0, -1, True, 1.5, "2"],
            "target_batch_seconds": [0, -1, float("nan"), float("inf")],
        }.items():
            for value in values:
                with self.subTest(name=name, value=value), self.assertRaises((ValueError, TypeError)):
                    executor.map_adaptive(identity, [], **{name: value})
        with self.assertRaises(ValueError):
            executor.map_adaptive(identity, [], min_batch_size=5, max_batch_size=2)

    def test_predictive_cancellation_releases_slot_without_training(self):
        executor = self.executor(max_workers=2, submit_timeout=0,
                                 objective=ExecutionObjective(max_concurrency=1))
        # A pending genuine Future isolates cancellation from scheduling races.
        queued = Future()
        with patch.object(executor._threads, "submit", return_value=queued):
            future = executor.submit_profiled(identity, 7)
            with self.assertRaises(BackpressureError):
                executor.submit(identity, 8)
            self.assertTrue(future.cancel())
        self.assertEqual(executor.export_profiles(), ())
        self.assertEqual(executor.submit(identity, 9).result(WAIT), 9)
        executor.shutdown()
        self.assertEqual(self.invariants(executor)["counts"]["cancelled"], 1)

    def test_deterministic_profile_sampling_is_bounded(self):
        executor = self.executor(profile_sample_interval=4, max_profile_keys=2)
        for index in range(10):
            self.assertEqual(executor.submit(identity, index).result(WAIT), index)
        self.assertEqual(len(executor.export_profiles()[0].samples), 3)
        self.assertEqual(executor.snapshot()["predictive"]["profile_sample_interval"], 4)
        for key in ("one", "two", "three"):
            executor.submit_task(identity, 1, hints=TaskHints(key=key)).result(WAIT)
        self.assertLessEqual(len(executor._sample_counts), 2)
        self.assertLessEqual(len(executor.export_profiles()), 2)
        for interval in (0, -1, True, 1.5, None):
            with self.assertRaises(ValueError):
                IntelligentExecutor(predictive=True, profile_sample_interval=interval)

    def test_explicit_thread_policy_overrides_predictive_process_recommendation(self):
        executor = self.executor(policy="threads", process_workers=1)
        pid = executor.submit_task(os.getpid, hints=TaskHints(kind="cpu", process_safe=True)).result(WAIT)
        self.assertEqual(pid, os.getpid())
        self.assertEqual(executor.snapshot()["counts"]["process_submitted"], 0)

    def test_adaptive_window_waits_for_completion_measurement(self):
        executor = self.executor()
        callback_entered, callback_release = threading.Event(), threading.Event()
        self.addCleanup(callback_release.set)
        original_submit = executor.submit_task
        consumed = []
        results = []
        errors = []

        def submit(*args, **kwargs):
            future = original_submit(*args, **kwargs)
            def preceding_callback(_future):
                callback_entered.set()
                callback_release.wait(WAIT)
            future.add_done_callback(preceding_callback)
            return future

        task_release = threading.Event()
        task_entered = threading.Event()
        self.addCleanup(task_release.set)
        def work(value):
            task_entered.set()
            task_release.wait(WAIT)
            return value
        def source():
            for value in range(4):
                consumed.append(value)
                yield value
        with patch.object(executor, "submit_task", side_effect=submit):
            iterator = executor.map_adaptive(work, source(), min_batch_size=1,
                                            max_batch_size=4, target_batch_seconds=1e-9)
            def first():
                try:
                    results.append(next(iterator))
                except BaseException as exc:
                    errors.append(exc)
            thread = threading.Thread(target=first)
            thread.start()
            self.assertTrue(task_entered.wait(WAIT))
            task_release.set()
            thread.join(WAIT)
            self.assertFalse(thread.is_alive())
            self.assertEqual(results, [0])
            self.assertTrue(callback_entered.wait(WAIT))
            advance = threading.Thread(target=first)
            advance.start()
            # The generator cannot consume a new window before callback release.
            self.assertEqual(consumed, [0])
            callback_release.set()
            advance.join(WAIT)
            self.assertFalse(advance.is_alive())
            self.assertFalse(errors)
            self.assertEqual(results, [0, 1])
            self.assertEqual(consumed, [0, 1])
            iterator.close()

    def test_profiles_are_available_when_future_result_returns(self):
        executor = self.executor()
        for value in range(10):
            executor.submit_profiled(identity, value, hints=TaskHints(key="known"),
                                     features={"size": 4.0}).result(WAIT)
            profiles = executor.export_profiles()
            self.assertEqual(sum(len(p.samples) for p in profiles), value + 1)
        profile = profiles[0]
        self.assertEqual(profile.features, (("size", 4.0),))
        self.assertTrue(all(sample.wall_seconds >= 0 for sample in profile.samples))
        self.assertTrue(all(sample.thread_cpu_seconds is None or sample.thread_cpu_seconds >= 0
                            for sample in profile.samples))

    def test_profile_import_preserves_explicit_data(self):
        executor = self.executor()
        executor.submit_profiled(identity, 1, hints=TaskHints(key="transfer"),
                                 features={"size": 2}).result(WAIT)
        profiles = executor.export_profiles()
        second = self.executor(workload_profiles=profiles)
        self.assertEqual(second.export_profiles(), profiles)

    def test_predictive_configuration_is_not_silently_enabled(self):
        for kwargs in ({"objective": ExecutionObjective()},
                       {"hardware": HardwareCapabilities()}, {"predictive": "yes"}):
            with self.subTest(kwargs=kwargs), self.assertRaises((ValueError, TypeError)):
                IntelligentExecutor(**kwargs)
        with IntelligentExecutor() as baseline:
            with self.assertRaises(ValueError):
                baseline.submit_profiled(identity, 1)
        for kwargs in ({"hardware": object()}, {"objective": object()},
                       {"workload_profiles": [object()]},
                       {"max_workers": 1, "objective": ExecutionObjective(min_concurrency=2)}):
            with self.subTest(kwargs=kwargs), self.assertRaises((ValueError, TypeError)):
                IntelligentExecutor(predictive=True, **kwargs)

    def test_features_reject_raw_or_nonfinite_values_without_execution(self):
        executor = self.executor()
        for features in ({"text": "secret"}, {"size": float("nan")},
                         {"size": float("inf")}, {"size": object()}):
            with self.subTest(features=features), self.assertRaises((ValueError, TypeError)):
                executor.submit_profiled(identity, 1, features=features)
        self.assertEqual(executor.snapshot()["counts"]["submitted"], 0)

    def test_standard_submit_cannot_grant_implicit_process_consent(self):
        executor = self.executor(process_workers=1)
        self.assertEqual(executor.submit(os.getpid).result(WAIT), os.getpid())
        executor.shutdown()
        snapshot = self.invariants(executor)
        self.assertEqual(snapshot["counts"]["process_submitted"], 0)
        self.assertIsNotNone(snapshot["predictive"]["last_prediction"])
        self.assertIn("hardware", snapshot["predictive"])
        self.assertEqual(snapshot["predictive"]["profiling_errors"], 0)

    def test_extreme_finite_target_is_bounded(self):
        executor = self.executor()
        self.assertEqual(list(executor.map_adaptive(identity, range(10),
                         min_batch_size=2, max_batch_size=4,
                         target_batch_seconds=1e308)), list(range(10)))

    def test_map_batches_reduces_invocations_without_replay(self):
        executor = self.executor()
        calls = []
        def batch(values):
            self.assertIsInstance(values, tuple)
            calls.append(values)
            return tuple(value * 2 for value in values)
        outputs = list(executor.map_batches(batch, range(101), min_batch_size=4,
                       max_batch_size=12, buffersize=2))
        self.assertEqual([item for batch in outputs for item in batch], [i * 2 for i in range(101)])
        self.assertEqual(sorted(item for batch in calls for item in batch), list(range(101)))
        self.assertLess(len(calls), 101)
        self.assertTrue(all(1 <= len(batch) <= 12 for batch in calls))
        executor.shutdown()
        self.assertEqual(self.invariants(executor)["counts"]["submitted"], len(calls))

    def test_map_batches_does_not_flatten_arbitrary_return_values(self):
        executor = self.executor()
        self.assertEqual(list(executor.map_batches(sum, range(7), min_batch_size=3,
                                                  max_batch_size=3)), [3, 12, 6])

    def test_map_batches_lazy_prefetch_and_close(self):
        executor = self.executor()
        consumed = []
        def source():
            for value in range(100):
                consumed.append(value)
                yield value
        output = executor.map_batches(tuple, source(), buffersize=2,
                                      min_batch_size=3, max_batch_size=5)
        self.assertEqual(consumed, [])
        self.assertEqual(next(output), (0, 1, 2))
        self.assertLessEqual(len(consumed), 10)
        output.close()
        executor.shutdown()
        self.assertLessEqual(len(consumed), 10)

    def test_map_batches_midbatch_failure_is_not_retried(self):
        executor = self.executor()
        calls = []
        def failing_batch(values):
            for value in values:
                calls.append(value)
                if value == 1:
                    raise ValueError("partial batch failure")
            return values
        with self.assertRaisesRegex(ValueError, "partial batch failure"):
            list(executor.map_batches(failing_batch, range(100), buffersize=1,
                                     min_batch_size=3, max_batch_size=3))
        executor.shutdown()
        self.assertEqual(calls, [0, 1])
        self.assertEqual(self.invariants(executor)["counts"]["failed"], 1)

    def test_map_batches_process_spawn_routes_actual_batches(self):
        executor = self.executor(process_workers=1)
        batches = list(executor.map_batches(batch_identity, range(17),
                       hints=TaskHints(kind="cpu", process_safe=True),
                       min_batch_size=4, max_batch_size=4, buffersize=2))
        self.assertTrue(all(pid != os.getpid() for pid, _ in batches))
        self.assertEqual([item for _, batch in batches for item in batch], list(range(17)))
        executor.shutdown()
        self.assertEqual(self.invariants(executor)["counts"]["process_submitted"], 5)

    def test_map_batches_extreme_target_and_validation(self):
        executor = self.executor()
        results = list(executor.map_batches(tuple, range(10), min_batch_size=2,
                       max_batch_size=4, target_batch_seconds=1e308))
        self.assertEqual([item for batch in results for item in batch], list(range(10)))
        for kwargs in ({"buffersize": 0}, {"buffersize": True},
                       {"min_batch_size": 0}, {"max_batch_size": 0},
                       {"min_batch_size": 5, "max_batch_size": 3},
                       {"target_batch_seconds": float("inf")}, {"target_batch_seconds": 0}):
            with self.subTest(kwargs=kwargs), self.assertRaises((TypeError, ValueError)):
                executor.map_batches(tuple, [], **kwargs)
        self.assertEqual(list(executor.map_batches(tuple, [])), [])

    def test_process_crash_never_replays_in_threads(self):
        executor = self.executor(max_workers=2, process_workers=1, policy="hints")
        future = executor.submit_profiled(crash_worker, hints=TaskHints(kind="cpu", process_safe=True))
        with self.assertRaises(BrokenProcessPool):
            future.result(WAIT)
        self.settled(executor)
        snapshot = self.invariants(executor)
        self.assertEqual(snapshot["counts"]["process_submitted"], 1)
        self.assertEqual(snapshot["counts"]["thread_submitted"], 0)
        self.assertEqual(executor.export_profiles(), ())
        self.assertEqual(executor.submit(identity, "healthy").result(WAIT), "healthy")


if __name__ == "__main__":
    unittest.main()
