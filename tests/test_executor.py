"""Deterministic integration tests, including portable spawn workers.

Run from the repository root with:
    PYTHONPATH=src python -m unittest discover -s tests -v

Synchronization uses events and bounded waits instead of scheduling sleeps.
"""
from concurrent.futures import (
    CancelledError, Executor, FIRST_COMPLETED, Future, TimeoutError,
    as_completed, wait,
)
from concurrent.futures.process import BrokenProcessPool
from dataclasses import FrozenInstanceError
import json
import multiprocessing
import os
import pickle
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from intelligent_futures import BackpressureError, IntelligentExecutor, TaskHints


WAIT = 10


class FileEvent:
    """Pickleable, bounded interprocess barrier without a manager socket.

    Polling is conditional on a durable signal rather than a fixed scheduling
    sleep. This works in restricted CI environments and with spawn on Windows.
    """
    def __init__(self, path):
        self.path = str(path)

    def set(self):
        Path(self.path).touch()

    def wait(self, timeout):
        deadline = time.monotonic() + timeout
        pause = threading.Event()
        while not Path(self.path).exists():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            pause.wait(min(0.005, remaining))
        return True


def add(left, right=0):
    return left + right


def identity(value):
    return value


def process_identity(value):
    return os.getpid(), value


def process_gate(entered, release, value):
    entered.set()
    if not release.wait(WAIT):
        raise AssertionError("process test gate was not released")
    return value


def raise_value_error():
    raise ValueError("intentional task failure")


def raise_system_exit():
    raise SystemExit("intentional task exit")


def raise_keyboard_interrupt():
    raise KeyboardInterrupt("intentional interrupt")


def unpickleable_result():
    return threading.Lock()


def exit_worker():
    os._exit(7)


def keyword_echo(**kwargs):
    return kwargs


class ExecutorCase(unittest.TestCase):
    def executor(self, **kwargs):
        executor = IntelligentExecutor(**kwargs)
        self.addCleanup(executor.shutdown, wait=True, cancel_futures=True)
        return executor

    def gate(self):
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)

        def blocked(value=None):
            entered.set()
            if not release.wait(WAIT):
                raise AssertionError("test gate was not released")
            return value

        return blocked, entered, release

    def settled(self, executor):
        # Future.result() may precede the instrumentation callback. The
        # condition establishes that final accounting is visible to the test.
        with executor._condition:
            self.assertTrue(executor._condition.wait_for(
                lambda: executor._pending == 0, timeout=WAIT),
                "executor bookkeeping did not settle")

    def assert_invariants(self, executor):
        snapshot = executor.snapshot()
        counts = snapshot["counts"]
        self.assertEqual(counts["submitted"], counts["thread_submitted"] + counts["process_submitted"])
        self.assertEqual(counts["submitted"], snapshot["pending"] + counts["completed"] + counts["failed"] + counts["cancelled"])
        self.assertGreaterEqual(snapshot["pending"], 0)
        self.assertLessEqual(snapshot["pending"], snapshot["max_pending"])
        self.assertLessEqual(counts["peak_pending"], snapshot["max_pending"])
        return snapshot

    def observe_admission_wait(self, executor):
        waiting = threading.Event()
        original = executor._condition.wait

        def observed_wait(timeout=None):
            waiting.set()
            return original(timeout)

        patcher = patch.object(executor._condition, "wait", observed_wait)
        patcher.start()
        self.addCleanup(patcher.stop)
        return waiting


class FutureSemanticsTests(ExecutorCase):
    def test_real_future_and_executor(self):
        executor = self.executor()
        self.assertIsInstance(executor, Executor)
        future = executor.submit(add, 2, right=3)
        self.assertIs(type(future), Future)
        self.assertEqual(future.result(WAIT), 5)
        self.assertIsNone(future.exception(WAIT))
        self.assertTrue(future.done())
        self.assertFalse(future.cancelled())

    def test_submit_preserves_callable_keyword_arguments(self):
        executor = self.executor()
        expected = {"hints": "the callable owns this", "timeout": 5, "fn": 9}
        self.assertEqual(executor.submit(keyword_echo, **expected).result(WAIT), expected)
        self.assertEqual(executor.submit_task(keyword_echo, hints=TaskHints(), timeout=5).result(WAIT), {"timeout": 5})

    def test_running_future_cannot_be_cancelled(self):
        executor = self.executor(max_workers=1)
        blocked, entered, release = self.gate()
        future = executor.submit(blocked, 41)
        self.assertTrue(entered.wait(WAIT))
        self.assertTrue(future.running())
        self.assertFalse(future.cancel())
        release.set()
        self.assertEqual(future.result(WAIT), 41)
        self.settled(executor)
        self.assertEqual(self.assert_invariants(executor)["counts"]["completed"], 1)

    def test_queued_future_cancels_and_never_runs(self):
        executor = self.executor(max_workers=1)
        blocked, entered, release = self.gate()
        running = executor.submit(blocked)
        self.assertTrue(entered.wait(WAIT))
        executed = threading.Event()
        queued = executor.submit(executed.set)
        self.assertFalse(queued.running())
        self.assertTrue(queued.cancel())
        self.assertTrue(queued.cancelled())
        with self.assertRaises(CancelledError):
            queued.result(WAIT)
        release.set()
        running.result(WAIT)
        executor.shutdown()
        self.assertFalse(executed.is_set())
        self.assertEqual(wait([queued], timeout=WAIT).done, {queued})
        self.assertEqual(self.assert_invariants(executor)["counts"]["cancelled"], 1)

    def test_result_timeout_does_not_cancel_task(self):
        executor = self.executor(max_workers=1)
        blocked, entered, release = self.gate()
        future = executor.submit(blocked, "eventually")
        self.assertTrue(entered.wait(WAIT))
        with self.assertRaises(TimeoutError):
            future.result(timeout=0)
        self.assertFalse(future.done())
        release.set()
        self.assertEqual(future.result(WAIT), "eventually")

    def test_callbacks_observe_success_failure_and_cancellation_once(self):
        executor = self.executor(max_workers=1)
        blocked, entered, release = self.gate()
        first = executor.submit(blocked, 1)
        self.assertTrue(entered.wait(WAIT))
        second = executor.submit(raise_value_error)
        third = executor.submit(identity, 3)
        calls = []
        events = [threading.Event() for _ in range(3)]
        for i, future in enumerate((first, second, third)):
            future.add_done_callback(lambda done, i=i: (calls.append((i, done)), events[i].set()))
        self.assertTrue(third.cancel())
        release.set()
        self.assertTrue(all(event.wait(WAIT) for event in events))
        self.assertCountEqual(calls, [(0, first), (1, second), (2, third)])
        self.assertEqual(first.result(WAIT), 1)
        self.assertIsInstance(second.exception(WAIT), ValueError)

    def test_exception_and_baseexception_do_not_break_worker(self):
        executor = self.executor(max_workers=1)
        for fn, exception in ((raise_value_error, ValueError), (raise_system_exit, SystemExit),
                              (raise_keyboard_interrupt, KeyboardInterrupt)):
            with self.subTest(exception=exception.__name__):
                future = executor.submit(fn)
                with self.assertRaises(exception):
                    future.result(WAIT)
                self.assertIsInstance(future.exception(WAIT), exception)
        self.assertEqual(executor.submit(identity, 5).result(WAIT), 5)
        executor.shutdown()
        snapshot = self.assert_invariants(executor)
        self.assertEqual(snapshot["counts"]["failed"], 3)
        self.assertEqual(snapshot["counts"]["completed"], 1)
        self.assertEqual(snapshot["profiles"]["default"]["thread"]["failures"], 3)
        self.assertEqual(snapshot["profiles"]["default"]["thread"]["samples"], 1)

    def test_wait_first_completed_and_as_completed(self):
        executor = self.executor(max_workers=2)
        blocked, entered, release = self.gate()
        slow = executor.submit(blocked, "slow")
        self.assertTrue(entered.wait(WAIT))
        quick = executor.submit(identity, "quick")
        self.assertEqual(wait([slow, quick], timeout=WAIT, return_when=FIRST_COMPLETED).done, {quick})
        completions = as_completed([slow, quick], timeout=WAIT)
        self.assertIs(next(completions), quick)
        release.set()
        self.assertIs(next(completions), slow)
        with self.assertRaises(StopIteration):
            next(completions)

    def test_standard_map_multiple_iterables_order_and_exception(self):
        executor = self.executor(max_workers=2)
        self.assertEqual(list(executor.map(add, [1, 2, 3], [10, 20])), [11, 22])
        self.assertEqual(list(executor.map(identity, [])), [])
        values = executor.map(int, ["1", "invalid", "3"])
        self.assertEqual(next(values), 1)
        with self.assertRaises(ValueError):
            next(values)

    def test_standard_map_timeout_is_call_relative(self):
        executor = self.executor(max_workers=1)
        blocked, entered, release = self.gate()
        iterator = executor.map(blocked, [1], timeout=0)
        self.assertTrue(entered.wait(WAIT))
        with self.assertRaises(TimeoutError):
            next(iterator)
        release.set()

    def test_completion_callback_can_reenter_snapshot_and_submit(self):
        executor = self.executor(max_workers=1, max_pending=1, submit_timeout=0)
        blocked, entered, release = self.gate()
        first = executor.submit(blocked, 1)
        self.assertTrue(entered.wait(WAIT))
        finished = threading.Event()
        output, errors = [], []

        def callback(done):
            try:
                self.assertEqual(done.result(), 1)
                self.assertEqual(executor.snapshot()["pending"], 0)
                output.append(executor.submit(identity, 2))
            except BaseException as exc:
                errors.append(exc)
            finally:
                finished.set()

        first.add_done_callback(callback)
        release.set()
        self.assertTrue(finished.wait(WAIT))
        self.assertFalse(errors)
        self.assertEqual(output[0].result(WAIT), 2)
        executor.shutdown()
        self.assertEqual(self.assert_invariants(executor)["counts"]["completed"], 2)

    def test_cancellation_callback_can_reenter_submit(self):
        executor = self.executor(max_workers=1, max_pending=2, submit_timeout=0)
        blocked, entered, release = self.gate()
        executor.submit(blocked)
        self.assertTrue(entered.wait(WAIT))
        queued = executor.submit(identity, 1)
        replacements = []
        queued.add_done_callback(lambda done: replacements.append(executor.submit(identity, 2)))
        self.assertTrue(queued.cancel())
        self.assertEqual(len(replacements), 1)
        release.set()
        self.assertEqual(replacements[0].result(WAIT), 2)
        executor.shutdown()
        snapshot = self.assert_invariants(executor)
        self.assertEqual(snapshot["counts"]["cancelled"], 1)
        self.assertEqual(snapshot["counts"]["submitted"], 3)

    def test_worker_can_request_nonblocking_shutdown(self):
        executor = self.executor(max_workers=1)

        def worker_shutdown():
            executor.shutdown(wait=False)
            return executor.snapshot()["closed"]

        self.assertTrue(executor.submit(worker_shutdown).result(WAIT))
        executor.shutdown(wait=True)
        self.assertEqual(self.assert_invariants(executor)["pending"], 0)
        with self.assertRaises(RuntimeError):
            executor.submit(identity, 2)

    def test_inherited_map_uses_single_call_relative_deadline(self):
        executor = self.executor(max_workers=1)
        blocked, entered, release = self.gate()
        executor.submit(blocked)
        self.assertTrue(entered.wait(WAIT))
        clock = [10.0]
        with patch("concurrent.futures._base.time.monotonic", side_effect=lambda: clock[0]):
            iterator = executor.map(add, [1, 2], [10, 20], timeout=5, chunksize=2)
            clock[0] = 16.0
            with self.assertRaises(TimeoutError):
                next(iterator)
        release.set()
        executor.shutdown()
        self.assertEqual(self.assert_invariants(executor)["counts"]["cancelled"], 2)

    def test_shutdown_is_idempotent_and_rejects_new_submissions(self):
        executor = self.executor()
        executor.submit(identity, 1).result(WAIT)
        executor.shutdown()
        executor.shutdown()
        with self.assertRaises(RuntimeError):
            executor.submit(identity, 2)
        self.assertTrue(self.assert_invariants(executor)["closed"])

    def test_context_manager_waits_even_when_body_raises(self):
        executor = IntelligentExecutor(max_workers=1)
        with self.assertRaisesRegex(RuntimeError, "body failure"):
            with executor:
                future = executor.submit(identity, "finished")
                raise RuntimeError("body failure")
        self.assertEqual(future.result(), "finished")
        self.assertEqual(executor.snapshot()["pending"], 0)
        self.assertTrue(executor.snapshot()["closed"])

    def test_shutdown_cancel_futures_preserves_running_and_cancels_queued(self):
        executor = self.executor(max_workers=1)
        blocked, entered, release = self.gate()
        first = executor.submit(blocked, 1)
        self.assertTrue(entered.wait(WAIT))
        queued = [executor.submit(identity, n) for n in range(5)]
        executor.shutdown(wait=False, cancel_futures=True)
        self.assertTrue(all(future.cancelled() for future in queued))
        self.assertFalse(first.cancelled())
        release.set()
        executor.shutdown(wait=True)
        self.assertEqual(first.result(), 1)
        snapshot = self.assert_invariants(executor)
        self.assertEqual(snapshot["counts"]["cancelled"], 5)
        self.assertEqual(snapshot["pending"], 0)


class BackpressureTests(ExecutorCase):
    def test_zero_timeout_rejects_without_accepting_task(self):
        executor = self.executor(max_workers=1, max_pending=1, submit_timeout=0)
        blocked, entered, release = self.gate()
        first = executor.submit(blocked)
        self.assertTrue(entered.wait(WAIT))
        with self.assertRaises(BackpressureError):
            executor.submit(identity, 2)
        snapshot = self.assert_invariants(executor)
        self.assertEqual(snapshot["counts"]["rejected"], 1)
        self.assertEqual(snapshot["counts"]["submitted"], 1)
        self.assertEqual(snapshot["pending"], 1)
        release.set()
        first.result(WAIT)
        self.settled(executor)
        self.assertEqual(executor.submit(identity, 3).result(WAIT), 3)

    def test_finite_admission_timeout(self):
        executor = self.executor(max_workers=1, max_pending=1, submit_timeout=0.01)
        blocked, entered, release = self.gate()
        executor.submit(blocked)
        self.assertTrue(entered.wait(WAIT))
        with self.assertRaises(BackpressureError):
            executor.submit(identity, "rejected")
        release.set()

    def test_completion_wakes_waiting_producer(self):
        executor = self.executor(max_workers=1, max_pending=1)
        blocked, entered, release = self.gate()
        first = executor.submit(blocked)
        self.assertTrue(entered.wait(WAIT))
        waiting = self.observe_admission_wait(executor)
        output, errors = [], []

        def produce():
            try:
                output.append(executor.submit(identity, 2))
            except BaseException as exc:
                errors.append(exc)

        producer = threading.Thread(target=produce, daemon=True)
        producer.start()
        self.assertTrue(waiting.wait(WAIT))
        self.assertEqual(executor.snapshot()["pending"], 1)
        release.set()
        producer.join(WAIT)
        self.assertFalse(producer.is_alive())
        self.assertFalse(errors)
        self.assertEqual(output[0].result(WAIT), 2)
        first.result(WAIT)
        self.settled(executor)
        self.assertEqual(self.assert_invariants(executor)["counts"]["peak_pending"], 1)

    def test_cancellation_releases_admission_slot(self):
        executor = self.executor(max_workers=1, max_pending=2, submit_timeout=0)
        blocked, entered, release = self.gate()
        executor.submit(blocked)
        self.assertTrue(entered.wait(WAIT))
        queued = executor.submit(identity, 1)
        with self.assertRaises(BackpressureError):
            executor.submit(identity, 2)
        self.assertTrue(queued.cancel())
        replacement = executor.submit(identity, 3)
        release.set()
        self.assertEqual(replacement.result(WAIT), 3)
        executor.shutdown()
        self.assertEqual(self.assert_invariants(executor)["counts"]["cancelled"], 1)

    def test_shutdown_wakes_blocked_producer_with_runtime_error(self):
        executor = self.executor(max_workers=1, max_pending=1)
        blocked, entered, release = self.gate()
        executor.submit(blocked)
        self.assertTrue(entered.wait(WAIT))
        waiting = self.observe_admission_wait(executor)
        outcomes = []

        def produce():
            try:
                outcomes.append(executor.submit(identity, 2))
            except BaseException as exc:
                outcomes.append(exc)

        producer = threading.Thread(target=produce, daemon=True)
        producer.start()
        self.assertTrue(waiting.wait(WAIT))
        executor.shutdown(wait=False)
        producer.join(WAIT)
        self.assertFalse(producer.is_alive(), "shutdown did not wake blocked submit")
        self.assertEqual(len(outcomes), 1)
        self.assertIsInstance(outcomes[0], RuntimeError)
        release.set()
        executor.shutdown()
        self.assertEqual(self.assert_invariants(executor)["counts"]["submitted"], 1)

    def test_concurrent_producers_respect_bound_and_metric_invariants(self):
        executor = self.executor(max_workers=4, max_pending=3)
        barrier = threading.Barrier(9)
        futures, errors = [], []
        guard = threading.Lock()

        def produce(start):
            try:
                barrier.wait(WAIT)
                local = []
                for offset in range(40):
                    local.append(executor.submit(identity, start + offset))
                    self.assert_invariants(executor)
                with guard:
                    futures.extend(local)
            except BaseException as exc:
                with guard:
                    errors.append(exc)

        producers = [threading.Thread(target=produce, args=(index * 40,), daemon=True) for index in range(8)]
        for producer in producers:
            producer.start()
        barrier.wait(WAIT)
        for producer in producers:
            producer.join(WAIT)
        self.assertTrue(all(not producer.is_alive() for producer in producers))
        self.assertEqual(errors, [])
        self.assertCountEqual([future.result(WAIT) for future in futures], range(320))
        executor.shutdown()
        snapshot = self.assert_invariants(executor)
        self.assertEqual(snapshot["counts"]["completed"], 320)
        self.assertEqual(snapshot["pending"], 0)


class InstrumentationTests(ExecutorCase):
    def test_snapshot_is_json_safe_and_isolated(self):
        executor = self.executor()
        executor.submit(identity, 1).result(WAIT)
        self.settled(executor)
        snapshot = executor.snapshot()
        json.dumps(snapshot, allow_nan=False)
        snapshot["counts"]["submitted"] = -100
        snapshot["profiles"]["default"]["thread"]["samples"] = -100
        fresh = self.assert_invariants(executor)
        self.assertEqual(fresh["counts"]["submitted"], 1)
        self.assertEqual(fresh["profiles"]["default"]["thread"]["samples"], 1)

    def test_success_ewma_and_failure_does_not_train_latency(self):
        executor = self.executor(ewma_alpha=0.25)
        clock = [10.0]
        with patch("intelligent_futures.executor.time.perf_counter", side_effect=lambda: clock[0]):
            for start, finish in ((10.0, 14.0), (20.0, 22.0)):
                blocked, entered, release = self.gate()
                clock[0] = start
                future = executor.submit(blocked)
                self.assertTrue(entered.wait(WAIT))
                clock[0] = finish
                release.set()
                future.result(WAIT)
                self.settled(executor)
            clock[0] = 100.0
            future = executor.submit(raise_value_error)
            with self.assertRaises(ValueError):
                future.result(WAIT)
            self.settled(executor)
        profile = executor.snapshot()["profiles"]["default"]["thread"]
        self.assertEqual(profile["samples"], 2)
        self.assertEqual(profile["failures"], 1)
        self.assertEqual(profile["total_seconds"], 6.0)
        self.assertEqual(profile["ewma_seconds"], 3.5)

    def test_profiles_have_a_bounded_lru_and_recent_completion_refreshes_key(self):
        executor = self.executor(max_profile_keys=2)
        for key in ("first", "second", "first", "third"):
            executor.submit_task(identity, 1, hints=TaskHints(key=key)).result(WAIT)
            self.settled(executor)
        snapshot = self.assert_invariants(executor)
        self.assertEqual(set(snapshot["profiles"]), {"first", "third"})
        self.assertEqual(snapshot["profiles"]["first"]["thread"]["samples"], 2)
        self.assertEqual(snapshot["counts"]["profiles_evicted"], 1)

    def test_cancelled_tasks_do_not_add_profiles(self):
        executor = self.executor(max_workers=1)
        blocked, entered, release = self.gate()
        executor.submit_task(blocked, hints=TaskHints(key="running"))
        self.assertTrue(entered.wait(WAIT))
        future = executor.submit_task(identity, 1, hints=TaskHints(key="cancelled"))
        self.assertTrue(future.cancel())
        release.set()
        executor.shutdown()
        self.assertNotIn("cancelled", executor.snapshot()["profiles"])

    def test_policy_receives_deeply_immutable_detached_view(self):
        class RecordingPolicy:
            def __init__(self):
                self.views = []

            def choose(self, hints, profiles, process_available):
                self.views.append(profiles)
                return "thread"

        policy = RecordingPolicy()
        executor = self.executor(policy=policy)
        executor.submit(identity, 1).result(WAIT)
        self.settled(executor)
        executor.submit(identity, 2).result(WAIT)
        self.settled(executor)
        view = policy.views[1]
        with self.assertRaises(TypeError):
            view["new"] = {}
        with self.assertRaises(TypeError):
            view["default"]["thread"] = None
        with self.assertRaises(FrozenInstanceError):
            view["default"]["thread"].samples = -1
        self.assertEqual(view["default"]["thread"].samples, 1)
        self.assertEqual(executor.snapshot()["profiles"]["default"]["thread"]["samples"], 2)
        self.assertEqual(dict(policy.views[0]), {})

    def test_policy_exception_does_not_leak_admission_or_metrics(self):
        class OnceFailingPolicy:
            def __init__(self):
                self.calls = 0

            def choose(self, *args):
                self.calls += 1
                if self.calls == 1:
                    raise LookupError("policy failed")
                return "thread"

        executor = self.executor(policy=OnceFailingPolicy(), max_pending=1, submit_timeout=0)
        with self.assertRaises(LookupError):
            executor.submit(identity, 1)
        self.assertEqual(executor.submit(identity, 2).result(WAIT), 2)
        executor.shutdown()
        self.assertEqual(self.assert_invariants(executor)["counts"]["submitted"], 1)

    def test_backend_submit_failure_does_not_consume_admission(self):
        executor = self.executor(max_workers=1, max_pending=1, submit_timeout=0)
        with patch.object(executor._threads, "submit", side_effect=RuntimeError("backend unavailable")):
            with self.assertRaisesRegex(RuntimeError, "backend unavailable"):
                executor.submit(identity, 1)
        self.assertEqual(executor.snapshot()["pending"], 0)
        self.assertEqual(executor.snapshot()["counts"]["submitted"], 0)
        self.assertEqual(executor.submit(identity, 2).result(WAIT), 2)
        executor.shutdown()
        self.assertEqual(self.assert_invariants(executor)["counts"]["completed"], 1)

    def test_invalid_policy_output_does_not_accept_task(self):
        class InvalidPolicy:
            def choose(self, *args):
                return "gpu"

        executor = self.executor(policy=InvalidPolicy())
        with self.assertRaises(ValueError):
            executor.submit(identity, 1)
        self.assertEqual(self.assert_invariants(executor)["counts"]["submitted"], 0)


class BoundedMapTests(ExecutorCase):
    def test_order_and_bounded_source_consumption(self):
        executor = self.executor(max_workers=2)
        consumed = []

        def source():
            for item in range(8):
                consumed.append(item)
                yield item

        iterator = executor.map_bounded(identity, source(), buffersize=3)
        self.assertEqual(consumed, [])
        self.assertEqual(next(iterator), 0)
        self.assertEqual(consumed, [0, 1, 2])
        self.assertEqual(next(iterator), 1)
        self.assertEqual(consumed, [0, 1, 2, 3])
        self.assertEqual(list(iterator), list(range(2, 8)))

    def test_hints_are_forwarded_to_every_streamed_task(self):
        class RecordingPolicy:
            def __init__(self):
                self.hints = []

            def choose(self, hints, profiles, process_available):
                self.hints.append(hints)
                return "thread"

        policy = RecordingPolicy()
        executor = self.executor(policy=policy)
        hints = TaskHints(kind="io", key="streamed")
        self.assertEqual(list(executor.map_bounded(identity, range(5), buffersize=2, hints=hints)), list(range(5)))
        self.assertEqual(policy.hints, [hints] * 5)
        executor.shutdown()
        self.assertEqual(executor.snapshot()["profiles"]["streamed"]["thread"]["samples"], 5)

    def test_single_buffer_and_empty_iterable(self):
        executor = self.executor()
        self.assertEqual(list(executor.map_bounded(identity, [], buffersize=1)), [])
        self.assertEqual(list(executor.map_bounded(identity, range(5), buffersize=1)), list(range(5)))

    def test_close_cancels_unstarted_buffered_tasks(self):
        executor = self.executor(max_workers=1)
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        executed = []

        def task(value):
            executed.append(value)
            if value == 1:
                entered.set()
                if not release.wait(WAIT):
                    raise AssertionError("test map gate timed out")
            return value

        iterator = executor.map_bounded(task, range(6), buffersize=4)
        self.assertEqual(next(iterator), 0)
        self.assertTrue(entered.wait(WAIT))
        iterator.close()
        release.set()
        executor.shutdown()
        self.assertEqual(executed, [0, 1])
        snapshot = self.assert_invariants(executor)
        self.assertEqual(snapshot["counts"]["cancelled"], 2)
        self.assertEqual(snapshot["counts"]["submitted"], 4)

    def test_task_exception_cancels_remaining_buffer(self):
        executor = self.executor(max_workers=1)
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)

        def task(value):
            if value == 0:
                raise ValueError("map task failed")
            entered.set()
            if not release.wait(WAIT):
                raise AssertionError("test map gate timed out")
            return value

        iterator = executor.map_bounded(task, range(6), buffersize=4)
        with self.assertRaisesRegex(ValueError, "map task failed"):
            next(iterator)
        release.set()
        executor.shutdown()
        snapshot = self.assert_invariants(executor)
        self.assertEqual(snapshot["counts"]["failed"], 1)
        self.assertGreaterEqual(snapshot["counts"]["cancelled"], 2)

    def test_source_exception_cancels_already_buffered_tasks(self):
        executor = self.executor(max_workers=1)
        blocked, entered, release = self.gate()
        executor.submit(blocked)
        self.assertTrue(entered.wait(WAIT))
        executed = threading.Event()

        def source():
            yield 1
            yield 2
            raise LookupError("source failed")

        iterator = executor.map_bounded(lambda value: executed.set(), source(), buffersize=4)
        with self.assertRaisesRegex(LookupError, "source failed"):
            next(iterator)
        release.set()
        executor.shutdown()
        self.assertFalse(executed.is_set())
        self.assertEqual(self.assert_invariants(executor)["counts"]["cancelled"], 2)

    def test_timeout_deadline_is_set_before_iteration_starts(self):
        executor = self.executor(max_workers=1)
        blocked, entered, release = self.gate()
        executor.submit(blocked)
        self.assertTrue(entered.wait(WAIT))
        clock = [10.0]
        with patch("intelligent_futures.executor.time.monotonic", side_effect=lambda: clock[0]):
            iterator = executor.map_bounded(identity, range(3), buffersize=3, timeout=5)
            clock[0] = 16.0
            with self.assertRaises(TimeoutError):
                next(iterator)
        release.set()
        executor.shutdown()
        self.assertEqual(self.assert_invariants(executor)["counts"]["cancelled"], 3)

    def test_timeout_uses_call_time_and_cancels_unstarted_buffer(self):
        executor = self.executor(max_workers=1)
        blocked, entered, release = self.gate()
        executor.submit(blocked)
        self.assertTrue(entered.wait(WAIT))
        iterator = executor.map_bounded(identity, range(3), buffersize=3, timeout=0)
        with self.assertRaises(TimeoutError):
            next(iterator)
        release.set()
        executor.shutdown()
        self.assertEqual(self.assert_invariants(executor)["counts"]["cancelled"], 3)

    def test_admission_failure_cancels_already_buffered_tasks(self):
        executor = self.executor(max_workers=1, max_pending=3, submit_timeout=0)
        blocked, entered, release = self.gate()
        executor.submit(blocked)
        self.assertTrue(entered.wait(WAIT))
        iterator = executor.map_bounded(identity, range(5), buffersize=4)
        with self.assertRaises(BackpressureError):
            next(iterator)
        self.assertEqual(executor.snapshot()["counts"]["cancelled"], 2)
        release.set()
        executor.shutdown()
        self.assertEqual(self.assert_invariants(executor)["counts"]["rejected"], 1)

    def test_close_before_first_next_consumes_nothing(self):
        executor = self.executor()
        consumed = []

        def source():
            consumed.append(1)
            yield 1

        iterator = executor.map_bounded(identity, source())
        iterator.close()
        self.assertEqual(consumed, [])
        self.assertEqual(executor.snapshot()["counts"]["submitted"], 0)


class ProcessTests(ExecutorCase):
    def test_spawn_executes_top_level_function_in_another_process(self):
        executor = self.executor(process_workers=1, policy="hints", mp_context=multiprocessing.get_context("spawn"))
        future = executor.submit_task(process_identity, "payload", hints=TaskHints(kind="cpu", process_safe=True))
        pid, payload = future.result(WAIT)
        self.assertNotEqual(pid, os.getpid())
        self.assertEqual(payload, "payload")
        executor.shutdown()
        snapshot = self.assert_invariants(executor)
        self.assertEqual(snapshot["counts"]["process_submitted"], 1)
        self.assertEqual(snapshot["profiles"]["default"]["process"]["samples"], 1)

    def process_gate_fixture(self):
        context = multiprocessing.get_context("spawn")
        temporary = tempfile.TemporaryDirectory(prefix="intelligent-futures-test-")
        self.addCleanup(temporary.cleanup)
        executor = self.executor(process_workers=1, policy="hints", mp_context=context)
        entered = FileEvent(Path(temporary.name) / "entered")
        release = FileEvent(Path(temporary.name) / "release")
        self.addCleanup(release.set)
        future = executor.submit_task(process_gate, entered, release, "finished",
                                      hints=TaskHints(kind="cpu", process_safe=True))
        self.assertTrue(entered.wait(WAIT))
        self.assertTrue(future.running())
        self.assertFalse(future.cancel())
        return executor, future, release

    def test_running_process_shutdown_waits_without_prior_result(self):
        executor, future, release = self.process_gate_fixture()
        started, finished = threading.Event(), threading.Event()
        errors = []

        def shutdown():
            started.set()
            try:
                executor.shutdown(wait=True)
            except BaseException as exc:
                errors.append(exc)
            finally:
                finished.set()

        waiter = threading.Thread(target=shutdown, daemon=True)
        waiter.start()
        self.assertTrue(started.wait(WAIT))
        self.assertFalse(finished.is_set())
        release.set()
        self.assertTrue(finished.wait(WAIT))
        waiter.join(WAIT)
        self.assertFalse(errors)
        self.assertEqual(future.result(WAIT), "finished")
        self.assertEqual(self.assert_invariants(executor)["pending"], 0)
        self.assertEqual(executor.snapshot()["counts"]["completed"], 1)

    def test_process_wait_false_then_true_waits_for_final_metrics(self):
        executor, future, release = self.process_gate_fixture()
        executor.shutdown(wait=False)
        self.assertFalse(future.done())
        self.assertEqual(executor.snapshot()["pending"], 1)
        waiting = self.observe_admission_wait(executor)
        finished = threading.Event()
        errors = []

        def shutdown():
            try:
                executor.shutdown(wait=True)
            except BaseException as exc:
                errors.append(exc)
            finally:
                finished.set()

        waiter = threading.Thread(target=shutdown, daemon=True)
        waiter.start()
        self.assertTrue(waiting.wait(WAIT))
        self.assertFalse(finished.is_set())
        release.set()
        self.assertTrue(finished.wait(WAIT))
        waiter.join(WAIT)
        self.assertFalse(errors)
        self.assertEqual(future.result(WAIT), "finished")
        self.assertEqual(self.assert_invariants(executor)["pending"], 0)

    def test_repeated_shutdown_preserves_process_cancellation_request(self):
        executor, first, release = self.process_gate_fixture()
        hints = TaskHints(kind="cpu", process_safe=True)
        queued = [executor.submit_task(identity, i, hints=hints) for i in range(12)]
        executor.shutdown(wait=False, cancel_futures=True)
        executor.shutdown(wait=False, cancel_futures=False)
        release.set()
        executor.shutdown(wait=True, cancel_futures=False)
        self.assertEqual(first.result(WAIT), "finished")
        self.assertTrue(all(future.done() for future in queued))
        self.assertTrue(any(future.cancelled() for future in queued))
        snapshot = self.assert_invariants(executor)
        self.assertEqual(snapshot["pending"], 0)
        self.assertEqual(snapshot["counts"]["cancelled"], sum(future.cancelled() for future in queued))

    def test_no_process_consent_keeps_cpu_task_in_threads(self):
        executor = self.executor(process_workers=1, policy="hints")
        pid, _ = executor.submit_task(process_identity, 1, hints=TaskHints(kind="cpu")).result(WAIT)
        self.assertEqual(pid, os.getpid())
        executor.shutdown()
        self.assertEqual(self.assert_invariants(executor)["counts"]["process_submitted"], 0)

    def test_disabled_process_pool_falls_back_to_threads(self):
        executor = self.executor(process_workers=0, policy="hints")
        pid, _ = executor.submit_task(process_identity, 1, hints=TaskHints(kind="cpu", process_safe=True)).result(WAIT)
        self.assertEqual(pid, os.getpid())

    def test_standard_submit_never_grants_implicit_process_consent(self):
        executor = self.executor(process_workers=1, policy="adaptive")
        pid, _ = executor.submit(process_identity, 1).result(WAIT)
        self.assertEqual(pid, os.getpid())

    def test_custom_policy_cannot_bypass_process_consent(self):
        class AlwaysProcess:
            def choose(self, *args):
                return "process"

        for workers, hints in ((0, TaskHints(process_safe=True)), (1, TaskHints())):
            with self.subTest(process_workers=workers):
                executor = self.executor(process_workers=workers, policy=AlwaysProcess())
                with self.assertRaises(ValueError):
                    executor.submit_task(identity, 1, hints=hints)
                self.assertEqual(self.assert_invariants(executor)["counts"]["submitted"], 0)

    def test_pickle_failure_is_future_exception_and_pool_remains_usable(self):
        executor = self.executor(process_workers=1, policy="hints")
        hints = TaskHints(kind="cpu", process_safe=True)
        future = executor.submit_task(lambda: "not pickleable", hints=hints)
        self.assertIs(type(future), Future)
        # Python 3.14 reports local-function serialization as PicklingError.
        with self.assertRaises((AttributeError, TypeError, pickle.PicklingError)):
            future.result(WAIT)
        self.assertEqual(executor.submit_task(identity, "healthy", hints=hints).result(WAIT), "healthy")
        executor.shutdown()
        snapshot = self.assert_invariants(executor)
        self.assertEqual(snapshot["counts"]["failed"], 1)
        self.assertEqual(snapshot["counts"]["completed"], 1)
        self.assertEqual(snapshot["profiles"]["default"]["process"]["samples"], 1)

    def test_unpickleable_argument_and_result_fail_without_thread_fallback(self):
        executor = self.executor(process_workers=1, policy="hints")
        hints = TaskHints(kind="cpu", process_safe=True)
        for fn, args in ((identity, (threading.Lock(),)), (unpickleable_result, ())):
            with self.subTest(fn=fn.__name__), self.assertRaises(TypeError):
                executor.submit_task(fn, *args, hints=hints).result(WAIT)
        self.assertEqual(executor.submit_task(identity, "still healthy", hints=hints).result(WAIT), "still healthy")
        executor.shutdown()
        snapshot = self.assert_invariants(executor)
        self.assertEqual(snapshot["counts"]["thread_submitted"], 0)
        self.assertEqual(snapshot["counts"]["failed"], 2)
        self.assertEqual(snapshot["counts"]["completed"], 1)

    def test_abrupt_worker_exit_fails_future_and_rejects_later_process_work(self):
        executor = self.executor(process_workers=1, policy="hints")
        hints = TaskHints(kind="cpu", process_safe=True)
        future = executor.submit_task(exit_worker, hints=hints)
        with self.assertRaises(BrokenProcessPool):
            future.result(WAIT)
        self.settled(executor)
        with self.assertRaises(BrokenProcessPool):
            executor.submit_task(identity, 1, hints=hints)
        # A broken process pool must not replay work in the thread backend.
        snapshot = self.assert_invariants(executor)
        self.assertEqual(snapshot["counts"]["process_submitted"], 1)
        self.assertEqual(snapshot["counts"]["thread_submitted"], 0)
        self.assertEqual(snapshot["counts"]["failed"], 1)
        self.assertEqual(executor.submit(identity, "threads still healthy").result(WAIT), "threads still healthy")

    def test_process_exceptions_and_baseexception_preserve_pool(self):
        executor = self.executor(process_workers=1, policy="hints")
        hints = TaskHints(kind="cpu", process_safe=True)
        for fn, exception in ((raise_value_error, ValueError), (raise_system_exit, SystemExit)):
            with self.subTest(exception=exception.__name__):
                with self.assertRaises(exception):
                    executor.submit_task(fn, hints=hints).result(WAIT)
        self.assertEqual(executor.submit_task(identity, 7, hints=hints).result(WAIT), 7)
        executor.shutdown()
        self.assertEqual(self.assert_invariants(executor)["counts"]["failed"], 2)


class ValidationTests(ExecutorCase):
    def test_integer_bounds(self):
        for name, values in {
            "max_workers": [0, -1, True, 1.5, "2", None],
            "process_workers": [-1, True, 1.5, "2", None],
            "max_pending": [0, -1, True, 1.5, "2", None],
            "max_profile_keys": [0, -1, True, 1.5, "2", None],
        }.items():
            for value in values:
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    IntelligentExecutor(**{name: value})

    def test_finite_nonnegative_admission_timeout(self):
        for value in (-1, float("inf"), float("-inf"), float("nan")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                IntelligentExecutor(submit_timeout=value)

    def test_alpha_bounds(self):
        for value in (0, -0.1, 1.1, float("inf"), float("nan")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                IntelligentExecutor(ewma_alpha=value)

    def test_policy_must_exist_and_implement_choose(self):
        with self.assertRaises(ValueError):
            IntelligentExecutor(policy="missing")
        for invalid in (None, object(), 1):
            with self.subTest(policy=invalid), self.assertRaises(TypeError):
                IntelligentExecutor(policy=invalid)

    def test_callable_and_task_hints_validation(self):
        executor = self.executor()
        with self.assertRaises(TypeError):
            executor.submit(123)
        with self.assertRaises(TypeError):
            executor.submit_task(identity, 1, hints={"kind": "cpu"})
        self.assertEqual(executor.snapshot()["counts"]["submitted"], 0)

    def test_bounded_map_buffersize_validation(self):
        executor = self.executor()
        for value in (0, -1, True, 1.5, "2", None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                executor.map_bounded(identity, [], buffersize=value)


if __name__ == "__main__":
    unittest.main()
