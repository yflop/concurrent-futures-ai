"""Owned-queue and physical-dispatch invariants, with bounded test barriers."""
from concurrent.futures import Future, as_completed, wait
import gc
import os
import subprocess
import sys
from pathlib import Path
from queue import SimpleQueue
import tempfile
import threading
import unittest
from unittest.mock import patch
import weakref

from intelligent_futures import BackpressureError, IntelligentExecutor, TaskHints
from tests.test_executor import FileEvent, process_gate, process_identity

WAIT = 10


def identity(value):
    return value


class Payload:
    def __init__(self):
        self.data = bytearray(4096)

    def __call__(self):
        return len(self.data)


class PhysicalDispatchTests(unittest.TestCase):
    def executor(self, **kwargs):
        executor = IntelligentExecutor(**kwargs)
        self.addCleanup(executor.shutdown, wait=True, cancel_futures=True)
        return executor

    def blocker(self, executor):
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)

        def blocked():
            entered.set()
            if not release.wait(WAIT):
                raise AssertionError("test barrier not released")
            return "finished"

        future = executor.submit(blocked)
        self.assertTrue(entered.wait(WAIT))
        return future, release

    def assert_bounds(self, executor):
        snapshot = executor.snapshot()
        physical = snapshot["physical"]
        self.assertEqual(physical["retained"], sum(physical["queued"].values()) +
                         sum(physical["in_flight"].values()))
        self.assertLessEqual(physical["retained"], physical["max_retained"])
        self.assertLessEqual(physical["peak_retained"], snapshot["max_pending"])
        self.assertGreaterEqual(physical["retained"], 0)
        for backend, budget in physical["dispatch_limits"].items():
            self.assertLessEqual(physical["in_flight"][backend], budget)
            self.assertLessEqual(physical["peak_in_flight"][backend], budget)
        counts = snapshot["counts"]
        self.assertEqual(counts["submitted"], snapshot["pending"] +
                         sum(counts[k] for k in ("completed", "failed", "cancelled")))
        return snapshot

    def test_cancel_churn_removes_payload_and_never_submits_tombstones(self):
        executor = self.executor(max_workers=1, max_pending=2, submit_timeout=0)
        with patch.object(executor._threads, "submit", wraps=executor._threads.submit) as submit:
            first, release = self.blocker(executor)
            refs, cancelled = [], []
            for _ in range(1000):
                argument, callable_payload = Payload(), Payload()
                refs.extend((weakref.ref(argument), weakref.ref(callable_payload)))
                future = executor.submit(identity, argument)
                self.assertTrue(future.cancel())
                # Keep the caller-owned Future: cancellation must release its
                # executor-owned payload even before the Future is collected.
                cancelled.append(future)
                future = executor.submit(callable_payload)
                self.assertTrue(future.cancel())
                cancelled.append(future)
                del argument, callable_payload
                if len(cancelled) % 100 == 0:
                    state = self.assert_bounds(executor)
                    self.assertEqual(state["physical"]["retained"], 1)
                    self.assertEqual(state["physical"]["queued"]["thread"], 0)
            gc.collect()
            self.assertTrue(all(ref() is None for ref in refs))
            self.assertEqual(submit.call_count, 1)
            self.assertEqual(wait(cancelled, timeout=0).not_done, set())
            self.assertEqual(len(list(as_completed(cancelled, timeout=0))), 2000)
            release.set()
            self.assertEqual(first.result(WAIT), "finished")
        executor.shutdown()
        state = self.assert_bounds(executor)
        self.assertEqual(state["physical"]["retained"], 0)
        self.assertEqual(state["counts"]["cancelled"], 2000)

    def test_dispatched_cancellation_keeps_physical_slot_until_acknowledged(self):
        executor = self.executor(max_workers=1, max_pending=1, submit_timeout=0)
        raw = Future()
        with patch.object(executor._threads, "submit", return_value=raw):
            future = executor.submit(identity, "not run")
        try:
            self.assertTrue(future.cancel())
            state = self.assert_bounds(executor)
            self.assertEqual(state["pending"], 0)
            self.assertEqual(state["physical"]["retained"], 1)
            self.assertEqual(wait([future], timeout=0).done, {future})
            with self.assertRaises(BackpressureError):
                executor.submit(identity, "not admitted")
        finally:
            raw.set_result(None)
        with executor._condition:
            self.assertTrue(executor._condition.wait_for(lambda: executor._retained() == 0, WAIT))
        self.assertEqual(executor.submit(identity, "replacement").result(WAIT), "replacement")
        executor.shutdown()
        self.assertEqual(self.assert_bounds(executor)["physical"]["retained"], 0)

    def test_physical_acknowledgement_wakes_blocked_producer(self):
        executor = self.executor(max_workers=1, max_pending=1)
        raw, waiting = Future(), threading.Event()
        with patch.object(executor._threads, "submit", return_value=raw):
            cancelled = executor.submit(identity, 1)
        cancelled.cancel()
        outcomes, errors = [], []
        original_wait = executor._condition.wait

        def observed_wait(timeout=None):
            waiting.set()
            return original_wait(timeout)

        def produce():
            try:
                outcomes.append(executor.submit(identity, 2))
            except BaseException as error:
                errors.append(error)

        with patch.object(executor._condition, "wait", side_effect=observed_wait):
            producer = threading.Thread(target=produce, daemon=True)
            producer.start()
            try:
                self.assertTrue(waiting.wait(WAIT))
                self.assertEqual(outcomes, [])
            finally:
                raw.set_result(None)
            producer.join(WAIT)
        self.assertFalse(producer.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(outcomes[0].result(WAIT), 2)

    def test_predictive_custom_policy_chooses_once_per_submission_despite_wakes(self):
        class CountingPolicy:
            def __init__(self):
                self.calls = 0

            def choose(self, *_args):
                self.calls += 1
                return "thread"

        policy = CountingPolicy()
        executor = self.executor(max_workers=1, max_pending=2, policy=policy, predictive=True)
        _, release = self.blocker(executor)
        waits = []
        waiting = threading.Event()
        original_wait = executor._condition.wait
        output = []

        def observed_wait(timeout=None):
            waits.append(1)
            waiting.set()
            return original_wait(timeout)

        with patch.object(executor._condition, "wait", side_effect=observed_wait):
            producer = threading.Thread(target=lambda: output.append(executor.submit(identity, 2)), daemon=True)
            producer.start()
            try:
                self.assertTrue(waiting.wait(WAIT))
                with executor._condition:
                    waiting.clear()
                    executor._condition.notify_all()
                self.assertTrue(waiting.wait(WAIT))
                self.assertGreaterEqual(len(waits), 2)
                self.assertEqual(policy.calls, 2)
            finally:
                release.set()
            producer.join(WAIT)
        self.assertFalse(producer.is_alive())
        self.assertEqual(output[0].result(WAIT), 2)
        self.assertEqual(policy.calls, 2)

    def test_max_pending_smaller_than_worker_budget_is_still_physical_limit(self):
        executor = self.executor(max_workers=8, process_workers=4,
                                 max_pending=1, submit_timeout=0)
        _, release = self.blocker(executor)
        with self.assertRaises(BackpressureError):
            executor.submit(identity, 1)
        state = self.assert_bounds(executor)
        self.assertEqual(state["physical"]["retained"], 1)
        release.set()
        executor.shutdown()
        self.assertEqual(self.assert_bounds(executor)["physical"]["peak_retained"], 1)

    def test_cancel_races_dispatch_without_false_cancellation_or_leaks(self):
        executor = self.executor(max_workers=1, max_pending=2)
        cancelled_calls = []
        for _ in range(100):
            _, release = self.blocker(executor)
            called = threading.Event()
            candidate = executor.submit(called.set)
            barrier = threading.Barrier(2, timeout=WAIT)
            outcome = []

            def cancel():
                barrier.wait()
                outcome.append(candidate.cancel())

            canceller = threading.Thread(target=cancel)
            canceller.start()
            barrier.wait()
            release.set()
            canceller.join(WAIT)
            self.assertFalse(canceller.is_alive())
            self.assertEqual(len(outcome), 1)
            self.assertEqual(wait([candidate], timeout=WAIT).done, {candidate})
            if outcome[0]:
                cancelled_calls.append(called)
                self.assertFalse(called.is_set())
                self.assertTrue(candidate.cancelled())
            else:
                candidate.result(WAIT)
                self.assertTrue(called.is_set())
            self.assert_bounds(executor)
        executor.shutdown()
        self.assertTrue(all(not called.is_set() for called in cancelled_calls))
        self.assertEqual(self.assert_bounds(executor)["physical"]["retained"], 0)

    def test_shutdown_nonblocking_drains_owned_queue_then_joins_same_coordinator(self):
        executor = self.executor(max_workers=1, max_pending=16)
        first, release = self.blocker(executor)
        seen = []
        queued = [executor.submit(seen.append, value) for value in range(10)]
        self.assertEqual(self.assert_bounds(executor)["physical"]["queued"]["thread"], 10)
        executor.shutdown(wait=False)
        coordinator = executor._shutdown_thread
        executor.shutdown(wait=False)
        self.assertIs(executor._shutdown_thread, coordinator)
        self.assertTrue(coordinator.is_alive())
        self.assertFalse(any(f.done() for f in queued))
        release.set()
        executor.shutdown(wait=True)
        self.assertFalse(coordinator.is_alive())
        self.assertTrue(all(f.done() and not f.cancelled() for f in queued))
        self.assertEqual(seen, list(range(10)))
        self.assertEqual(first.result(), "finished")
        self.assertEqual(self.assert_bounds(executor)["physical"]["retained"], 0)

    def test_shutdown_cancellation_has_immediate_wait_visibility(self):
        executor = self.executor(max_workers=1, max_pending=8)
        first, release = self.blocker(executor)
        queued = [executor.submit(identity, value) for value in range(6)]
        executor.shutdown(wait=False, cancel_futures=True)
        self.assertEqual(wait(queued, timeout=0).done, set(queued))
        self.assertEqual(set(as_completed(queued, timeout=0)), set(queued))
        self.assertEqual(self.assert_bounds(executor)["physical"]["retained"], 1)
        release.set()
        executor.shutdown(wait=True)
        self.assertEqual(first.result(), "finished")

    def test_shutdown_cancel_callback_can_snapshot_from_another_thread(self):
        executor = self.executor(max_workers=1, max_pending=2)
        _, release = self.blocker(executor)
        future = executor.submit(identity, 1)
        observed, errors = [], []

        def callback(_future):
            reader = threading.Thread(target=lambda: observed.append(executor.snapshot()))
            reader.start()
            reader.join(WAIT)
            if reader.is_alive():
                errors.append("callback invoked under scheduler lock")

        future.add_done_callback(callback)
        executor.shutdown(wait=False, cancel_futures=True)
        self.assertEqual(errors, [])
        self.assertEqual(len(observed), 1)
        release.set()

    def test_waiting_shutdown_from_own_task_raises_instead_of_deadlocking(self):
        executor = self.executor(max_workers=1)
        future = executor.submit(executor.shutdown, True)
        with self.assertRaisesRegex(RuntimeError, "worker or completion callback"):
            future.result(WAIT)
        self.assertFalse(executor.snapshot()["closed"])
        self.assertEqual(executor.submit(identity, "still open").result(WAIT), "still open")

    def test_waiting_shutdown_from_completion_callback_is_rejected(self):
        executor = self.executor(max_workers=1)
        first, release = self.blocker(executor)
        done, errors = threading.Event(), []

        def close(_future):
            try:
                executor.shutdown(wait=True)
            except RuntimeError as error:
                errors.append(str(error))
            finally:
                done.set()

        first.add_done_callback(close)
        release.set()
        self.assertTrue(done.wait(WAIT))
        self.assertEqual(len(errors), 1)
        executor.shutdown()
        self.assertEqual(self.assert_bounds(executor)["physical"]["retained"], 0)

    def test_deferred_backend_failure_settles_accepted_future_without_replay(self):
        executor = self.executor(max_workers=1, max_pending=3)
        _, release = self.blocker(executor)
        called = threading.Event()
        queued = executor.submit(called.set)
        with patch.object(executor._threads, "submit", side_effect=RuntimeError("backend stopped")):
            release.set()
            with self.assertRaisesRegex(RuntimeError, "backend stopped"):
                queued.result(WAIT)
        self.assertFalse(called.is_set())
        executor.shutdown()
        state = self.assert_bounds(executor)
        self.assertEqual(state["counts"]["failed"], 1)
        self.assertEqual(state["physical"]["retained"], 0)

    def test_executor_shutdown_and_cancellation_do_not_affect_another_executor(self):
        left = self.executor(max_workers=1, max_pending=2)
        right = self.executor(max_workers=1, max_pending=2)
        _, left_release = self.blocker(left)
        _, right_release = self.blocker(right)
        left_pending = left.submit(identity, "left")
        right_pending = right.submit(identity, "right")
        left.shutdown(wait=False, cancel_futures=True)
        self.assertTrue(left_pending.cancelled())
        self.assertFalse(right_pending.done())
        self.assertFalse(right.snapshot()["closed"])
        self.assertEqual(self.assert_bounds(right)["physical"]["retained"], 2)
        left_release.set()
        right_release.set()
        self.assertEqual(right_pending.result(WAIT), "right")
        left.shutdown()
        self.assertEqual(right.submit(identity, 3).result(WAIT), 3)

    def test_process_churn_does_not_submit_transport_tombstones(self):
        executor = self.executor(max_workers=1, process_workers=1, policy="hints",
                                 max_pending=2, submit_timeout=0)
        hints = TaskHints(kind="cpu", process_safe=True)
        with tempfile.TemporaryDirectory() as directory:
            entered = FileEvent(Path(directory) / "entered")
            release = FileEvent(Path(directory) / "release")
            with patch.object(executor._processes, "submit", wraps=executor._processes.submit) as submit:
                first = executor.submit_task(process_gate, entered, release, "process", hints=hints)
                try:
                    self.assertTrue(entered.wait(WAIT))
                    self.assertTrue(first.running())
                    self.assertFalse(first.cancel())
                    for value in range(500):
                        future = executor.submit_task(identity, value, hints=hints)
                        self.assertTrue(future.cancel())
                        self.assert_bounds(executor)
                    self.assertEqual(submit.call_count, 1)
                    self.assertEqual(executor.snapshot()["physical"]["queued"]["process"], 0)
                finally:
                    release.set()
                    executor.shutdown()
                self.assertEqual(first.result(), "process")
        self.assertEqual(self.assert_bounds(executor)["physical"]["retained"], 0)

    def test_omitted_shutdown_exits_without_hanging_and_settles_undispatched_futures(self):
        # CPython closes stdlib pools before ordinary atexit callbacks. The
        # owned queue cannot promise dispatch during teardown, but no Future
        # should silently remain pending or block interpreter termination.
        script = r'''
import atexit, threading, time
from intelligent_futures import IntelligentExecutor
entered = threading.Event()
def running():
    entered.set()
    time.sleep(0.1)
    return "finished"
executor = IntelligentExecutor(max_workers=1, max_pending=8)
first = executor.submit(running)
assert entered.wait(5)
queued = [executor.submit(str, value) for value in range(5)]
def verify():
    assert first.result() == "finished"
    assert all(future.done() for future in queued)
    for future in queued:
        try:
            future.result()
        except RuntimeError:
            pass
    state = executor.snapshot()
    assert state["pending"] == state["physical"]["retained"] == 0
    print("settled")
atexit.register(verify)
'''
        completed = subprocess.run([sys.executable, "-c", script],
            capture_output=True, text=True, timeout=WAIT, check=True)
        self.assertEqual(completed.stdout.strip(), "settled")
        self.assertEqual(completed.stderr, "")

    def test_idle_completion_pump_does_not_retain_previous_result_payload(self):
        executor = self.executor(max_workers=2, max_pending=2)
        waiting_again = threading.Event()

        class ObservedQueue:
            def __init__(self):
                self.queue = SimpleQueue()
                self.gets = 0

            def put(self, value):
                self.queue.put(value)

            def get(self):
                self.gets += 1
                if self.gets >= 2:
                    waiting_again.set()
                return self.queue.get()

        executor._completions = ObservedQueue()
        _, release = self.blocker(executor)
        future = executor.submit(Payload)
        payload = future.result(WAIT)
        reference = weakref.ref(payload)
        del payload, future
        self.assertTrue(waiting_again.wait(WAIT))
        gc.collect()
        self.assertIsNone(reference())
        self.assertEqual(executor.snapshot()["pending"], 1)
        release.set()

    def test_cancelling_payload_runs_finalizers_outside_scheduler_lock(self):
        executor = self.executor(max_workers=1, max_pending=2)
        _, release = self.blocker(executor)
        errors, finalized = [], threading.Event()

        class Finalizer:
            def __del__(self):
                reader = threading.Thread(target=executor.snapshot, daemon=True)
                reader.start()
                reader.join(WAIT)
                if reader.is_alive():
                    errors.append("payload finalizer ran under scheduler lock")
                finalized.set()

        payload = Finalizer()
        future = executor.submit(identity, payload)
        del payload
        self.assertTrue(future.cancel())
        self.assertTrue(finalized.wait(WAIT))
        self.assertEqual(errors, [])
        release.set()

    def test_shutdown_waits_for_public_completion_callbacks_on_owned_pump(self):
        executor = self.executor(max_workers=1)
        first, release = self.blocker(executor)
        entered, callback_release = threading.Event(), threading.Event()
        self.addCleanup(callback_release.set)

        def callback(_future):
            entered.set()
            callback_release.wait(WAIT)

        first.add_done_callback(callback)
        release.set()
        self.assertTrue(entered.wait(WAIT))
        self.assertEqual(first.result(WAIT), "finished")
        executor.shutdown(wait=False)
        self.assertTrue(executor._shutdown_thread.is_alive())
        self.assertEqual(executor.snapshot()["pending"], 0)
        callback_release.set()
        executor.shutdown(wait=True)
        self.assertFalse(executor._shutdown_thread.is_alive())
        self.assertFalse(executor._completion_threads)

    def test_completion_pump_retires_between_idle_epochs_without_thread_leaks(self):
        executor = self.executor(max_workers=1)
        for value in range(30):
            self.assertEqual(executor.submit(identity, value).result(WAIT), value)
            with executor._condition:
                self.assertTrue(executor._condition.wait_for(
                    lambda: not executor._completion_threads, WAIT))
                self.assertFalse(executor._completion_active)
        executor.shutdown()
        self.assertFalse(executor._completion_threads)

    def test_process_crash_with_owned_queue_fails_all_futures_without_deadlock(self):
        # A real broken ProcessPool invokes raw Future callbacks while holding
        # a non-reentrant backend lock. Keep this deadlock regression isolated.
        script = r'''
from pathlib import Path
import tempfile
from concurrent.futures import wait
from concurrent.futures.process import BrokenProcessPool
from intelligent_futures import IntelligentExecutor, TaskHints
from tests.test_executor import FileEvent, process_gate, exit_worker, identity
with tempfile.TemporaryDirectory() as directory:
    entered = FileEvent(Path(directory) / "entered")
    release = FileEvent(Path(directory) / "release")
    executor = IntelligentExecutor(max_workers=1, process_workers=1, policy="hints", max_pending=12)
    hints = TaskHints(kind="cpu", process_safe=True)
    first = executor.submit_task(process_gate, entered, release, 1, hints=hints)
    try:
        assert entered.wait(5)
        crash = executor.submit_task(exit_worker, hints=hints)
        queued = [executor.submit_task(identity, value, hints=hints) for value in range(8)]
        assert queued[3].cancel()
        release.set()
        assert first.result(5) == 1
        assert not wait([crash, *queued], timeout=5).not_done
        assert all(f.cancelled() or isinstance(f.exception(), BrokenProcessPool) for f in [crash, *queued])
    finally:
        release.set()
        executor.shutdown(wait=True, cancel_futures=True)
    snapshot = executor.snapshot()
    assert snapshot["pending"] == snapshot["physical"]["retained"] == 0
    assert snapshot["counts"]["failed"] == 8
    assert snapshot["counts"]["completed"] == snapshot["counts"]["cancelled"] == 1
print("settled")
'''
        completed = subprocess.run([sys.executable, "-c", script],
            capture_output=True, text=True, timeout=WAIT, check=True)
        self.assertEqual(completed.stdout.strip(), "settled")
        self.assertEqual(completed.stderr, "")

    def test_backend_callback_enqueue_avoids_cross_thread_lock_inversion(self):
        # Deterministic non-reentrant backend lock reproduces the producer vs
        # broken-manager inversion without relying on a lucky process race.
        script = r'''
from concurrent.futures import Future
from unittest.mock import patch
import threading
from intelligent_futures import IntelligentExecutor, TaskHints
executor = IntelligentExecutor(max_workers=1, process_workers=2, max_pending=4, policy="hints")
hints = TaskHints(kind="cpu", process_safe=True)
backend_lock = threading.Lock()
manager_ready, producer_entered, callback_returned = [threading.Event() for _ in range(3)]
raws, calls, output = [Future(), Future()], [], []
def submit(*args, **kwargs):
    index = len(calls)
    calls.append(index)
    if index:
        producer_entered.set()
    with backend_lock:
        return raws[index]
def manager():
    with backend_lock:
        manager_ready.set()
        assert producer_entered.wait(3)
        raws[0].set_result(1)
        callback_returned.set()
with patch.object(executor._processes, "submit", side_effect=submit):
    first = executor.submit_task(str, 1, hints=hints)
    manager_thread = threading.Thread(target=manager)
    manager_thread.start()
    assert manager_ready.wait(3)
    producer = threading.Thread(target=lambda: output.append(executor.submit_task(str, 2, hints=hints)))
    producer.start()
    assert callback_returned.wait(3)
    manager_thread.join(3)
    producer.join(3)
    assert not manager_thread.is_alive() and not producer.is_alive()
    raws[1].set_result(2)
    assert first.result(3) == 1 and output[0].result(3) == 2
executor.shutdown()
assert executor.snapshot()["pending"] == executor.snapshot()["physical"]["retained"] == 0
print("settled")
'''
        completed = subprocess.run([sys.executable, "-c", script],
            capture_output=True, text=True, timeout=WAIT, check=True)
        self.assertEqual(completed.stdout.strip(), "settled")
        self.assertEqual(completed.stderr, "")

    def test_process_dispatch_commits_cancellation_before_backend_execution(self):
        executor = self.executor(max_workers=1, process_workers=1, policy="hints")
        raw = Future()
        with patch.object(executor._processes, "submit", return_value=raw):
            future = executor.submit_task(identity, 7, hints=TaskHints(kind="cpu", process_safe=True))
        try:
            self.assertTrue(future.running())
            self.assertFalse(future.cancel())
            self.assertEqual(self.assert_bounds(executor)["physical"]["in_flight"]["process"], 1)
        finally:
            raw.set_result(7)
        self.assertEqual(future.result(WAIT), 7)

    def test_omitted_process_shutdown_settles_owned_queue_without_hanging(self):
        script = r'''
import atexit, time
from intelligent_futures import IntelligentExecutor, TaskHints
executor = IntelligentExecutor(max_workers=1, process_workers=1, max_pending=8, policy="hints")
hints = TaskHints(kind="cpu", process_safe=True)
first = executor.submit_task(time.sleep, 0.1, hints=hints)
queued = [executor.submit_task(str, value, hints=hints) for value in range(5)]
def verify():
    assert first.result() is None
    assert all(future.done() for future in queued)
    for future in queued:
        try:
            future.result()
        except RuntimeError:
            pass
    state = executor.snapshot()
    assert state["pending"] == state["physical"]["retained"] == 0
    print("settled")
atexit.register(verify)
'''
        completed = subprocess.run([sys.executable, "-c", script],
            capture_output=True, text=True, timeout=WAIT, check=True)
        self.assertEqual(completed.stdout.strip(), "settled")
        self.assertEqual(completed.stderr, "")

    def test_queued_threads_do_not_block_available_process_dispatch(self):
        executor = self.executor(max_workers=1, process_workers=1, policy="hints", max_pending=4)
        _, release = self.blocker(executor)
        thread_queued = executor.submit(identity, "thread")
        process = executor.submit_task(process_identity, "process",
                                      hints=TaskHints(kind="cpu", process_safe=True))
        pid, value = process.result(WAIT)
        self.assertNotEqual(pid, os.getpid())
        self.assertEqual(value, "process")
        self.assertFalse(thread_queued.done())
        release.set()
        self.assertEqual(thread_queued.result(WAIT), "thread")
        executor.shutdown()
        self.assert_bounds(executor)


if __name__ == "__main__":
    unittest.main()
