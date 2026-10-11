"""Same-executor saturation fails fast without altering genuine Future APIs."""
from concurrent.futures import Future
import threading
import unittest
from intelligent_futures import (BackpressureError, IntelligentExecutor,
                                 ExecutionObjective, TaskHints)

WAIT = 10

def identity(value):
    return value

class ReentrantAdmissionTests(unittest.TestCase):
    def pool(self, **kwargs):
        pool = IntelligentExecutor(policy=kwargs.pop("policy", "threads"), **kwargs)
        self.addCleanup(pool.shutdown, wait=True, cancel_futures=True)
        return pool

    def gate(self):
        started, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        def task():
            started.set()
            if not release.wait(WAIT):
                raise TimeoutError("test gate")
            return 11
        return task, started, release

    def test_saturated_worker_rejects_with_no_admission_timeout(self):
        pool = self.pool(max_workers=2, max_pending=1)
        def outer():
            with self.assertRaisesRegex(BackpressureError, "reentrant submission"):
                pool.submit(identity, 7)
            return 9
        future = pool.submit(outer)
        self.assertIs(type(future), Future)
        self.assertEqual(future.result(WAIT), 9)
        pool.shutdown()
        counts = pool.snapshot()["counts"]
        self.assertEqual(counts["submitted"], 1)
        self.assertEqual(counts["rejected"], 1)
        self.assertEqual(counts["reentrant_rejected"], 1)

    def test_worker_nested_submission_with_capacity_still_succeeds(self):
        pool = self.pool(max_workers=2, max_pending=2)
        future = pool.submit(lambda: pool.submit(identity, 7))
        self.assertEqual(future.result(WAIT).result(WAIT), 7)
        pool.shutdown()
        self.assertEqual(pool.snapshot()["counts"]["reentrant_rejected"], 0)

    def test_predictive_limit_below_physical_budget_triggers_guard(self):
        pool = self.pool(max_workers=2, max_pending=8, predictive=True,
                         objective=ExecutionObjective(max_concurrency=1))
        def outer():
            with self.assertRaisesRegex(BackpressureError, "reentrant submission"):
                pool.submit(identity, 7)
            snapshot = pool.snapshot()
            return (snapshot["pending"], snapshot["physical"]["retained"],
                    snapshot["predictive"]["concurrency_limit"])
        self.assertEqual(pool.submit(outer).result(WAIT), (1, 1, 1))
        pool.shutdown()
        self.assertEqual(pool.snapshot()["counts"]["reentrant_rejected"], 1)

    def test_process_completion_callback_uses_same_guard(self):
        from tests.test_executor import FileEvent, process_gate
        from pathlib import Path
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            entered, release = FileEvent(root / "entered"), FileEvent(root / "release")
            pool = self.pool(max_workers=2, process_workers=1, max_pending=1, policy="hints")
            first = pool.submit_task(process_gate, entered, release, 3,
                hints=TaskHints(kind="cpu", process_safe=True))
            self.assertTrue(entered.wait(WAIT))
            child_task, _, child_release = self.gate()
            finished = threading.Event()
            children, errors = [], []
            def callback(_):
                try:
                    children.append(pool.submit(child_task))
                    with self.assertRaisesRegex(BackpressureError, "reentrant submission"):
                        pool.submit(identity, 8)
                except BaseException as error:
                    errors.append(error)
                finally:
                    finished.set()
            first.add_done_callback(callback)
            release.set()
            self.assertTrue(finished.wait(WAIT))
            child_release.set()
            self.assertFalse(errors)
            self.assertEqual(first.result(WAIT), 3)
            self.assertEqual(children[0].result(WAIT), 11)
            pool.shutdown()
            self.assertEqual(pool.snapshot()["counts"]["reentrant_rejected"], 1)

    def test_saturated_completion_callback_rejects_but_pump_progresses(self):
        pool = self.pool(max_workers=2, max_pending=1)
        task, started, release = self.gate()
        child_task, _, child_release = self.gate()
        finished = threading.Event()
        children, errors = [], []
        first = pool.submit(task)
        self.assertTrue(started.wait(WAIT))
        def callback(_):
            try:
                children.append(pool.submit(child_task))
                with self.assertRaisesRegex(BackpressureError, "reentrant submission"):
                    pool.submit(identity, 8)
            except BaseException as error:
                errors.append(error)
            finally:
                finished.set()
        first.add_done_callback(callback)
        release.set()
        self.assertTrue(finished.wait(WAIT))
        child_release.set()
        self.assertFalse(errors)
        self.assertEqual(children[0].result(WAIT), 11)
        pool.shutdown()
        self.assertEqual(pool.snapshot()["counts"]["reentrant_rejected"], 1)

    def test_physical_retention_alone_triggers_guard(self):
        pool = self.pool(max_workers=2, max_pending=2)
        parked, allow_start = threading.Event(), threading.Event()
        self.addCleanup(allow_start.set)
        original = pool._run_thread
        def parked_task():
            self.fail("successfully cancelled body executed")
        def wrapper(item):
            if item.fn is parked_task:
                parked.set()
                if not allow_start.wait(WAIT):
                    raise TimeoutError("dispatch gate")
            return original(item)
        # Instrument only this executor's owned method, never backend queues.
        pool._run_thread = wrapper
        cancelled = pool.submit(parked_task)
        self.assertTrue(parked.wait(WAIT))
        self.assertTrue(cancelled.cancel())
        task, started, release = self.gate()
        child_task, _, child_release = self.gate()
        first = pool.submit(task)
        self.assertTrue(started.wait(WAIT))
        completed = threading.Event()
        observations, errors = [], []
        def callback(_):
            try:
                child = pool.submit(child_task)
                snapshot = pool.snapshot()
                observations.append((snapshot["pending"], snapshot["physical"]["retained"]))
                with self.assertRaisesRegex(BackpressureError, "reentrant submission"):
                    pool.submit(identity, 3)
                observations.append(child)
            except BaseException as error:
                errors.append(error)
            finally:
                completed.set()
        first.add_done_callback(callback)
        release.set()
        self.assertTrue(completed.wait(WAIT))
        allow_start.set()
        child_release.set()
        self.assertFalse(errors)
        self.assertEqual(observations[0], (1, 2))
        self.assertEqual(observations[1].result(WAIT), 11)
        pool.shutdown()
        self.assertEqual(pool.snapshot()["counts"]["reentrant_rejected"], 1)

    def test_external_producer_still_waits_for_capacity(self):
        pool = self.pool(max_workers=2, max_pending=1)
        task, started, release = self.gate()
        first = pool.submit(task)
        self.assertTrue(started.wait(WAIT))
        entered, submitted = threading.Event(), threading.Event()
        children = []
        def producer():
            entered.set()
            children.append(pool.submit(identity, 7))
            submitted.set()
        worker = threading.Thread(target=producer)
        worker.start()
        self.assertTrue(entered.wait(WAIT))
        self.assertFalse(submitted.wait(0.05))
        release.set()
        self.assertTrue(submitted.wait(WAIT))
        worker.join(WAIT)
        self.assertEqual(children[0].result(WAIT), 7)
        pool.shutdown()
        self.assertEqual(pool.snapshot()["counts"]["reentrant_rejected"], 0)

    def test_after_done_callback_on_external_caller_keeps_admission_wait(self):
        pool = self.pool(max_workers=2, max_pending=1)
        done = pool.submit(identity, 1)
        self.assertEqual(done.result(WAIT), 1)
        task, started, release = self.gate()
        blocked = pool.submit(task)
        self.assertTrue(started.wait(WAIT))
        entered, finished = threading.Event(), threading.Event()
        children = []
        def callback(_):
            entered.set()
            children.append(pool.submit(identity, 2))
            finished.set()
        caller = threading.Thread(target=done.add_done_callback, args=(callback,))
        caller.start()
        self.assertTrue(entered.wait(WAIT))
        self.assertFalse(finished.wait(0.05))
        release.set()
        self.assertTrue(finished.wait(WAIT))
        caller.join(WAIT)
        self.assertEqual(children[0].result(WAIT), 2)
        pool.shutdown()
        self.assertEqual(pool.snapshot()["counts"]["reentrant_rejected"], 0)

    def test_external_timeout_retains_original_reason_and_counter(self):
        pool = self.pool(max_workers=1, max_pending=1, submit_timeout=0)
        task, started, release = self.gate()
        first = pool.submit(task)
        self.assertTrue(started.wait(WAIT))
        with self.assertRaisesRegex(BackpressureError, "^executor admission limit reached$"):
            pool.submit(identity, 1)
        release.set()
        pool.shutdown()
        counts = pool.snapshot()["counts"]
        self.assertEqual(counts["rejected"], 1)
        self.assertEqual(counts["reentrant_rejected"], 0)

if __name__ == "__main__":
    unittest.main()
