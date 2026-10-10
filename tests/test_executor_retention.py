"""Completed-Future ownership must not require cyclic garbage collection."""
from contextlib import contextmanager
from concurrent.futures import wait
import gc
import subprocess
import sys
import threading
import unittest
import weakref

from intelligent_futures import IntelligentExecutor

WAIT = 10


class Token:
    pass


@contextmanager
def without_cyclic_gc():
    was_enabled = gc.isenabled()
    gc.collect()
    gc.disable()
    try:
        yield
    finally:
        if was_enabled:
            gc.enable()


class ExecutorRetentionTests(unittest.TestCase):
    def test_discarded_success_results_and_futures_reclaim_without_cyclic_gc(self):
        def run():
            executor = IntelligentExecutor(max_workers=2)
            owner_ref = weakref.ref(executor)
            result_refs, future_refs = [], []
            try:
                for _ in range(20):
                    future = executor.submit(Token)
                    result = future.result(WAIT)
                    result_refs.append(weakref.ref(result))
                    future_refs.append(weakref.ref(future))
                    del result, future
            finally:
                executor.shutdown()
            return owner_ref, result_refs, future_refs

        with without_cyclic_gc():
            owner_ref, result_refs, future_refs = run()
            self.assertTrue(all(reference() is None for reference in result_refs))
            self.assertTrue(all(reference() is None for reference in future_refs))
            self.assertIsNone(owner_ref())

    def test_retained_completed_future_does_not_retain_shutdown_executor(self):
        def run():
            executor = IntelligentExecutor(max_workers=1)
            owner_ref = weakref.ref(executor)
            try:
                future = executor.submit(Token)
                future.result(WAIT)
            finally:
                executor.shutdown()
            return owner_ref, future, weakref.ref(future.result())

        with without_cyclic_gc():
            owner_ref, future, result_ref = run()
            self.assertIsNone(owner_ref())
            self.assertIs(future.result(), result_ref())
            del future
            self.assertIsNone(result_ref())

    def test_retained_cancelled_future_releases_payload_and_shutdown_executor(self):
        def run():
            executor = IntelligentExecutor(max_workers=1, max_pending=2)
            owner_ref = weakref.ref(executor)
            entered, release = threading.Event(), threading.Event()

            def block():
                entered.set()
                if not release.wait(WAIT):
                    raise AssertionError("test gate not released")

            try:
                running = executor.submit(block)
                self.assertTrue(entered.wait(WAIT))
                payload = Token()
                payload_ref = weakref.ref(payload)
                cancelled = executor.submit(lambda value: value, payload)
                del payload
                self.assertTrue(cancelled.cancel())
                self.assertIsNone(payload_ref())
                self.assertEqual(wait([cancelled], timeout=0).done, {cancelled})
            finally:
                release.set()
                executor.shutdown()
            return owner_ref, cancelled, payload_ref

        with without_cyclic_gc():
            owner_ref, cancelled, payload_ref = run()
            self.assertIsNone(owner_ref())
            self.assertIsNone(payload_ref())
            self.assertTrue(cancelled.cancelled())

    def test_completion_records_release_results_while_executor_stays_open(self):
        executor = IntelligentExecutor(max_workers=1)
        try:
            with without_cyclic_gc():
                refs = []
                for _ in range(20):
                    future = executor.submit(Token)
                    result = future.result(WAIT)
                    refs.append(weakref.ref(result))
                    del result, future
                # Establish callback/pump retirement without closing the owner.
                with executor._condition:
                    self.assertTrue(executor._condition.wait_for(
                        lambda: not executor._completion_threads, WAIT))
                self.assertTrue(all(reference() is None for reference in refs))
                self.assertFalse(executor.snapshot()["closed"])
        finally:
            executor.shutdown()

    def test_dispatch_removal_before_cancel_callback_keeps_accounting_and_wakes_pump(self):
        # Bound an intentional shutdown-deadlock reproducer in a subprocess.
        script = r'''
from queue import SimpleQueue
from unittest.mock import patch
import threading
from intelligent_futures import IntelligentExecutor
executor = IntelligentExecutor(max_workers=1, max_pending=2)
waiting = threading.Event()
class ObservedQueue:
    def __init__(self):
        self.queue = SimpleQueue()
        self.calls = 0
    def put(self, value):
        self.queue.put(value)
    def get(self):
        self.calls += 1
        if self.calls == 2:
            waiting.set()
        return self.queue.get()
executor._completions = ObservedQueue()
entered, release = threading.Event(), threading.Event()
def block():
    entered.set()
    assert release.wait(5)
first = executor.submit(block)
assert entered.wait(3)
candidate = executor.submit(str, 1)
record_entered, record_release = threading.Event(), threading.Event()
original_record = executor._record
def record(done, item):
    if done is candidate:
        record_entered.set()
        assert record_release.wait(5)
    original_record(done, item)
with patch.object(executor, "_record", side_effect=record):
    canceller = threading.Thread(target=candidate.cancel)
    canceller.start()
    assert record_entered.wait(3)
    release.set()
    assert waiting.wait(3)
    snapshot = executor.snapshot()
    assert snapshot["pending"] == 1 and snapshot["physical"]["retained"] == 0
    record_release.set()
    canceller.join(3)
    assert not canceller.is_alive()
executor.shutdown()
snapshot = executor.snapshot()
assert snapshot["pending"] == snapshot["physical"]["retained"] == 0
assert snapshot["counts"]["completed"] == snapshot["counts"]["cancelled"] == 1
assert not executor._completion_threads
print("settled")
'''
        completed = subprocess.run([sys.executable, "-c", script],
            capture_output=True, text=True, timeout=WAIT, check=True)
        self.assertEqual(completed.stdout.strip(), "settled")
        self.assertEqual(completed.stderr, "")


if __name__ == "__main__":
    unittest.main()
