"""Adversarial lifecycle regressions beyond the baseline executor contract.

Run this module under an outer process timeout: no thread-based test timeout can
kill an arbitrary running Python callable. Test-owned gates always have bounds.
"""
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from intelligent_futures import IntelligentExecutor, TaskHints
from intelligent_futures.framework import TaskRegistry, TaskService

WAIT = 10


def identity(value):
    return value


def crash_after_side_effect(path):
    with open(path, "a", encoding="utf-8") as stream:
        stream.write(str(os.getpid()) + "\n")
        stream.flush()
    os._exit(23)


class ProductionReliabilityTests(unittest.TestCase):
    def executor(self, **kwargs):
        executor = IntelligentExecutor(**kwargs)
        self.addCleanup(executor.shutdown, wait=True, cancel_futures=True)
        return executor

    def block_worker(self, executor):
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)

        def block():
            entered.set()
            if not release.wait(WAIT):
                raise AssertionError("test gate timed out")
            return "released"

        future = executor.submit(block)
        self.assertTrue(entered.wait(WAIT))
        return future, release

    def assert_settled(self, executor):
        executor.shutdown(wait=True)
        snapshot = executor.snapshot()
        counts = snapshot["counts"]
        self.assertEqual(snapshot["pending"], 0)
        self.assertEqual(counts["submitted"], sum(counts[k] for k in
                         ("completed", "failed", "cancelled")))
        self.assertTrue(all(not thread.is_alive() for thread in executor._threads._threads))
        return snapshot

    def test_shutdown_wakes_every_blocked_producer_without_rejections(self):
        executor = self.executor(max_workers=1, max_pending=1)
        _, release = self.block_worker(executor)
        waiting = set()
        all_waiting = threading.Event()
        original_wait = executor._condition.wait

        def observed_wait(timeout=None):
            waiting.add(threading.get_ident())
            if len(waiting) == 8:
                all_waiting.set()
            return original_wait(timeout)

        def producer():
            try:
                executor.submit(identity, 1)
            except RuntimeError:
                return "closed"
            return "unexpectedly admitted"

        with patch.object(executor._condition, "wait", side_effect=observed_wait):
            with ThreadPoolExecutor(max_workers=8) as producers:
                futures = [producers.submit(producer) for _ in range(8)]
                try:
                    self.assertTrue(all_waiting.wait(WAIT))
                    executor.shutdown(wait=False)
                    self.assertEqual([f.result(WAIT) for f in futures], ["closed"] * 8)
                finally:
                    executor.shutdown(wait=False, cancel_futures=True)
                    release.set()
        snapshot = self.assert_settled(executor)
        self.assertEqual(snapshot["counts"]["submitted"], 1)
        self.assertEqual(snapshot["counts"]["rejected"], 0)

    def test_repeated_shutdown_escalates_to_cancel_queued_work(self):
        executor = self.executor(max_workers=1, max_pending=16)
        first, release = self.block_worker(executor)
        ran = []
        queued = [executor.submit(ran.append, value) for value in range(10)]
        executor.shutdown(wait=False, cancel_futures=False)
        self.assertFalse(any(f.done() for f in queued))
        executor.shutdown(wait=False, cancel_futures=True)
        executor.shutdown(wait=False, cancel_futures=False)
        self.assertTrue(all(f.cancelled() for f in queued))
        self.assertFalse(first.done())
        release.set()
        snapshot = self.assert_settled(executor)
        self.assertEqual(ran, [])
        self.assertEqual(snapshot["counts"]["cancelled"], 10)

    def test_cancellation_callback_baseexception_keeps_prior_accounting(self):
        # Future callback BaseException behavior is undefined in stdlib. This
        # tests only that our earlier instrumentation callback has finished;
        # it does not claim a worker survives a user callback's SystemExit.
        executor = self.executor(max_workers=1, max_pending=2, submit_timeout=0)
        _, release = self.block_worker(executor)
        future = executor.submit(identity, "queued")

        def callback(_):
            raise SystemExit("intentional callback exit on cancelling caller")

        future.add_done_callback(callback)
        with self.assertRaises(SystemExit):
            future.cancel()
        self.assertEqual(executor.snapshot()["pending"], 1)
        self.assertEqual(executor.snapshot()["counts"]["cancelled"], 1)
        replacement = executor.submit(identity, "replacement")
        release.set()
        self.assertEqual(replacement.result(WAIT), "replacement")
        self.assert_settled(executor)

    def test_predictive_backend_baseexception_does_not_leak_admission(self):
        executor = self.executor(predictive=True, max_workers=1, max_pending=1,
                                 submit_timeout=0)
        with patch.object(executor._threads, "submit", side_effect=SystemExit("submit")):
            with self.assertRaises(SystemExit):
                executor.submit_profiled(identity, 1, features={"count": 1})
        self.assertEqual(executor.snapshot()["counts"]["submitted"], 0)
        self.assertEqual(executor.snapshot()["pending"], 0)
        self.assertEqual(executor.export_profiles(), ())
        self.assertEqual(executor.submit_profiled(identity, 2).result(WAIT), 2)
        self.assert_settled(executor)

    def test_partial_source_failure_cancels_adaptive_window(self):
        executor = self.executor(max_workers=1)
        _, release = self.block_worker(executor)
        ran = []

        def source():
            yield 1
            yield 2
            raise LookupError("source interrupted")

        iterator = executor.map_adaptive(ran.append, source(), min_batch_size=4)
        with self.assertRaisesRegex(LookupError, "source interrupted"):
            next(iterator)
        release.set()
        snapshot = self.assert_settled(executor)
        self.assertEqual(ran, [])
        self.assertEqual(snapshot["counts"]["cancelled"], 2)

    def test_partial_batch_source_failure_discards_chunk_and_cancels_buffer(self):
        executor = self.executor(max_workers=1)
        _, release = self.block_worker(executor)
        consumed, ran = [], []

        def source():
            for value in range(3):
                consumed.append(value)
                yield value
            raise LookupError("source interrupted")

        iterator = executor.map_batches(ran.append, source(), buffersize=3,
                                        min_batch_size=2, max_batch_size=4)
        with self.assertRaisesRegex(LookupError, "source interrupted"):
            next(iterator)
        release.set()
        snapshot = self.assert_settled(executor)
        self.assertEqual(consumed, [0, 1, 2])
        self.assertEqual(ran, [])
        self.assertEqual(snapshot["counts"]["submitted"], 2)  # blocker + full chunk
        self.assertEqual(snapshot["counts"]["cancelled"], 1)

    def test_registry_duplicate_registration_is_atomic_under_contention(self):
        registry = TaskRegistry()
        start = threading.Barrier(12, timeout=WAIT)

        def register(index):
            start.wait()
            try:
                registry.register("same", identity, description=str(index))
                return index
            except ValueError:
                return None

        with ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(register, range(12)))
        winners = [value for value in results if value is not None]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(registry.list()), 1)
        self.assertEqual(registry.list()[0].description, str(winners[0]))

    def test_registry_discovery_and_service_freezes_race_with_registration(self):
        registry = TaskRegistry()
        start = threading.Barrier(5, timeout=WAIT)

        def writer(group):
            start.wait()
            for index in range(30):
                registry.register(f"task_{group}_{index:02d}", identity)

        def reader():
            start.wait()
            for _ in range(60):
                names = [definition.name for definition in registry.list()]
                self.assertEqual(names, sorted(set(names)))
                with TaskService(registry) as service:
                    frozen = service.list_definitions()
                    # Caller mutation cannot change the frozen service metadata.
                    if frozen:
                        frozen[0]["input_schema"]["properties"].clear()
                        self.assertIn("value", service.list_definitions()[0]["input_schema"]["properties"])
                    self.assertEqual([d["name"] for d in service.list_definitions()],
                                     [d["name"] for d in frozen])

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(writer, group) for group in range(4)] + [pool.submit(reader)]
            for future in futures:
                future.result(WAIT)
        self.assertEqual(len(registry.list()), 120)

    def test_feature_churn_bounds_profiles_samples_and_sampling_counters(self):
        executor = self.executor(predictive=True, max_profile_keys=3, max_workers=1)
        for index in range(150):
            executor.submit_profiled(identity, index, hints=TaskHints(key="stable"),
                                     features={"bucket": index}).result(WAIT)
            self.assertLessEqual(len(executor.export_profiles()), 3)
            self.assertLessEqual(len(executor._sample_counts), 3)
        for index in range(150):
            executor.submit_profiled(identity, index, hints=TaskHints(key="stable"),
                                     features={"bucket": 149}).result(WAIT)
        snapshot = self.assert_settled(executor)
        profiles = executor.export_profiles()
        self.assertEqual(len(profiles), 3)
        self.assertTrue(all(len(profile.samples) <= 128 for profile in profiles))
        self.assertEqual(max(len(profile.samples) for profile in profiles), 128)
        self.assertEqual(snapshot["counts"]["completed"], 300)
        self.assertTrue(all(0 <= n < executor._sample_interval for n in executor._sample_counts.values()))

    def test_process_crash_side_effect_occurs_once_and_accounting_settles(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "effects.txt")
            executor = self.executor(process_workers=1, policy="hints")
            hints = TaskHints(kind="cpu", process_safe=True)
            future = executor.submit_task(crash_after_side_effect, path, hints=hints)
            workers = tuple(executor._processes._processes.values())
            with self.assertRaises(BrokenProcessPool):
                future.result(WAIT)
            with self.assertRaises(BrokenProcessPool):
                executor.submit_task(crash_after_side_effect, path, hints=hints)
            snapshot = self.assert_settled(executor)
            self.assertTrue(all(not worker.is_alive() for worker in workers))
            lines = Path(path).read_text().splitlines()
            self.assertEqual(len(lines), 1)
            self.assertNotEqual(int(lines[0]), os.getpid())
            self.assertEqual(snapshot["counts"]["failed"], 1)
            self.assertEqual(snapshot["counts"]["thread_submitted"], 0)

    def test_core_api_import_does_not_replace_stdlib_or_load_optional_layers(self):
        script = '''
import concurrent.futures as standard
original = standard.ThreadPoolExecutor
import intelligent_futures as package
import sys
assert standard.ThreadPoolExecutor is original
assert issubclass(package.IntelligentExecutor, standard.Executor)
assert "intelligent_futures.framework" not in sys.modules
assert "intelligent_futures.integrations.mcp" not in sys.modules
assert "mcp" not in sys.modules
assert all(hasattr(package, name) for name in package.__all__)
with package.IntelligentExecutor() as executor:
    future = executor.submit(lambda **kwargs: kwargs, hints="payload", features="payload")
    assert type(future) is standard.Future
    assert future.result(5) == {"hints": "payload", "features": "payload"}
print("ok")
'''
        completed = subprocess.run([sys.executable, "-c", script], check=True,
                                   capture_output=True, text=True, timeout=WAIT)
        self.assertEqual(completed.stdout.strip(), "ok")


if __name__ == "__main__":
    unittest.main()
