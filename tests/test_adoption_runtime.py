"""Deterministic routing and opt-in sanitized trace adoption contracts."""
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import FrozenInstanceError, asdict
import gc
import json
import multiprocessing
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
import weakref

from intelligent_futures import AdaptivePolicy, BackpressureError, IntelligentExecutor, TaskHints
from intelligent_futures.executor import RoutingDecision
from intelligent_futures.policies import PolicyRegistry, ProcessPolicy
from intelligent_futures.prediction import ExecutionObjective, HardwareCapabilities

WAIT = 10


def identity(value):
    return value


def process_identity(value):
    return os.getpid(), multiprocessing.get_start_method(), value


def fail_after_side_effect(path):
    with open(path, "a", encoding="utf-8") as stream:
        stream.write("called\n")
    raise ValueError("private process exception")


class Payload:
    def __call__(self, value):
        return value


class RuntimeCase(unittest.TestCase):
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
                raise AssertionError("test gate not released")

        future = executor.submit(blocked)
        self.assertTrue(entered.wait(WAIT))
        return future, release

    def retired(self, executor):
        with executor._condition:
            self.assertTrue(executor._condition.wait_for(
                lambda: not executor._completion_threads, WAIT))


class ProcessPolicyTests(RuntimeCase):
    def test_trace_admission_and_shutdown_are_isolated_between_executors(self):
        first = self.executor(policy="threads", max_workers=1, max_pending=1,
                              submit_timeout=0, trace_capacity=1)
        second = self.executor(policy="process", max_workers=1, process_workers=1,
                               max_pending=2, trace_capacity=2)
        _, release = self.blocker(first)
        try:
            with self.assertRaises(BackpressureError):
                first.submit(identity, "full")
            self.assertEqual(second.routing_counters()["recorded"], 0)
            self.assertEqual(second.submit_task(identity, "independent",
                hints=TaskHints(process_safe=True)).result(WAIT), "independent")
            self.assertEqual(first.routing_trace()[0].backend, "thread")
            self.assertEqual(second.routing_trace()[0].backend, "process")
            self.assertEqual(first.routing_counters()["recorded"], 1)
            self.assertEqual(second.routing_counters()["recorded"], 1)
            second.shutdown()
            self.assertEqual(first.snapshot()["pending"], 1)
        finally:
            release.set()
        first.shutdown()
        self.assertEqual(first.snapshot()["pending"], 0)
        self.assertEqual(second.snapshot()["pending"], 0)

    def test_registered_and_requires_both_consent_and_backend(self):
        self.assertIsInstance(PolicyRegistry().create("process"), ProcessPolicy)
        policy = ProcessPolicy()
        for kind in ("unknown", "cpu", "io"):
            for available in (False, True):
                for consent in (False, True):
                    hints = TaskHints(kind=kind, process_safe=consent)
                    with self.subTest(kind=kind, available=available, consent=consent):
                        if available and consent:
                            self.assertEqual(policy.choose(hints, {}, available), "process")
                        else:
                            with self.assertRaisesRegex(ValueError, "process_safe=True.*process_workers > 0"):
                                policy.choose(hints, {}, available)

    def test_missing_consent_and_disabled_backend_never_dispatch(self):
        for workers, hints in ((0, TaskHints(process_safe=True)), (1, TaskHints())):
            for predictive in (False, True):
                with self.subTest(workers=workers, predictive=predictive):
                    executor = self.executor(policy="process", process_workers=workers,
                                             predictive=predictive, trace_capacity=2)
                    called = []
                    with self.assertRaisesRegex(ValueError, "process_safe=True.*process_workers > 0"):
                        executor.submit_task(called.append, 1, hints=hints)
                    with self.assertRaisesRegex(ValueError, "process_safe=True"):
                        executor.submit(called.append, 2)
                    self.assertEqual(called, [])
                    self.assertEqual(executor.snapshot()["counts"]["submitted"], 0)
                    self.assertEqual(executor.routing_trace(), ())
                    self.assertEqual(executor.routing_counters()["recorded"], 0)

    def test_missing_consent_fails_before_saturated_admission(self):
        executor = self.executor(policy="process", process_workers=1, max_pending=1,
                                 submit_timeout=0, trace_capacity=2)
        raw = Future()
        with patch.object(executor._processes, "submit", return_value=raw):
            future = executor.submit_task(identity, 1, hints=TaskHints(process_safe=True))
        try:
            with self.assertRaisesRegex(ValueError, "process_safe=True"):
                executor.submit(identity, 2)
            with self.assertRaises(BackpressureError):
                executor.submit_task(identity, 3, hints=TaskHints(process_safe=True))
        finally:
            raw.set_result(1)
        self.assertEqual(future.result(WAIT), 1)
        self.assertEqual(executor.routing_counters()["recorded"], 1)

    def test_spawn_process_route_is_fixed_for_every_kind_and_predictive_budget(self):
        for predictive in (False, True):
            with self.subTest(predictive=predictive):
                executor = self.executor(policy="process", process_workers=1,
                                         predictive=predictive, trace_capacity=4)
                for kind in ("unknown", "cpu", "io"):
                    child, method, value = executor.submit_task(process_identity, kind,
                        hints=TaskHints(kind=kind, process_safe=True)).result(WAIT)
                    self.assertNotEqual(child, os.getpid())
                    self.assertEqual(method, "spawn")
                    self.assertEqual(value, kind)
                executor.shutdown()
                counts = executor.snapshot()["counts"]
                self.assertEqual(counts["process_submitted"], 3)
                self.assertEqual(counts["thread_submitted"], 0)
                self.assertTrue(all(entry.reason == "process-policy" for entry in executor.routing_trace()))

    def test_process_failure_and_pickle_failure_never_reroute_or_replay(self):
        executor = self.executor(policy="process", process_workers=1, trace_capacity=4)
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "calls.txt")
            with self.assertRaisesRegex(ValueError, "private process exception"):
                executor.submit_task(fail_after_side_effect, path,
                    hints=TaskHints(process_safe=True)).result(WAIT)
            calls = []
            with self.assertRaises((AttributeError, TypeError)):
                executor.submit_task(lambda: calls.append(1),
                    hints=TaskHints(process_safe=True)).result(WAIT)
            executor.shutdown()
            self.assertEqual(Path(path).read_text(encoding="utf-8"), "called\n")
            self.assertEqual(calls, [])
        self.assertEqual(executor.snapshot()["counts"]["failed"], 2)
        self.assertEqual(executor.routing_counters()["by_backend"], {"thread": 0, "process": 2})
        self.assertNotIn("private process exception", repr(executor.routing_trace()))


class RoutingTraceTests(RuntimeCase):
    def test_opt_in_validation_and_disabled_hot_path(self):
        for capacity in (True, False, -1, 1.5, None, "2", float("nan")):
            with self.subTest(capacity=capacity), self.assertRaisesRegex(ValueError, "trace_capacity"):
                IntelligentExecutor(trace_capacity=capacity)
        executor = self.executor()
        with patch.object(executor, "_routing_reason", side_effect=AssertionError("disabled trace called")):
            self.assertEqual(executor.submit(identity, 1).result(WAIT), 1)
        self.assertEqual(executor.routing_trace(), ())
        counters = executor.routing_counters()
        self.assertFalse(counters["enabled"])
        self.assertEqual(counters["recorded"], 0)
        self.assertEqual(sum(counters["by_reason"].values()), 0)

    def test_ring_is_bounded_and_counters_and_entries_are_detached(self):
        executor = self.executor(policy="threads", trace_capacity=7, max_profile_keys=2)
        for index in range(200):
            self.assertEqual(executor.submit_task(identity, index,
                hints=TaskHints(key=str(index))).result(WAIT), index)
        executor.shutdown()
        trace = executor.routing_trace()
        self.assertIsInstance(trace, tuple)
        self.assertEqual([entry.sequence for entry in trace], list(range(194, 201)))
        self.assertTrue(all(isinstance(entry, RoutingDecision) for entry in trace))
        with self.assertRaises(FrozenInstanceError):
            trace[-1].reason = "changed"
        with self.assertRaises(FrozenInstanceError):
            trace[-1].load.thread.queued = 99
        counters = executor.routing_counters()
        self.assertEqual((counters["recorded"], counters["retained"], counters["dropped"]), (200, 7, 193))
        self.assertEqual(counters["by_reason"]["thread-policy"], 200)
        categories = set(counters["by_reason"])
        counters["by_reason"]["private"] = 200
        counters["by_backend"]["thread"] = -1
        self.assertEqual(set(executor.routing_counters()["by_reason"]), categories)
        self.assertEqual(executor.routing_counters()["by_backend"]["thread"], 200)
        json.dumps([asdict(entry) for entry in trace], allow_nan=False)

    def test_rejections_and_failed_immediate_dispatch_are_not_accepted_routes(self):
        executor = self.executor(policy="threads", max_workers=1, max_pending=1,
                                 submit_timeout=0, trace_capacity=3)
        _, release = self.blocker(executor)
        try:
            with self.assertRaises(BackpressureError):
                executor.submit(identity, 2)
        finally:
            release.set()
        self.retired(executor)
        with patch.object(executor._threads, "submit", side_effect=RuntimeError("private submit error")):
            with self.assertRaisesRegex(RuntimeError, "private submit error"):
                executor.submit(identity, 3)
        self.retired(executor)
        self.assertEqual(executor.routing_counters()["recorded"], 1)
        self.assertEqual(executor.snapshot()["counts"]["submitted"], 1)
        self.assertNotIn("private submit error", repr(executor.routing_trace()))

    def test_pre_admission_counts_and_physical_limits_are_exact(self):
        executor = self.executor(policy="threads", max_workers=1, max_pending=3,
                                 submit_timeout=0, trace_capacity=4)
        _, release = self.blocker(executor)
        second = executor.submit(identity, 2)
        third = executor.submit(identity, 3)
        try:
            trace = executor.routing_trace()
            self.assertEqual([entry.pending for entry in trace], [0, 1, 2])
            self.assertEqual([entry.load.thread.in_flight for entry in trace], [0, 1, 1])
            self.assertEqual([entry.load.thread.queued for entry in trace], [0, 0, 1])
            self.assertTrue(all(entry.admission_limit == entry.load.max_pending == 3 for entry in trace))
            self.assertTrue(all(entry.load.thread.workers == 1 and entry.load.process.workers == 0 for entry in trace))
            with self.assertRaises(BackpressureError):
                executor.submit(identity, 4)
        finally:
            release.set()
        self.assertEqual([second.result(WAIT), third.result(WAIT)], [2, 3])

    def test_predictive_admission_limit_is_separate_from_worker_capacity_and_samples(self):
        executor = self.executor(policy="adaptive", predictive=True, trace_capacity=2,
            max_workers=4, max_pending=8, submit_timeout=0,
            hardware=HardwareCapabilities(32), objective=ExecutionObjective(max_concurrency=1))
        _, release = self.blocker(executor)
        try:
            entry, = executor.routing_trace()
            self.assertEqual(entry.admission_limit, 1)
            self.assertEqual(entry.load.thread.workers, 4)
            self.assertEqual(entry.load.max_pending, 8)
            self.assertEqual(entry.reason, "predictive-heuristic")
            self.assertEqual(set(asdict(entry)), {"sequence", "backend", "reason", "exploration",
                                                 "load", "pending", "admission_limit"})
            with self.assertRaises(BackpressureError):
                executor.submit(identity, 1)
        finally:
            release.set()

    def test_profile_keys_feature_names_hardware_and_payload_stay_out_of_trace(self):
        secret = "private-routing-marker"
        executor = self.executor(predictive=True, trace_capacity=2,
                                 hardware=HardwareCapabilities(2, cpu_model=secret))
        self.assertEqual(executor.submit_profiled(identity, secret,
            hints=TaskHints(key=secret), features={secret: 42}).result(WAIT), secret)
        executor.shutdown()
        telemetry = json.dumps({"trace": [asdict(entry) for entry in executor.routing_trace()],
                                "counters": executor.routing_counters()}, allow_nan=False)
        self.assertNotIn(secret, telemetry)
        self.assertNotIn("42", telemetry)

    def test_custom_policy_names_text_and_string_subclasses_never_enter_trace(self):
        secret = "custom-policy-private-identity"
        refs = []

        class Route(str):
            pass

        class CustomPolicy(AdaptivePolicy):
            def choose_with_load(self, *args):
                route = Route("thread")
                route.private = Payload()
                refs.append(weakref.ref(route.private))
                return route

            def snapshot(self):
                raise AssertionError("custom telemetry must never be inspected")

            def __repr__(self):
                return secret

        def run():
            # Keep only telemetry alive: ordinary profile keys have a separate
            # lifetime and are outside the routing-trace privacy contract.
            with IntelligentExecutor(policy=CustomPolicy(), trace_capacity=3) as executor:
                self.assertEqual(executor.submit_task(identity, secret,
                    hints=TaskHints(key=secret)).result(WAIT), secret)
            return executor.routing_trace(), executor.routing_counters(), weakref.ref(executor)

        trace, counters, owner_ref = run()
        self.assertEqual(trace[0].reason, "custom-policy")
        self.assertIs(type(trace[0].backend), str)
        self.assertNotIn(secret, repr(trace))
        self.assertNotIn(secret, repr(counters))
        self.assertIsNone(owner_ref())
        self.assertTrue(all(reference() is None for reference in refs))

    def test_trace_retains_no_callable_inputs_results_or_completed_future(self):
        executor = self.executor(policy="threads", trace_capacity=8)
        was_enabled = gc.isenabled()
        gc.collect()
        gc.disable()
        try:
            callback, payload = Payload(), Payload()
            refs = [weakref.ref(callback), weakref.ref(payload)]
            future = executor.submit(callback, payload)
            self.assertIs(future.result(WAIT), payload)
            refs.append(weakref.ref(future))
            del callback, payload, future
            self.retired(executor)
            self.assertTrue(all(reference() is None for reference in refs))
            self.assertEqual(len(executor.routing_trace()), 1)
        finally:
            if was_enabled:
                gc.enable()

    def test_cancel_churn_keeps_trace_bounded_and_releases_payloads_immediately(self):
        executor = self.executor(policy="threads", max_workers=1, max_pending=2,
                                 submit_timeout=0, trace_capacity=4)
        with patch.object(executor._threads, "submit", wraps=executor._threads.submit) as dispatch:
            _, release = self.blocker(executor)
            try:
                for _ in range(100):
                    payload = Payload()
                    reference = weakref.ref(payload)
                    future = executor.submit(identity, payload)
                    del payload
                    self.assertTrue(future.cancel())
                    self.assertIsNone(reference())
                    self.assertEqual(wait([future], timeout=0).done, {future})
                self.assertEqual(dispatch.call_count, 1)
                self.assertEqual(len(executor.routing_trace()), 4)
                self.assertEqual(executor.routing_counters()["recorded"], 101)
                self.assertEqual(executor.snapshot()["physical"]["retained"], 1)
            finally:
                release.set()

    def test_cancelled_dispatch_holds_physical_slot_without_duplicate_trace(self):
        executor = self.executor(policy="threads", max_workers=1, max_pending=1,
                                 submit_timeout=0, trace_capacity=3)
        raw = Future()
        with patch.object(executor._threads, "submit", return_value=raw):
            cancelled = executor.submit(identity, "never called")
        try:
            self.assertTrue(cancelled.cancel())
            state = executor.snapshot()
            self.assertEqual(state["pending"], 0)
            self.assertEqual(state["physical"]["retained"], 1)
            with self.assertRaises(BackpressureError):
                executor.submit(identity, "no physical slot")
            self.assertEqual(executor.routing_counters()["recorded"], 1)
        finally:
            raw.set_result(None)
        self.retired(executor)
        self.assertEqual(executor.submit(identity, "available").result(WAIT), "available")
        self.assertEqual([entry.sequence for entry in executor.routing_trace()], [1, 2])
        self.assertEqual(executor.routing_trace()[1].load.thread.outstanding, 0)

    def test_completion_callbacks_can_read_trace_from_another_thread(self):
        executor = self.executor(policy="threads", trace_capacity=3)
        future, release = self.blocker(executor)
        finished = threading.Event()
        snapshots, errors = [], []

        def callback(_):
            def read():
                snapshots.append((executor.routing_trace(), executor.routing_counters()))

            reader = threading.Thread(target=read, daemon=True)
            reader.start()
            reader.join(WAIT)
            if reader.is_alive():
                errors.append("callback held the scheduler lock")
            finished.set()

        future.add_done_callback(callback)
        release.set()
        self.assertTrue(finished.wait(WAIT))
        self.assertEqual(errors, [])
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0][1]["recorded"], 1)

    def test_contending_producers_and_snapshot_readers_preserve_consistent_bounds(self):
        executor = self.executor(policy="threads", max_workers=3, max_pending=5, trace_capacity=11)
        barrier = threading.Barrier(5, timeout=WAIT)
        finished = threading.Event()
        self.addCleanup(finished.set)

        def produce(offset):
            barrier.wait()
            return [executor.submit(identity, offset + index).result(WAIT) for index in range(30)]

        def inspect():
            barrier.wait()
            observed = 0
            while not finished.is_set():
                trace = executor.routing_trace()
                counters = executor.routing_counters()
                self.assertLessEqual(len(trace), 11)
                self.assertEqual(counters["recorded"], sum(counters["by_backend"].values()))
                self.assertEqual(counters["recorded"], sum(counters["by_reason"].values()))
                self.assertEqual(counters["recorded"], counters["retained"] + counters["dropped"])
                self.assertEqual([entry.sequence for entry in trace], sorted(entry.sequence for entry in trace))
                for entry in trace:
                    self.assertLess(entry.pending, entry.admission_limit)
                    self.assertLess(entry.load.thread.outstanding, entry.load.max_pending)
                    self.assertLessEqual(entry.load.thread.in_flight, entry.load.thread.workers)
                observed += 1
            return observed

        with ThreadPoolExecutor(max_workers=5) as pool:
            inspection = pool.submit(inspect)
            producers = [pool.submit(produce, offset) for offset in (0, 30, 60, 90)]
            try:
                values = [value for producer in producers for value in producer.result(WAIT)]
            finally:
                finished.set()
            self.assertGreater(inspection.result(WAIT), 0)
        executor.shutdown()
        self.assertEqual(sorted(values), list(range(120)))
        self.assertEqual(executor.routing_counters()["recorded"], 120)
        self.assertEqual(executor.routing_counters()["by_reason"]["thread-policy"], 120)

    def test_builtin_reason_categories_include_adaptive_exploration(self):
        cases = (
            ("hints", 0, TaskHints(kind="cpu", process_safe=True), "process-unavailable"),
            ("hints", 1, TaskHints(kind="cpu"), "process-consent-required"),
            ("hints", 0, TaskHints(kind="io"), "io-hint"),
            ("hints", 0, TaskHints(), "default-thread"),
        )
        for policy, workers, hints, reason in cases:
            with self.subTest(reason=reason):
                executor = self.executor(policy=policy, process_workers=workers, trace_capacity=1)
                executor.submit_task(identity, 1, hints=hints).result(WAIT)
                self.assertEqual(executor.routing_trace()[0].reason, reason)

        executor = self.executor(policy=AdaptivePolicy(exploration_interval=2),
                                 process_workers=1, trace_capacity=3)
        raw = Future()
        hints = TaskHints(kind="cpu", process_safe=True)
        with patch.object(executor._processes, "submit", return_value=raw):
            first = executor.submit_task(identity, 1, hints=hints)
        try:
            second = executor.submit_task(identity, 2, hints=hints)
            self.assertEqual(second.result(WAIT), 2)
            first_trace, second_trace = executor.routing_trace()
            self.assertEqual(first_trace.reason, "task-hint")
            self.assertFalse(first_trace.exploration)
            self.assertEqual(second_trace.reason, "task-hint")
            self.assertTrue(second_trace.exploration)
            self.assertEqual(second_trace.backend, "thread")
        finally:
            raw.set_result(1)
        self.assertEqual(first.result(WAIT), 1)

    def test_fixed_predictive_route_preserves_its_reason_across_contending_waits(self):
        executor = self.executor(policy=AdaptivePolicy(exploration_interval=2),
            predictive=True, max_workers=1, process_workers=1, max_pending=2,
            objective=ExecutionObjective(max_concurrency=1), trace_capacity=4)
        hints = TaskHints(kind="cpu", process_safe=True)
        raw = Future()
        waiting = {"second": threading.Event(), "third": threading.Event()}
        outputs, errors = {}, []
        original_wait = executor._condition.wait

        def observed_wait(timeout=None):
            event = waiting.get(threading.current_thread().name)
            if event is not None:
                event.set()
            return original_wait(timeout)

        def process_submit(fn, value):
            if value == 1:
                return raw
            done = Future()
            done.set_result(value)
            return done

        def produce(value):
            try:
                outputs[value] = executor.submit_task(identity, value, hints=hints)
            except BaseException as error:
                errors.append(error)

        producers = [threading.Thread(target=produce, args=(value,), name=name, daemon=True)
                     for name, value in (("second", 2), ("third", 3))]
        with patch.object(executor._processes, "submit", side_effect=process_submit), \
                patch.object(executor._condition, "wait", side_effect=observed_wait):
            first = executor.submit_task(identity, 1, hints=hints)
            try:
                for producer in producers:
                    producer.start()
                    self.assertTrue(waiting[producer.name].wait(WAIT))
                # Both have chosen, but neither is admitted. The third choice
                # has overwritten the policy's latest (non-exploratory) state.
                self.assertFalse(executor._policy.snapshot()["last_decision"]["exploration"])
                self.assertEqual(executor.routing_counters()["recorded"], 1)
            finally:
                raw.set_result(1)
                for producer in producers:
                    if producer.ident is not None:
                        producer.join(WAIT)
            self.assertFalse(any(producer.is_alive() for producer in producers))
            self.assertEqual(errors, [])
            self.assertEqual(first.result(WAIT), 1)
            self.assertEqual([outputs[value].result(WAIT) for value in (2, 3)], [2, 3])
        traces = executor.routing_trace()
        exploratory, = [entry for entry in traces if entry.backend == "thread"]
        self.assertTrue(exploratory.exploration)
        self.assertEqual(exploratory.reason, "task-hint")
        self.assertTrue(all(not entry.exploration for entry in traces if entry.backend == "process"))


if __name__ == "__main__":
    unittest.main()
