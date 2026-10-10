"""Deterministic tests for the dependency-free registered task lifecycle."""
from concurrent.futures import Future
import json
import threading
import unittest
from unittest.mock import patch
from typing import Optional

from intelligent_futures import TaskHints
from intelligent_futures.framework import TaskRegistry, TaskService, TaskServiceError
from intelligent_futures.framework.tasks import _Outcome

WAIT = 10


def add(left: int, right: int = 0):
    return left + right


class FrameworkCase(unittest.TestCase):
    def service(self, function=add, name="add", **settings):
        registry = TaskRegistry()
        registry.register(name, function)
        service = TaskService(registry, **settings)
        self.addCleanup(service.close)
        return service

    def settled(self, service):
        with service._condition:
            self.assertTrue(service._condition.wait_for(lambda: not service._active, WAIT))

    def assert_code(self, code, action, *args, **kwargs):
        with self.assertRaises(TaskServiceError) as caught:
            action(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)
        self.assertEqual(caught.exception.as_dict()["message"], caught.exception.message)
        return caught.exception

    def blocked_service(self, **settings):
        entered, release = threading.Event(), threading.Event()
        calls = []

        def task(value: int = 1):
            calls.append(value)
            entered.set()
            if not release.wait(WAIT):
                raise AssertionError("test gate did not open")
            return value

        service = self.service(task, name="blocked", max_workers=1, **settings)
        self.addCleanup(release.set)
        first = service.submit("blocked", {"value": 1})
        self.assertTrue(entered.wait(WAIT))
        return service, first, release, calls


class RegistryTests(FrameworkCase):
    def test_discovery_and_defaults(self):
        registry = TaskRegistry()
        definition = registry.register("math.add", add, description="Add integers")
        self.assertEqual(definition.name, "math.add")
        self.assertEqual(definition.input_schema, {
            "type": "object", "properties": {
                "left": {"type": "integer"}, "right": {"type": "integer", "default": 0}},
            "required": ["left"], "additionalProperties": False})
        self.assertEqual(registry.list(), [definition])
        self.assertEqual(json.loads(json.dumps(definition.as_dict()))["description"], "Add integers")
        altered = definition.input_schema
        altered["properties"].clear()
        self.assertIn("left", definition.input_schema["properties"])

    def test_reject_invalid_and_duplicate_registrations(self):
        registry = TaskRegistry()
        for name in (None, "", "a b", "a/b", "1task", "a" * 65, "écho"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                registry.register(name, add)
        registry.register("add", add)
        with self.assertRaises(ValueError):
            registry.register("add", add)
        with self.assertRaises(TypeError):
            registry.register("not_callable", "os.system")
        with self.assertRaises(TypeError):
            registry.register("bad_hints", add, hints={})
        with self.assertRaises(ValueError):
            registry.register("bad_description", add, description=42)

    def test_supported_annotations(self):
        def kinds(text: str, flag: bool, count: int, number: float, empty: None,
                  sequence: list[int], object_: dict[str, list[Optional[float]]],
                  raw_list: list, raw_dict: dict, anything):
            return text

        service = self.service(kinds, name="kinds")
        arguments = {"text": "ok", "flag": True, "count": 3, "number": 1,
                     "empty": None, "sequence": [1, 2], "object_": {"x": [None, 1.5]},
                     "raw_list": [True, {}], "raw_dict": {"x": []}, "anything": None}
        submitted = service.submit("kinds", arguments)
        self.settled(service)
        self.assertEqual(service.get(submitted["task_id"])["result"], "ok")
        for key, value in (("flag", 1), ("count", True), ("count", 1.0), ("text", 2),
                           ("number", True), ("empty", 0), ("sequence", [True]),
                           ("object_", {"x": ["bad"]})):
            with self.subTest(key=key, value=value):
                self.assert_code("invalid_arguments", service.submit, "kinds", dict(arguments, **{key: value}))

    def test_postponed_annotations_without_evaluation(self):
        def task(value: "dict[str, list[int | None]]", number: "typing.Optional[float]" = None):
            return value

        service = self.service(task, name="strings")
        submission = service.submit("strings", {"value": {"a": [1, None]}})
        self.settled(service)
        self.assertEqual(service.get(submission["task_id"])["result"], {"a": [1, None]})
        self.assert_code("invalid_arguments", service.submit, "strings", {"value": {"a": [False]}})

        def malicious(value: "__import__('os').system('echo unsafe')"):
            return value

        with self.assertRaises(ValueError):
            TaskRegistry().register("malicious", malicious)

    def test_unsupported_signatures_and_defaults_fail_early(self):
        def positional(value, /):
            return value

        def variadic(*values):
            return values

        def arbitrary(**values):
            return values

        def wrong_annotation(value: set):
            return value

        def wrong_keys(value: dict[int, str]):
            return value

        def bad_default(value: int = False):
            return value

        def unsafe_default(value=object()):
            return value

        async def asynchronous(value):
            return value

        def generator(value):
            yield value

        for function in (positional, variadic, arbitrary, wrong_annotation, wrong_keys,
                         bad_default, unsafe_default, asynchronous, generator):
            with self.subTest(function=function.__name__), self.assertRaises(ValueError):
                TaskRegistry().register(function.__name__, function)

    def test_registry_snapshot_is_frozen_per_service(self):
        registry = TaskRegistry()
        registry.register("first", add)
        service = TaskService(registry)
        self.addCleanup(service.close)
        registry.register("second", add)
        self.assertEqual([item["name"] for item in service.list_definitions()], ["first"])
        self.assert_code("unknown_task", service.submit, "second", {"left": 1})
        descriptions = service.list_definitions()
        descriptions[0]["input_schema"]["properties"].clear()
        self.assertIn("left", service.list_definitions()[0]["input_schema"]["properties"])


class ValidationTests(FrameworkCase):
    def test_input_signature_and_json_safety(self):
        service = self.service()
        cyclic = []
        cyclic.append(cyclic)
        for arguments in ([], None, {"right": 2}, {"left": 1, "unknown": 4},
                          {"left": float("nan")}, {"left": float("inf")},
                          {"left": (1, 2)}, {"left": {1: 2}}, {"left": cyclic},
                          {"left": object()}, {1: "not a string key"}):
            with self.subTest(arguments=repr(arguments)):
                self.assert_code("invalid_arguments", service.submit, "add", arguments)
        self.assertEqual(service.snapshot()["counts"]["submitted"], 0)

    def test_exact_builtin_json_types_only(self):
        class String(str):
            pass

        class Number(int):
            pass

        def identity(value):
            return value

        service = self.service(identity)
        for value in (String("hello"), Number(1)):
            self.assert_code("invalid_arguments", service.submit, "add", {"value": value})

    def test_input_size_and_nesting_bounds(self):
        service = self.service(lambda value: value, max_input_bytes=20)
        self.assert_code("invalid_arguments", service.submit, "add", {"value": "x" * 20})
        self.assert_code("invalid_arguments", service.submit, "add", {"value": "é" * 3})
        deep = 1
        for _ in range(66):
            deep = [deep]
        unrestricted = self.service(lambda value: value)
        self.assert_code("invalid_arguments", unrestricted.submit, "add", {"value": deep})

    def test_defaults_are_copied_and_counted_in_input_limit(self):
        default = []

        def append(value: list = default):
            value.append(1)
            return value

        service = self.service(append)
        default.append(99)  # Registration has already frozen this default.
        submissions = [service.submit("add", {}) for _ in range(2)]
        self.settled(service)
        self.assertEqual([service.get(item["task_id"])["result"] for item in submissions], [[1], [1]])

        limited = self.service(lambda value="long default": value, max_input_bytes=5)
        self.assert_code("invalid_arguments", limited.submit, "add", {})

    def test_input_and_result_are_detached(self):
        entered, release = threading.Event(), threading.Event()

        def echo(value: list):
            entered.set()
            if not release.wait(WAIT):
                raise AssertionError("test gate did not open")
            return value

        service = self.service(echo)
        self.addCleanup(release.set)
        value = [1]
        submitted = service.submit("add", {"value": value})
        self.assertTrue(entered.wait(WAIT))
        value.append(2)
        release.set()
        self.settled(service)
        result = service.get(submitted["task_id"])
        self.assertEqual(result["result"], [1])
        result["result"].append(999)
        self.assertEqual(service.get(submitted["task_id"])["result"], [1])

    def test_exceptions_are_sanitized_including_baseexceptions(self):
        for error_type in (ValueError, RuntimeError, SystemExit, KeyboardInterrupt):
            def fail():
                raise error_type("secret-token-sensitive-path")

            service = self.service(fail)
            submitted = service.submit("add", {})
            self.settled(service)
            snapshot = service.get(submitted["task_id"])
            self.assertEqual(snapshot["status"], "failed")
            self.assertEqual(snapshot["error"]["code"], "execution_failed")
            self.assertNotIn("secret", json.dumps(snapshot))
            self.assertEqual(service.snapshot()["counts"]["failed"], 1)

    def test_bad_and_oversized_results_are_recorded_as_failures(self):
        cycle = []
        cycle.append(cycle)
        for result in (object(), (1, 2), float("nan"), {1: 2}, cycle, "x" * 100):
            service = self.service(lambda: result, max_result_bytes=32)
            submitted = service.submit("add", {})
            self.settled(service)
            snapshot = service.get(submitted["task_id"])
            self.assertEqual(snapshot["status"], "failed")
            self.assertEqual(snapshot["error"]["code"], "invalid_result")
            self.assertIsNone(snapshot["result"])
            self.assertEqual(service.snapshot()["pending"], 0)

    def test_lone_surrogates_are_rejected_before_transport(self):
        service = self.service(lambda value: value)
        for value in ("\ud800", {"nested": ["\udfff"]}, {"\ud800": 1},
                      {"nested": [{"\udfff": "value"}]}):
            with self.subTest(value=repr(value)):
                self.assert_code("invalid_arguments", service.submit, "add", {"value": value})
                result_service = self.service(lambda: value)
                submitted = result_service.submit("add", {})
                self.settled(result_service)
                result = result_service.get(submitted["task_id"])
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["error"]["code"], "invalid_result")
                json.dumps(result, ensure_ascii=False).encode("utf-8")

        # Actual non-BMP Unicode scalar values remain valid.
        submitted = service.submit("add", {"value": {"😀": ["é", "😀"]}})
        self.settled(service)
        self.assertEqual(service.get(submitted["task_id"])["result"], {"😀": ["é", "😀"]})

    def test_surrogates_in_defaults_and_descriptions_fail_registration(self):
        registry = TaskRegistry()
        for default in ("\ud800", {"nested": ["\udfff"]}, {"\ud800": 1}):
            def task(value=default):
                return value

            with self.subTest(default=repr(default)), self.assertRaises(ValueError):
                registry.register("task", task)
        for description in ("\ud800", "nested \udfff text"):
            with self.subTest(description=repr(description)), self.assertRaises(ValueError):
                registry.register("task", add, description=description)
        for key in ("\ud800", "profile \udfff"):
            with self.subTest(key=repr(key)), self.assertRaises(ValueError):
                registry.register("task", add, hints=TaskHints(key=key))
        self.assertEqual(registry.list(), [])

    def test_constructor_limits(self):
        for name in ("max_workers", "max_pending", "max_tasks", "max_input_bytes", "max_result_bytes"):
            for value in (0, -1, True, 1.5, None):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    TaskService(TaskRegistry(), **{name: value})


class LifecycleTests(FrameworkCase):
    def test_completed_result_retained_until_forget(self):
        service = self.service(max_tasks=1)
        submitted = service.submit("add", {"left": 3, "right": 4})
        self.settled(service)
        result = service.get(submitted["task_id"])
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["result"], 7)
        self.assertLessEqual(result["submitted_at"], result["started_at"])
        self.assertLessEqual(result["started_at"], result["completed_at"])
        self.assert_code("task_capacity", service.submit, "add", {"left": 2})
        self.assertEqual(service.forget(result["task_id"]), {"task_id": result["task_id"], "forgotten": True})
        self.assert_code("task_not_found", service.get, result["task_id"])
        service.submit("add", {"left": 2})
        self.settled(service)
        self.assertEqual(service.snapshot()["counts"]["forgotten"], 1)

    def test_nonblocking_admission_and_running_cancellation(self):
        service, first, release, calls = self.blocked_service(max_pending=1)
        snapshot = service.cancel(first["task_id"])
        self.assertFalse(snapshot["cancel_accepted"])
        self.assertEqual(snapshot["status"], "running")
        self.assert_code("busy", service.submit, "blocked", {"value": 2})
        self.assert_code("task_not_finished", service.forget, first["task_id"])
        self.assertEqual(service.snapshot()["pending"], 1)
        release.set()
        self.settled(service)
        self.assertEqual(service.get(first["task_id"])["result"], 1)
        self.assertEqual(calls, [1])
        self.assertFalse(service.cancel(first["task_id"])["cancel_accepted"])

    def test_cancelled_queue_slots_stay_bounded_even_after_forget(self):
        service, first, release, calls = self.blocked_service(max_pending=2)
        second = service.submit("blocked", {"value": 2})
        cancelled = service.cancel(second["task_id"])
        self.assertTrue(cancelled["cancel_accepted"])
        self.assertEqual(cancelled["status"], "cancelled")
        self.assertIsNone(cancelled["started_at"])
        self.assertTrue(service.cancel(second["task_id"])["cancel_accepted"])
        service.forget(second["task_id"])
        for _ in range(20):
            self.assert_code("busy", service.submit, "blocked", {"value": 3})
        self.assertEqual(service.snapshot()["pending"], 2)
        self.assertEqual(service.snapshot()["executor"]["counts"]["submitted"], 2)
        release.set()
        self.settled(service)
        self.assertEqual(calls, [1])
        self.assertEqual(service.snapshot()["counts"]["cancelled"], 1)
        third = service.submit("blocked", {"value": 3})
        self.settled(service)
        self.assertEqual(service.get(third["task_id"])["result"], 3)

    def test_shutdown_marks_queued_cancelled_and_preserves_running(self):
        service, first, release, calls = self.blocked_service(max_pending=3)
        second = service.submit("blocked", {"value": 2})
        third = service.submit("blocked", {"value": 3})
        service.close(wait=False, cancel_futures=True)
        self.assertEqual(service.get(first["task_id"])["status"], "running")
        self.assertEqual(service.get(second["task_id"])["status"], "cancelled")
        self.assertEqual(service.get(third["task_id"])["status"], "cancelled")
        self.assert_code("service_closed", service.submit, "blocked", {"value": 4})
        release.set()
        service.close(wait=True)
        self.assertEqual(calls, [1])
        snapshot = service.snapshot()
        self.assertEqual(snapshot["pending"], 0)
        self.assertEqual(snapshot["counts"]["completed"], 1)
        self.assertEqual(snapshot["counts"]["cancelled"], 2)
        self.assertTrue(snapshot["closed"])

    def test_close_without_cancellation_drains_queue(self):
        service, first, release, calls = self.blocked_service(max_pending=2)
        second = service.submit("blocked", {"value": 2})
        service.close(wait=False, cancel_futures=False)
        release.set()
        service.close(wait=True, cancel_futures=False)
        self.assertEqual(calls, [1, 2])
        self.assertEqual(service.get(second["task_id"])["status"], "completed")

    def test_submit_close_race_leaves_no_orphaned_records(self):
        service = self.service(max_tasks=100, max_pending=100)
        barrier = threading.Barrier(3)
        results, failures = [], []

        def submit():
            barrier.wait(WAIT)
            for index in range(20):
                try:
                    results.append(service.submit("add", {"left": index}))
                except TaskServiceError as error:
                    failures.append(error.code)

        def close():
            barrier.wait(WAIT)
            service.close()

        threads = [threading.Thread(target=submit), threading.Thread(target=close)]
        for thread in threads:
            thread.start()
        barrier.wait(WAIT)
        for thread in threads:
            thread.join(WAIT)
            self.assertFalse(thread.is_alive())
        service.close()
        self.assertTrue(all(code == "service_closed" for code in failures))
        self.assertEqual(len(results) + len(failures), 20)
        self.assertEqual(service.snapshot()["pending"], 0)
        self.assertEqual(service.snapshot()["retained"], len(results))
        self.assertTrue(all(task["status"] in ("completed", "cancelled") for task in service.list_tasks()))

    def test_already_completed_backend_future_is_accounted_once(self):
        service = self.service()
        future = Future()
        future.set_result(_Outcome(result_json="3"))
        with patch.object(service._executor, "submit_task", return_value=future):
            task = service.submit("add", {"left": 3})
        self.assertEqual(task["status"], "completed")
        self.assertEqual(task["result"], 3)
        self.assertEqual(service.snapshot()["pending"], 0)
        self.assertEqual(service.snapshot()["counts"]["completed"], 1)

    def test_backend_submission_failure_rolls_back(self):
        service = self.service()
        with patch.object(service._executor, "submit_task", side_effect=RuntimeError("secret")):
            error = self.assert_code("submission_failed", service.submit, "add", {"left": 1})
        self.assertNotIn("secret", str(error))
        self.assertEqual(service.snapshot()["retained"], 0)
        self.assertEqual(service.snapshot()["pending"], 0)
        service.submit("add", {"left": 1})
        self.settled(service)

    def test_task_listing_and_unknown_ids(self):
        service = self.service()
        tasks = [service.submit("add", {"left": index}) for index in range(3)]
        self.settled(service)
        self.assertEqual([task["task_id"] for task in service.list_tasks(2)],
                         [task["task_id"] for task in tasks[:2]])
        for limit in (0, 101, True, 1.0):
            self.assert_code("invalid_limit", service.list_tasks, limit)
        for task_id in ("unknown", None, []):
            for method in (service.get, service.cancel, service.forget):
                self.assert_code("task_not_found", method, task_id)
        self.assert_code("unknown_task", service.submit, "os.system", {"command": "echo unsafe"})

    def test_hints_cannot_enable_process_execution(self):
        registry = TaskRegistry()
        registry.register("add", add, hints=TaskHints(kind="cpu", process_safe=True))
        with TaskService(registry) as service:
            submitted = service.submit("add", {"left": 2})
            self.settled(service)
            self.assertEqual(service.get(submitted["task_id"])["result"], 2)
            snapshot = service.snapshot()["executor"]
            self.assertEqual(snapshot["counts"]["process_submitted"], 0)
            self.assertEqual(snapshot["counts"]["thread_submitted"], 1)


if __name__ == "__main__":
    unittest.main()
